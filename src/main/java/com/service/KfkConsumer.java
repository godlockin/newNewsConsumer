package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.FailureQueue;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants.*;
import com.common.constants.KfkProperties;
import com.common.utils.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

@Slf4j
@Service
@DependsOn("redisUtil")
public class KfkConsumer extends AbsService {

    @Autowired
    private ESService esService;

    private static String INDEX;
    private static String APPID;
    private static String IMTERMEDIA_TOPIC;
    private static String REMOTE_LANID_URL;
    private static String REMOTE_SUMMARY_URL;
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;

    private ScheduledExecutorService scheduledExecutorService;
    private AtomicLong processedCount = new AtomicLong(0);
    private AtomicLong redisCachedCount = new AtomicLong(0);
    private AtomicLong summaryCount = new AtomicLong(0);
    private AtomicLong consumedCount = new AtomicLong(0);
    private AtomicLong producedCount = new AtomicLong(0);
    private AtomicLong errorCount = new AtomicLong(0);
    private ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {

        String activeFlg = LocalConfig.get("spring.profiles.active", String.class, "monthly");
        if ("job".equalsIgnoreCase(activeFlg)) {
            return;
        }

        log.info("Init {}", KfkConsumer.class.getName());

        APPID = LocalConfig.get(KfkConfig.INPUT_APPID_KEY, String.class, "");
        String indexPattern = LocalConfig.get(ESConfig.ES_INDEX_KEY, String.class, ESConfig.DEFAULT_ES_INDEX);
        INDEX = String.format(indexPattern, APPID);

        REMOTE_LANID_URL = LocalConfig.get(LandIdConfig.REMOTE_URL_KEY, String.class, "");
        REMOTE_SUMMARY_URL = LocalConfig.get(SummaryConfig.REMOTE_URL_KEY, String.class, "");

        producer = new KafkaProducer<>(KfkProperties.getProps(true, APPID));

        consumer = new KafkaConsumer<>(KfkProperties.getProps(false, APPID));
        List<String> topicList = Collections.singletonList(LocalConfig.get(KfkConfig.INPUT_TOPIC_KEY, String.class, ""));
        consumer.subscribe(topicList);

        IMTERMEDIA_TOPIC = LocalConfig.get(KfkConfig.OUTPUT_TOPIC_KEY, String.class, "");

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("Original-news-consumer-%d")
                .setDaemon(false)
                .build();

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduledExecutorService.scheduleWithFixedDelay(this::loop, 1000, 5, TimeUnit.MILLISECONDS);

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(statementRunnable(), 0, 5, TimeUnit.SECONDS);
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(failureItemsDumpRunnable(), 0, 1, TimeUnit.HOURS);
    }

    @Override
    public Map statement() {
        return new HashMap(statement);
    }

    @PreDestroy
    public void destroy() {
        scheduledExecutorService.shutdown();
    }

    private void loop() {

        try {

            StreamSupport.stream(consumer.poll(Duration.ofSeconds(20)).spliterator(), true)
                    .filter(x -> !(null == x || StringUtils.isBlank(x.value())))
                    .map(ConsumerRecord::value)
                    .map(JSON::parseObject)
                    .filter(v -> StringUtils.isNotBlank(v.getString(DataConfig.CONTENT_KEY)))
                    .map(NewsKfkHandleUtil.sourceMapper())
                    .peek(x -> countAndLog(processedCount, "Operated {} data", y -> {}, x))
                    .filter(x -> !CollectionUtils.isEmpty(x))
                    .filter(x -> !RedisUtil.exists(0, (String) x.get(DataConfig.BUNDLE_KEY)))
                    .peek(x -> x.put(DataConfig.ENTRYTIME_KEY, DateUtils.getSHDate()))
                    .peek(x -> countAndLog(redisCachedCount, "Cached {} data into redis", NewsKfkHandleUtil.redisSinker(), x))
                    .peek(x -> x.remove(DataConfig.ENTRYTIME_KEY))
                    .peek(x -> countAndLog(summaryCount, "Generated {} summary data", this.summaryGenerater(), x))
                    .peek(x -> countAndLog(consumedCount, "Saved {} data into ES", this.esSinker(), x))
                    .forEach(x -> countAndLog(producedCount, "Published {} data", this.kfkOutputSinker(), x));

        } catch (Exception e) {
            log.error("Original-news-consumer error", e);
            errorCount.incrementAndGet();
        } finally {
            consumer.commitSync();
        }
    }

    private void countAndLog(AtomicLong count, String logPattern, Consumer<Map> consumer, Map data) {
        consumer.accept(data);
        if (0 == count.incrementAndGet() % 1000) {
            log.info(logPattern, consumedCount.longValue());
        }
    }

    private Consumer<Map> summaryGenerater() {
        return value -> {
            String content = (String) value.get(DataConfig.CONTENT_KEY);
            content = content.length() > 10000 ? content.substring(0, 10000) : content;
            Map<String, String> param = new HashMap<>();
            param.put("content", content);
            param.put("size", "200");

            String summary = "";

            try {
                String resultStr = RestHttpClient.doPost(REMOTE_SUMMARY_URL, param);
                JSONObject resultJson = JSON.parseObject(resultStr);
                if ("success".equalsIgnoreCase(resultJson.getString("message"))) {
                    summary = resultJson.getString("data");
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened when we call for summary for id:[{}]", value.get(DataConfig.BUNDLE_KEY));
            }

            value.put(DataConfig.SUMMARY_KEY, summary);
        };
    }

    private Consumer<Map> kfkOutputSinker() { return value -> producer.send(new ProducerRecord(IMTERMEDIA_TOPIC, value.get(DataConfig.BUNDLE_KEY), JSON.toJSONString(value))); }

    private Consumer<Map> esSinker() { return value -> esService.bulkInsert(INDEX, DataConfig.BUNDLE_KEY, value); }

    private Runnable statementRunnable() {
        return () -> {

            if (null == consumer) {
                return;
            }

            ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();
            statement.put("consumedCount", consumedCount.longValue());
            statement.put("redisCachedCount", redisCachedCount.longValue());
            statement.put("producedCount", producedCount.longValue());
            statement.put("errorCount", errorCount.longValue());

            this.statement = new ConcurrentHashMap<>(statement);
        };
    }

    private Runnable failureItemsDumpRunnable() {
        return () -> {
            List<String> list = FailureQueue.getAndClean();
            if (CollectionUtils.isEmpty(list)) {
                return;
            }

            String filePath = "/usr/local/failureLog/" + new Date().getTime() + "_failRecords.log";
            try (BufferedWriter out = new BufferedWriter(new FileWriter(new File(filePath)))) {
                for (String line : list) {
                    out.write(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened during we record failure data into:[{}], {}", filePath, e);
            }
        };
    }
}
