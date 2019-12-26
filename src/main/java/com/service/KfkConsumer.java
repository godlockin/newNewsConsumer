package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants;
import com.common.constants.KfkProperties;
import com.common.utils.CommonDataPipeline;
import com.common.utils.DateUtils;
import com.common.utils.RedisUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class KfkConsumer {

    @Autowired
    protected ESService esService;
    protected static String APPID;
    protected static String IMTERMEDIA_TOPIC;
    protected static String TRGT_INDEX;
    protected static String CONSUMER_TYPE_FLAG;
    protected KafkaConsumer<String, String> consumer;
    protected KafkaProducer<String, String> producer;

    protected ScheduledExecutorService scheduledExecutorService;
    protected AtomicLong consumedCount = new AtomicLong(0);
    protected AtomicLong processedCount = new AtomicLong(0);
    protected AtomicLong redisCachedCount = new AtomicLong(0);
    protected AtomicLong summaryCount = new AtomicLong(0);
    protected AtomicLong normalDataCount = new AtomicLong(0);
    protected AtomicLong esSinkDataCount = new AtomicLong(0);
    protected AtomicLong extraDataCount = new AtomicLong(0);
    protected AtomicLong issueDataCount = new AtomicLong(0);
    protected AtomicLong producedCount = new AtomicLong(0);
    protected AtomicLong errorCount = new AtomicLong(0);
    protected ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();

    protected void init() {
        if (!isTrgtConsume()) {
            return;
        }

        log.info("Init {}", getConsumerName());

        APPID = LocalConfig.get(BusinessConstants.KfkConfig.INPUT_APPID_KEY, String.class, "");

        Boolean isTest = LocalConfig.get(BusinessConstants.SysConfig.IS_TEST_FLG_KEY, Boolean.class, false);
        if (isTest) {
            APPID += "_" + new Random().nextInt(10);
        }

        producer = new KafkaProducer<>(KfkProperties.getProps(true, APPID));

        consumer = new KafkaConsumer<>(KfkProperties.getProps(false, APPID));
        List<String> topicList = Collections.singletonList(LocalConfig.get(BusinessConstants.KfkConfig.INPUT_TOPIC_KEY, String.class, ""));
        consumer.subscribe(topicList);

        TRGT_INDEX = LocalConfig.get(BusinessConstants.TasksConfig.TRGT_ES_INDEX_KEY, String.class, "");

        IMTERMEDIA_TOPIC = LocalConfig.get(BusinessConstants.KfkConfig.OUTPUT_TOPIC_KEY, String.class, "");

        initLoopingConsume();

        initStatementMonitorJob();
    }

    protected void initLoopingConsume() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(getConsumerName() + "-%d")
                .setDaemon(false)
                .build();

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduledExecutorService.scheduleWithFixedDelay(this::groupingConsume, 1000, 1000, TimeUnit.MILLISECONDS);
//        groupingConsume();
    }

    protected void initStatementMonitorJob() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(statementRunnable(), 0, 5, TimeUnit.SECONDS);
    }

    protected void groupingConsume() {

        try {

            List<JSONObject> data = StreamSupport.stream(consumer.poll(Duration.ofSeconds(20)).spliterator(), true)
                    .filter(x -> !(null == x || StringUtils.isBlank(x.value())))
                    .peek(x -> log.info("Key:{} value:{}", x.key(), x.value()))
                    .map(ConsumerRecord::value)
                    .filter(x -> x.contains("{") && x.contains("}"))
                    .map(x -> {
                        try {
                            return JSON.parseObject(x);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return new JSONObject();
                        }
                    })
                    .filter(v -> StringUtils.isNotBlank(v.getString(BusinessConstants.DataConfig.CONTENT_KEY)))
                    .collect(Collectors.toList());

            if (CollectionUtils.isEmpty(data)) {
                return;
            }

            parallelHandleData(data);

        } catch (Exception e) {
            e.printStackTrace();
            log.error(getConsumerName() + " error", e);
            errorCount.incrementAndGet();
        } finally {
            consumer.commitSync();
        }
    }

    protected void parallelHandleData(List<JSONObject> data) {

        data.parallelStream()
                .peek(x -> countAndLog(processedCount, "Operated {} data", CommonDataPipeline.bundleKeyMapper(), x))
                .filter(x -> StringUtils.isNotBlank((String) x.get(BusinessConstants.DataConfig.BUNDLE_KEY)))
                .filter(x -> !RedisUtil.exists(0, (String) x.get(BusinessConstants.DataConfig.BUNDLE_KEY)))
                .map(CommonDataPipeline.sourceMapper())
                .filter(x -> !CollectionUtils.isEmpty(x))
                .peek(x -> x.put(BusinessConstants.DataConfig.ENTRYTIME_KEY, DateUtils.getSHDate()))
                .peek(x -> countAndLog(redisCachedCount, "Cached {} data into redis", CommonDataPipeline.redisSinker(), x))
                .peek(x -> x.remove(BusinessConstants.DataConfig.ENTRYTIME_KEY))
                .peek(x -> countAndLog(esSinkDataCount, "Submitted ES {} data", this.esSinker(TRGT_INDEX), x))
                .forEach(x -> countAndLog(producedCount, "Published {} data", this.kfkOutputSinker(), x));
    }

    protected Consumer<Map> kfkOutputSinker() { return value -> producer.send(new ProducerRecord(IMTERMEDIA_TOPIC, value.get(BusinessConstants.DataConfig.BUNDLE_KEY), JSON.toJSONString(value))); }

    protected Consumer<Map> esSinker(String index) { return value -> esService.bulkInsert(index, BusinessConstants.DataConfig.BUNDLE_KEY, value); }

    protected String getConsumerName() { return this.getClass().toString(); }

    protected String getTrgtConsumeFlg() { return ""; }

    protected Boolean isTrgtConsume() {
        CONSUMER_TYPE_FLAG = LocalConfig.get(BusinessConstants.SysConfig.ENV_FLG_KEY, String.class, "");

        return getTrgtConsumeFlg().equalsIgnoreCase(CONSUMER_TYPE_FLAG);
    }

    public Map statement() { return new HashMap(statement); }

    protected void countAndLog(AtomicLong count, String logPattern, Consumer<Map> consumer, Map data) {
        consumer.accept(data);
        if (0 == count.incrementAndGet() % 1000) {
            log.info(logPattern, count.longValue());
        }
    }

    protected Runnable statementRunnable() {
        return () -> {

            if (null == consumer) {
                return;
            }

            Map<String, Object> statement = new HashMap<>();
            statement.put("consumedCount", consumedCount.longValue());
            statement.put("processedCount", processedCount.longValue());
            statement.put("normalDataCount", normalDataCount.longValue());
            statement.put("extraDataCount", extraDataCount.longValue());
            statement.put("issueDataCount", issueDataCount.longValue());
            statement.put("redisCachedCount", redisCachedCount.longValue());
            statement.put("producedCount", producedCount.longValue());
            statement.put("errorCount", errorCount.longValue());

            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) scheduledExecutorService;
            statement.put("activeCount", threadPoolExecutor.getActiveCount());
            statement.put("completedTaskCount", threadPoolExecutor.getCompletedTaskCount());
            statement.put("maximumPoolSize", threadPoolExecutor.getMaximumPoolSize());
            statement.put("poolSize", threadPoolExecutor.getPoolSize());
            statement.put("taskCount", threadPoolExecutor.getTaskCount());
            statement.put("queueSize", threadPoolExecutor.getQueue().size());

            this.statement = new ConcurrentHashMap<>(statement);
        };
    }

    @PreDestroy
    public void destroy() { if (null != scheduledExecutorService) scheduledExecutorService.shutdown(); }

}
