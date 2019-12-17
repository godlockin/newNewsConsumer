package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
@Service
@DependsOn("redisUtil")
public class KfkConsumer extends AbsService {

    @Autowired
    private ESService esService;

    private static String NORMAL_INDEX;
    private static String EXTRA_INDEX;
    private static String ISSUE_INDEX;
    private static String APPID;
    private static String IMTERMEDIA_TOPIC;
    private static String REMOTE_LANID_URL;
    private static String REMOTE_SUMMARY_URL;
    private static String CONSUMER_TYPE_FLAG;
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;

    private ScheduledExecutorService scheduledExecutorService;
    private AtomicLong processedCount = new AtomicLong(0);
    private AtomicLong redisCachedCount = new AtomicLong(0);
    private AtomicLong summaryCount = new AtomicLong(0);
    private AtomicLong normalDataCount = new AtomicLong(0);
    private AtomicLong extraDataCount = new AtomicLong(0);
    private AtomicLong issueDataCount = new AtomicLong(0);
    private AtomicLong producedCount = new AtomicLong(0);
    private AtomicLong errorCount = new AtomicLong(0);
    private ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();
    private ExecutorService executorService;

    @PostConstruct
    void init() {

        CONSUMER_TYPE_FLAG = LocalConfig.get(SysConfig.ENV_FLG_KEY, String.class, TasksConfig.NORMAL_JOB_KEY);
        if (TasksConfig.CORE_JOB_KEY.equalsIgnoreCase(CONSUMER_TYPE_FLAG)) {
            return;
        }

        log.info("Init {}", KfkConsumer.class.getName());

        APPID = LocalConfig.get(KfkConfig.INPUT_APPID_KEY, String.class, "");
        if (TasksConfig.BATCH_JOB_KEY.equalsIgnoreCase(CONSUMER_TYPE_FLAG)) {
            APPID += "_" + DateUtils.formatDate(new Date(), DateUtils.DATE_YYYYMMDD);
        }

        NORMAL_INDEX = LocalConfig.get(TasksConfig.NORMAL_ES_INDEX_KEY, String.class, TasksConfig.DEFAULT_ISSUE_INDEX);
        EXTRA_INDEX = LocalConfig.get(TasksConfig.EXTRA_ES_INDEX_KEY, String.class, TasksConfig.DEFAULT_ISSUE_INDEX);
        ISSUE_INDEX = LocalConfig.get(TasksConfig.ISSUE_ES_INDEX_KEY, String.class, TasksConfig.DEFAULT_ISSUE_INDEX);

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
        scheduledExecutorService.scheduleWithFixedDelay(this::loop, 1000, 3, TimeUnit.MILLISECONDS);

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(statementRunnable(), 0, 5, TimeUnit.SECONDS);

        // init thread pool
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(1024);
        RejectedExecutionHandler policy = new ThreadPoolExecutor.DiscardPolicy();
        executorService = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, queue, policy);
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

            List<JSONObject> data = StreamSupport.stream(consumer.poll(Duration.ofSeconds(20)).spliterator(), true)
                    .filter(x -> !(null == x || StringUtils.isBlank(x.value())))
                    .map(ConsumerRecord::value)
                    .map(JSON::parseObject)
                    .filter(v -> StringUtils.isNotBlank(v.getString(DataConfig.CONTENT_KEY)))
                    .collect(Collectors.toList());

            if (TasksConfig.BATCH_JOB_KEY.equalsIgnoreCase(CONSUMER_TYPE_FLAG)) {
                executorService.submit(() -> parallelHandleData(data));
            } else {
                parallelHandleData(data);
            }

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Original-news-consumer error", e);
            errorCount.incrementAndGet();
        } finally {
            consumer.commitSync();
        }
    }

    private void parallelHandleData(List<JSONObject> data) {
        Date nextDate = DateUtils.getDateDiff(new Date(), 1, DateUtils.DATE_TYPE.DAY);
        Date lastThreeMonthDate = DateUtils.getDateDiff(new Date(), -3, DateUtils.DATE_TYPE.MONTH);

        Integer nextDateInt = getDateInteger(nextDate, DateUtils.DATE_YYYYMMDD);
        Integer lastThreeMonthDateInt = getDateInteger(lastThreeMonthDate, DateUtils.DATE_YYYYMM) * 100 + 1;

        data.parallelStream()
                .map(NewsKfkHandleUtil.sourceMapper())
                .peek(x -> countAndLog(processedCount, "Operated {} data", y -> {}, x))
                .filter(x -> !CollectionUtils.isEmpty(x))
                .filter(x -> !RedisUtil.exists(0, (String) x.get(DataConfig.BUNDLE_KEY)))
                .peek(x -> x.put(DataConfig.ENTRYTIME_KEY, DateUtils.getSHDate()))
                .peek(x -> countAndLog(redisCachedCount, "Cached {} data into redis", NewsKfkHandleUtil.redisSinker(), x))
                .peek(x -> x.remove(DataConfig.ENTRYTIME_KEY))
                .peek(x -> countAndLog(summaryCount, "Generated {} summary data", summaryGenerater(), x))
                .filter(x -> StringUtils.isNotBlank((String) x.getOrDefault(DataConfig.PUBLISHDATE_KEY, "")))
                .peek(x -> countAndLog(producedCount, "Published {} data", this.kfkOutputSinker(), x))
                .forEach(x -> {
                    try {
                        String publishDate = (String) x.get(DataConfig.PUBLISHDATE_KEY);
                        publishDate = publishDate.trim().replaceAll("-", "");
                        publishDate = (8 < publishDate.length()) ? publishDate.substring(0, 8) : publishDate;
                        Integer publishDateInt = Integer.parseInt(publishDate);

                        if (publishDateInt <= nextDateInt) {
                            countAndLog(normalDataCount, "Saved {} data into [" + NORMAL_INDEX + "]", esSinker(NORMAL_INDEX), x);

                            if (publishDateInt >= lastThreeMonthDateInt) {
                                countAndLog(extraDataCount, "Saved {} data into [" + EXTRA_INDEX + "]", esSinker(EXTRA_INDEX), x);
                            }
                        } else {
                            countAndLog(issueDataCount, "Saved {} data into [" + ISSUE_INDEX + "]", esSinker(ISSUE_INDEX), x);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Date parse failure:[{}], {}", x, e);
                    }
                });
    }

    private Integer getDateInteger(Date date, String format) {
        String dateStr = DateUtils.formatDate(date, format);
        return Integer.parseInt(dateStr);
    }

    private void countAndLog(AtomicLong count, String logPattern, Consumer<Map> consumer, Map data) {
        consumer.accept(data);
        if (0 == count.incrementAndGet() % 1000) {
            log.info(logPattern, normalDataCount.longValue());
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

                summary = summary
                        .replaceAll("<[.[^>]]*>", "")
                        .replaceAll("[\\s\\p{Zs}]+", "")
                        .replaceAll("\\s*|\t|\r|\n", "")
                        .replaceAll("\n|\r\n|\\n|\\t", "")
                        .replaceAll("&nbsp", "")
                        .trim();

            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened when we call for summary for id:[{}]", value.get(DataConfig.BUNDLE_KEY));
            }

            value.put(DataConfig.SUMMARY_KEY, StringUtils.isNotBlank(summary) ? summary : "this will be replaced later");
        };
    }

    private Consumer<Map> kfkOutputSinker() { return value -> producer.send(new ProducerRecord(IMTERMEDIA_TOPIC, value.get(DataConfig.BUNDLE_KEY), JSON.toJSONString(value))); }

    private Consumer<Map> esSinker(String index) { return value -> esService.bulkInsert(index, DataConfig.BUNDLE_KEY, value); }

    private Runnable statementRunnable() {
        return () -> {

            if (null == consumer) {
                return;
            }

            ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();
            statement.put("normalDataCount", normalDataCount.longValue());
            statement.put("extraDataCount", extraDataCount.longValue());
            statement.put("issueDataCount", issueDataCount.longValue());
            statement.put("redisCachedCount", redisCachedCount.longValue());
            statement.put("producedCount", producedCount.longValue());
            statement.put("errorCount", errorCount.longValue());

            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
            statement.put("activeCount", threadPoolExecutor.getActiveCount());
            statement.put("completedTaskCount", threadPoolExecutor.getCompletedTaskCount());
            statement.put("maximumPoolSize", threadPoolExecutor.getMaximumPoolSize());
            statement.put("poolSize", threadPoolExecutor.getPoolSize());
            statement.put("taskCount", threadPoolExecutor.getTaskCount());
            statement.put("queueSize", threadPoolExecutor.getQueue().size());

            this.statement = new ConcurrentHashMap<>(statement);
        };
    }
}
