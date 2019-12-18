package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants.*;
import com.common.constants.KfkProperties;
import com.common.utils.DateUtils;
import com.common.utils.NewsKfkHandleUtil;
import com.common.utils.RedisUtil;
import com.common.utils.RestHttpClient;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
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
import java.util.concurrent.atomic.AtomicInteger;
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
    private AtomicLong consumedCount = new AtomicLong(0);
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
    private BlockingQueue<JSONObject> metaDataQueue = new LinkedBlockingQueue<>();
    private LocalConsumerManager localConsumerManager;

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

        // init thread pool
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(1024);
        RejectedExecutionHandler policy = new ThreadPoolExecutor.DiscardPolicy();
        executorService = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, queue, policy);

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("Original-news-consumer-%d")
                .setDaemon(false)
                .build();

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduledExecutorService.scheduleWithFixedDelay(this::loop, 1000, 3, TimeUnit.MILLISECONDS);

        localConsumerManager = new LocalConsumerManager();
        executorService.submit(localConsumerManager);

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(statementRunnable(), 0, 5, TimeUnit.SECONDS);
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

            metaDataQueue.addAll(data);
            consumedCount.getAndAdd(data.size());

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Original-news-consumer error", e);
            errorCount.incrementAndGet();
        } finally {
            consumer.commitSync();
        }
    }

    @Data
    private class LocalConsumerManager extends Thread {

        private Integer dataCount;
        private AtomicInteger slaverNum;
        private AtomicInteger slaverCount;
        private ExecutorService executor;

        @Override
        public void run() {
            log.info("Start to manage consumers");
            init();

            while (true) {
                try {
                    Thread.sleep(1000);

                    if (metaDataQueue.isEmpty()) {
                        Thread.sleep(1000);
                    }

                    List<JSONObject> data = new ArrayList<>();
                    metaDataQueue.drainTo(data, 5000);
                    if (CollectionUtils.isEmpty(data)) {
                        continue;
                    }

                    while (1024 < slaverCount.intValue()) {
                        Thread.sleep(1000);
                    }

                    executor.submit(new LocalConsumer(slaverNum.incrementAndGet(), data));
                    slaverCount.incrementAndGet();
                    dataCount += data.size();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("Error happened during we manage local consumer, {}", e);
                }
            }
        }

        private void init() {
            dataCount = 0;
            slaverNum = new AtomicInteger(0);
            slaverCount = new AtomicInteger(0);
            executor = new ThreadPoolExecutor(1024, 1024, 10, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 2),
                    new ThreadPoolExecutor.AbortPolicy());
        }

        private class LocalConsumer extends Thread {
            private Integer slaverNum;
            private List<JSONObject> data;
            LocalConsumer(Integer slaverNum, List<JSONObject> data) {
                this.slaverNum = slaverNum;
                this.data = data;
            }

            @Override
            public void run() {
                try {
                    log.info("Slaver {} handle {} data", slaverNum, data.size());
                    parallelHandleData(data);
                    log.info("Slaver {} finished {} data", slaverNum, data.size());
                    slaverCount.decrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("Error happened during we local consume data, {}", e);
                }
            }
        }
    }

    private void parallelHandleData(List<JSONObject> data) {
        Date nextDate = DateUtils.getDateDiff(new Date(), 1, DateUtils.DATE_TYPE.DAY);
        Date lastThreeMonthDate = DateUtils.getDateDiff(new Date(), -3, DateUtils.DATE_TYPE.MONTH);

        data.parallelStream()
                .peek(x -> countAndLog(processedCount, "Operated {} data", y -> {}, x))
                .filter(x -> !RedisUtil.exists(0, (String) x.get(DataConfig.BUNDLE_KEY)))
                .peek(x -> x.put(DataConfig.ENTRYTIME_KEY, DateUtils.getSHDate()))
                .peek(x -> countAndLog(redisCachedCount, "Cached {} data into redis", NewsKfkHandleUtil.redisSinker(), x))
                .peek(x -> x.remove(DataConfig.ENTRYTIME_KEY))
                .map(NewsKfkHandleUtil.sourceMapper())
                .filter(x -> !CollectionUtils.isEmpty(x))
                .peek(x -> countAndLog(summaryCount, "Generated {} summary data", summaryGenerater(), x))
                .filter(x -> StringUtils.isNotBlank((String) x.getOrDefault(DataConfig.PUBLISHDATE_KEY, "")))
                .peek(x -> countAndLog(producedCount, "Published {} data", this.kfkOutputSinker(), x))
                .forEach(x -> {
                    try {
                        String publishDate = (String) x.get(DataConfig.PUBLISHDATE_KEY);
                        publishDate = publishDate.trim();
                        publishDate = 10 == publishDate.length() ? publishDate + " 00:00:00" : publishDate;

                        Date pubDate = DateUtils.parseDate(publishDate);

                        if (nextDate.after(pubDate)) {
                            countAndLog(normalDataCount, "Saved {} data into [" + NORMAL_INDEX + "]", esSinker(NORMAL_INDEX), x);

                            if (lastThreeMonthDate.before(pubDate)) {
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

    private void countAndLog(AtomicLong count, String logPattern, Consumer<Map> consumer, Map data) {
        consumer.accept(data);
        if (0 == count.incrementAndGet() % 1000) {
            log.info(logPattern, normalDataCount.longValue());
        }
    }

    private Runnable statementRunnable() {
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

            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
            statement.put("activeCount", threadPoolExecutor.getActiveCount());
            statement.put("completedTaskCount", threadPoolExecutor.getCompletedTaskCount());
            statement.put("maximumPoolSize", threadPoolExecutor.getMaximumPoolSize());
            statement.put("poolSize", threadPoolExecutor.getPoolSize());
            statement.put("taskCount", threadPoolExecutor.getTaskCount());
            statement.put("queueSize", threadPoolExecutor.getQueue().size());

            if (null == localConsumerManager) {
                localConsumerManager = new LocalConsumerManager();
                executorService.submit(localConsumerManager);
            }

            Map<String, Object> localConsumerStatement = new HashMap<>();
            localConsumerStatement.put("metaQueueSize", metaDataQueue.size());
            localConsumerStatement.put("slaverNum", localConsumerManager.getSlaverNum().intValue());
            localConsumerStatement.put("handlingDataSize", localConsumerManager.getDataCount());
            localConsumerStatement.put("workingSlaveNum", localConsumerManager.getSlaverCount().intValue());
            statement.put("localConsumer", localConsumerStatement);

            this.statement = new ConcurrentHashMap<>(statement);
        };
    }
}
