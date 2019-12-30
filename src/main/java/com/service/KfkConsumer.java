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
import org.springframework.util.CollectionUtils;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class KfkConsumer {

    @Autowired
    protected ESService esService;
    protected static String APPID;
    protected static String INTERMEDIA_TOPIC;
    protected static String TRGT_INDEX;
    protected static String CONSUMER_TYPE_FLAG;
    protected static String REMOTE_ANALYSIS_URL;
    protected String REMOTE_SUMMARY_URL;
    protected KafkaConsumer<String, String> consumer;
    protected KafkaProducer<String, String> producer;

    protected ScheduledExecutorService scheduledExecutorService;
    protected AtomicLong consumedCount = new AtomicLong(0);
    protected AtomicLong convertCount = new AtomicLong(0);
    protected AtomicLong processedCount = new AtomicLong(0);
    protected AtomicLong redisCachedCount = new AtomicLong(0);
    protected AtomicLong functionalMappingCount = new AtomicLong(0);
    protected AtomicLong downstreamDataCleanedCount = new AtomicLong(0);
    protected AtomicLong downstreamDataFulfilledCount = new AtomicLong(0);
    protected AtomicLong esDataCleanedCount = new AtomicLong(0);
    protected AtomicLong summaryCount = new AtomicLong(0);
    protected AtomicLong normalDataCount = new AtomicLong(0);
    protected AtomicLong esSinkDataCount = new AtomicLong(0);
    protected AtomicLong producedCount = new AtomicLong(0);
    protected AtomicLong fulfillCount = new AtomicLong(0);
    protected AtomicLong errorCount = new AtomicLong(0);
    protected ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();
    protected Boolean isTest = false;

    protected void init() {
        if (!isTrgtConsume()) {
            return;
        }

        log.info("Init {}", getConsumerName());

        APPID = LocalConfig.get(KfkConfig.INPUT_APPID_KEY, String.class, "");

        isTest = LocalConfig.get(SysConfig.IS_TEST_FLG_KEY, Boolean.class, false);
        if (isTest) {
            APPID += "_" + new Random().nextInt(10);
        }

        producer = new KafkaProducer<>(KfkProperties.getProps(true, APPID));

        consumer = new KafkaConsumer<>(KfkProperties.getProps(false, APPID));
        List<String> topicList = Collections.singletonList(LocalConfig.get(KfkConfig.INPUT_TOPIC_KEY, String.class, ""));
        consumer.subscribe(topicList);

        TRGT_INDEX = LocalConfig.get(TasksConfig.TRGT_ES_INDEX_KEY, String.class, "");

        INTERMEDIA_TOPIC = LocalConfig.get(KfkConfig.OUTPUT_TOPIC_KEY, String.class, "");

        REMOTE_SUMMARY_URL = LocalConfig.get(SummaryConfig.REMOTE_URL_KEY, String.class, "");
        REMOTE_ANALYSIS_URL = LocalConfig.get(TasksConfig.REMOTE_ANALYSIS_URL_KEY, String.class, "");

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
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(statementRunnable(), 1000, 5000, TimeUnit.MILLISECONDS);
    }

    protected void groupingConsume() {

        try {

            List<JSONObject> data = StreamSupport.stream(consumer.poll(Duration.ofSeconds(20)).spliterator(), true)
                    .filter(x -> !(null == x || StringUtils.isBlank(x.value())))
                    .map(ConsumerRecord::value)
                    .filter(x -> x.contains("{") && x.contains("}"))
                    .map(x -> countAndLog(convertCount, "Converted {} data", dataConverter(), x))
                    .filter(v -> StringUtils.isNotBlank(v.getString(DataConfig.CONTENT_KEY)))
                    .collect(Collectors.toList());

            if (CollectionUtils.isEmpty(data)) {
                return;
            }

            if (isTest) {
                normalHandleData(data);
            } else {
                parallelHandleData(data);
            }

        } catch (Exception e) {
            e.printStackTrace();
            log.error(getConsumerName() + " error", e);
            errorCount.incrementAndGet();
        } finally {
            consumer.commitSync();
        }
    }

    protected Function<String, JSONObject> dataConverter() {
        return string -> {
            try {
                return JSON.parseObject(string);
            } catch (Exception e) {
                e.printStackTrace();
                return new JSONObject();
            }
        };
    }

    protected void normalHandleData(List<JSONObject> data) {

        data.stream()
                .peek(x -> countAndLog(processedCount, "Operated {} data", CommonDataPipeline.bundleKeyMapper(), x))
                .filter(x -> {
                    if (!StringUtils.isNotBlank((String) x.get(DataConfig.BUNDLE_KEY))) {
                        log.error("No bundleKey:[{}]", x.toJSONString());
                    }

                    return StringUtils.isNotBlank((String) x.get(DataConfig.BUNDLE_KEY));
                })
                .map(CommonDataPipeline.sourceMapper())
                .filter(x -> {
                    if (CollectionUtils.isEmpty(x)) {
                        log.error("Mapped empty");
                    }

                    return !CollectionUtils.isEmpty(x);
                })
                .peek(x -> countAndLog(functionalMappingCount, "Functional mapping {} data ", functionalMapper(), x))
                .peek(x -> countAndLog(summaryCount, "Generated {} summary", summaryGenerator(), x))
                .peek(x -> countAndLog(fulfillCount, "Fulfilled {} data", esDataFulFiller(), x))
                .peek(x -> countAndLog(esDataCleanedCount, "Cleaned {} es data ", esDataCleaner(), x))
                .peek(x -> countAndLog(esSinkDataCount, "Submitted ES {} data", esTestSinker(), x))
                .peek(x -> countAndLog(downstreamDataCleanedCount, "Cleaned {} downstream message ", downstreamDataCleaner(), x))
                .peek(x -> countAndLog(downstreamDataFulfilledCount, "Fulfilled {} downstream message ", downstreamDataFulfiller(), x))
                .peek(x -> countAndLog(redisCachedCount, "Cached {} data into redis", CommonDataPipeline.redisTestSinker(), x))
                .peek(x -> countAndLog(producedCount, "Published {} data", kfkTestOutputSinker(), x))
                .forEach(x -> {});
    }

    protected void parallelHandleData(List<JSONObject> data) {

        data.parallelStream()
                .peek(x -> countAndLog(processedCount, "Operated {} data", CommonDataPipeline.bundleKeyMapper(), x))
                .filter(x -> StringUtils.isNotBlank((String) x.get(DataConfig.BUNDLE_KEY)))
                .filter(x -> !RedisUtil.exists(0, (String) x.get(DataConfig.BUNDLE_KEY)))
                .map(CommonDataPipeline.sourceMapper())
                .filter(x -> !CollectionUtils.isEmpty(x))
                .peek(x -> countAndLog(functionalMappingCount, "Functional mapping {} data ", functionalMapper(), x))
                .peek(x -> countAndLog(summaryCount, "Generated {} summary", summaryGenerator(), x))
                .peek(x -> countAndLog(fulfillCount, "Fulfilled {} data", esDataFulFiller(), x))
                .peek(x -> countAndLog(esDataCleanedCount, "Cleaned {} es data ", esDataCleaner(), x))
                .peek(x -> countAndLog(esSinkDataCount, "Submitted ES {} data", esSinker(), x))
                .peek(x -> countAndLog(downstreamDataFulfilledCount, "Fulfilled {} downstream message ", downstreamDataFulfiller(), x))
                .peek(x -> countAndLog(downstreamDataCleanedCount, "Cleaned {} downstream message ", downstreamDataCleaner(), x))
                .peek(x -> countAndLog(redisCachedCount, "Cached {} data into redis", CommonDataPipeline.redisSinker(), x))
                .peek(x -> countAndLog(producedCount, "Published {} data", kfkOutputSinker(), x))
                .forEach(x -> {});
    }

    protected Consumer<Map> summaryGenerator() {
        return value -> {
            String content = (String) value.get(DataConfig.CONTENT_KEY);
            if (StringUtils.isBlank(content) || 200 >= content.length()) {
                value.put(DataConfig.SUMMARY_KEY, content);
            } else {
                content = content.length() > 10000 ? content.substring(0, 10000) : content;
                Map<String, String> param = new HashMap<>();
                param.put(DataConfig.CONTENT_KEY, content);
                param.put("size", "200");

                String summary = "";
                try {
                    String resultStr = RestHttpClient.doPost(REMOTE_SUMMARY_URL, param);
                    JSONObject resultJson = JSON.parseObject(resultStr);
                    if ("success".equalsIgnoreCase(resultJson.getString("message"))) {
                        summary = resultJson.getString("data");
                    }

                    summary = DataUtils.cleanHttpString(summary);
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("Error happened when we call for summary for id:[{}]", value.get(DataConfig.BUNDLE_KEY));
                }

                value.put(DataConfig.SUMMARY_KEY, StringUtils.isNotBlank(summary) ? summary : "this will be replaced later");
            }
        };
    }

    protected Consumer<Map> debugConsumer(String step) { return value -> log.info(step + ":" + JSON.toJSONString(value)); }

    protected Consumer<Map> functionalMapper() { return value -> {}; }

    protected Consumer<Map> downstreamDataFulfiller() { return value -> value.put(DataConfig.ENTRYTIME_KEY, DateUtils.getSHDate()); }

    protected Consumer<Map> downstreamDataCleaner() {
        return value -> {
            value.remove(DataConfig.SUMMARY_KEY);
            value.remove(DataConfig.SUMMARYSEG_KEY);
            value.remove(DataConfig.CONTENT_KEY);
        };
    }

    protected Consumer<Map> esDataCleaner() {
        return value -> {
            List<String> aliveKeys = getESAliveKeys();
            Iterator<Map.Entry> it = value.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry entry = it.next();
                if (!aliveKeys.contains(entry.getKey())) {
                    value.remove(entry.getKey());
                }
            }
        };
    }

    protected List<String> getESAliveKeys() {
        return DataConfig.ES_ALIVE_KEYS;
    }

    protected Consumer<Map> esDataFulFiller() {
        return value -> {
            if (StringUtils.isBlank(REMOTE_ANALYSIS_URL)) {
                return;
            }

            String title = (String) value.get(DataConfig.TITLE_KEY);
            List<String> titleItems = doAnalysisText(title);
            value.put(DataConfig.TITLESEG_KEY, String.join(" ", titleItems));

            String summary = (String) value.get(DataConfig.SUMMARY_KEY);
            List<String> summaryItems = doAnalysisText(summary);
            value.put(DataConfig.SUMMARYSEG_KEY, String.join(" ", summaryItems));
        };
    }

    private List<String> doAnalysisText(String text) {
        List<String> result = new ArrayList<>();
        try {
            String resultStr = RestHttpClient.doPost(REMOTE_ANALYSIS_URL, new HashMap<String, String>() {{put("text", text);}});
            List<String> resultList = JSON.parseObject(resultStr, List.class);
            result = resultList.stream()
                    .map(String::trim)
                    .filter(StringUtils::isNotBlank)
                    .map(x -> x.split(","))
                    .sorted(Comparator.comparing(a -> a[1]))
                    .map(x -> x[0])
                    .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened during we do analysis query:[{}], {}", text, e);
        }
        return result;
    }

    protected Consumer<Map> kfkOutputSinker() { return value -> producer.send(new ProducerRecord(INTERMEDIA_TOPIC, value.get(DataConfig.BUNDLE_KEY), JSON.toJSONString(value))); }
    protected Consumer<Map> kfkTestOutputSinker() { return value -> log.info("kfk output: [{}]-> [{}]:[{}]", INTERMEDIA_TOPIC, value.get(DataConfig.BUNDLE_KEY), JSON.toJSONString(value)); }

    protected Consumer<Map> esSinker() { return esSinker(TRGT_INDEX); }
    protected Consumer<Map> esTestSinker() { return value -> log.info("ES output: [{}] -> [{}]", TRGT_INDEX, JSON.toJSONString(value)); }

    protected Consumer<Map> esSinker(String index) { return value -> esService.bulkInsert(index, DataConfig.BUNDLE_KEY, value); }

    protected String getConsumerName() { return this.getClass().toString(); }

    protected String getTrgtConsumeFlg() { return ""; }

    protected Boolean isTrgtConsume() {
        CONSUMER_TYPE_FLAG = LocalConfig.get(SysConfig.ENV_FLG_KEY, String.class, "");

        return getTrgtConsumeFlg().equalsIgnoreCase(CONSUMER_TYPE_FLAG);
    }

    public Map statement() { return new HashMap(statement); }

    protected void countAndLog(AtomicLong count, String logPattern, Consumer<Map> consumer, Map data) {
        consumer.accept(data);
        if (0 == count.incrementAndGet() % consumeLogSed()) {
            log.info(logPattern, count.longValue());
        }
    }
    protected JSONObject countAndLog(AtomicLong count, String logPattern, Function<String, JSONObject> function, String data) {
        if (0 == count.incrementAndGet() % operateLogSed()) {
            log.info(logPattern, count.longValue());
        }
        return function.apply(data);
    }

    protected Integer consumeLogSed() { return 1000; }
    protected Integer operateLogSed() { return 1000; }

    protected Runnable statementRunnable() {
        return () -> {

            if (null == consumer) {
                return;
            }

            Map<String, Object> statement = new HashMap<>();
            buildBasicStatement(statement);
            buildExtraStatement(statement);
            this.statement = new ConcurrentHashMap<>(statement);
        };
    }

    protected void buildExtraStatement(Map<String, Object> statement) { }

    protected void buildBasicStatement(Map<String, Object> statement) {
        statement.put("consumedCount", consumedCount.longValue());
        statement.put("processedCount", processedCount.longValue());
        statement.put("normalDataCount", normalDataCount.longValue());
        statement.put("redisCachedCount", redisCachedCount.longValue());
        statement.put("producedCount", producedCount.longValue());
        statement.put("errorCount", errorCount.longValue());
    }

    @PreDestroy
    public void destroy() { if (null != scheduledExecutorService) scheduledExecutorService.shutdown(); }

}
