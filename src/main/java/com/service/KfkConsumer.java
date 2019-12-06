package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants.ESConfig;
import com.common.constants.BusinessConstants.KfkConfig;
import com.common.constants.BusinessConstants.LandIdConfig;
import com.common.utils.GuidService;
import com.common.utils.RedisCache;
import com.common.utils.RedisUtil;
import com.common.utils.RestHttpClient;
import com.exception.ConsumerException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.StreamSupport;

@Slf4j
@Service
public class KfkConsumer extends AbsService {

    @Autowired
    private ESService esService;

    private static String INDEX;
    private static String APPID;
    private static String REMOTE_LANID_URL;
    private KafkaConsumer<String, String> consumer;
    private ScheduledExecutorService scheduledExecutorService;
    private AtomicLong atomicLong = new AtomicLong(0);
    private ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {

        APPID = LocalConfig.get(KfkConfig.INPUT_APPID_KEY, String.class, "");
        String indexPattern = LocalConfig.get(ESConfig.ES_INDEX_KEY, String.class, ESConfig.DEFAULT_ES_INDEX);
        INDEX = String.format(indexPattern, APPID);

        REMOTE_LANID_URL = LocalConfig.get(LandIdConfig.REMOTE_URL_KEY, String.class, "");

        consumer = new KafkaConsumer<>(getProps());
        List<String> topicList = Collections.singletonList(LocalConfig.get(KfkConfig.INPUT_TOPIC_KEY, String.class, ""));
        consumer.subscribe(topicList);

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("Original-news-consumer-%d")
                .setDaemon(false)
                .build();

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduledExecutorService.scheduleWithFixedDelay(this::loop, 1000, 5, TimeUnit.MILLISECONDS);

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

        ConsumerRecords<String, String> records = null;
        try {
            records = consumer.poll(Duration.ofSeconds(20));
        } catch (Exception e) {
            log.error("Original-news-consumer poll fail", e);
        }

        if (null == records || records.isEmpty()) {
            return;
        }

        try {
            StreamSupport.stream(records.spliterator(), true)
                    .filter(x -> StringUtils.isNotBlank(x.value()))
                    .map(ConsumerRecord::value)
                    .map(JSON::parseObject)
                    .filter(v -> StringUtils.isNotBlank(v.getString("content")))
                    .map(sourceMapper())
                    .filter(v -> !CollectionUtils.isEmpty(v))
                    .peek(x -> {

                        String bundleKey = "";
                        Map<String, String> tmp = new HashMap<>();
                        for (Object obj : x.keySet()) {
                            String k = String.valueOf(obj);
                            String v = String.valueOf(x.getOrDefault(k, ""));
                            if ("content".equalsIgnoreCase(k)) {
                                continue;
                            }

                            if ("bundleKey".equalsIgnoreCase(k)) {
                                bundleKey = v;
                            }

                            tmp.put(k, v);
                        }

                        RedisUtil.hmset(0, bundleKey, tmp);
                    })
                    .forEach(sinker());

            consumer.commitSync();
        } catch (Exception e) {
            log.error("Original-news-consumer error", e);
            consumer.commitSync();
        }
    }

    private java.util.function.Consumer<Map> sinker() {
        return value -> {
            atomicLong.incrementAndGet();
            esService.bulkInsert(INDEX, "bundleKey", value);
            if (0 == atomicLong.longValue() % 1000) {
                log.info("Handled {} info", atomicLong.longValue() * 1000);
            }
        };
    }

    private Function<JSONObject, Map> sourceMapper() {
        return value -> {
            Map<String, Object> result = new HashMap<>();
            String content;

            try {
                Map<String, Object> tmp = new HashMap<>();

                String url = value.getString("content");
                tmp.put("ossUrl", url);

                content = RestHttpClient.doGet(url);
                if (StringUtils.isBlank(content)) {
                    log.error("No content for:[{}]", value);
                    return result;
                }

                content = new String(content.getBytes(StandardCharsets.UTF_8));

                content = content
                        .replaceAll("<[.[^>]]*>", "")
                        .replaceAll("[\\s\\p{Zs}]+", "")
                        .replaceAll("\\s*|\t|\r|\n", "")
                        .replaceAll("\\n", "")
                        .replaceAll("&nbsp", "")
                        .trim();

//                Map<String, Object> langParam = new HashMap<>();
//                langParam.put("text", content);
//                String langStr = RestHttpClient.doPost(REMOTE_LANID_URL, langParam);
//                JSONObject langResult = JSON.parseObject(langStr);
//                String langCode = langResult.getString("langCode");
//
//                if ("zh".equalsIgnoreCase(langCode)) {
                    tmp.put("content", content);
//                } else {
//                    log.error("Not Chinese content for:[{}]", value);
//                    return result;
//                }

                tmp.put("sourceName", value.get("source"));
                tmp.put("title", value.get("title"));

                String sourceUrl = value.getString("url");
                tmp.put("sourceUrl", sourceUrl);

                String bundleKey = (String) value.getOrDefault("bundle_key", GuidService.getMd5(sourceUrl).toLowerCase());
                tmp.put("bundleKey", bundleKey);

                tmp.put("publishDate", value.get("pub_date"));

                tmp.put("separateDate", value.getOrDefault("pub_date", System.currentTimeMillis()));

                result = new HashMap<>(tmp);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened on handling:[{}], {}", value, e);
            }
            return result;
        };
    }

    private Properties getProps() {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LocalConfig.get(KfkConfig.HOSTS_KEY, String.class, ""));
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, APPID + GuidService.getGuid("SC"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, APPID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return properties;
    }

    private Runnable statementRunnable() {
        return () -> {

            if (null == consumer) {
                return;
            }

            ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();
            statement.put("handledData", atomicLong.longValue());

            this.statement = new ConcurrentHashMap<>(statement);
        };
    }
}
