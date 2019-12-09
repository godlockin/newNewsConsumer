package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants.ESConfig;
import com.common.constants.BusinessConstants.KfkConfig;
import com.common.constants.BusinessConstants.LandIdConfig;
import com.common.utils.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
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
@DependsOn("redisUtil")
public class KfkConsumer extends AbsService {

    @Autowired
    private ESService esService;

    private static String INDEX;
    private static String APPID;
    private static String IMTERMEDIA_TOPIC;
    private static String REMOTE_LANID_URL;
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;

    private ScheduledExecutorService scheduledExecutorService;
    private AtomicLong atomicLong = new AtomicLong(0);
    private ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        log.info("Init {}", KfkConsumer.class.getName());

        APPID = LocalConfig.get(KfkConfig.INPUT_APPID_KEY, String.class, "");
        String indexPattern = LocalConfig.get(ESConfig.ES_INDEX_KEY, String.class, ESConfig.DEFAULT_ES_INDEX);
        INDEX = String.format(indexPattern, APPID);

        REMOTE_LANID_URL = LocalConfig.get(LandIdConfig.REMOTE_URL_KEY, String.class, "");

        producer = new KafkaProducer<>(getProps());

        consumer = new KafkaConsumer<>(getProps());
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
                    .filter(v -> StringUtils.isNotBlank(v.getString("content")))
                    .map(NewsKfkHandleUtil.sourceMapper())
                    .filter(v -> !CollectionUtils.isEmpty(v))
                    .peek(NewsKfkHandleUtil.redisSinker())
                    .peek(esSinker())
                    .peek(x -> x.put("entryTime", DateUtils.getSHDate()))
                    .forEach(kfkOutputSinker());
        } catch (Exception e) {
            log.error("Original-news-consumer error", e);
        } finally {
            consumer.commitSync();
        }
    }

    private java.util.function.Consumer<Map> kfkOutputSinker() {

        return value -> producer.send(new ProducerRecord(IMTERMEDIA_TOPIC, value.get("bundleKey"), JSON.toJSONString(value)));
    }

    private java.util.function.Consumer<Map> esSinker() {
        return value -> {
            atomicLong.incrementAndGet();
            esService.bulkInsert(INDEX, "bundleKey", value);
            if (0 == atomicLong.longValue() % 1000) {
                log.info("Handled {} info", atomicLong.longValue());
            }
        };
    }

    private Properties getProps() {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LocalConfig.get(KfkConfig.HOSTS_KEY, String.class, ""));
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, APPID + GuidService.getGuid("SC"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, APPID);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

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
