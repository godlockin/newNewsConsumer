package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants;
import com.common.constants.BusinessConstants.ESConfig;
import com.common.constants.BusinessConstants.KfkConfig;
import com.common.utils.GuidService;
import com.common.utils.RestHttpClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Service
public class Consumer extends AbsService{

    @Autowired
    private ESService esService;

    private static String INDEX;
    private static String APPID;
    private static String REMOTE_LANID_URL;
    private KafkaStreams kafkaStreams;
    private ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();
    private AtomicLong atomicLong = new AtomicLong(0);

    @PostConstruct
    void doHandle() {
        APPID = LocalConfig.get(KfkConfig.INPUT_APPID_KEY, String.class, "");
        String indexPattern = LocalConfig.get(ESConfig.ES_INDEX_KEY, String.class, ESConfig.DEFAULT_ES_INDEX);
        INDEX = String.format(indexPattern, APPID);

        REMOTE_LANID_URL = LocalConfig.get(BusinessConstants.LandIdConfig.REMOTE_URL_KEY, String.class, "");

        kafkaStreams = initKafkaStreams();
        kafkaStreams.start();

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(statementRunnable(), 0, 5, TimeUnit.SECONDS);
    }

    public boolean stop() {
        return kafkaStreams.close(Duration.ofSeconds(60));
    }

    public void start() {
        kafkaStreams.start();
    }

    public Map statement() {

        return new HashMap(statement);
    }

    private KafkaStreams initKafkaStreams() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream((String) LocalConfig.get(KfkConfig.INPUT_TOPIC_KEY, String.class, ""));

        final KStream<String, Map<String, Object>> mapKStream = source
                .filter((k, v) -> StringUtils.isNotBlank(v))
                .mapValues((ValueMapper<String, JSONObject>) JSON::parseObject)
                .filter((k, v) -> StringUtils.isNotBlank(v.getString("oss_url")))
                .map(sourceMapper())
                .filter((k, v) -> !CollectionUtils.isEmpty(v));

        mapKStream.peek(this::sinker);

        return new KafkaStreams(builder.build(), getProps());
    }

    private Properties getProps() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, LocalConfig.get(KfkConfig.HOSTS_KEY, String.class, ""));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        return props;
    }

    private void sinker(String key, Map<String, Object> value) {
        atomicLong.incrementAndGet();
        esService.bulkInsert(INDEX, "id", value);
        if (0 == atomicLong.longValue() % 1000) {
            log.info("Handled 1000 info");
        }
    }

    private KeyValueMapper<String, JSONObject, KeyValue<String, Map<String, Object>>> sourceMapper() {
        return (String key, JSONObject value) -> {

            Map<String, Object> result = new HashMap<>();
            String content;

            try {
                Map<String, Object> tmp = new HashMap<>();

                tmp.put("id", value.get("id"));

                String url = value.getString("oss_url");
                tmp.put("ossUrl", url);

                content = RestHttpClient.doGet(url);
                if (StringUtils.isBlank(content)) {
                    log.error("No content for:[{}]", value);
                    return KeyValue.pair(key, result);
                }

                content = new String(content.getBytes(StandardCharsets.UTF_8));

                Map<String, Object> langParam = new HashMap<>();
                langParam.put("text", content);
                String langStr = RestHttpClient.doPost(REMOTE_LANID_URL, langParam);
                JSONObject langResult = JSON.parseObject(langStr);
                String langCode = langResult.getString("langCode");

                if ("zh".equalsIgnoreCase(langCode)) {
                    tmp.put("content", content);
                }

                tmp.put("sourceName", value.get("source"));
                tmp.put("title", value.get("title"));

                String sourceUrl = value.getString("url");
                tmp.put("sourceUrl", sourceUrl);
                tmp.put("bundleKey", GuidService.getMd5(sourceUrl).toLowerCase());

                tmp.put("publishDate", value.get("pub_date"));

                tmp.put("separateDate", value.getOrDefault("pub_date", System.currentTimeMillis()));

                tmp.put("delFlg", 0);

                result = new HashMap<>(tmp);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened on handling:[{}], {}", value, e);
            }

            return KeyValue.pair(key, result);
        };
    }

    public Runnable statementRunnable() {
        return () -> {

            if (null == kafkaStreams) {
                return;
            }

            ConcurrentHashMap<String, Object> statement = new ConcurrentHashMap<>();
            statement.put("handledData", atomicLong.longValue());

            if (kafkaStreams.state().isRunning()) {
                kafkaStreams.localThreadsMetadata().forEach(x -> {
                    String threadName = x.threadName();
                    Map<String, Object> threadStatement = new HashMap<>();
                    threadStatement.put("threadState", x.threadState());
                    threadStatement.put("stactiveTasksate", x.activeTasks().stream().map(TaskMetadata::toString).collect(Collectors.toList()));
                    threadStatement.put("adminClientId", x.adminClientId());
                    threadStatement.put("consumerClientId", x.consumerClientId());
                    statement.put(threadName, threadStatement);
                });
            }

            this.statement = new ConcurrentHashMap<>(statement);
        };
    }
}
