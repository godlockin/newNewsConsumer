package com.common.constants;

import com.common.LocalConfig;
import com.common.utils.GuidService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.Random;

@Component
public class KfkProperties {

    public static Properties getProps(boolean isProducer, String appId) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LocalConfig.get(BusinessConstants.KfkConfig.HOSTS_KEY, String.class, ""));
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, appId + "_" + GuidService.getGuid(isProducer ? "producer" : "consumer" + new Random().nextInt(100)));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, appId);
        properties.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60 * 1000 + 10);
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60 * 1000 + 9);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60 * 1000 + 8);
//        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000 + 7);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60 * 1000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
