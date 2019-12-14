package com.common.constants;

import com.common.LocalConfig;
import com.common.utils.GuidService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KfkProperties {

    public static Properties getProps(boolean isProducer, String appId) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LocalConfig.get(BusinessConstants.KfkConfig.HOSTS_KEY, String.class, ""));
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, appId + "_" + GuidService.getGuid(isProducer ? "producer" : "consumer"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, appId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
