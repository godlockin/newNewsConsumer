package com.service.kfkHack;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

@Slf4j
public class MyEventTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        final long timestamp = record.timestamp();

        return (0 > timestamp) ? System.currentTimeMillis() : timestamp;
    }
}
