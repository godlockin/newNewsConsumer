package com.service.jobs;

import com.alibaba.fastjson.JSON;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants.DataConfig;
import com.common.constants.BusinessConstants.KfkConfig;
import com.common.constants.KfkProperties;
import com.common.utils.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
@Service
public class DownStreamRerunJob extends ESRelatedJobs {

    private String topic;
    private KafkaProducer<String, String> producer;

    @Override
    protected void init() {
        super.init();

        String appid = LocalConfig.get(KfkConfig.INPUT_APPID_KEY, String.class, "");
        topic = LocalConfig.get(KfkConfig.OUTPUT_TOPIC_KEY, String.class, "");
        producer = new KafkaProducer<>(KfkProperties.getProps(appid));
    }

    @Override
    public String jobName() { return "_downStreamRerunJob"; }

    @Override
    protected Consumer<List<Map>> functionLogic() {
        return list -> list.parallelStream()
                .filter(Objects::nonNull)
                .peek(x -> x.put(DataConfig.ENTRYTIME_KEY, x.get("dataCreateTimestamp")))
                .peek(x -> x.remove(DataConfig.CONTENT_KEY))
                .peek(x -> x.put("delFlg", "0"))
                .peek(x -> x.forEach((k, v) -> x.put(k, Optional.ofNullable(v).orElse(""))))
                .peek(x -> RedisUtil.hmset(0, (String) x.get(DataConfig.BUNDLE_KEY), (Map<String, String>) x))
                .forEach(x -> producer.send(new ProducerRecord(topic, x.get(DataConfig.BUNDLE_KEY), JSON.toJSONString(x))));
    }
}
