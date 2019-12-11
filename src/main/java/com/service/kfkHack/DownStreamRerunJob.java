package com.service.kfkHack;

import com.alibaba.fastjson.JSON;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants.DataConfig;
import com.common.constants.BusinessConstants.KfkConfig;
import com.common.constants.KfkProperties;
import com.common.utils.RedisUtil;
import com.service.ESService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@Service
public class DownStreamRerunJob {

    @Autowired
    private ESService sourceClient;

//    @PostConstruct
    private void rerun() throws IOException {
        log.info("Do rerun job");
        LocalDate startDate = LocalDate.of(2019, 7, 1);
        LocalDate endDate = LocalDate.of(1900, 1, 1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMM");

        String indexPatterm = "newton_original_doc_monthly_";

        String appid = LocalConfig.get(KfkConfig.INPUT_APPID_KEY, String.class, "");
        String topic = LocalConfig.get(KfkConfig.OUTPUT_TOPIC_KEY, String.class, "");

        KafkaProducer<String, String> producer = new KafkaProducer<>(KfkProperties.getProps(appid));

        while (startDate.isAfter(endDate)) {
            String trgtIndex = indexPatterm + startDate.format(formatter);

            Map queryParam = new HashMap();
            queryParam.put("index", trgtIndex);
            queryParam.put("size", 10000);
            queryParam.put("timeValue", "1h");
            Map result = sourceClient.scroll(queryParam);
            List<Map> dataList = (List) result.getOrDefault("data", new ArrayList<>());
            String scrollId = (String) result.getOrDefault("scrollId", "");
            queryParam.put("scrollId", scrollId);

            int count = 0;
            while (!CollectionUtils.isEmpty(dataList)) {
                count += dataList.size();

                dataList.parallelStream()
                        .filter(Objects::nonNull)
                        .peek(x -> x.put(DataConfig.ENTRYTIME_KEY, x.get("dataCreateTimestamp")))
                        .peek(x -> x.remove(DataConfig.CONTENT_KEY))
                        .peek(x -> x.put("delFlg", "0"))
                        .peek(x -> x.forEach((k, v) -> x.put(k, Optional.ofNullable(v).orElse(""))))
                        .peek(x -> RedisUtil.hmset(0, (String) x.get(DataConfig.BUNDLE_KEY), (Map<String, String>) x))
                        .forEach(x -> producer.send(new ProducerRecord(topic, x.get(DataConfig.BUNDLE_KEY), JSON.toJSONString(x))));

                log.info("Dumped {} data", count);
                result = sourceClient.scroll(queryParam);
                dataList = (List) result.getOrDefault("data", new ArrayList<>());
            }

            if (0 < count) {
                log.info("Found {} data for {}", count, startDate.format(formatter));
            }

            startDate = startDate.plusMonths(-1L);
        }
    }
}
