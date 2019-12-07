package com.service.kfkHack;

import com.common.utils.RedisUtil;
import com.service.ESService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class RedisCleanJob {

    @Autowired
    private ESService sourceClient;

//    @PostConstruct
    private void clean() throws IOException {
        LocalDate startDate = LocalDate.now().plusYears(-5L);
        LocalDate endDate = LocalDate.of(1878, 7, 1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMM");

        String indexPatterm = "newton_original_doc_monthly_";

        while (startDate.isAfter(endDate)) {
            String trgtIndex = indexPatterm + startDate.format(formatter);

            Map queryParam = new HashMap();
            queryParam.put("index", trgtIndex);
            queryParam.put("size", 1000);
            queryParam.put("timeValue", "1h");
            Map result = sourceClient.scroll(queryParam);
            List dataList = (List) result.getOrDefault("data", new ArrayList<>());
            String scrollId = (String) result.getOrDefault("scrollId", "");
            queryParam.put("scrollId", scrollId);

            int count = 0;
            while (!CollectionUtils.isEmpty(dataList)) {
                count += dataList.size();
                dataList.parallelStream().forEach(x -> RedisUtil.del(0, (String) ((Map) x).get("bundleKey")));

                result = sourceClient.scroll(queryParam);
                dataList = (List) result.getOrDefault("data", new ArrayList<>());
            }

            if (0 < count) {
                log.info("Removed {} data for {}", count, startDate.format(formatter));
            }

            startDate = startDate.plusMonths(-1L);
        }
    }
}
