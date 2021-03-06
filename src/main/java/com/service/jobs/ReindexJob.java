package com.service.jobs;

import com.common.RetryQueue;
import com.common.utils.DataUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ReindexJob extends ESRelatedJobs {

    private String fromIndex;
    private String toIndex;

    @Override
    public String jobName() { return "_reindexJob"; }

    @Override
    protected Consumer<List<Map>> functionLogic() {
        return list -> list
                .parallelStream()
                .forEach(x -> esClient.bulkInsert(toIndex, "bundleKey", (Map<String, ?>) x));
    }

    @Override
    public void scrollJob(Map param) {
        log.info("Start to do [{}] job for param:[{}]", jobName(), param);

        fromIndex = DataUtils.getNotNullValue(param, "fromIndex", String.class, "");
        toIndex = DataUtils.getNotNullValue(param, "toIndex", String.class, "");

        Map queryParam = new HashMap();
        queryParam.put("index", fromIndex);
        queryParam.put("size", 10000);
        queryParam.put("timeValue", "1h");

        if (param.containsKey("ids")) {
            queryParam.put("ids", param.get("ids"));
        }

        int count = 0;

        try {
            Map result = esClient.complexSearch(queryParam);
            List dataList = (List) result.getOrDefault("data", new ArrayList<>());
            String scrollId = (String) result.getOrDefault("scrollId", "");
            queryParam.put("scrollId", scrollId);

            while (!CollectionUtils.isEmpty(dataList)) {
                count += dataList.size();

                functionLogic().accept(dataList);

                result = esClient.complexSearch(queryParam);
                dataList = (List) result.getOrDefault("data", new ArrayList<>());
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened during we do scroll for:[{}]", queryParam);
        }

        if (0 < count) {
            log.info("Reindex {} data from {} to {}", count, fromIndex, toIndex);
        }
    }
}
