package com.service.jobs;

import com.common.utils.DataUtils;
import com.service.ESService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Slf4j
@Component
public class ESRelatedJobs {

    protected ESService esClient;
    protected Map param;

    protected ConcurrentHashMap<String, Consumer<Map>> jobs = new ConcurrentHashMap<>();

    public ESRelatedJobs() {}

    @PostConstruct
    protected void init() {
        log.info("Init {} job", jobName());
    }

    public static class JobsBuilder {

        private String jobName;
        private Map param;
        private ESService esClient;
        
        public JobsBuilder() { }

        public JobsBuilder jobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        public JobsBuilder esClient(ESService esClient) {
            this.esClient = esClient;
            return this;
        }

        public JobsBuilder param(Map param) {
            this.param = param;
            return this;
        }
        public ESRelatedJobs build() {

            ESRelatedJobs job;
            switch (this.jobName) {
                case "_redisCleanJob":
                    job = new RedisCleanJob();
                    break;
                case "_monthlyDocAggsJob":
                    job = new MonthlyDocAggsJob();
                    break;
                case "_downStreamRerunJob":
                    job = new DownStreamRerunJob();
                    break;
                case "_reindexJob":
                    job = new ReindexJob();
                    break;
                default:
                    job = new ESRelatedJobs();
                    break;
            }

            job.esClient = esClient;
            job.param = param;

            return job;
        }
    }

    public String jobName() { return "_parentJob"; }

    protected Consumer<List<Map>> functionLogic() {
        return list -> log.info("Load {} data in this batch", list.size());
    }

    public void scrollJob(Map param) {
        log.info("Start to do [{}] job for param:[{}]", jobName(), param);

        String startTime = DataUtils.getNotNullValue(param, "startTime", String.class, "");
        String endTime = DataUtils.getNotNullValue(param, "endTime", String.class, "");

        LocalDate startDate = StringUtils.isBlank(startTime) ? LocalDate.now() : LocalDate.parse(startTime);
        LocalDate endDate = StringUtils.isBlank(endTime) ? LocalDate.of(1900, 1, 1) : LocalDate.parse(endTime);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMM");

        String indexPatterm = "newton_original_doc_monthly_";

        while (startDate.isAfter(endDate)) {
            String trgtIndex = indexPatterm + startDate.format(formatter);

            Map queryParam = new HashMap();
            queryParam.put("index", trgtIndex);
            queryParam.put("size", 1000);
            queryParam.put("timeValue", "1h");
            int count = 0;

            try {
                Map result = esClient.scroll(queryParam);
                List dataList = (List) result.getOrDefault("data", new ArrayList<>());
                String scrollId = (String) result.getOrDefault("scrollId", "");
                queryParam.put("scrollId", scrollId);

                while (!CollectionUtils.isEmpty(dataList)) {
                    count += dataList.size();

                    functionLogic().accept(dataList);

                    result = esClient.scroll(queryParam);
                    dataList = (List) result.getOrDefault("data", new ArrayList<>());
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened during we do scroll for:[{}]", queryParam);
            }

            if (0 < count) {
                log.info("Handled {} data for {}", count, startDate.format(formatter));
            }

            startDate = startDate.plusMonths(-1L);
        }
    }
}
