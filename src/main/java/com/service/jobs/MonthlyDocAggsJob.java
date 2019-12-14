package com.service.jobs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Service
public class MonthlyDocAggsJob extends ESRelatedJobs {

    @Override
    public String jobName() { return "_monthlyDocAggsJob"; }

    @Override
    protected Consumer<List<Map>> functionLogic() {
        return list -> list.forEach(x -> esClient.bulkInsert("newton_original_doc_yearly", "bundleKey", (Map<String, ?>) x));
    }
}
