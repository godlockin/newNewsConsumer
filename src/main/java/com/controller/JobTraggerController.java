package com.controller;

import com.common.utils.DataUtils;
import com.service.ESService;
import com.service.jobs.ESRelatedJobs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class JobTraggerController {

    @Autowired
    protected ESService esClient;

    @RequestMapping(value = "/_job", method = RequestMethod.POST)
    public Map jobTragger(@RequestBody Map param) {

        String jobName = DataUtils.getNotNullValue(param, "job", String.class, "_parentJob");

        ESRelatedJobs jobs = new ESRelatedJobs.JobsBuilder()
                .jobName(jobName)
                .esClient(esClient)
                .build();
        jobs.scrollJob(param);

        return param;
    }
}
