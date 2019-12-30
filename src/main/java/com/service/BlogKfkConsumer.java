package com.service;

import com.common.constants.BusinessConstants.DataConfig;
import com.common.constants.BusinessConstants.TasksConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Service
@DependsOn("redisUtil")
public class BlogKfkConsumer extends KfkConsumer {


    @PostConstruct
    protected void init() {

        super.init();
    }

    @Override
    protected String getTrgtConsumeFlg() { return TasksConfig.BLOG_JOB_KEY; }

    @Override
    protected String getConsumerName() { return this.getClass().toString(); }

    @Override
    protected Consumer<Map> summaryGenerator() {
        return value -> {
            String excerpt = (String) value.get(DataConfig.EXCERPT_KEY);
            if (StringUtils.isNotBlank(excerpt)) {
                value.put(DataConfig.SUMMARY_KEY, excerpt);
            } else {
                super.summaryGenerator().accept(value);
            }
        };
    }
}
