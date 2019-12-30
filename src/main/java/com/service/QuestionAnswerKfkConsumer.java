package com.service;

import com.common.constants.BusinessConstants.DataConfig;
import com.common.constants.BusinessConstants.TasksConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
@Service
@DependsOn("redisUtil")
public class QuestionAnswerKfkConsumer extends KfkConsumer {

    private List<String> aliveKeys = new ArrayList<>();

    @PostConstruct
    protected void init() {

        super.init();

        List<String> defaultKeys = DataConfig.ES_ALIVE_KEYS;
        List<String> tmp = new ArrayList<>();
        defaultKeys.stream().filter(x -> !(DataConfig.SEPARATEDATE_KEY.equalsIgnoreCase(x))).forEach(tmp::add);
        tmp.add(DataConfig.LIKE_KEY);
        aliveKeys = tmp;
    }

    @Override
    protected String getTrgtConsumeFlg() { return TasksConfig.QUESTION_ANSWER_JOB_KEY; }

    @Override
    protected String getConsumerName() { return this.getClass().toString(); }

    @Override
    protected Consumer<Map> functionalMapper() {
        return value -> {
            int like = 0;
            String likeStr = Optional.ofNullable((String) value.getOrDefault("like_num", "0")).orElse("0");

            try {
                like = Integer.parseInt(likeStr);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Failure to handle like count [{}], {}", value, e);
            }

            value.remove("like_num");
            value.put(DataConfig.LIKE_KEY, like);
        };
    }

    @Override
    protected List<String> getESAliveKeys() {
        return aliveKeys;
    }
}
