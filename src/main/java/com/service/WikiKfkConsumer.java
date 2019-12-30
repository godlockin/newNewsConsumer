package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.constants.BusinessConstants.DataConfig;
import com.common.constants.BusinessConstants.TasksConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@Service
@DependsOn("redisUtil")
public class WikiKfkConsumer extends KfkConsumer {

    private List<String> aliveKeys = new ArrayList<>();

    @PostConstruct
    protected void init() {

        super.init();
    }

    @Override
    protected String getTrgtConsumeFlg() { return TasksConfig.WIKI_JOB_KEY; }

    @Override
    protected String getConsumerName() { return this.getClass().toString(); }

    @Override
    protected Function<String, JSONObject> dataConverter() {
        return string -> {
            try {
                return JSON.parseObject((String) JSON.parse(string));
            } catch (Exception e) {
                e.printStackTrace();
                return new JSONObject();
            }
        };
    }

    @Override
    protected Consumer<Map> summaryGenerator() {
        return value -> {
            String content = (String) value.get(DataConfig.CONTENT_KEY);
            value.put(DataConfig.SUMMARY_KEY, content.length() > 200 ? content.substring(0, 200) : content);
        };
    }

    @Override
    protected Consumer<Map> esDataFulFiller() { return value -> {}; }

    @Override
    protected Consumer<Map> esDataCleaner() {
        return value -> {
            super.esDataCleaner().accept(value);

            String title = (String) value.get(DataConfig.TITLE_KEY);
            String source = (String) value.get(DataConfig.SOURCENAME_KEY);
            String tag = "_" + source;
            if (title.length() > tag.length() && title.contains(tag)) {
                value.put(DataConfig.TITLE_KEY, title.replace(tag, "").trim());
            }
        };
    }

    @Override
    protected List<String> getESAliveKeys() {
        if (CollectionUtils.isEmpty(aliveKeys)) {
            synchronized (QuestionAnswerKfkConsumer.class) {
                if (CollectionUtils.isEmpty(aliveKeys)) {
                    List<String> defaultKeys = DataConfig.ES_ALIVE_KEYS;
                    List<String> tmp = new ArrayList<>();
                    defaultKeys.stream()
                            .filter(x -> !(DataConfig.PUBLISHDATE_KEY.equalsIgnoreCase(x) ||
                                    DataConfig.SEPARATEDATE_KEY.equalsIgnoreCase(x)))
                            .forEach(tmp::add);
                    aliveKeys = tmp;
                }
            }
        }

        return aliveKeys;
    }
}
