package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants.DataConfig;
import com.common.constants.BusinessConstants.LandIdConfig;
import com.common.constants.BusinessConstants.SummaryConfig;
import com.common.constants.BusinessConstants.TasksConfig;
import com.common.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Service
@DependsOn("redisUtil")
public class NewsKfkConsumer extends KfkConsumer {

    private static String NORMAL_INDEX;
    private static String EXTRA_INDEX;
    private static String ISSUE_INDEX;
    private static String REMOTE_LANID_URL;
    private static String REMOTE_SUMMARY_URL;

    @PostConstruct
    protected void init() {
        super.init();

        NORMAL_INDEX = LocalConfig.get(TasksConfig.NEWS_NORMAL_ES_INDEX_KEY, String.class, TasksConfig.DEFAULT_ISSUE_INDEX);
        EXTRA_INDEX = LocalConfig.get(TasksConfig.NEWS_EXTRA_ES_INDEX_KEY, String.class, TasksConfig.DEFAULT_ISSUE_INDEX);
        ISSUE_INDEX = LocalConfig.get(TasksConfig.NEWS_ISSUE_ES_INDEX_KEY, String.class, TasksConfig.DEFAULT_ISSUE_INDEX);

        REMOTE_LANID_URL = LocalConfig.get(LandIdConfig.REMOTE_URL_KEY, String.class, "");
        REMOTE_SUMMARY_URL = LocalConfig.get(SummaryConfig.REMOTE_URL_KEY, String.class, "");
    }

    @Override
    protected String getTrgtConsumeFlg() { return TasksConfig.DAILY_NEWS_JOB_KEY; }

    @Override
    protected String getConsumerName() { return this.getClass().toString(); }

    @Override
    protected void parallelHandleData(List<JSONObject> data) {
        Date nextDate = DateUtils.getDateDiff(new Date(), 1, DateUtils.DATE_TYPE.DAY);
        Date lastThreeMonthDate = DateUtils.getDateDiff(new Date(), -3, DateUtils.DATE_TYPE.MONTH);

        data.parallelStream()
                .peek(x -> countAndLog(processedCount, "Operated {} data", CommonDataPipeline.bundleKeyMapper(), x))
                .filter(x -> StringUtils.isNotBlank((String) x.get(DataConfig.BUNDLE_KEY)))
                .filter(x -> !RedisUtil.exists(0, (String) x.get(DataConfig.BUNDLE_KEY)))
                .map(CommonDataPipeline.sourceMapper())
                .filter(x -> !CollectionUtils.isEmpty(x))
                .peek(x -> x.put(DataConfig.ENTRYTIME_KEY, DateUtils.getSHDate()))
                .peek(x -> countAndLog(redisCachedCount, "Cached {} data into redis", CommonDataPipeline.redisSinker(), x))
                .peek(x -> x.remove(DataConfig.ENTRYTIME_KEY))
                .peek(x -> countAndLog(summaryCount, "Generated {} summary data", summaryGenerater(), x))
                .filter(x -> StringUtils.isNotBlank((String) x.getOrDefault(DataConfig.PUBLISHDATE_KEY, "")))
                .peek(x -> {
                    try {
                        String publishDate = (String) x.get(DataConfig.PUBLISHDATE_KEY);

                        if (StringUtils.isBlank(publishDate)) {
                            x.remove(DataConfig.PUBLISHDATE_KEY);

                            countAndLog(normalDataCount, "Saved {} data into [" + NORMAL_INDEX + "]", esSinker(NORMAL_INDEX), x);
                        } else {
                            publishDate = publishDate.trim();
                            publishDate = 10 == publishDate.length() ? publishDate + " 00:00:00" : publishDate;

                            Date pubDate = DateUtils.parseDate(publishDate);
                            if (nextDate.after(pubDate)) {
                                countAndLog(normalDataCount, "Saved {} data into [" + NORMAL_INDEX + "]", esSinker(NORMAL_INDEX), x);

                                if (lastThreeMonthDate.before(pubDate)) {
                                    countAndLog(extraDataCount, "Saved {} data into [" + EXTRA_INDEX + "]", esSinker(EXTRA_INDEX), x);
                                }
                            } else {
                                countAndLog(issueDataCount, "Saved {} data into [" + ISSUE_INDEX + "]", esSinker(ISSUE_INDEX), x);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Date parse failure:[{}], {}", x, e);
                    }
                })
                .forEach(x -> countAndLog(producedCount, "Published {} data", this.kfkOutputSinker(), x));
    }

    private Consumer<Map> summaryGenerater() {
        return value -> {
            String content = (String) value.get(DataConfig.CONTENT_KEY);
            content = content.length() > 10000 ? content.substring(0, 10000) : content;
            Map<String, String> param = new HashMap<>();
            param.put(DataConfig.CONTENT_KEY, content);
            param.put("size", "200");

            String summary = "";

            try {
                String resultStr = RestHttpClient.doPost(REMOTE_SUMMARY_URL, param);
                JSONObject resultJson = JSON.parseObject(resultStr);
                if ("success".equalsIgnoreCase(resultJson.getString("message"))) {
                    summary = resultJson.getString("data");
                }

                summary = DataUtils.cleanHttpString(summary);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened when we call for summary for id:[{}]", value.get(DataConfig.BUNDLE_KEY));
            }

            value.put(DataConfig.SUMMARY_KEY, StringUtils.isNotBlank(summary) ? summary : "this will be replaced later");
        };
    }
}
