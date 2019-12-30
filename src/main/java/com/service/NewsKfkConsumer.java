package com.service;

import com.common.LocalConfig;
import com.common.constants.BusinessConstants.DataConfig;
import com.common.constants.BusinessConstants.TasksConfig;
import com.common.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
@Service
@DependsOn("redisUtil")
public class NewsKfkConsumer extends KfkConsumer {

    private static String NORMAL_INDEX;
    private static String EXTRA_INDEX;
    private static String ISSUE_INDEX;
    protected AtomicLong extraDataCount = new AtomicLong(0);
    protected AtomicLong issueDataCount = new AtomicLong(0);

    @PostConstruct
    protected void init() {
        super.init();

        NORMAL_INDEX = LocalConfig.get(TasksConfig.NEWS_NORMAL_ES_INDEX_KEY, String.class, TasksConfig.DEFAULT_ISSUE_INDEX);
        EXTRA_INDEX = LocalConfig.get(TasksConfig.NEWS_EXTRA_ES_INDEX_KEY, String.class, TasksConfig.DEFAULT_ISSUE_INDEX);
        ISSUE_INDEX = LocalConfig.get(TasksConfig.NEWS_ISSUE_ES_INDEX_KEY, String.class, TasksConfig.DEFAULT_ISSUE_INDEX);
    }

    @Override
    protected String getTrgtConsumeFlg() { return TasksConfig.DAILY_NEWS_JOB_KEY; }

    @Override
    protected String getConsumerName() { return this.getClass().toString(); }

    @Override
    protected Consumer<Map> esSinker() {
        Date nextDate = DateUtils.getDateDiff(new Date(), 1, DateUtils.DATE_TYPE.DAY);
        Date lastThreeMonthDate = DateUtils.getDateDiff(new Date(), -3, DateUtils.DATE_TYPE.MONTH);
        return x -> {
            try {
                String publishDate = (String) x.get(DataConfig.PUBLISHDATE_KEY);

                if (StringUtils.isBlank(publishDate)) {
                    x.remove(DataConfig.PUBLISHDATE_KEY);

                    countAndLog(issueDataCount, "Saved {} data into [" + ISSUE_INDEX + "]", esSinker(ISSUE_INDEX), x);
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
        };
    }

    protected void buildExtraStatement(Map<String, Object> statement) {
        statement.put("extraDataCount", extraDataCount.longValue());
        statement.put("issueDataCount", issueDataCount.longValue());
    }

}
