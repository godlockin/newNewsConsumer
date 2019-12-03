package com.service;

import com.common.LocalConfig;
import com.common.constants.BusinessConstants;
import com.common.constants.BusinessConstants.ESConfig;
import com.exception.ConsumerException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Slf4j
@Service
//@EnableScheduling
public class Schedule {

    @Autowired
    private ESService esService;
    private static DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static DateTimeFormatter MONTH_FORMAT = DateTimeFormatter.ofPattern("yyyyMM");

    private static Boolean ENABLE_DAILY_JOB;
    private static Boolean ENABLE_MONTHLY_JOB;

    @PostConstruct
    void init() {
        ENABLE_DAILY_JOB = LocalConfig.get(BusinessConstants.TasksConfig.ENABLE_DAILY_JOB_KEY, Boolean.class, false);
        ENABLE_MONTHLY_JOB = LocalConfig.get(BusinessConstants.TasksConfig.ENABLE_MONTHLY_JOB_KEY, Boolean.class, false);
    }

    @Scheduled(cron = "0 0 2 * * *")
    void dailyJob() {

        if (!ENABLE_DAILY_JOB) {
            log.debug("Disabled daily job");
            return;
        }

        LocalDate localDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
        LocalDate lastDayDate = localDate.plusDays(-1L);

        esService.reIndex(
                ESConfig.MONTHLY_INDEX_ALIASES
                , ESConfig.YEARLY_INDEX_ALIAS
                , lastDayDate.format(DATE_FORMAT)
                , localDate.format(DATE_FORMAT)
        );
    }

    @Scheduled(cron = "0 30 2 1 * ?")
    void monthlyJob() throws ConsumerException {

        if (!ENABLE_MONTHLY_JOB) {
            log.debug("Disabled monthly job");
            return;
        }

        LocalDate localDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
        if (1 == localDate.getMonthValue()) {
            LocalDate twoYearsAgo = localDate.plusYears(-2L);
            String yearlyIndex = String.format("new_news_yearly_%s", twoYearsAgo.getYear());
            doCheckAliases(yearlyIndex, ESConfig.YEARLY_INDEX_ALIAS);
        }

        LocalDate fourMonthsAgo = localDate.plusMonths(-4L);
        String monthlyIndex = String.format("new_news_monthly_%s", fourMonthsAgo.getYear());
        doCheckAliases(monthlyIndex, ESConfig.MONTHLY_INDEX_ALIASES);

        LocalDate twoMonthsAgo = localDate.plusMonths(-2L);
        esService.reIndex(String.format("new_news_monthly_%s", twoMonthsAgo.format(MONTH_FORMAT))
                , ESConfig.YEARLY_INDEX_ALIAS
                , twoMonthsAgo.format(DATE_FORMAT)
                , localDate.format(DATE_FORMAT)
        );
    }

    private void doCheckAliases(String indexName, String indexAliases) throws ConsumerException {

        boolean isRemoved = esService.doCheckAliases(indexName, indexAliases);
        if (isRemoved) {
            log.info("Successes to remove alias [{}] from [{}]", indexAliases, indexName);
        } else {
            log.info("Index [{}] or alias [{}] doesn't exists, nothing happened", indexName, indexAliases);
        }
    }
}
