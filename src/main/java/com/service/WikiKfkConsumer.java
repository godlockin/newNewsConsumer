package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.constants.BusinessConstants.DataConfig;
import com.common.constants.BusinessConstants.TasksConfig;
import com.common.utils.CommonDataPipeline;
import com.common.utils.DateUtils;
import com.common.utils.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
@Service
@DependsOn("redisUtil")
public class WikiKfkConsumer extends KfkConsumer {

    @PostConstruct
    protected void init() {

        super.init();
    }

    @Override
    protected String getTrgtConsumeFlg() { return TasksConfig.WIKI_JOB_KEY; }

    @Override
    protected String getConsumerName() { return this.getClass().toString(); }

    @Override
    protected void groupingConsume() {

        try {

            List<JSONObject> data = StreamSupport.stream(consumer.poll(Duration.ofSeconds(20)).spliterator(), true)
                    .filter(x -> !(null == x || StringUtils.isBlank(x.value())))
//                    .peek(x -> log.info("Key:{} value:{}", x.key(), x.value()))
                    .map(ConsumerRecord::value)
                    .filter(x -> x.contains("{") && x.contains("}"))
                    .map(x -> {
                        try {
                            return JSON.parseObject((String) JSON.parse(x));
                        } catch (Exception e) {
                            e.printStackTrace();
                            return new JSONObject();
                        }
                    })
                    .filter(v -> StringUtils.isNotBlank(v.getString(DataConfig.CONTENT_KEY)))
                    .collect(Collectors.toList());

            if (CollectionUtils.isEmpty(data)) {
                return;
            }

            parallelHandleData(data);

        } catch (Exception e) {
            e.printStackTrace();
            log.error(getConsumerName() + " error", e);
            errorCount.incrementAndGet();
        } finally {
            consumer.commitSync();
        }
    }

    @Override
    protected void parallelHandleData(List<JSONObject> data) {

        data.parallelStream()
                .peek(x -> countAndLog(processedCount, "Operated {} data", CommonDataPipeline.bundleKeyMapper(), x))
                .filter(x -> StringUtils.isNotBlank((String) x.get(DataConfig.BUNDLE_KEY)))
                .filter(x -> !RedisUtil.exists(0, (String) x.get(DataConfig.BUNDLE_KEY)))
                .map(CommonDataPipeline.sourceMapper())
                .filter(x -> !CollectionUtils.isEmpty(x))
                .peek(this::dataCleaner)
                .peek(x -> x.put(DataConfig.ENTRYTIME_KEY, DateUtils.getSHDate()))
                .peek(x -> countAndLog(redisCachedCount, "Cached {} data into redis", CommonDataPipeline.redisSinker(), x))
                .peek(x -> x.remove(DataConfig.ENTRYTIME_KEY))
                .peek(x -> countAndLog(esSinkDataCount, "Submitted ES {} data", this.esSinker(TRGT_INDEX), x))
//                .forEach(x -> log.debug(x.toString()));
                .forEach(x -> countAndLog(producedCount, "Published {} data", this.kfkOutputSinker(), x));
    }

    @Override
    protected void dataCleaner(Map value) {

        value.remove(DataConfig.PUBLISHDATE_KEY);
        value.remove(DataConfig.SEPARATEDATE_KEY);

        String content = (String) value.get(DataConfig.CONTENT_KEY);
        value.put(DataConfig.SUMMARY_KEY, content.length() > 200 ? content.substring(0, 200) : content);

        String title = (String) value.get(DataConfig.TITLE_KEY);
        String source = (String) value.get(DataConfig.SOURCENAME_KEY);
        String tag = "_" + source;
        if (title.length() > tag.length() && title.contains(tag)) {
            value.put(DataConfig.TITLE_KEY, title.replace(tag, "").trim());
        }
    }
}
