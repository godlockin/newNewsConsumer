package com.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.constants.BusinessConstants.DataConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@Component
public class CommonDataPipeline {

    public static Consumer<Map> bundleKeyMapper() {
        return value -> {
            String sourceUrl = (String) value.getOrDefault("url", "");
            String bundleKey = (String) value.getOrDefault("bundle_key", GuidService.getMd5(sourceUrl).toLowerCase());
            value.put(DataConfig.BUNDLE_KEY, bundleKey);
        };
    }

    public static Function<JSONObject, Map> sourceMapper() {
        return value -> {

            Map<String, Object> result = new HashMap<>();
            try {
                Map<String, Object> tmp = new HashMap<>();

                String url = value.getString(DataConfig.CONTENT_KEY);
                tmp.put(DataConfig.OSSURL_KEY, url);

                String content = RestHttpClient.doGet(url);
                if (StringUtils.isBlank(content)) {
                    log.error("No content for:[{}]", value);
                    return result;
                }

                content = new String(content.getBytes(StandardCharsets.UTF_8));
                content = DataUtils.cleanHttpString(content);

                if (StringUtils.isBlank(content)) {
                    log.error("No content for:[{}]", value);
                    return result;
                }

                tmp.put(DataConfig.CONTENT_KEY, content);

                String domain = (String) value.getOrDefault(DataConfig.DOMAIN_KEY, "");
                String sourceUrl = value.getString("url");
                domain = Optional.ofNullable(domain).orElse(URLUtil.getDomainName(sourceUrl));

                tmp.put(DataConfig.DOMAIN_KEY, domain);
                tmp.put(DataConfig.SOURCENAME_KEY, value.getOrDefault("source", ""));
                tmp.put(DataConfig.TITLE_KEY, value.get(DataConfig.TITLE_KEY));

                tmp.put(DataConfig.SOURCEURL_KEY, sourceUrl);

                String bundleKey = (String) value.getOrDefault("bundle_key", GuidService.getMd5(sourceUrl).toLowerCase());
                tmp.put(DataConfig.BUNDLE_KEY, bundleKey);

                Object publishDate = value.get("pub_date");
                Long timestamp = System.currentTimeMillis();

                tmp.put(DataConfig.PUBLISHDATE_KEY, publishDate);

                tmp.put(DataConfig.SEPARATEDATE_KEY, Optional.ofNullable(publishDate).orElse(timestamp));

                value.entrySet().stream().filter(e -> !DataConfig.MAPPINGFIELDS.contains(e.getKey())).forEach(e -> tmp.put(e.getKey(), e.getValue()));
                result = new HashMap<>(tmp);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened on handling:[{}], {}", value, e);
            }

            return result;
        };
    }

    public static Consumer<Map> redisSinker() {
        return (Map value) -> {
            String bundleKey = (String) value.get(DataConfig.BUNDLE_KEY);
            Map<String, String> tmp = new HashMap<>();
            value.entrySet().parallelStream()
                    .filter(e -> DataConfig.REDIS_ALIVE_KEYS.contains(((Map.Entry) e).getKey()))
                    .forEach(e -> {
                        Map.Entry entry = (Map.Entry) e;
                        tmp.put(entry.getKey().toString(), Optional.ofNullable(entry.getValue()).orElse("").toString());
                    });
            RedisUtil.hmset(0, bundleKey, tmp);
        };
    }

    public static Consumer<Map> redisTestSinker() {
        return (Map value) -> {
            String bundleKey = (String) value.get(DataConfig.BUNDLE_KEY);
            Map<String, String> tmp = new HashMap<>();
            value.entrySet().parallelStream()
                    .filter(e -> DataConfig.REDIS_ALIVE_KEYS.contains(((Map.Entry) e).getKey()))
                    .forEach(e -> {
                        Map.Entry entry = (Map.Entry) e;
                        tmp.put(entry.getKey().toString(), Optional.ofNullable(entry.getValue()).orElse("").toString());
                    });
            log.info("redisSink:[{}] -> [{}]", JSON.toJSONString(tmp), bundleKey);
        };
    }
}
