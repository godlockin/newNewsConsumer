package com.common.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@Component
public class NewsKfkHandleUtil {

    public static Function<JSONObject, Map> sourceMapper() {
        return value -> {

            Map<String, Object> result = new HashMap<>();
            try {
                Map<String, Object> tmp = new HashMap<>();

                String url = value.getString("content");
                tmp.put("ossUrl", url);

                String content = RestHttpClient.doGet(url);
                if (StringUtils.isBlank(content)) {
                    log.error("No content for:[{}]", value);
                    return result;
                }

                content = new String(content.getBytes(StandardCharsets.UTF_8));

                content = content
                        .replaceAll("<[.[^>]]*>", "")
                        .replaceAll("[\\s\\p{Zs}]+", "")
                        .replaceAll("\\s*|\t|\r|\n", "")
                        .replaceAll("\\n", "")
                        .replaceAll("&nbsp", "")
                        .trim();

                if (StringUtils.isBlank(content)) {
                    log.error("No content for:[{}]", value);
                    return result;
                }

                tmp.put("content", content);

                tmp.put("domain", value.getOrDefault("domain", ""));
                tmp.put("sourceName", value.getOrDefault("source", ""));
                tmp.put("title", value.get("title"));

                String sourceUrl = value.getString("url");
                tmp.put("sourceUrl", sourceUrl);

                String bundleKey = (String) value.getOrDefault("bundle_key", GuidService.getMd5(sourceUrl).toLowerCase());
                tmp.put("bundleKey", bundleKey);

                Object publishDate = value.get("pub_date");
                Long timestamp = System.currentTimeMillis();

                tmp.put("publishDate", publishDate);

                tmp.put("separateDate", Optional.ofNullable(publishDate).orElse(timestamp));

                result = new HashMap<>(tmp);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error happened on handling:[{}], {}", value, e);
            }

            return result;
        };
    }


    public static Consumer<Map> redisSinker() {
        return value -> {
            String bundleKey = (String) value.get("bundleKey");
            Map<String, String> tmp = new HashMap(value);
            tmp.remove("content");
            RedisUtil.hmset(0, bundleKey, tmp);
        };
    }

}
