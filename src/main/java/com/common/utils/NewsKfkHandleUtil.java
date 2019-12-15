package com.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.common.constants.BusinessConstants.DataConfig;
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

                String url = value.getString(DataConfig.CONTENT_KEY);
                tmp.put(DataConfig.OSSURL_KEY, url);

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
                        .replaceAll("\n|\r\n|\\n|\\t", "")
                        .replaceAll("&nbsp", "")
                        .trim();

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
                    .filter(e -> !DataConfig.CONTENT_KEY.equalsIgnoreCase((String) ((Map.Entry) e).getKey()))
                    .forEach(e -> {
                        Map.Entry entry = (Map.Entry) e;
                        tmp.put(entry.getKey().toString(), Optional.ofNullable(entry.getValue()).orElse("").toString());
                    });
            RedisUtil.hmset(0, bundleKey, tmp);
        };
    }

}
