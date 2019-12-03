package com.common;

import com.common.constants.BusinessConstants.SysConfig;
import com.common.utils.DataUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class LocalConfig {

    private static ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();

    public static Object put(String k, Object v) { return cache.put(k, v); }

    public static void putAll(Map<String, Object> config) { cache.putAll(config); }

    public static <T> T get(String k, Class clazz, Object defaultValue) {
        if (cache.isEmpty()) {
            synchronized (LocalConfig.class) {
                if (cache.isEmpty()) {
                    sysInit();
                }
            }
        }

        Object value = DataUtils.getNotNullValue(cache, k, clazz, defaultValue);
        if (value instanceof String && String.valueOf(value).contains("$")) {
            String valueStr = String.valueOf(value).trim();
            valueStr = valueStr.replace("${", "").replace("}", "");
            String envStr = System.getProperty(valueStr);
            return (T) DataUtils.handleNullValue(envStr, clazz, defaultValue);
        } else {
            return (T) value;
        }
    }

    public static Map<String, Object> get() {
        return new HashMap(cache);
    }

    @PostConstruct
    public static void sysInit() {
        // cache base config
        putAll(loadYamlConfig(SysConfig.BASE_CONFIG));

        // load config for env
        String envFlg = get(SysConfig.ENV_FLG_KEY, String.class, "");
        if (StringUtils.isNotBlank(envFlg)) {
            Map trgt = loadYamlConfig(String.format(SysConfig.CONFIG_TEMPLATE, envFlg));
            trgt.entrySet().parallelStream().forEach(x -> {
                Map.Entry<String, Object> entry = (Map.Entry) x;
                String key = entry.getKey();
                Object value = entry.getValue();
                if (!(StringUtils.isBlank(key) || null == value)) {
                    cache.put(key, value);
                }
            });
        }

        log.info("Init {} config done", cache.size());
    }

    private static Map<String, Object> loadYamlConfig(String fileName) {

        Map trgt = new HashMap();
        try (InputStream fis = LocalConfig.class.getClassLoader().getResourceAsStream(fileName)) {
            Map base = new Yaml().load(fis);
            flattenMap("", base, trgt);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return trgt;
    }

    private static void flattenMap(String prefixKey, Map base, Map trgt) {

        String key = (StringUtils.isNotBlank(prefixKey)) ? prefixKey + "." : "";
        base.forEach((k, v) -> {
            if (v instanceof Map) {
                flattenMap(key + k, (Map) v, trgt);
            } else {
                trgt.put(key + k, v);
            }
        });
    }
}
