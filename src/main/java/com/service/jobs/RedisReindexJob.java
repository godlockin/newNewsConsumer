package com.service.jobs;

import com.common.utils.DataUtils;
import com.common.utils.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Service
public class RedisReindexJob extends ESRelatedJobs {

    @Override
    public String jobName() { return "_redisReindexJob"; }

    @Override
    public void scrollJob(Map param) {
        log.info("Start to do [{}] job for param:[{}]", jobName(), param);

        String scanPattern = DataUtils.getNotNullValue(param, "scanPattern", String.class, "*");
        Integer keysMaxCount = DataUtils.getNotNullValue(param, "keysMaxCount", Integer.class, 100);
        List<String> aliveKeys = DataUtils.getNotNullValue(param, "aliveKeys", List.class, new ArrayList<>());
        String trgtKeysPattern = DataUtils.getNotNullValue(param, "trgtKeysPattern", String.class, "%s");

        Map source = DataUtils.getNotNullValue(param, "source", Map.class, new HashMap<>());
        Map dest = DataUtils.getNotNullValue(param, "dest", Map.class, new HashMap<>());

        JedisPool sourcePool = RedisUtil.buildJedisPool(0, source);
        JedisPool destPool = RedisUtil.buildJedisPool(0, dest);
        int count = 0;

        ConcurrentLinkedQueue<String> errorList = new ConcurrentLinkedQueue<>();
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams scanParams = new ScanParams().match(scanPattern).count(keysMaxCount);
        boolean first = true;
        while (first || !ScanParams.SCAN_POINTER_START.equals(cursor)) {

            try (Jedis sourceJ = getClient(sourcePool);
                 Jedis destJ = getClient(destPool)) {
                ScanResult<String> stringScanResult = getClient(sourcePool).scan(cursor, scanParams);
                List<String> result = stringScanResult.getResult();
                count += result.size();

                log.info("Dump {} data for {} in total", result.size(), count);
                result.stream()
                        .filter(x -> !(x.contains(":") || x.contains("_")))
                        .forEach(x -> {
                            try {
                                Map<String, String> map = sourceJ.hgetAll(x);
                                Map<String, String> output = new HashMap<>();
                                map.entrySet().parallelStream()
                                        .filter(e -> aliveKeys.isEmpty() || aliveKeys.contains(e.getKey()))
                                        .forEach(e -> output.put(e.getKey(), e.getValue()));

                                String key = String.format(trgtKeysPattern, x);
                                log.info(key);
                                destJ.hmset(key, output);
                            } catch (Exception e) {
                                e.printStackTrace();
                                log.error("Error happened during we do reindex for [{}]", x);
                                errorList.add(x);
                            }
                        });

                first = false;
                cursor = stringScanResult.getStringCursor();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        log.info("Reindex {} data with {} error from [{}] to [{}]", count, errorList.size(), source, dest);
        log.error("Error data:\n" + errorList);
    }

    private Jedis getClient(JedisPool pool) {
        try {
            return pool.getResource();
        } catch (JedisException e) {
            return getClient(pool);
        }
    }
}
