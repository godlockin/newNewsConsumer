package com.common.utils;

import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class RedisCache {

    private static ConcurrentHashMap<Integer, JedisPool> cache = new ConcurrentHashMap<>();

    public static JedisPool setPool(Integer db, JedisPool jedisPool) {
        return cache.put(db, jedisPool);
    }

    public static JedisPool getPool(Integer db) {
        JedisPool jedisPool = cache.get(db);
        if (null == jedisPool) {
            synchronized (RedisCache.class) {
                jedisPool = cache.get(db);
                if (null == jedisPool) {
                    RedisUtil.init();
                }
            }
        }
        return cache.get(db);
    }
}
