package com.common;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RetryQueue {

    private static ConcurrentHashMap<String, Integer> queue = new ConcurrentHashMap<>();

    public static Boolean isEmpty() {
        return queue.isEmpty();
    }

    public static void add(String key) {
        queue.put(key, queue.getOrDefault(key, 0) + 1);
    }

    public static void add(String key, Integer count) {
        queue.put(key, Optional.ofNullable(count).orElse(1));
    }

    public static Map<String, Integer> get() {
        return new HashMap<>(queue);
    }

    public static void remove(String key) {
        queue.remove(key);
    }

    synchronized public static Map<String, Integer> getAndClean() {
        Map<String, Integer> map = new HashMap<>(queue);
        queue.clear();
        return map;
    }

    public static void clean() {
        queue.clear();
    }
}
