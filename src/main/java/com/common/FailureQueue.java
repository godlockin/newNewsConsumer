package com.common;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
public class FailureQueue {

    private static ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();

    public static Boolean isEmpty() {
        return queue.isEmpty();
    }

    public static void add(String key) {
        queue.add(key);
    }

    public static List<String> get() {
        return new ArrayList(queue);
    }

    synchronized public static List<String> getAndClean() {
        List<String> list = new ArrayList(queue);
        queue.clear();
        return list;
    }

    public static void clean() {
        queue.clear();
    }
}
