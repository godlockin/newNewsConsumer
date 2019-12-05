package com.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class MonitorService {

    private static OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
    private static MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private static MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
    private static Runtime runtime = Runtime.getRuntime();

    private static ConcurrentHashMap<String, Object> sysStatement = new ConcurrentHashMap<>();

    @Autowired
    private ESService esService;
    @Autowired
    private KfkConsumer consumer;

    public Map statement() {

        return new HashMap(sysStatement);
    }

    @PostConstruct
    void init() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {

            // 初始的总内存
            sysStatement.put("initTotalMemorySize", memoryUsage.getInit());

            // 最大可用内存
            sysStatement.put("maxMemorySize", memoryUsage.getMax());

            // 已使用的内存
            sysStatement.put("usedMemorySize", memoryUsage.getUsed());

            // 操作系统
            sysStatement.put("osName", System.getProperty("os.name"));

            // JVM可以使用的总内存
            sysStatement.put("totalMemory", runtime.totalMemory());

            // JVM可以使用的剩余内存
            sysStatement.put("freeMemory", runtime.freeMemory());

            // JVM可以使用的处理器个数
            sysStatement.put("availableProcessors", runtime.availableProcessors());

            // operating system name
            sysStatement.put("systemName", osmxb.getName());

            // operating system version
            sysStatement.put("systemVersion", osmxb.getVersion());

            // SystemLoadAverage
            sysStatement.put("systemLoad", osmxb.getSystemLoadAverage());

            // es statement
            sysStatement.put("esService", esService.statement());

            // consumer statement
            sysStatement.put("consumer", consumer.statement());
        }, 0, 5, TimeUnit.SECONDS);
    }
}
