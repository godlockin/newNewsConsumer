package com.service;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
    private Consumer consumer;

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

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", "10.0.9.62:9092,10.0.9.63:9092,10.0.9.67:9092");

        Producer<String, String> producer = new KafkaProducer<>(props);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("pub_date", "2019-12-03 15:10:12");
        jsonObject.put("title", "扩散转发！贫困户 4 万余斤芋子滞销，快来帮帮他们！-大江网-中国江西网首页");
        jsonObject.put("url", "http://www.jxcn.cn/system/2019/12/03/018677384.shtml");
        jsonObject.put("content", "http://donuts.aigauss.com/broadspiderraw/www_jxcn_cn_48320597fdd08a46524bdaf5e108e743.html");
        jsonObject.put("bundle_key", "48320597fdd08a46524bdaf5e108e743");
        jsonObject.put("source", "中国新闻网");

        int j = 10;
        while (--j > 0) {
            for (int i = 0; i < 11; i++) {
                RecordMetadata metadata = producer.send(new ProducerRecord<>("spider-formated"
                        , i
                        , System.currentTimeMillis()
                        , null
                        , jsonObject.toJSONString())).get();
                System.out.println("offset:" + metadata.offset() + ", partition:" + metadata.partition() + ", timestamp:" + metadata.timestamp());
            }
        }

        System.out.println("a");
    }
}
