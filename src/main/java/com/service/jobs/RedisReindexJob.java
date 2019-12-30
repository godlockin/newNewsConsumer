package com.service.jobs;

import com.common.utils.DataUtils;
import com.common.utils.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Service
public class RedisReindexJob extends ESRelatedJobs {

    @Override
    public String jobName() { return "_redisReindexJob"; }

    static final int MAX_RETRY = 10;
    static final int MAX_THREADS = 10000;
    private AtomicInteger threadCount = new AtomicInteger(0);
    private AtomicInteger idGroupCount = new AtomicInteger(0);
    private AtomicInteger trgtDataCount = new AtomicInteger(0);
    private List<String> errList = new ArrayList<>();
    private ExecutorService executorService;
    private ConcurrentLinkedQueue<List<String>> idQueue = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<Future<List<String>>> slaverQueue = new ConcurrentLinkedQueue<>();
    private Boolean stillWorking = false;

    private JedisPool sourcePool;
    private JedisPool destPool;
    private List<String> aliveKeys;
    private String trgtKeysPattern;

    @Override
    public void scrollJob(Map param) {
        log.info("Start to do [{}] job for param:[{}]", jobName(), param);
        init();
        executorService.submit(new Monitor());
        Future<List<String>> future = executorService.submit(new Master());

        String scanPattern = DataUtils.getNotNullValue(param, "scanPattern", String.class, "*");
        Integer keysMaxCount = DataUtils.getNotNullValue(param, "keysMaxCount", Integer.class, 10000);
        aliveKeys = DataUtils.getNotNullValue(param, "aliveKeys", List.class, new ArrayList<>());
        trgtKeysPattern = DataUtils.getNotNullValue(param, "trgtKeysPattern", String.class, "%s");

        Map source = DataUtils.getNotNullValue(param, "source", Map.class, new HashMap<>());
        Map dest = DataUtils.getNotNullValue(param, "dest", Map.class, new HashMap<>());

        sourcePool = RedisUtil.buildJedisPool(0, source);
        destPool = RedisUtil.buildJedisPool(0, dest);
        int count = 0;
        int trgtCount = 0;

        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams scanParams = new ScanParams().match(scanPattern).count(keysMaxCount);
        boolean first = true;
        while (first || !ScanParams.SCAN_POINTER_START.equals(cursor)) {

            try (Jedis sourceJ = getClient(sourcePool)) {
                ScanResult<String> stringScanResult = sourceJ.scan(cursor, scanParams);
                List<String> result = stringScanResult.getResult();
                count += result.size();

                List<String> trgtList = result.stream()
                        .filter(x -> !(x.contains(":") || x.contains("_")))
                        .collect(Collectors.toList());

                if (CollectionUtils.isEmpty(trgtList)) {
                    continue;
                }

                trgtCount += trgtList.size();

                while (idGroupCount.intValue() > 1000) {
                    Thread.sleep(1000 * 10);
                    log.info("Too many data in queue, sleep for a while");
                    stillWorking = true;
                }

                idQueue.add(trgtList);
                idGroupCount.incrementAndGet();
                if (0 == count % 100_000) {
                    log.info("Dump {} data for {} in total", result.size(), count);
                }

                first = false;
                cursor = stringScanResult.getStringCursor();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            errList = future.get();
            log.info("Reindex {}/{} data with {} trgt ones and {} error from [{}] to [{}]", trgtCount, count, trgtDataCount, errList.size(), source, dest);
            log.error("Error data:\n" + errList);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened during we reindex ids");
        }
    }

    @Override
    protected void init() {

        // init thread pool
        int poolSize = Runtime.getRuntime().availableProcessors() * 20;
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(10240);
        RejectedExecutionHandler policy = new ThreadPoolExecutor.DiscardPolicy();
        executorService = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, queue, policy);
    }

    private class Monitor extends Thread {

        @Override
        public void run() {

            int dataCount = 0;
            int slaverCount = 0;
            int retry = 0;
            try {
                while (++retry < MAX_RETRY || !idQueue.isEmpty() || stillWorking) {
                    List<String> list = idQueue.poll();
                    if (!CollectionUtils.isEmpty(list)) {
                        while (threadCount.intValue() > MAX_THREADS) {
                            Thread.sleep(1000 * 10);
                            log.info("Too many slavers are working, sleep for a while");
                        }

                        while (!list.isEmpty()) {
                            List<String> tmp = list.subList(0, Math.min(1000, list.size()));
                            slaverQueue.add(executorService.submit(new Slaver(++slaverCount, new ArrayList<>(tmp))));
                            list = list.subList(tmp.size(), list.size());
                        }

                        dataCount += list.size();
                        log.info("Start {} to handle {} data in total", slaverCount, dataCount);
                        retry--;
                    } else {
                        Thread.sleep(1000 * 10);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Failure to monitor" + e);
            }

            log.info("Finished reindex {} data by {} slavers", dataCount, slaverCount);
        }
    }

    private class Master implements Callable<List<String>> {

        @Override
        public List<String> call() {
            int slaverCount = 0;
            List<String> errList = new ArrayList<>();
            int retry = 0;
            int groupMmNum = 0;
            try {
                while (++retry < MAX_RETRY || !slaverQueue.isEmpty() || stillWorking) {
                    Future<List<String>> future = slaverQueue.poll();
                    if (null == future) {
                        Thread.sleep(1000 * 10);
                        log.info("No slaver found, sleep for a while");
                    } else {
                        errList.addAll(future.get());
                        slaverCount++;
                        log.info("Finished {} slavers and got {} error ids", slaverCount, errList.size());
                        if (0 == ++groupMmNum% 10) {
                            groupMmNum = 0;
                            idGroupCount.decrementAndGet();
                        }
                        retry--;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Failure to be a master" + e);
            }

            log.info("Finished all {} slavers and got {} error ids", slaverCount, errList.size());
            return errList;
        }
    }

    private class Slaver implements Callable<List<String>> {

        private List<String> idList;
        private Integer slaverNo;

        public Slaver(int slaverNo, List<String> idList) {
            this.slaverNo = slaverNo;
            this.idList = idList;
        }

        @Override
        public List<String> call() {

            log.debug("Start NO.{} slaver to handle {} data", slaverNo, idList.size());
            List<String> errorList = new ArrayList<>();

            try (Jedis sourceJ = getClient(sourcePool);
                 Jedis destJ = getClient(destPool)) {

                idList.stream()
                        .filter(x -> !(x.contains(":") || x.contains("_")))
                        .forEach(x -> {
                            try {
                                Map<String, String> map = sourceJ.hgetAll(x);
                                Map<String, String> output = new HashMap<>();
                                map.entrySet().parallelStream()
                                        .filter(e -> aliveKeys.isEmpty() || aliveKeys.contains(e.getKey()))
                                        .forEach(e -> output.put(e.getKey(), e.getValue()));

                                String key = String.format(trgtKeysPattern, x);
//                                log.info(key);
                                destJ.hmset(key, output);
                                trgtDataCount.incrementAndGet();
                            } catch (Exception e) {
                                e.printStackTrace();
                                log.error("Error happened during we do reindex for [{}], {}", x, e);
                                errorList.add(x);
                            }
                        });

            } catch (Exception e) {
                errorList.addAll(idList);
                e.printStackTrace();
                log.error("Error happened with error {} in slaver {}, during we handle ids:[{}]", e, slaverNo, idList);
            }

            threadCount.decrementAndGet();

            log.debug("NO.{} slaver finished to handle {} data with {} error", slaverNo, idList.size(), errorList.size());
            return errorList;
        }
    }

    private Jedis getClient(JedisPool pool) {
        try {
            return pool.getResource();
        } catch (JedisException e) {
            return getClient(pool);
        }
    }
}
