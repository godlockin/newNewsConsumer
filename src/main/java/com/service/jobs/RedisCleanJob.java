package com.service.jobs;

import com.common.utils.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Service
public class RedisCleanJob extends ESRelatedJobs {

    @Override
    public String jobName() { return "_redisCleanJob"; }

    @Override
    protected Consumer<List<Map>> functionLogic() {
        return list -> list.parallelStream()
                .forEach(x -> RedisUtil.del(0, (String) x.get("bundleKey")));
    }
}
