package com.common.constants;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings({"unchecked"})
public class BusinessConstants {

    public static class SysConfig {
        private SysConfig() {}

        public static final String ENV_FLG_KEY = "spring.profiles.active";
        public static final String BASE_CONFIG = "application.yml";
        public static final String CONFIG_TEMPLATE = "application-%s.yml";
    }

    public static class KfkConfig {
        private KfkConfig() {}

        public static final String INPUT_APPID_KEY = "kafka.input.appId";
        public static final String HOSTS_KEY = "kafka.input.hosts";
        public static final String INPUT_TOPIC_KEY = "kafka.input.topic";

        public static final String TOPIC_POSITION_INFO = "Topic:[{}] Position:[{}]";
    }

    public static class ESConfig {
        private ESConfig() {}

        public static final String ES_BULK_SIZE_KEY = "elasticsearch.bulk.size";
        public static final String ES_BULK_FLUSH_KEY = "elasticsearch.bulk.flush";
        public static final String ES_BULK_CONCURRENT_KEY = "elasticsearch.bulk.concurrent";
        public static final String ES_CONNECT_TIMEOUT_KEY = "elasticsearch.connect-timeout";
        public static final String ES_SOCKET_TIMEOUT_KEY = "elasticsearch.socket-timeout";
        public static final String ES_CONNECTION_REQUEST_TIMEOUT_KEY = "elasticsearch.connection-request-timeout";

        public static final String ES_ADDRESSES_KEY = "elasticsearch.address";
        public static final String ES_INDEX_KEY = "elasticsearch.index";

        public static final String DEFAULT_ES_ADDRESSES = "localhost";
        public static final String DEFAULT_ES_INDEX = "new_news_current_writer";
        public static final int DEFAULT_ES_BULK_SIZE = 10;
        public static final int DEFAULT_ES_BULK_FLUSH = 5000;
        public static final int DEFAULT_ES_BULK_CONCURRENT = 3;
        public static final int DEFAULT_ES_CONNECT_TIMEOUT = 5000;
        public static final int DEFAULT_ES_SOCKET_TIMEOUT = 40000;
        public static final int DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT = 1000;

        public static final String YEARLY_INDEX_ALIAS = "new_news_yearly_current_reader";
        public static final String MONTHLY_INDEX_ALIASES = "new_news_monthly_current_reader";
    }

    public static class TasksConfig {
        private TasksConfig() {}

        public static final String ENABLE_DAILY_JOB_KEY = "tasks.enableDailyJob";
        public static final String ENABLE_MONTHLY_JOB_KEY = "tasks.enableMonthlyJob";
    }

    public static class LandIdConfig {
        private LandIdConfig() {}

        public static final String REMOTE_URL_KEY = "langid.url";
    }

    public static class RedisConfig {
        public RedisConfig() { }

        public static String HOST_KEY = "host";
        public static String PORT_KEY = "port";
        public static String PASSWORD_KEY = "password";

        public static String KEY_KEY = "key";
        public static String OPT_KEY = "opt";
        public static String DB_KEY = "db";
        public static String EXPIRED_KEY = "expired";
        public static String DELETE_OPT = "delete";
        public static String REFRESH_OPT = "refresh";
    }
}
