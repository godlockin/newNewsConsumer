package com.common.constants;

import java.util.Arrays;
import java.util.List;

public class BusinessConstants {

    public static class SysConfig {
        private SysConfig() {}

        public static final String ENV_FLG_KEY = "spring.profiles.active";
        public static final String IS_TEST_FLG_KEY = "isTest";
        public static final String BASE_CONFIG = "application.yml";
        public static final String CONFIG_TEMPLATE = "application-%s.yml";


        public static final String ES_BULK_SIZE_KEY = "elasticsearch.bulk.size";
        public static final String ES_BULK_FLUSH_KEY = "elasticsearch.bulk.flush";
        public static final String ES_BULK_CONCURRENT_KEY = "elasticsearch.bulk.concurrent";
        public static final String ES_CONNECT_TIMEOUT_KEY = "elasticsearch.connect-timeout";
        public static final String ES_SOCKET_TIMEOUT_KEY = "elasticsearch.socket-timeout";
        public static final String ES_CONNECTION_REQUEST_TIMEOUT_KEY = "elasticsearch.connection-request-timeout";
    }

    public static class DataConfig {
        private DataConfig() {}

        public static final String BUNDLE_KEY = "bundleKey";
        public static final String CONTENT_KEY = "content";
        public static final String SUMMARY_KEY = "summary";
        public static final String EXCERPT_KEY = "excerpt";
        public static final String SUMMARYSEG_KEY = "summarySeg";
        public static final String OSSURL_KEY = "ossUrl";
        public static final String DOMAIN_KEY = "domain";
        public static final String SOURCENAME_KEY = "sourceName";
        public static final String TITLE_KEY = "title";
        public static final String TITLESEG_KEY = "titleSeg";
        public static final String SOURCEURL_KEY = "sourceUrl";
        public static final String PUBLISHDATE_KEY = "publishDate";
        public static final String SEPARATEDATE_KEY = "separateDate";
        public static final String ENTRYTIME_KEY = "entryTime";
        public static final String HTML_OSS_URL_KEY = "htmlOssUrl";

        public static final String LIKE_KEY = "like";

        public static final List<String> MAPPINGFIELDS = Arrays.asList(
                "pub_date"
                ,"url"
                ,"content"
                ,"bundle_key"
                ,"source"
                ,"html"
        );

        public static final List<String> ES_ALIVE_KEYS = Arrays.asList(
                "sourceUrl"
                ,"bundleKey"
                ,"ossUrl"
                ,"htmlOssUrl"
                ,"publishDate"
                ,"separateDate"
                ,"sourceName"
                ,"domain"
                ,"title"
                ,"summary"
                ,"content"
                ,"titleSeg"
                ,"summarySeg"
                ,"contentSeg"
        );

        public static final List<String> REDIS_ALIVE_KEYS = Arrays.asList(
                "sourceUrl"
                ,"bundleKey"
                ,"entryTime"
                ,"ossUrl"
                ,"htmlOssUrl"
                ,"publishDate"
                ,"sourceName"
                ,"domain"
                ,"title"
        );
    }

    public static class KfkConfig {
        private KfkConfig() {}

        public static final String INPUT_APPID_KEY = "kafka.input.appId";
        public static final String HOSTS_KEY = "kafka.input.hosts";
        public static final String INPUT_TOPIC_KEY = "kafka.input.topic";
        public static final String OUTPUT_TOPIC_KEY = "kafka.output.topic";
    }

    public static class ESConfig {
        private ESConfig() {}

        public static final int DEFAULT_ES_HTTP_PORT = 9200;
        public static final int DEFAULT_ES_BULK_SIZE = 10;
        public static final int DEFAULT_ES_BULK_FLUSH = 5000;
        public static final int DEFAULT_ES_BULK_CONCURRENT = 3;
        public static final int DEFAULT_ES_FROM = 0;
        public static final int DEFAULT_ES_SIZE = 10;
        public static final int DEFAULT_ES_MAX_SIZE = 10000;
        public static final int DEFAULT_ES_CONNECT_TIMEOUT = 5000;
        public static final int DEFAULT_ES_SOCKET_TIMEOUT = 40000;
        public static final int DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT = 1000;
        public static final int DEFAULT_ES_MAX_RETRY_TINEOUT_MILLIS = 60000;
        public static final String DEFAULT_ES_INDEX = "";

        public static final String QUERY_KEY = "query";
        public static final String SCORE_KEY = "score";
        public static final String FILTER_KEY = "filter";
        public static final String HIGHLIGHT_KEY = "highlight";
        public static final String AGGREGATION_KEY = "aggregation";
        public static final String FETCHSOURCE_KEY = "fetchSource";
        public static final String INCLUDE_KEY = "include";
        public static final String EXCLUDE_KEY = "exclude";
        public static final String SORT_KEY = "sort";

        public static final String NAME_KEY = "name";
        public static final String TYPE_KEY = "type";
        public static final String INDEX_KEY = "index";
        public static final String SIZE_KEY = "size";
        public static final String FIELD_KEY = "field";
        public static final String VALUE_KEY = "value";
        public static final String BOOST_KEY = "boost";

        public static final String MUST_KEY = "must";
        public static final String SHOULD_KEY = "should";
        public static final String MUST_NOT_KEY = "must_not";
        public static final String POST_FILTER_KEY = "post_filter";

        public static final List<String> BOOL_CONDITION_LIST = Arrays.asList(MUST_KEY, SHOULD_KEY, MUST_NOT_KEY);

        public static final String TERM_KEY = "term";
        public static final String TERMS_KEY = "terms";
        public static final String MATCH_KEY = "match";
        public static final String EXISTS_KEY = "exists";
        public static final String FUZZY_KEY = "fuzzy";
        public static final String PREFIX_KEY = "prefix";
        public static final String REGEXP_KEY = "regexp";
        public static final String WRAPPER_KEY = "wrapper";
        public static final String WILDCARD_KEY = "wildcard";
        public static final String COMMONTERMS_KEY = "commonTerms";
        public static final String QUERY_STRING_KEY = "queryString";
        public static final String MATCH_PHRASE_KEY = "matchPhrase";
        public static final String MATCH_PHRASE_PREFIX_KEY = "matchPhrasePrefix";

        public static final List<String> SIMPLE_CONDITION_LIST = Arrays.asList(TERM_KEY, TERMS_KEY, MATCH_KEY,
                FUZZY_KEY, PREFIX_KEY, REGEXP_KEY, WRAPPER_KEY, WILDCARD_KEY, QUERY_STRING_KEY,
                MATCH_PHRASE_KEY, MATCH_PHRASE_PREFIX_KEY, EXISTS_KEY);

        public static final String RANGE_KEY = "range";

        public static final String INCLUDE_LOWER_KEY = "include_lower";
        public static final String INCLUDE_UPPER_KEY = "include_upper";
        public static final String FROM_KEY = "from";
        public static final String LTE_KEY = "lte";
        public static final String GTE_KEY = "gte";
        public static final String LT_KEY = "lt";
        public static final String GT_KEY = "gt";
        public static final String TO_KEY = "to";

        public static final String MULTIMATCH_KEY = "multiMatch";
        public static final String FIELDNAMES_KEY = "fieldNames";

        public static final String NESTED_KEY = "nested";
        public static final String PATH_KEY = "path";

        public static final String COUNT_KEY = "count";
        public static final String MAX_KEY = "max";
        public static final String MIN_KEY = "min";
        public static final String SUM_KEY = "sum";
        public static final String AVG_KEY = "avg";
        public static final String STATS_KEY = "stats";

        public static final String SHARD_SIZE_KEY = "shardSize";
        public static final String MISSING_KEY = "missing";
        public static final String MIN_DOC_COUNT_KEY = "minDocCount";
        public static final String SHARD_MIN_DOC_COUNT_KEY = "shardMinDocCount";

        public static final List<String> SIMPLE_AGGREGATION_LIST = Arrays.asList(COUNT_KEY, MAX_KEY, MIN_KEY,
                SUM_KEY, AVG_KEY, TERMS_KEY);

        public static final String SUB_AGG_KEY = "subAgg";
        public static final String DATE_RANGE_KEY = "dateRange";

        public static final String ANALYZER_KEY = "analyzer";
        public static final String CHAR_FILTER_KEY = "charFilter";
        public static final String TOKENIZER_KEY = "tokenizer";
        public static final String TOKEN_FILTER_KEY = "tokenFilter";
        public static final String NORMALIZER_KEY = "normalizer";

        public static final String DEFAULT_ANALYZER = "standard";

        public static final String SCROLL_TIME_VALUE_KEY = "timeValue";
        public static final String DEFAULT_SCROLL_TIME_VALUE = "1h";

        public static final String ORDER_KEY = "order";
        public static final String SORT_ORDER_ASC = "asc";
        public static final String SORT_ORDER_DESC = "desc";
        public static final String FIELD_SORT_TYPE = "field";
        public static final String SCRIPT_SORT_TYPE = "script";
        public static final String SCRIPT_TYPE = "scriptType";
        public static final String SCRIPT_LANG = "scriptLang";
        public static final String SCRIPT_SORT_SCRIPT_TYPE = "scriptSortType";
        public static final String SORT_MODE = "sortMode";
        public static final String SCRIPT_OPTIONS = "scriptOptions";
        public static final String SCRIPT_PARAMS = "scriptParams";

        public static final String NUMBER_TYPE = "number";
        public static final String STRING_TYPE = "string";

        public static final String MIN_MODE = "min";
        public static final String MAX_MODE = "max";
        public static final String SUM_MODE = "sum";
        public static final String AVG_MODE = "avg";
        public static final String MEDIAN_MODE = "median";

        public static final String INLINE_TYPE = "inline";
        public static final String STORED_TYPE = "stored";

        public static final String PAINLESS_TYPE = "painless";
    }

    public static class TasksConfig {
        private TasksConfig() {}

        public static final String CORE_JOBS_KEY = "jobs";
        public static final String WIKI_JOB_KEY = "wiki";
        public static final String QUESTION_ANSWER_JOB_KEY = "qa";
        public static final String BLOG_JOB_KEY = "blog";
        public static final String DAILY_NEWS_JOB_KEY = "dailynews";
        public static final String TRGT_ES_INDEX_KEY = "elasticsearch.index";
        public static final String TRGT_ES_ADDRESS_KEY = "elasticsearch.address";

        public static final String NEWS_NORMAL_ES_INDEX_KEY = "elasticsearch.news.normalIndex";
        public static final String NEWS_EXTRA_ES_INDEX_KEY = "elasticsearch.news.extraIndex";
        public static final String NEWS_ISSUE_ES_INDEX_KEY = "elasticsearch.news.issueIndex";
        public static final String DEFAULT_ISSUE_INDEX = "newton_original_doc_issue";
        public static final String REMOTE_ANALYSIS_URL_KEY = "remote-analysis.url";
    }

    public static class LandIdConfig {
        private LandIdConfig() {}

        public static final String REMOTE_URL_KEY = "langid.url";
    }

    public static class SummaryConfig {
        private SummaryConfig() {}

        public static final String REMOTE_URL_KEY = "summary.url";
    }

    public static class RedisConfig {
        public RedisConfig() { }

        public static String HOST_KEY = "host";
        public static String PORT_KEY = "port";
        public static String PASSWORD_KEY = "password";
    }

    public static class ResultConfig {
        private ResultConfig() {}

        public static final String DATA_KEY = "data";
        public static final String TOTAL_KEY = "total";
        public static final String ENTITY_KEY = "entity";
        public static final String HIGHLIGH_KEY = "highlight";
        public static final String AGGREGATION_KEY = "aggregation";
        public static final String SCROLL_ID_KEY = "scrollId";
    }
}
