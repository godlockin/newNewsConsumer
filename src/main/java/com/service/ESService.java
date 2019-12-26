package com.service;

import com.alibaba.fastjson.JSON;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants;
import com.common.constants.BusinessConstants.ESConfig;
import com.common.constants.BusinessConstants.SysConfig;
import com.common.constants.BusinessConstants.ResultConfig;
import com.common.constants.ResultEnum;
import com.common.utils.DataUtils;
import com.exception.ConsumerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.*;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
@EnableScheduling
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class ESService extends KfkConsumer {

    private String ES_ADDRESSES;

    private int ES_BULK_SIZE;
    private int ES_BULK_FLUSH;
    private int ES_SOCKET_TIMEOUT;
    private int ES_CONNECT_TIMEOUT;
    private int ES_BULK_CONCURRENT;
    private int ES_CONNECTION_REQUEST_TIMEOUT;

    private RequestOptions COMMON_OPTIONS = RequestOptions.DEFAULT.toBuilder().build();
    private static RestHighLevelClient restHighLevelClient;
    private static BulkProcessor bulkProcessor;

    private AtomicLong submitCount = new AtomicLong(0);
    private AtomicLong failureCount = new AtomicLong(0);

    public Map statement() {
        return new HashMap() {{
            put("submitted", submitCount.longValue());
            put("failure", failureCount.longValue());
        }};
    }

    private Boolean isInited = false;

    private ESService() throws ConsumerException {

        String esAddrs = LocalConfig.get(BusinessConstants.TasksConfig.TRGT_ES_ADDRESS_KEY, String.class, "");
        if (StringUtils.isBlank(esAddrs)) {
            log.error("No esAddrs found");
            return;
        }

        initStaticVariables(esAddrs);
        initESClients();

        this.isInited = true;
    }

    public ESService(String esAddrs) throws ConsumerException {

        if (StringUtils.isBlank(esAddrs)) {
            log.error("No esAddrs found");
            return;
        }

        initStaticVariables(esAddrs);
        initESClients();

        this.isInited = true;
    }

    private void initStaticVariables(String esAddrs) {
        ES_ADDRESSES = esAddrs;

        ES_BULK_SIZE = LocalConfig.get(SysConfig.ES_BULK_SIZE_KEY, Integer.class, ESConfig.DEFAULT_ES_BULK_SIZE);
        ES_BULK_FLUSH = LocalConfig.get(SysConfig.ES_BULK_FLUSH_KEY, Integer.class, ESConfig.DEFAULT_ES_BULK_FLUSH);
        ES_SOCKET_TIMEOUT = LocalConfig.get(SysConfig.ES_SOCKET_TIMEOUT_KEY, Integer.class, ESConfig.DEFAULT_ES_SOCKET_TIMEOUT);
        ES_BULK_CONCURRENT = LocalConfig.get(SysConfig.ES_BULK_CONCURRENT_KEY, Integer.class, ESConfig.DEFAULT_ES_BULK_CONCURRENT);
        ES_CONNECT_TIMEOUT = LocalConfig.get(SysConfig.ES_CONNECT_TIMEOUT_KEY, Integer.class, ESConfig.DEFAULT_ES_CONNECT_TIMEOUT);
        ES_CONNECTION_REQUEST_TIMEOUT = LocalConfig.get(SysConfig.ES_CONNECTION_REQUEST_TIMEOUT_KEY, Integer.class, ESConfig.DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT);
    }

    private void initESClients() throws ConsumerException {
        log.info("Init ES client");

        try {
            HttpHost[] httpHosts = Arrays.stream(ES_ADDRESSES.split(",")).parallel().map(HttpHost::create).toArray(HttpHost[]::new);

            RestClientBuilder builder = RestClient.builder(httpHosts)
                    .setRequestConfigCallback((RequestConfig.Builder requestConfigBuilder) ->
                            requestConfigBuilder.setConnectTimeout(ES_CONNECT_TIMEOUT)
                                    .setSocketTimeout(ES_SOCKET_TIMEOUT)
                                    .setConnectionRequestTimeout(ES_CONNECTION_REQUEST_TIMEOUT));

            restHighLevelClient = new RestHighLevelClient(builder);

            bulkProcessor = BulkProcessor.builder((request, bulkListener) -> {
                        try {
                            restHighLevelClient.bulkAsync(request, COMMON_OPTIONS, bulkListener);
                        } catch (IllegalStateException e) {
                            e.printStackTrace();
                            log.error("Error happened during we async bulk handle info");
                            try {
                                this.closeESClient();

                                this.initESClients();
                            } catch (ConsumerException e1) {
                                e1.printStackTrace();
                                log.error("Re init client failure");
                                throw e;
                            }
                        }
                    },
                    getBPListener())
                    .setBulkActions(ES_BULK_FLUSH)
                    .setBulkSize(new ByteSizeValue(ES_BULK_SIZE, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(10L))
                    .setConcurrentRequests(ES_BULK_CONCURRENT)
                    .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we init ES transport client" + e;
            log.error(errMsg);
            throw new ConsumerException(ResultEnum.ES_CLIENT_INIT);
        }
    }

    public Map complexSearch(Map param) throws ConsumerException {
        SearchSourceBuilder sourceBuilder = makeBaseSearchBuilder(param);

        boolean isQuery = false;
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        // build query conditions
        Map query = DataUtils.getNotNullValue(param, ESConfig.QUERY_KEY, Map.class, new HashMap<>());
        if (!query.isEmpty()) {
            boolQueryBuilder.must(buildBoolQuery(query));
            isQuery = true;
        }

        // build filter conditions
        Map filter = DataUtils.getNotNullValue(param, ESConfig.FILTER_KEY, Map.class, new HashMap<>());
        if (!filter.isEmpty()) {
            boolQueryBuilder.filter(buildBoolQuery(filter));
            isQuery = true;
        }

        // set query & filter conditions
        if (isQuery) {
            sourceBuilder.query(boolQueryBuilder);
        }

        // build aggregation
        Map aggregationInfo = DataUtils.getNotNullValue(param, ESConfig.AGGREGATION_KEY, Map.class, new HashMap<>());
        if (!aggregationInfo.isEmpty()) {
            aggregationInfo.forEach((k, v) -> sourceBuilder.aggregation(buildCommonAgg((Map) v)));
        }

        // if highlight exists
        List<String> highlightList = DataUtils.getNotNullValue(param, ESConfig.HIGHLIGHT_KEY, List.class, new ArrayList<>());
        if (!highlightList.isEmpty()) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightList.parallelStream().forEach(highlightBuilder::field);
            sourceBuilder.highlighter(highlightBuilder);
        }

        Map fetchSrouce = DataUtils.getNotNullValue(param, ESConfig.FETCHSOURCE_KEY, Map.class, new HashMap<>());
        if (!fetchSrouce.isEmpty()) {
            List includeList = DataUtils.getNotNullValue(fetchSrouce, ESConfig.INCLUDE_KEY, List.class, new ArrayList<>());
            String[] includeFields = (includeList.isEmpty()) ? new String[0] : (String[]) includeList.parallelStream().toArray(String[]::new);
            List excludeList = DataUtils.getNotNullValue(fetchSrouce, ESConfig.EXCLUDE_KEY, List.class, new ArrayList<>());
            String[] excludeFields = (excludeList.isEmpty()) ? new String[0] : (String[]) excludeList.parallelStream().toArray(String[]::new);
            sourceBuilder.fetchSource(includeFields, excludeFields);
        }

        List sortConfig = DataUtils.getNotNullValue(param, ESConfig.SORT_KEY, List.class, new ArrayList<>());
        if (!sortConfig.isEmpty()) {
            doBuildSorts(sourceBuilder, sortConfig);
        }

        Map postfilter = DataUtils.getNotNullValue(param, ESConfig.POST_FILTER_KEY, Map.class, new HashMap<>());
        if (!postfilter.isEmpty()) {
            sourceBuilder.postFilter(buildBoolQuery(postfilter));
        }

        return fullSearch(param, sourceBuilder);
    }

    private void doBuildSorts(SearchSourceBuilder sourceBuilder, List sortConfig) {

        sortConfig.forEach(x -> {
            Map config = (Map) x;
            String type = DataUtils.getNotNullValue(config, ESConfig.TYPE_KEY, String.class, "");
            String order = DataUtils.getNotNullValue(config, ESConfig.ORDER_KEY, String.class, "");
            if (ESConfig.FIELD_SORT_TYPE.equalsIgnoreCase(type)) {
                String field = DataUtils.getNotNullValue(config, ESConfig.FIELD_KEY, String.class, "");
                FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort(field);
                fieldSortBuilder.order(SortOrder.fromString(order));
                sourceBuilder.sort(fieldSortBuilder);
            } else if (ESConfig.SCRIPT_SORT_TYPE.equalsIgnoreCase(type)) {
                String scriptStr = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_SORT_TYPE, String.class, "");
                String scriptSortTypeStr = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_SORT_SCRIPT_TYPE, String.class, ESConfig.NUMBER_TYPE);
                String sortMode = DataUtils.getNotNullValue(config, ESConfig.SORT_MODE, String.class, "");
                String scriptTypeStr = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_TYPE, String.class, ESConfig.INLINE_TYPE);
                String scriptLangStr = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_LANG, String.class, ESConfig.PAINLESS_TYPE);
                Map options = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_OPTIONS, Map.class, Collections.emptyMap());
                Map params = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_PARAMS, Map.class, Collections.emptyMap());
                if (StringUtils.isNotBlank(scriptStr)) {
                    ScriptType scriptType = (ESConfig.INLINE_TYPE.equalsIgnoreCase(scriptTypeStr)) ? ScriptType.INLINE : ScriptType.STORED;
                    Script script = new Script(scriptType, scriptLangStr, scriptStr, options, params);
                    ScriptSortBuilder.ScriptSortType scriptSortType = ScriptSortBuilder.ScriptSortType.fromString(scriptSortTypeStr);
                    ScriptSortBuilder scriptSortBuilder = SortBuilders.scriptSort(script, scriptSortType);
                    scriptSortBuilder.order(SortOrder.fromString(order));
                    if (StringUtils.isNotBlank(sortMode)) {
                        scriptSortBuilder.sortMode(SortMode.fromString(sortMode));
                    }
                    sourceBuilder.sort(scriptSortBuilder);
                }
            }
        });
    }

    private AggregationBuilder buildCommonAgg(Map param) {
        String type = DataUtils.getNotNullValue(param, ESConfig.TYPE_KEY, String.class, "");
        String aggrName = DataUtils.getNotNullValue(param, ESConfig.NAME_KEY, String.class, "");
        String field = DataUtils.getNotNullValue(param, ESConfig.FIELD_KEY, String.class, "");
        if (ESConfig.SIMPLE_AGGREGATION_LIST.contains(type)) {
            switch (type) {
                case ESConfig.COUNT_KEY:
                    return AggregationBuilders.count(aggrName).field(field);
                case ESConfig.SUM_KEY:
                    return AggregationBuilders.sum(aggrName).field(field);
                case ESConfig.MAX_KEY:
                    return AggregationBuilders.max(aggrName).field(field);
                case ESConfig.MIN_KEY:
                    return AggregationBuilders.min(aggrName).field(field);
                case ESConfig.AVG_KEY:
                    return AggregationBuilders.avg(aggrName).field(field);
                case ESConfig.STATS_KEY:
                    return AggregationBuilders.stats(aggrName).field(field);
                case ESConfig.TERMS_KEY:
                    return getTermsAggregation(param, aggrName, field);
            }
        } else if (ESConfig.NESTED_KEY.equalsIgnoreCase(type)) {
            List subAgg = DataUtils.getNotNullValue(param, ESConfig.SUB_AGG_KEY, List.class, new ArrayList<>());
            String path = DataUtils.getNotNullValue(param, ESConfig.PATH_KEY, String.class, "");
            AggregationBuilder aggregationBuilder = AggregationBuilders.nested(aggrName, path);
            subAgg.parallelStream().forEach(x -> aggregationBuilder.subAggregation(buildCommonAgg((Map) x)));
            return aggregationBuilder;
        }

        String from = DataUtils.getNotNullValue(param, ESConfig.FROM_KEY, String.class, "");
        String to = DataUtils.getNotNullValue(param, ESConfig.TO_KEY, String.class, "");
        return AggregationBuilders.dateRange(field).addRange(from, to);
    }

    private AggregationBuilder getTermsAggregation(Map param, String aggrName, String field) {
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms(aggrName).field(field);
        Integer size = DataUtils.getNotNullValue(param, ESConfig.SIZE_KEY, Integer.class, 0);
        if (0 < size) {
            termsAgg.size(size);
        }

        Integer shard_size = DataUtils.getNotNullValue(param, ESConfig.SHARD_SIZE_KEY, Integer.class, 0);
        if (0 < shard_size) {
            termsAgg.shardSize(shard_size);
        }

        Long minDocCount = DataUtils.getNotNullValue(param, ESConfig.MIN_DOC_COUNT_KEY, Integer.class, 0).longValue();
        if (0L < minDocCount) {
            termsAgg.minDocCount(minDocCount);
        }

        Long shardMinDocCount = DataUtils.getNotNullValue(param, ESConfig.SHARD_MIN_DOC_COUNT_KEY, Integer.class, 0).longValue();
        if (0L < shardMinDocCount) {
            termsAgg.shardMinDocCount(shardMinDocCount);
        }

        termsAgg.missing(DataUtils.getNotNullValue(param, ESConfig.MISSING_KEY, String.class, ""));

        Object includeObj = param.get(ESConfig.INCLUDE_KEY);
        Object excludeObj = param.get(ESConfig.EXCLUDE_KEY);
        if (null != includeObj || null != excludeObj) {
            IncludeExclude includeExclude = null;
            if (includeObj instanceof String || excludeObj instanceof String) {
                includeExclude = new IncludeExclude(Optional.ofNullable(includeObj).orElse("").toString()
                        , Optional.ofNullable(includeObj).orElse("").toString());
            } else if (includeObj instanceof List || excludeObj instanceof List) {
                List includeList = (List) Optional.ofNullable(includeObj).orElse(new ArrayList<>());
                List excludeList = (List) Optional.ofNullable(excludeObj).orElse(new ArrayList<>());
                Object firstItem = null;
                if (!includeList.isEmpty()) {
                    firstItem = includeList.get(0);
                } else if (!excludeList.isEmpty()) {
                    firstItem = excludeList.get(0);
                }

                if (null != firstItem) {
                    if (firstItem instanceof String) {
                        String[] inArr = (String[]) includeList.parallelStream().toArray(String[]::new);
                        String[] exArr = (String[]) excludeList.parallelStream().toArray(String[]::new);
                        includeExclude = new IncludeExclude((includeList.isEmpty()) ? null : inArr, (excludeList.isEmpty()) ? null : exArr);
                    } else if (firstItem instanceof Double) {
                        double[] inArr = new double[includeList.size()];
                        double[] exArr = new double[excludeList.size()];
                        int inIndex = 0;
                        DataUtils.forEach(inIndex, includeList, (index, list) -> inArr[index] = (double) ((List) list).get(index));
                        int exIndex = 0;
                        DataUtils.forEach(exIndex, excludeList, (index, list) -> exArr[index] = (double) ((List) list).get(index));
                        includeExclude = new IncludeExclude((includeList.isEmpty()) ? null : inArr, (excludeList.isEmpty()) ? null : exArr);
                    } else if (firstItem instanceof Long) {
                        long[] inArr = new long[includeList.size()];
                        long[] exArr = new long[excludeList.size()];
                        int inIndex = 0;
                        DataUtils.forEach(inIndex, includeList, (index, list) -> inArr[index] = (long) ((List) list).get(index));
                        int exIndex = 0;
                        DataUtils.forEach(exIndex, excludeList, (index, list) -> exArr[index] = (long) ((List) list).get(index));
                        includeExclude = new IncludeExclude((includeList.isEmpty()) ? null : inArr, (excludeList.isEmpty()) ? null : exArr);
                    }
                }
            }

            if (null != includeExclude) {
                termsAgg.includeExclude(includeExclude);
            }
        }
        return termsAgg;
    }

    private BoolQueryBuilder buildBoolQuery(Map param) {

        BoolQueryBuilder query = QueryBuilders.boolQuery();
        param.keySet().stream().filter(ESConfig.BOOL_CONDITION_LIST::contains).forEach(x -> {
            String key = (String) x;
            List trgt = DataUtils.getNotNullValue(param, key, List.class, new ArrayList<>());
            switch (key) {
                case ESConfig.MUST_KEY:
                    trgt.stream().forEach(y -> query.must(buildCommonQuery().apply((Map) y)));
                    break;
                case ESConfig.SHOULD_KEY:
                    trgt.stream().forEach(y -> query.should(buildCommonQuery().apply((Map) y)));
                    break;
                case ESConfig.MUST_NOT_KEY:
                    trgt.stream().forEach(y -> query.mustNot(buildCommonQuery().apply((Map) y)));
                    break;
            }
        });
        return query;
    }

    private Function<Map, QueryBuilder> buildCommonQuery() {

        return (param) -> {
            QueryBuilder builder;
            Double boost = DataUtils.getNotNullValue(param, ESConfig.BOOST_KEY, Double.class, 1D);
            String type = DataUtils.getNotNullValue(param, ESConfig.TYPE_KEY, String.class, "");
            if (ESConfig.SIMPLE_CONDITION_LIST.contains(type)) {
                builder = buildSimpleQuery(param);
            } else if (ESConfig.RANGE_KEY.equalsIgnoreCase(type)) {
                builder = buildRangeQuery(param);
            } else if (ESConfig.MULTIMATCH_KEY.equalsIgnoreCase(type)) {
                builder = buildMultiMatchQuery(param);
            } else if (ESConfig.NESTED_KEY.equalsIgnoreCase(type)) {
                builder = buildNestedQuery(param);
            } else {
                builder = QueryBuilders.matchAllQuery();
            }

            builder.boost(boost.floatValue());

            return builder;
        };
    }

    private QueryBuilder buildNestedQuery(Map param) {
        String path = DataUtils.getNotNullValue(param, ESConfig.PATH_KEY, String.class, "");
        Map query = DataUtils.getNotNullValue(param, ESConfig.QUERY_KEY, Map.class, new HashMap<>());
        return QueryBuilders.nestedQuery(path, buildCommonQuery().apply(query), ScoreMode.Avg);
    }

    private QueryBuilder buildMultiMatchQuery(Map param) {
        Object value = DataUtils.getNotNullValue(param, ESConfig.VALUE_KEY, Object.class, new Object());
        Object fieldNames = DataUtils.getNotNullValue(param, ESConfig.FIELDNAMES_KEY, Object.class, new Object());
        Collection fieldNamesCollection = (fieldNames instanceof Collection) ? (Collection) fieldNames : Collections.singletonList(fieldNames);
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(value, new String[0]);
        fieldNamesCollection.parallelStream().forEach(x -> {
            String nameStr = (String) x;
            if (0 > nameStr.indexOf('^')) {
                multiMatchQueryBuilder.field(nameStr);
            } else {
                String[] arr = nameStr.split("\\^");
                multiMatchQueryBuilder.field(arr[0], Float.valueOf(arr[1]));
            }
        });
        return multiMatchQueryBuilder;
    }

    private QueryBuilder buildRangeQuery(Map param) {
        Object field = DataUtils.getNotNullValue(param, ESConfig.FIELD_KEY, Object.class, new Object());
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery(String.valueOf(field).trim());
        param.keySet().parallelStream().forEach(x -> {
            String key = (String) x;
            Object value = DataUtils.getNotNullValue(param, key, Object.class, new Object());
            switch (key) {
                case ESConfig.INCLUDE_LOWER_KEY:
                    queryBuilder.includeLower((Boolean) value);
                    break;
                case ESConfig.INCLUDE_UPPER_KEY:
                    queryBuilder.includeUpper((Boolean) value);
                    break;
                case ESConfig.FROM_KEY:
                    queryBuilder.from(value);
                    break;
                case ESConfig.LTE_KEY:
                    queryBuilder.lte(value);
                    break;
                case ESConfig.GTE_KEY:
                    queryBuilder.gte(value);
                    break;
                case ESConfig.LT_KEY:
                    queryBuilder.lt(value);
                    break;
                case ESConfig.GT_KEY:
                    queryBuilder.gt(value);
                    break;
                case ESConfig.TO_KEY:
                    queryBuilder.to(value);
                    break;
                default:
                    break;
            }
        });
        return queryBuilder;
    }

    private QueryBuilder buildSimpleQuery(Map param) {

        String type = DataUtils.getNotNullValue(param, ESConfig.TYPE_KEY, String.class, "");
        Object field = DataUtils.getNotNullValue(param, ESConfig.FIELD_KEY, Object.class, new Object());
        Object value = DataUtils.getNotNullValue(param, ESConfig.VALUE_KEY, Object.class, new Object());

        QueryBuilder queryBuilder;
        switch (type) {
            case ESConfig.EXISTS_KEY:
                queryBuilder = QueryBuilders.existsQuery(String.valueOf(field).trim());
                break;
            case ESConfig.MATCH_KEY:
                queryBuilder = QueryBuilders.matchQuery(String.valueOf(field).trim(), value)
                        .fuzzyTranspositions(false)
                        .lenient(true)
                        .maxExpansions(25)
                        .autoGenerateSynonymsPhraseQuery(false)
                ;
                break;
            case ESConfig.TERM_KEY:
                queryBuilder = QueryBuilders.termQuery(String.valueOf(field).trim(), value);
                break;
            case ESConfig.FUZZY_KEY:
                queryBuilder = QueryBuilders.fuzzyQuery(String.valueOf(field).trim(), value);
                break;
            case ESConfig.PREFIX_KEY:
                queryBuilder = QueryBuilders.prefixQuery(String.valueOf(field).trim(), String.valueOf(value).trim());
                break;
            case ESConfig.REGEXP_KEY:
                queryBuilder = QueryBuilders.regexpQuery(String.valueOf(field).trim(), String.valueOf(value).trim());
                break;
            case ESConfig.WRAPPER_KEY:
                queryBuilder = QueryBuilders.wrapperQuery(String.valueOf(value).trim());
                break;
            case ESConfig.WILDCARD_KEY:
                queryBuilder = QueryBuilders.wildcardQuery(String.valueOf(field).trim(), String.valueOf(value).trim());
                break;
            case ESConfig.COMMONTERMS_KEY:
                queryBuilder = QueryBuilders.commonTermsQuery(String.valueOf(field).trim(), value);
                break;
            case ESConfig.QUERY_STRING_KEY:
                queryBuilder = QueryBuilders.queryStringQuery(String.valueOf(value).trim());
                break;
            case ESConfig.MATCH_PHRASE_KEY:
                queryBuilder = QueryBuilders.matchPhraseQuery(String.valueOf(field).trim(), value);
                break;
            case ESConfig.MATCH_PHRASE_PREFIX_KEY:
                queryBuilder = QueryBuilders.matchPhrasePrefixQuery(String.valueOf(field).trim(), value);
                break;
            default:
                queryBuilder = QueryBuilders.termsQuery(String.valueOf(field).trim(), (Collection<?>) value);
                break;
        }
        return queryBuilder;
    }

    private SearchSourceBuilder makeBaseSearchBuilder(Map param) {
        Integer from = DataUtils.getNotNullValue(param, ESConfig.FROM_KEY, Integer.class, ESConfig.DEFAULT_ES_FROM);
        Integer size = DataUtils.getNotNullValue(param, ESConfig.SIZE_KEY, Integer.class, ESConfig.DEFAULT_ES_SIZE);
        if (from + size > ESConfig.DEFAULT_ES_MAX_SIZE) {
            log.error("Over size limit, please try scroll");
            size = ESConfig.DEFAULT_ES_MAX_SIZE - from;
        }

        return new SearchSourceBuilder().from(from).size(size);
    }

    private Map fullSearch(Map param, SearchSourceBuilder sourceBuilder) throws ConsumerException {
        String trgtIndex = DataUtils.getNotNullValue(param, ESConfig.INDEX_KEY, String.class, "");
        SearchRequest searchRequest = new SearchRequest().indices(trgtIndex).source(sourceBuilder);

        try {
            Long startTime = System.nanoTime();
            SearchResponse response = doQuery(param, searchRequest);
            log.debug("Got response as:[{}]", response);
            Long endTime = System.nanoTime();
            log.info("Finish query at:[{}] which took:[{}]", endTime, (endTime - startTime) / 1_000_000);
            return buildResult(response);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we try to query ES as:【{}】, {}" + searchRequest, e);
            throw new ConsumerException(ResultEnum.ES_QUERY);
        }
    }

    private SearchResponse doQuery(Map param, SearchRequest searchRequest) throws IOException  {
        String scrollId = DataUtils.getNotNullValue(param, ResultConfig.SCROLL_ID_KEY, String.class, "");
        String timeValue = DataUtils.getNotNullValue(param, ESConfig.SCROLL_TIME_VALUE_KEY, String.class, "");
        if (StringUtils.isNotBlank(timeValue)) {
            TimeValue tv = TimeValue.parseTimeValue(timeValue, ESConfig.SCROLL_TIME_VALUE_KEY);
            if (StringUtils.isNotBlank(scrollId)) {
                return restHighLevelClient.scroll(new SearchScrollRequest(scrollId).scroll(tv), COMMON_OPTIONS);
            } else {
                searchRequest.scroll(tv);
            }
        }

        String trgtIndex = DataUtils.getNotNullValue(param, ESConfig.INDEX_KEY, String.class, "");
        log.info("Try to query [{}] at:[{}] with request:[{}]", trgtIndex, System.currentTimeMillis(), searchRequest.source().toString());
        return restHighLevelClient.search(searchRequest, COMMON_OPTIONS);
    }

    private Map<String, Object> buildResult(SearchResponse response) {
        SearchHits hits = response.getHits();
        long total = hits.getTotalHits().value;
        log.debug("Got {} data in total", total);
        Map result = getBaseResult();
        result.put(ResultConfig.TOTAL_KEY, total);

        List dataList = Stream.of(hits.getHits()).map(x -> {
            Map<String, Object> sourceAsMap = x.getSourceAsMap();
            sourceAsMap.put(ESConfig.SCORE_KEY, x.getScore());
            Map<String, HighlightField> highlightFields = x.getHighlightFields();
            if (!highlightFields.isEmpty()) {
                Map highlight = new HashMap();
                highlightFields.forEach((k, v) -> highlight.put(k, v.fragments()[0].string()));
                sourceAsMap.put(ResultConfig.HIGHLIGH_KEY, highlight);
            }
            return sourceAsMap;
        }).collect(Collectors.toList());

        result.put(ResultConfig.DATA_KEY, dataList);
        log.debug("Build as {} data", dataList.size());

        Map<String, Object> aggMap = new HashMap<>();
        List<Aggregation> aggregations = Optional.ofNullable(response.getAggregations()).orElse(new Aggregations(Collections.emptyList())).asList();
        aggregations.parallelStream().forEach(aggregation -> getAggrInfo(aggMap, "", aggregation));
        log.debug("Build as {} aggregation data", aggMap.size());
        result.put(ResultConfig.AGGREGATION_KEY, aggMap);

        String scrollId = response.getScrollId();
        if (StringUtils.isNotBlank(scrollId)) {
            log.debug("Generated scroll id:[{}]", scrollId);
            result.put(ResultConfig.SCROLL_ID_KEY, scrollId);
        }

        return result;
    }

    private void getAggrInfo(Map<String, Object> aggMap, String parentName, Aggregation aggregation) {

        String key = (StringUtils.isNotBlank(parentName)) ? parentName + "." + aggregation.getName() : aggregation.getName();
        if (aggregation instanceof ParsedSingleBucketAggregation) {
            ParsedSingleBucketAggregation parsedNested = (ParsedSingleBucketAggregation) aggregation;
            aggMap.put(key + ".count", Long.toString(parsedNested.getDocCount()));
            List<Aggregation> aggregations = Optional.ofNullable(parsedNested.getAggregations()).orElse(new Aggregations(Collections.emptyList())).asList();
            aggregations.forEach(subAggregation -> getAggrInfo(aggMap, key, subAggregation));
        } else if (aggregation instanceof NumericMetricsAggregation.SingleValue) {
            NumericMetricsAggregation.SingleValue singleValue = (NumericMetricsAggregation.SingleValue) aggregation;
            aggMap.put(key + ".value", singleValue.getValueAsString());
        } else if (aggregation instanceof Stats) {
            Stats stats = (Stats) aggregation;
            Map data = new HashMap() {{
                put(ESConfig.MAX_KEY, stats.getMax());
                put(ESConfig.MIN_KEY, stats.getMin());
                put(ESConfig.AVG_KEY, stats.getAvg());
                put(ESConfig.COUNT_KEY, stats.getCount());
                put(ESConfig.SUM_KEY, stats.getSum());
            }};
            aggMap.put(key + ".value", data);
        } else if (aggregation instanceof MultiBucketsAggregation) {
            MultiBucketsAggregation multiBucketsAggregation = (MultiBucketsAggregation) aggregation;
            List bucketList = multiBucketsAggregation.getBuckets().parallelStream().map(x ->
                    new HashMap() {{
                        put("key", x.getKey());
                        put("count", x.getDocCount());
                    }}
            ).collect(Collectors.toList());
            aggMap.put(key + ".value", bucketList);
        }
    }

    public Integer bulkInsert(String index, String idKey, Map<String, ?> data) {

        String pk = String.valueOf(DataUtils.getNotNullValue(data, idKey, Object.class, "")).trim();
        IndexRequest indexRequest = new IndexRequest(index).source(data);
        if (StringUtils.isNotBlank(pk)) {
            indexRequest.id(pk);
        }

        bulkProcessor.add(indexRequest);
        return 1;
    }

    private Map getBaseResult() {
        return new HashMap() {{
            put(ResultConfig.TOTAL_KEY, 0);
            put(ResultConfig.DATA_KEY, new ArrayList<>());
            put(ResultConfig.AGGREGATION_KEY, new HashMap<>());
        }};
    }

    private BulkProcessor.Listener getBPListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info("Start to handle bulk commit executionId:[{}] for {} requests", executionId, request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                log.info("Finished handling bulk commit executionId:[{}] for {} requests", executionId, request.numberOfActions());

                if (response.hasFailures()) {
                    AtomicInteger count = new AtomicInteger();
                    response.spliterator().forEachRemaining(x -> {
                        if (x.isFailed()) {
                            BulkItemResponse.Failure failure = x.getFailure();
                            String msg = String.format(
                                    "Index:[%s], type:[%s], id:[%s], itemId:[%s], opt:[%s], version:[%s], errMsg:%s"
                                    , x.getIndex()
                                    , x.getType()
                                    , x.getId()
                                    , x.getItemId()
                                    , x.getOpType().getLowercase()
                                    , x.getVersion()
                                    , failure.getCause().getMessage()
                            );
                            log.error("Bulk executionId:[{}] has error messages:\t{}", executionId, msg);
                            count.incrementAndGet();
                        }
                    });
                    log.info("Finished handling bulk commit executionId:[{}] for {} requests with {} errors", executionId, request.numberOfActions(), count.intValue());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                failure.printStackTrace();
                Class clazz = failure.getClass();
                log.error("Bulk [{}] finished with [{}] requests of error:{}, {}, {}:-[{}]", executionId
                        , request.numberOfActions()
                        , clazz.getName()
                        , clazz.getSimpleName()
                        , clazz.getTypeName()
                        , clazz.getCanonicalName()
                        ,failure.getMessage());
                request.requests().stream().filter(x -> x instanceof IndexRequest)
                        .forEach(x -> {
                            Map source = ((IndexRequest) x).sourceAsMap();
                            String pk = DataUtils.getNotNullValue(source, "id", String.class, "");
                            log.error("Failure to handle index:[{}], type:[{}] id:[{}], data:[{}]", x.index(), x.type(), pk, JSON.toJSONString(source));
                        });

                if (failure instanceof IllegalStateException) {
                    synchronized (ESService.class) {
                        try {
                            initESClients();
                        } catch (ConsumerException e) {
                            e.printStackTrace();
                            log.error("Re init ES client failure");
                        }
                    }
                }
            }
        };
    }

    public Boolean isInited() { return this.isInited; }

    @PreDestroy
    private void closeESClient() throws ConsumerException {
        try {
            if (null != bulkProcessor) {
                boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
                if (terminated) {
                    if (null != restHighLevelClient) {
                        restHighLevelClient.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we try to close ES client" + e;
            log.error(errMsg);
            throw new ConsumerException(ResultEnum.ES_CLIENT_CLOSE);
        }
    }
}
