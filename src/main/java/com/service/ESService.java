package com.service;

import com.alibaba.fastjson.JSON;
import com.common.LocalConfig;
import com.common.constants.BusinessConstants.ESConfig;
import com.common.constants.ResultEnum;
import com.common.utils.DataUtils;
import com.exception.ConsumerException;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.*;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@EnableScheduling
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class ESService {

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

    public Map<String, Long> statement() {
        return new HashMap() {{
            put("submitted", submitCount.longValue());
            put("failure", failureCount.longValue());
        }};
    }

    public void reIndex(String fromIndex, String toIndex, String fromTime, String toTime) {
        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices(fromIndex);
        request.setDestIndex(toIndex);
        request.setSourceQuery(new RangeQueryBuilder("publishDate").from(fromTime).to(toTime));
        request.setSourceBatchSize(1000);
        request.setDestPipeline("new_news_yearly_pipeline");
        restHighLevelClient.reindexAsync(request, COMMON_OPTIONS, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                log.info("Reindex took {} for {} batches of {} data, {} failures, {} created {} updated"
                        , bulkByScrollResponse.getTook().getMinutes()
                        , bulkByScrollResponse.getBatches()
                        , bulkByScrollResponse.getTotal()
                        , bulkByScrollResponse.getBulkFailures().size()
                        , bulkByScrollResponse.getCreated()
                        , bulkByScrollResponse.getUpdated()
                        );
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
                log.error("Error happened during we do reindex from [{}] to [{}] range [{} - {}], {}"
                        , fromIndex
                        , toIndex
                        , fromTime
                        , toIndex
                        , e
                        );
            }
        });
    }

    public boolean doCheckAliases(String indexName, String indexAliases) throws ConsumerException {

        try {
            GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
            boolean isIndexExists = restHighLevelClient.indices().exists(getIndexRequest, COMMON_OPTIONS);
            if (isIndexExists) {
                GetAliasesRequest requestWithAlias = new GetAliasesRequest(indexAliases);
                boolean isAliasExists = restHighLevelClient.indices().existsAlias(requestWithAlias, COMMON_OPTIONS);
                if (isAliasExists) {
                    IndicesAliasesRequest indicesAliasesRequest =
                            new IndicesAliasesRequest()
                                    .addAliasAction(
                                            new AliasActions(AliasActions.Type.REMOVE)
                                                .index(indexName).alias(indexAliases));
                    AcknowledgedResponse indicesAliasesResponse = restHighLevelClient.indices().updateAliases(indicesAliasesRequest, COMMON_OPTIONS);

                    return indicesAliasesResponse.isAcknowledged();
                }
            }
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = String.format("Error happened during we remove alias [%s] from [%s], %s", indexAliases, indexName, e);
            log.error(errMsg);
            throw new ConsumerException(errMsg);
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

    @Scheduled(cron = "0 30 2 * * ?")
    public void refreshESClient() throws ConsumerException {
        initESClient();
    }

    @Synchronized
    @PostConstruct
    public void initESClient() throws ConsumerException {
        log.info("Init ES client");
        closeESClient();
        initStaticVariables();

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
                                this.initESClient();
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

    private BulkProcessor.Listener getBPListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info("Start to handle bulk commit executionId:[{}] for {} requests", executionId, request.numberOfActions());
                submitCount.addAndGet(request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                log.info("Finished handling bulk commit executionId:[{}] for {} requests", executionId, request.numberOfActions());

                if (response.hasFailures()) {
                    AtomicInteger count = new AtomicInteger();
                    response.spliterator().forEachRemaining(x -> {
                        if (x.isFailed()) {
                            failureCount.incrementAndGet();
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
                            failureCount.incrementAndGet();
                            Map source = ((IndexRequest) x).sourceAsMap();
                            String pk = String.valueOf(source.getOrDefault("id", ""));
                            log.error("Failure to handle index:[{}], type:[{}] id:[{}], data:[{}]", x.index(), x.type(), pk, JSON.toJSONString(source));
                        });

                if (failure instanceof IllegalStateException) {
                    synchronized (ESService.class) {
                        try {
                            initESClient();
                        } catch (ConsumerException e) {
                            e.printStackTrace();
                            log.error("Re init ES client failure");
                        }
                    }
                }
            }
        };
    }

    private void initStaticVariables() {
        ES_ADDRESSES = LocalConfig.get(ESConfig.ES_ADDRESSES_KEY, String.class, ESConfig.DEFAULT_ES_ADDRESSES);
        ES_BULK_SIZE = LocalConfig.get(ESConfig.ES_BULK_SIZE_KEY, Integer.class, ESConfig.DEFAULT_ES_BULK_SIZE);
        ES_BULK_FLUSH = LocalConfig.get(ESConfig.ES_BULK_FLUSH_KEY, Integer.class, ESConfig.DEFAULT_ES_BULK_FLUSH);
        ES_SOCKET_TIMEOUT = LocalConfig.get(ESConfig.ES_SOCKET_TIMEOUT_KEY, Integer.class, ESConfig.DEFAULT_ES_SOCKET_TIMEOUT);
        ES_BULK_CONCURRENT = LocalConfig.get(ESConfig.ES_BULK_CONCURRENT_KEY, Integer.class, ESConfig.DEFAULT_ES_BULK_CONCURRENT);
        ES_CONNECT_TIMEOUT = LocalConfig.get(ESConfig.ES_CONNECT_TIMEOUT_KEY, Integer.class, ESConfig.DEFAULT_ES_CONNECT_TIMEOUT);
        ES_CONNECTION_REQUEST_TIMEOUT = LocalConfig.get(ESConfig.ES_CONNECTION_REQUEST_TIMEOUT_KEY, Integer.class, ESConfig.DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT);
    }

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
