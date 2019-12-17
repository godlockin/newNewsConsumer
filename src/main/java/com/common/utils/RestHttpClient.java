package com.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter;
import com.common.constants.ResultEnum;
import com.exception.ConsumerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class RestHttpClient {

    private static RestTemplate restTemplate;
    private static RequestConfig requestConfig;
    private static CloseableHttpClient closeableHttpClient;

    @PostConstruct
    public void initClient() {
        requestConfig = RequestConfig.DEFAULT;

        restTemplate = new RestTemplate(getFactory());
        restTemplate.setMessageConverters(Arrays.asList(
                new StringHttpMessageConverter(StandardCharsets.UTF_8)
                , new ByteArrayHttpMessageConverter()
                , new ResourceHttpMessageConverter()
                , new FastJsonHttpMessageConverter()));
    }

    private HttpComponentsClientHttpRequestFactory getFactory() {
        // 长连接保持时长30秒
        PoolingHttpClientConnectionManager pollingConnectionManager = new PoolingHttpClientConnectionManager(30, TimeUnit.SECONDS);
        // 最大连接数
        pollingConnectionManager.setMaxTotal(2000);
        // 单路由的并发数
        pollingConnectionManager.setDefaultMaxPerRoute(500);

        closeableHttpClient = HttpClients.custom()
                .setConnectionManager(pollingConnectionManager)
                .setRetryHandler(new DefaultHttpRequestRetryHandler(2, true))  // 重试次数2次，并开启
                .setKeepAliveStrategy((response, context) -> 20 * 1000) // 保持长连接配置，需要在头添加Keep-Alive
                .build();

        // httpClient连接底层配置clientHttpRequestFactory
        HttpComponentsClientHttpRequestFactory clientHttpRequestFactory = new HttpComponentsClientHttpRequestFactory(closeableHttpClient);
        clientHttpRequestFactory.setReadTimeout(50000);
        clientHttpRequestFactory.setConnectTimeout(15000);
        return clientHttpRequestFactory;
    }

    public static String doPost(String url, Map param) throws ConsumerException {

//        HttpHeaders requestHeaders = new HttpHeaders();
//        requestHeaders.setContentType(MediaType.APPLICATION_JSON_UTF8);
//        HttpEntity<Map> httpEntity = new HttpEntity<>(param, requestHeaders);
//        return restTemplate.postForObject(url, httpEntity, String.class);
        PostHttp post = new PostHttp(url);
        StringEntity entity = new StringEntity(JSON.toJSONString(param), StandardCharsets.UTF_8);
        entity.setContentType(MediaType.APPLICATION_JSON_UTF8_VALUE);
        post.setEntity(entity);
        return doCall(post);
    }

    public static String doGet(String url) throws ConsumerException {
        return doCall(new GetHttp(url));
    }

    private static String doCall(HttpRequestBase httpRequestBase) throws ConsumerException {
        // 装载配置信息
        try (CloseableHttpResponse response = closeableHttpClient.execute(httpRequestBase)) {
            return  (200 == response.getStatusLine().getStatusCode()) ?
                    EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8) : "";
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened on calling:[{}], {}", httpRequestBase.getURI(), e);
            throw new ConsumerException(ResultEnum.FAILURE);
        }
    }

    private static class PostHttp extends HttpPost {
        PostHttp(String url) {
            super(url);
            super.setConfig(requestConfig);
        }
    }

    private static class GetHttp extends HttpGet {
        GetHttp(String url) {
            super(url);
            super.setConfig(requestConfig);
        }
    }
}
