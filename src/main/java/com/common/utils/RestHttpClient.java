package com.common.utils;

import com.alibaba.fastjson.JSON;
import com.common.constants.ResultEnum;
import com.exception.ConsumerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
@Component
public class RestHttpClient {

    private static RequestConfig requestConfig;
    private static CloseableHttpClient closeableHttpClient;

    @PostConstruct
    void initClient() {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(200);
        cm.setDefaultMaxPerRoute(50);
        closeableHttpClient = HttpClients.custom().setConnectionManager(cm).build();

        requestConfig = RequestConfig.DEFAULT;
    }

    public static String doPost(String url, Map param) throws ConsumerException {

        PostHttp post = new PostHttp(url);
        StringEntity requestEntity = new StringEntity(JSON.toJSONString(param), StandardCharsets.UTF_8);
        requestEntity.setContentEncoding("UTF-8");
        post.setEntity(requestEntity);
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
