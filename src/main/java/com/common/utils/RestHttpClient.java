package com.common.utils;

import com.common.constants.ResultEnum;
import com.exception.ConsumerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;

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

    public static String doGet(String url) throws ConsumerException {
        // 装载配置信息
        try (CloseableHttpResponse response = closeableHttpClient.execute(new GetHttp(url))) {
            return  (200 == response.getStatusLine().getStatusCode()) ?
                    EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8) : "";
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened on calling:[{}], {}", url, e);
            throw new ConsumerException(ResultEnum.FAILURE);
        }
    }

    private static class GetHttp extends HttpGet {
        GetHttp(String url) {
            super(url);
            super.setConfig(requestConfig);
        }
    }
}
