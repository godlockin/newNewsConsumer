package com.common.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class URLUtil {
    private final static Set<String> PublicSuffixSet = new HashSet<>(
            Arrays.asList("com|org|net|gov|edu|co|tv|mobi|info|asia|xxx|onion|cn|com.cn|edu.cn|gov.cn|net.cn|org.cn|jp|kr|tw|com.hk|hk|com.hk|org.hk|se|com.se|org.se".split("\\|")));

    private static Pattern IP_PATTERN = Pattern.compile("(\\d{1,3}\\.){3}(\\d{1,3})");

    /**
     * 获取url的顶级域名
     *
     * @param url
     * @return
     */
    public static String getDomainName(String url) throws MalformedURLException {

        String host = new URL(url).getHost();
        if (host.endsWith("."))
            host = host.substring(0, host.length() - 1);
        if (IP_PATTERN.matcher(host).matches())
            return host;

        int index = 0;
        String candidate = host;
        for (; index >= 0; ) {
            index = candidate.indexOf('.');
            String subCandidate = candidate.substring(index + 1);
            if (PublicSuffixSet.contains(subCandidate)) {
                return candidate;
            }
            candidate = subCandidate;
        }
        return candidate;
    }
}
