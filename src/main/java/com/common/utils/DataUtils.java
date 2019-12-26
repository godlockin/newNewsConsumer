package com.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

@Slf4j
public class DataUtils {

    public static String cleanHttpString(String base) {
        return StringUtils.isBlank(base) ? "" : base
                .replaceAll("<[.[^>]]*>", "")
                .replaceAll("[\\s\\p{Zs}]+", "")
                .replaceAll("\\s*|\t|\r|\n", "")
                .replaceAll("\n|\r\n|\\n|\\t", "")
                .replaceAll("&nbsp", "")
                .trim();
    }

    public static <T> T getNotNullValue(Map base, String key, Class<T> clazz, Object defaultValue) {
        return handleNullValue(base.get(key), clazz, defaultValue);
    }

    public static <T> T handleNullValue(Object base, Class<T> clazz, Object defaultValue) {
        return clazz.cast(Optional.ofNullable(base).orElse(defaultValue));
    }

    public static <E> void forEach(Integer maxIndex, Iterable<? extends E> elements, BiConsumer<Integer, ? super E> action) {
        Objects.requireNonNull(elements);
        Objects.requireNonNull(action);
        int index = 0;
        for (E element : elements) {
            action.accept(index++, element);
            if (maxIndex > 0 && maxIndex < index) {
                break;
            }
        }
    }
}
