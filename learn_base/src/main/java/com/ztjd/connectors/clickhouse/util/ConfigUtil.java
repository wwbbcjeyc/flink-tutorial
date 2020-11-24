package com.ztjd.connectors.clickhouse.util;

import java.util.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * @Author wangwenbo
 * @Date 2020/11/24 12:03 上午
 * @Version 1.0
 */
public class ConfigUtil {

    public static final String HOST_DELIMITER = ", ";

    private ConfigUtil(){

    }
    public static Properties toProperties(Config config) {
        Properties properties = new Properties();
        config.entrySet().forEach(e -> properties.put(e.getKey(), unwrapped(config.getValue(e.getKey()))));
        return properties;
    }

    public static Map<String, String> toMap(Config config) {
        Map<String, String> map = new HashMap<>();
        config.entrySet().forEach(e -> map.put(e.getKey(), unwrapped(e.getValue())));
        return map;
    }

    private static String unwrapped(ConfigValue configValue) {
        Object object = configValue.unwrapped();
        return object.toString();
    }

    static public String buildStringFromList(List<String> list) {
        return String.join(HOST_DELIMITER, list);
    }

    static public List<String> buildListFromString(String string) {
        return Arrays.asList(string.split(" "));
    }

}