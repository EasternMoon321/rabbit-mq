package com.eastern.utils;

import ch.qos.logback.core.util.StringCollectionUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author chensheng13
 * @Description TODO
 * @Date 2022/9/21 22:09
 * @Version 1.0
 */
@Slf4j
public class PropertiesUtil {

    private static final Properties properties = new Properties();

    static {
        String classPath = PropertiesUtil.class.getResource("/").getPath();
        try {
            properties.load(new FileInputStream(classPath + "/rabbitmq.properties"));
        } catch (IOException e) {
            log.error("load rabbitmq.properties failed: ", e);
        }
    }

    public static String get(String key) {
        String value = properties.getProperty(key);
        if (value == null || "".equals(value)) {
            throw new RuntimeException("can`t find the key value");
        }
        return value;
    }

    public static void main(String[] args) {
        String ip = PropertiesUtil.get("ip");
        System.out.println(ip);
    }

}
