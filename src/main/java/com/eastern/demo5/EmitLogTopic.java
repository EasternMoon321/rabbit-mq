package com.eastern.demo5;

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author chensheng13
 * @Description TODO direct：路由基于一个标准（严重性）
 * @Date 2022/9/22 10:12
 * @Version 1.0
 */
@Slf4j
public class EmitLogTopic {
    private final static String EXCHANGE_TYPE = "topic";
    private final static String EXCHANGE_NAME = "topic_logs";


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername("admin");
        factory.setPassword("Miss1314!");
        try (
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                ) {
            // 声明交换机
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
            String routingKey = getRoutingKey(args);
            String message = getMessage(args);

            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
            log.info(" [x] Sent '{}':'{}'", routingKey, message);
        }

    }

    private static String getRoutingKey(String[] args) {
        if (args.length < 1) {
            return "anonymous.info";
        }
        return args[0];
    }

    private static String getMessage(String[] args) {
        if (args.length < 2) {
            return "Hello World!";
        }
        return joinStrings(args, " ", 1);
    }

    private static String joinStrings(String[] args, String delimiter, int startIndex) {
        int length = args.length;
        if (length == 0) {
            return "";
        }
        if (length <= startIndex) {
            return "";
        }
        StringBuilder words = new StringBuilder(args[startIndex]);
        for (int i = startIndex + 1; i < length; ++i) {
            words.append(delimiter).append(args[i]);
        }
        return words.toString();
    }

}
