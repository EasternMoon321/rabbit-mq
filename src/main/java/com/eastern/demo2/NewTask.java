package com.eastern.demo2;

/**
 * @Author chensheng13
 * @Description TODO
 * @Date 2022/9/21 10:14
 * @Version 1.0
 */

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

@Slf4j
public class NewTask {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername(PropertiesUtil.get("userName"));
        factory.setPassword(PropertiesUtil.get("password"));
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            String message = String.join(" ", argv);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            log.info(" [x] Sent '{}'", message);
        }
    }
}
