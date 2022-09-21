package com.eastern.demo1;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * @Author chensheng13
 * @Description TODO
 * @Date 2022/9/21 9:44
 * @Version 1.0
 */
@Slf4j
public class Recv {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("43.138.222.27");
        factory.setUsername("admin");
        factory.setPassword("Miss1314!");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /**
         * 声明了队列：消费前，确保队列存在（Consumer可能先于Producer）
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        /**
         * DeliverCallback: 缓冲服务器发布的数据
         */
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            log.info(" [x] Received '{}'", message);
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    }
}
