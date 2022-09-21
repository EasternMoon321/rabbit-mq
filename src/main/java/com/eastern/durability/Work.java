package com.eastern.durability;

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * @Author chensheng13
 * @Description TODO
 * @Date 2022/9/21 10:16
 * @Version 1.0
 */
@Slf4j
public class Work {

    private final static String QUEUE_NAME = "task_queue";
    private final static boolean DURABLE = true;

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername(PropertiesUtil.get("userName"));
        factory.setPassword(PropertiesUtil.get("password"));
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /**
         * 声明了队列：消费前，确保队列存在（Consumer可能先于Producer）
         */
        channel.queueDeclare(QUEUE_NAME, DURABLE, false, false, null);
        log.info(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(1);  // accept only one unack-ed message at a time (see below)
        /**
         * DeliverCallback: 缓冲服务器发布的数据
         */
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            log.info(" [x] Received '{}'", message);
            try {
                doWork(message);
            } catch (InterruptedException e) {
                log.error("[x] error", e);
            } finally {
                log.info(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//                throw new RuntimeException("");
            }
        };
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                Thread.sleep(1000);
            }
        }
    }
}
