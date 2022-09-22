package com.eastern.demo4;

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author chensheng13
 * @Description TODO
 * @Date 2022/9/22 10:12
 * @Version 1.0
 */
@Slf4j
public class ReceiveLogsDirect {
    private final static int PREFETCH_COUNT = 1;
    private final static String EXCHANGE_TYPE = "direct";
    private final static String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername("admin");
        factory.setPassword("Miss1314!");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
        String queueName = channel.queueDeclare().getQueue();

        if (args.length < 1) {
            log.error("Usage: ReceiveLogsDirect [info] [warning] [error]");
            return;
        }
        // 绑定了三次
        for (String severity : args) {
            channel.queueBind(queueName, EXCHANGE_NAME, severity);
        }
        log.info(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody());
            log.info(" [x] Received '{}': '{}'", delivery.getEnvelope().getRoutingKey(), message);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }
}
