package com.eastern.demo6;

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.BasicProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeoutException;



/**
 * @Author chensheng13
 * @Description TODO
 * @Date 2022/9/22 14:01
 * @Version 1.0
 */
@Slf4j
public class RPCClient {
    private final static String QUEUE_NAME = "rpc_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername("admin");
        factory.setPassword("Miss1314!");
        try (
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
        ) {
            String callbackQueueName = channel.queueDeclare().getQueue();
            BasicProperties properties = new BasicProperties
                    .Builder()
                    .replyTo(callbackQueueName)
                    .build();

            String message = args == null ? "hello rpc" : String.join(" ", args);
            channel.basicPublish("", QUEUE_NAME, properties, message.getBytes());

            log.info(" [x] Sent '{}', callback '{}'", message, callbackQueueName);
        }

    }
}
