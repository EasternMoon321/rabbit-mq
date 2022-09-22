package com.eastern.demo3;

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ReceiveLogs {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername(PropertiesUtil.get("userName"));
        factory.setPassword(PropertiesUtil.get("password"));

        // 不能释放资源，否则程序直接结束（）
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明交换机,routingKey在fanout模式下被忽略
        channel.exchangeDeclare("logs", "fanout");

       /// 创建队列：non-durable, exclusive, autodelete queue with a generated name
        String queueName = channel.queueDeclare().getQueue();
        // 绑定交换机和队列
        channel.queueBind(queueName, "logs", "");

        // 声明接收回调
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody());
            log.info(" [x] Received '{}'", message);
        };

        // 绑定队列和回调
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }
}
