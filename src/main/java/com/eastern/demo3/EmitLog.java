package com.eastern.demo3;

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@Slf4j
public class EmitLog {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername(PropertiesUtil.get("userName"));
        factory.setPassword(PropertiesUtil.get("password"));

        try (
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                ){
            String message = args.length < 1 ? "info: Hello World!" :
                    String.join(" ", args);
            // 声明交换机,routingKey在fanout模式下被忽略
            channel.exchangeDeclare("logs", "fanout");

           /* // 创建队列：non-durable, exclusive, autodelete queue with a generated name
            String queueName = channel.queueDeclare().getQueue();
            // 绑定交换机和队列
            channel.queueBind(queueName, "logs", "");*/

            // 发送消息到交换机
            /**
             * The messages will be lost if no queue is bound to the exchange
             */
            channel.basicPublish("logs", "", null, message.getBytes());
            log.info(" [x] Sent '{}'", message);

        }

    }
}
