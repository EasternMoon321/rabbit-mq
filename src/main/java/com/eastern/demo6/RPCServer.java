package com.eastern.demo6;

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.*;
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
public class RPCServer {

    private final static String QUEUE_NAME = "rpc_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername("admin");
        factory.setPassword("Miss1314!");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        // 清除队列上消息
//        channel.queuePurge(QUEUE_NAME);
        channel.basicQos(1);
        log.info(" [x] Awaiting RPC requests");

        // 创建消费任务
        DeliverCallback deliverCallback = (consumerTag, delivery)-> {
            // 创建回调任务属性
            AMQP.BasicProperties replyProps  = new AMQP.BasicProperties()
                    .builder()
                    // 获取队列绑定的关联id
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();
            String response = "";
            try {
                String message = new String(delivery.getBody());
                int n = Integer.parseInt(message);
                log.info(" [.] fib({})", message);
                response += fib(n);
            } catch (RuntimeException e) {
                log.error(" [.] ", e);
            } finally {
                // 发送消息到回调任务队列
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes());
                // 应答本次消费
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };

        // 关联消费者和投递消费任务
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});

    }

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n-1) + fib(n-2);
    }

}
