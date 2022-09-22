package com.eastern.demo6;

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.BasicProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * @Author chensheng13
 * @Description TODO
 * @Date 2022/9/22 14:01
 * @Version 1.0
 */
@Slf4j
public class RPCClient implements AutoCloseable {
    private final String QUEUE_NAME = "rpc_queue";
    private final Connection connection;
    private final Channel channel;

    public  RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername("admin");
        factory.setPassword("Miss1314!");
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] args) throws IOException, TimeoutException {

        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 32; ++i) {
                String i_str = Integer.toString(i);
                log.info(" [x] Requesting fib({})", i_str);
                String response = fibonacciRpc.call(i_str);
                log.info(" [.] Got '{}'", response);
            }
        } catch (Exception e) {
            log.error("create RPCClient failed", e);
        }

    }

    private String call(String message) throws IOException, ExecutionException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();
        // 声明回调队列
        String replyQueueName  = channel.queueDeclare().getQueue();
        BasicProperties props = new BasicProperties()
                .builder()
                // 绑定消息关联的唯一id
                .correlationId(corrId)
                // 绑定回调队列到属性
                .replyTo(replyQueueName)
                .build();
        // 发送消息到队列
        channel.basicPublish("", QUEUE_NAME, props, message.getBytes());

        // 声明回调任务线程
        final CompletableFuture<String> response = new CompletableFuture<>();

        // 消息回调任务
        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                // 完成回调任务，唤醒回调任务线程
                response.complete(new String(delivery.getBody()));
            } else {
                log.error("unknwon corrid :{}, corrent corrid :{}", delivery.getProperties().getCorrelationId(), corrId);
            }
        } , consumerTag -> {
        });

        // 阻塞，等待回到任务完成
        String result = response.get();
        // 摧毁回调任务消费者（关联的队列[默认队列]也被消费）
        channel.basicCancel(ctag);
        return result;
    }

    @Override
    public void close() throws Exception {
        connection.close();

    }
}
