package com.eastern.demo7;

import com.eastern.utils.PropertiesUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BooleanSupplier;

@Slf4j
public class PublisherConfirms {

    static final int MESSAGE_COUNT = 50_000;

    static Connection createConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(PropertiesUtil.get("host"));
        factory.setUsername(PropertiesUtil.get("userName"));
        factory.setPassword(PropertiesUtil.get("password"));
        return factory.newConnection();
    }

    public static void main(String[] args) throws Exception {
//        publishMessagesIndividually();
//        publishMessagesInBatch();
        handlePublishConfirmsAsynchronously();
    }

    static void publishMessagesIndividually() throws Exception {
        try (Connection connection = createConnection()) {
            Channel channel = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            channel.queueDeclare(queue, false, false, true, null);
            channel.confirmSelect();
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; ++i) {
                String body = String.valueOf(i);
                channel.basicPublish("", queue, null, body.getBytes());
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            log.info("Publish {} message individually in {}", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    static void publishMessagesInBatch() throws Exception {
        try(Connection connection = createConnection()) {
            Channel channel = connection.createChannel();
            String queue = UUID.randomUUID().toString();

            channel.queueDeclare(queue, false, false, true, null);
            // Enables publisher acknowledgements on this channel.
            channel.confirmSelect();
            int batchSize = 100;
            int outstandingMessageCount = 0;
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; ++i) {
                String body = String.valueOf(i);
                channel.basicPublish("", queue, null, body.getBytes());
                outstandingMessageCount++;

                if (outstandingMessageCount == batchSize) {
                    channel.waitForConfirmsOrDie(5_000);
                    outstandingMessageCount = 0;
                }
            }

            if (outstandingMessageCount > 0) {
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            log.info("Publish {} message individually in {}", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    static void handlePublishConfirmsAsynchronously() throws Exception {
        try (Connection connection = createConnection()) {
            Channel channel = connection.createChannel();
            String queue = UUID.randomUUID().toString();

            channel.queueDeclare(queue, false, false, true, null);
            channel.confirmSelect();

            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

            ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
                if (multiple) {
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
                    confirmed.clear();
                } else {
                    outstandingConfirms.remove(sequenceNumber);
                }
            };

            channel.addConfirmListener(cleanOutstandingConfirms, new ConfirmCallback() {
                @Override
                public void handle(long sequenceNumber, boolean multiple) throws IOException {
                    String body = outstandingConfirms.get(sequenceNumber);
                    log.error("Message with body {} has been nack-ed. Sequence number: {}, multiple: {}",
                            body, sequenceNumber, multiple);
                    // 清除map中的消息
                    cleanOutstandingConfirms.handle(sequenceNumber, multiple);
                }
            });

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; ++i) {
                String body = String.valueOf(i);
                outstandingConfirms.put(channel.getNextPublishSeqNo(), body);
                channel.basicPublish("", queue, null, body.getBytes());

            }

            if (!waitUntil(Duration.ofSeconds(60), outstandingConfirms::isEmpty)) {
                throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
            }
            long end = System.nanoTime();
            log.info("Published {} messages and handled confirms asynchronously in {}", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());

        }
    }

    private static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited += 100;
        }
        return condition.getAsBoolean();
    }
}
