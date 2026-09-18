/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@CustomLog
public class MessageListenerExecutorTest extends SharedPulsarBaseTest {

    protected String methodName;

    @BeforeMethod(alwaysRun = true)
    public void setTestMethodName(Method m) {
        methodName = m.getName();
    }

    @Test
    public void testConsumerMessageListenerExecutorIsolation() throws Exception {
        log.info().attr("starting", methodName).log("-- Starting test");

        @Cleanup
        PulsarClient customClient = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .listenerThreads(1)
                .build();

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();
        List<CompletableFuture<Long>> maxConsumeDelayWithDisableIsolationFutures = new ArrayList<>();
        int loops = 5;
        long consumeSleepTimeMs = 10000;
        for (int i = 0; i < loops; i++) {
            // The first consumer will consume messages with sleep block 1s,
            // and the others will consume messages without sleep block.
            // The maxConsumeDelayWithDisableIsolation of all consumers
            // should be greater than sleepTimeMs cause by disable MessageListenerExecutor.
            CompletableFuture<Long> maxConsumeDelayFuture = startConsumeAndComputeMaxConsumeDelay(
                    customClient,
                    newTopicName(),
                    "my-sub-testConsumerMessageListenerDisableIsolation-" + i,
                    i == 0 ? Duration.ofMillis(consumeSleepTimeMs) : Duration.ofMillis(0),
                    false,
                    executor);
            maxConsumeDelayWithDisableIsolationFutures.add(maxConsumeDelayFuture);
        }

        // ensure all consumers consume messages delay more than consumeSleepTimeMs
        boolean allDelayMoreThanConsumeSleepTimeMs = maxConsumeDelayWithDisableIsolationFutures.stream()
                .map(CompletableFuture::join)
                .allMatch(delay -> delay > consumeSleepTimeMs);
        assertTrue(allDelayMoreThanConsumeSleepTimeMs);

        List<CompletableFuture<Long>> maxConsumeDelayWhitEnableIsolationFutures = new ArrayList<>();
        for (int i = 0; i < loops; i++) {
            // The first consumer will consume messages with sleep block 1s,
            // and the others will consume messages without sleep block.
            // The maxConsumeDelayWhitEnableIsolation of the first consumer
            // should be greater than sleepTimeMs, and the others should be
            // less than sleepTimeMs, cause by enable MessageListenerExecutor.
            CompletableFuture<Long> maxConsumeDelayFuture = startConsumeAndComputeMaxConsumeDelay(
                    customClient,
                    newTopicName(),
                    "my-sub-testConsumerMessageListenerEnableIsolation-" + i,
                    i == 0 ? Duration.ofMillis(consumeSleepTimeMs) : Duration.ofMillis(0),
                    true,
                    executor);
            maxConsumeDelayWhitEnableIsolationFutures.add(maxConsumeDelayFuture);
        }

        assertTrue(maxConsumeDelayWhitEnableIsolationFutures.get(0).join() > consumeSleepTimeMs);
        boolean remainingAlmostNoDelay = maxConsumeDelayWhitEnableIsolationFutures.stream()
                .skip(1)
                .map(CompletableFuture::join)
                .allMatch(delay -> delay < 1000);
        assertTrue(remainingAlmostNoDelay);

        log.info().attr("exiting", methodName).log("-- Exiting test");
    }

    @DataProvider
    public Object[][] rejectionTopics() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "rejectionTopics", timeOut = 30000)
    public void testListenerRecoversAfterExecutorRejection(boolean partitioned, boolean nonPersistent)
            throws Exception {
        String topic = newTopicName();
        if (nonPersistent) {
            topic = topic.replace("persistent://", "non-persistent://");
        }
        if (partitioned) {
            admin.topics().createPartitionedTopic(topic, 2);
        }
        CountDownLatch unblock = new CountDownLatch(1);
        ThreadPoolExecutor executor = saturatedExecutor(unblock);
        AtomicInteger rejections = new AtomicInteger();
        BlockingQueue<Integer> received = new LinkedBlockingQueue<>();
        try (Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic).subscriptionName("rejection").subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(3).poolMessages(true)
                .messageListenerExecutor((message, task) -> {
                    try {
                        executor.execute(task);
                    } catch (RejectedExecutionException e) {
                        rejections.incrementAndGet();
                        throw e;
                    }
                })
                .messageListener((c, message) -> {
                    try {
                        c.acknowledgeAsync(message);
                        received.add(message.getValue());
                    } finally {
                        message.release();
                    }
                }).subscribe();
             Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                     .topic(topic).enableBatching(false).create()) {
            // Fill one receiver window while a real bounded executor cannot accept any listener task.
            for (int i = 0; i < 3; i++) {
                producer.newMessage().key("same-key").value(i).send();
            }
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> rejections.get() >= 2);
            assertThat(received).isEmpty();
            unblock.countDown();
            // No new broker message, reconnect or explicit redelivery may be needed to resume delivery.
            for (int i = 0; i < 3; i++) {
                assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo(i);
            }
            // Verify that the recovered callbacks also return permits for further broker dispatch.
            for (int i = 3; i < 10; i++) {
                producer.newMessage().key("same-key").value(i).send();
                assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo(i);
            }
            assertThat(received).isEmpty();
        } finally {
            unblock.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(dataProvider = "rejectionTopics", timeOut = 30000)
    public void testCloseReleasesRejectedListenerMessage(boolean partitioned, boolean nonPersistent) throws Exception {
        String topic = newTopicName();
        if (nonPersistent) {
            topic = topic.replace("persistent://", "non-persistent://");
        }
        if (partitioned) {
            admin.topics().createPartitionedTopic(topic, 2);
        }
        CountDownLatch unblock = new CountDownLatch(1);
        ThreadPoolExecutor executor = saturatedExecutor(unblock);
        AtomicReference<ByteBuf> rejectedPayload = new AtomicReference<>();
        AtomicInteger rejections = new AtomicInteger();
        AtomicInteger delivered = new AtomicInteger();
        try (Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic).subscriptionName("close-rejection").receiverQueueSize(3).poolMessages(true)
                .messageListenerExecutor((message, task) -> {
                    try {
                        executor.execute(task);
                    } catch (RejectedExecutionException e) {
                        rejectedPayload.compareAndSet(null, payload(message));
                        rejections.incrementAndGet();
                        throw e;
                    }
                })
                .messageListener((c, message) -> {
                    delivered.incrementAndGet();
                    message.release();
                }).subscribe();
             Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                     .topic(topic).enableBatching(false).create()) {
            producer.newMessage().key("same-key").value(1).send();
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> rejections.get() >= 2);
            consumer.close();
            Awaitility.await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(rejectedPayload.get().refCnt()).isZero());
            int attemptsAtClose = rejections.get();
            unblock.countDown();
            Awaitility.await().during(Duration.ofMillis(1200)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                assertThat(delivered.get()).isZero();
                assertThat(rejections.get()).isEqualTo(attemptsAtClose);
            });
        } finally {
            unblock.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @DataProvider
    public Object[][] rejectionPartitions() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "rejectionPartitions", timeOut = 30000)
    public void testSeekDiscardsRejectedListenerMessage(boolean partitioned) throws Exception {
        String topic = newTopicName();
        if (partitioned) {
            admin.topics().createPartitionedTopic(topic, 2);
        }
        CountDownLatch unblock = new CountDownLatch(1);
        ThreadPoolExecutor executor = saturatedExecutor(unblock);
        AtomicReference<ByteBuf> rejectedPayload = new AtomicReference<>();
        AtomicInteger rejections = new AtomicInteger();
        BlockingQueue<Integer> received = new LinkedBlockingQueue<>();
        try (Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic).subscriptionName("seek-rejection").receiverQueueSize(3).poolMessages(true)
                .messageListenerExecutor((message, task) -> {
                    try {
                        executor.execute(task);
                    } catch (RejectedExecutionException e) {
                        rejectedPayload.compareAndSet(null, payload(message));
                        rejections.incrementAndGet();
                        throw e;
                    }
                })
                .messageListener((c, message) -> {
                    try {
                        c.acknowledgeAsync(message);
                        received.add(message.getValue());
                    } finally {
                        message.release();
                    }
                }).subscribe();
             Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                     .topic(topic).enableBatching(false).create()) {
            producer.newMessage().key("same-key").value(1).send();
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> rejections.get() >= 2);
            consumer.seek(MessageId.latest);
            Awaitility.await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(rejectedPayload.get().refCnt()).isZero());
            unblock.countDown();
            producer.newMessage().key("same-key").value(2).send();
            assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo(2);
            assertThat(received).isEmpty();
        } finally {
            unblock.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @DataProvider
    public Object[][] rejectionAttempts() {
        return new Object[][] {{1}, {2}};
    }

    @Test(dataProvider = "rejectionAttempts", timeOut = 30000)
    public void testCloseDuringListenerRejection(int rejectionAttempt) throws Exception {
        String topic = newTopicName();
        CountDownLatch unblock = new CountDownLatch(1);
        ThreadPoolExecutor executor = saturatedExecutor(unblock);
        CountDownLatch rejecting = new CountDownLatch(1);
        CountDownLatch finishRejection = new CountDownLatch(1);
        AtomicReference<ByteBuf> rejectedPayload = new AtomicReference<>();
        AtomicInteger rejections = new AtomicInteger();
        AtomicInteger delivered = new AtomicInteger();
        try (Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic).subscriptionName("close-in-flight-rejection").poolMessages(true)
                .messageListenerExecutor((message, task) -> {
                    try {
                        executor.execute(task);
                    } catch (RejectedExecutionException e) {
                        rejectedPayload.compareAndSet(null, payload(message));
                        if (rejections.incrementAndGet() == rejectionAttempt) {
                            // Coordinate close with a real rejection, before it reaches ConsumerBase.
                            rejecting.countDown();
                            try {
                                if (!finishRejection.await(10, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("Close did not finish", e);
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(interrupted);
                            }
                        }
                        throw e;
                    }
                })
                .messageListener((c, message) -> {
                    delivered.incrementAndGet();
                    message.release();
                }).subscribe();
             Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                     .topic(topic).enableBatching(false).create()) {
            producer.send(1);
            assertThat(rejecting.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> closed = consumer.closeAsync();
            assertThat(rejectedPayload.get().refCnt()).isPositive();
            finishRejection.countDown();
            closed.get(5, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(rejectedPayload.get().refCnt()).isZero());
            assertThat(delivered.get()).isZero();
        } finally {
            finishRejection.countDown();
            unblock.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(timeOut = 30000)
    public void testNonDurableReconnectRetainsRejectedListenerPosition() throws Exception {
        String topic = newTopicName();
        CountDownLatch unblock = new CountDownLatch(1);
        ThreadPoolExecutor executor = saturatedExecutor(unblock);
        AtomicInteger rejections = new AtomicInteger();
        BlockingQueue<Integer> received = new LinkedBlockingQueue<>();
        try (Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic).subscriptionName("reconnect-rejection")
                .subscriptionMode(SubscriptionMode.NonDurable).receiverQueueSize(3).poolMessages(true)
                .messageListenerExecutor((message, task) -> {
                    try {
                        executor.execute(task);
                    } catch (RejectedExecutionException e) {
                        rejections.incrementAndGet();
                        throw e;
                    }
                })
                .messageListener((c, message) -> {
                    try {
                        received.add(message.getValue());
                        c.acknowledgeAsync(message);
                    } finally {
                        message.release();
                    }
                }).subscribe();
             Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                     .topic(topic).enableBatching(false).create()) {
            for (int i = 0; i < 3; i++) {
                producer.send(i);
            }
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> rejections.get() >= 2);
            ConsumerImpl<Integer> singleConsumer = (ConsumerImpl<Integer>) consumer;
            ClientCnx oldConnection = singleConsumer.getClientCnx();
            oldConnection.close();
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> singleConsumer.isConnected()
                    && singleConsumer.getClientCnx() != oldConnection);
            unblock.countDown();
            for (int i = 0; i < 3; i++) {
                assertThat(received.poll(5, TimeUnit.SECONDS)).isEqualTo(i);
            }
        } finally {
            unblock.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static ByteBuf payload(Message<?> message) {
        Message<?> inner = message instanceof TopicMessageImpl<?> topicMessage ? topicMessage.getMessage() : message;
        return ((MessageImpl<?>) inner).getDataBuffer();
    }

    private static ThreadPoolExecutor saturatedExecutor(CountDownLatch unblock) throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1), new ExecutorProvider.ExtendedThreadFactory("rejected-listener", true));
        CountDownLatch started = new CountDownLatch(1);
        executor.execute(() -> {
            started.countDown();
            try {
                unblock.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        executor.execute(() -> { });
        return executor;
    }

    private CompletableFuture<Long> startConsumeAndComputeMaxConsumeDelay(PulsarClient theClient, String topic,
                                                                         String subscriptionName,
                                                                         Duration consumeSleepTime,
                                                                         boolean enableMessageListenerExecutorIsolation,
                                                                         ExecutorService executorService)
            throws Exception {
        int numMessages = 2;
        final CountDownLatch latch = new CountDownLatch(numMessages);
        int numPartitions = 50;
        TopicName nonIsolationTopicName = TopicName.get(topic);
        admin.topics().createPartitionedTopic(nonIsolationTopicName.toString(), numPartitions);

        AtomicLong maxConsumeDelay = new AtomicLong(-1);
        ConsumerBuilder<Long> consumerBuilder =
                theClient.newConsumer(Schema.INT64)
                        .topic(nonIsolationTopicName.toString())
                        .subscriptionName(subscriptionName)
                        .messageListener((c1, msg) -> {
                            Assert.assertNotNull(msg, "Message cannot be null");
                            log.debug().attr("value", msg.getValue()).log("Received message [] in the listener");
                            c1.acknowledgeAsync(msg);
                            maxConsumeDelay.set(Math.max(maxConsumeDelay.get(),
                                    System.currentTimeMillis() - msg.getValue()));
                            if (consumeSleepTime.toMillis() > 0) {
                                Uninterruptibles.sleepUninterruptibly(consumeSleepTime);
                            }
                            latch.countDown();
                        });

        ExecutorService executor = Executors.newSingleThreadExecutor(
                new ExecutorProvider.ExtendedThreadFactory(subscriptionName + "listener-executor-", true));
        if (enableMessageListenerExecutorIsolation) {
            consumerBuilder.messageListenerExecutor((message, runnable) -> executor.execute(runnable));
        }

        Consumer<Long> consumer = consumerBuilder.subscribe();
        ProducerBuilder<Long> producerBuilder = theClient.newProducer(Schema.INT64)
                .topic(nonIsolationTopicName.toString());

        Producer<Long> producer = producerBuilder.create();
        List<Future<MessageId>> futures = new ArrayList<>();

        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            Future<MessageId> future = producer.sendAsync(System.currentTimeMillis());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }

        CompletableFuture<Long> maxDelayFuture = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, executorService).whenCompleteAsync((v, ex) -> {
            maxDelayFuture.complete(maxConsumeDelay.get());
            try {
                producer.close();
                consumer.close();
                executor.shutdownNow();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });

        return maxDelayFuture;
    }
}
