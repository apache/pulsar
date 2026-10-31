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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
@CustomLog
public class ZeroQueueSizeTest extends SharedPulsarBaseTest {
    private final int totalMessages = 10;

    @Test
    public void validQueueSizeConfig() {
        pulsarClient.newConsumer().receiverQueueSize(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidQueueSizeConfig() {
        pulsarClient.newConsumer().receiverQueueSize(-1);
    }

    @Test(expectedExceptions = PulsarClientException.InvalidConfigurationException.class)
    public void zeroQueueSizeReceiveAsyncInCompatibility() throws PulsarClientException {
        final String topicName = newTopicName();
        final String subscriptionName = "my-ex-subscription";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .receiverQueueSize(0).subscribe();
        consumer.receive(10, TimeUnit.SECONDS);
    }

    @Test(expectedExceptions = PulsarClientException.class)
    public void zeroQueueSizePartitionedTopicInCompatibility() throws PulsarClientException, PulsarAdminException {
        final String topicName = newTopicName();
        final String subscriptionName = "my-ex-subscription";
        int numberOfPartitions = 3;
        admin.topics().createPartitionedTopic(topicName, numberOfPartitions);
        pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).receiverQueueSize(0).subscribe();
    }

    @Test
    public void zeroQueueSizeNormalConsumer() throws PulsarClientException {
        final String topicName = newTopicName();
        final String subscriptionName = "my-ex-subscription";
        final String messagePredicate = "my-message-";

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(0).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message
        Message<byte[]> message;
        for (int i = 0; i < totalMessages; i++) {
            assertEquals(consumer.numMessagesInQueue(), 0);
            message = consumer.receive();
            assertEquals(new String(message.getData()), messagePredicate + i);
            assertEquals(consumer.numMessagesInQueue(), 0);
            log.info("Consumer received : " + new String(message.getData()));
        }
    }

    @Test
    public void zeroQueueSizeConsumerListener() throws Exception {
        final String topicName = newTopicName();
        final String subscriptionName = "my-ex-subscription";
        final String messagePredicate = "my-message-";

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        List<Message<byte[]>> messages = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(totalMessages);
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(0).messageListener((cons, msg) -> {
                    assertEquals(((ConsumerImpl) cons).numMessagesInQueue(), 0);
                    synchronized (messages) {
                        messages.add(msg);
                    }
                    log.info("Consumer received: " + new String(msg.getData()));
                    latch.countDown();
                }).subscribe();

        // 3. producer publish messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            log.info("Producer produced: " + message);
            producer.send(message.getBytes());
        }

        // 4. Receiver receives the message
        latch.await();
        assertEquals(consumer.numMessagesInQueue(), 0);
        assertEquals(messages.size(), totalMessages);
        for (int i = 0; i < messages.size(); i++) {
            assertEquals(new String(messages.get(i).getData()), messagePredicate + i);
        }
    }

    @Test
    public void zeroQueueSizeSharedSubscription() throws PulsarClientException {
        final String topicName = newTopicName();
        final String subscriptionName = "my-ex-subscription";
        final String messagePredicate = "my-message-";

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        int numOfSubscribers = 4;
        ConsumerImpl<?>[] consumers = new ConsumerImpl[numOfSubscribers];
        for (int i = 0; i < numOfSubscribers; i++) {
            consumers[i] = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName(subscriptionName).receiverQueueSize(0).subscriptionType(SubscriptionType.Shared)
                    .subscribe();
        }

        // 4. Produce Messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        // 5. Consume messages
        Message<?> message;
        for (int i = 0; i < totalMessages; i++) {
            assertEquals(consumers[i % numOfSubscribers].numMessagesInQueue(), 0);
            message = consumers[i % numOfSubscribers].receive();
            assertEquals(new String(message.getData()), messagePredicate + i);
            assertEquals(consumers[i % numOfSubscribers].numMessagesInQueue(), 0);
            log.info("Consumer received : " + new String(message.getData()));
        }
    }

    @Test
    public void zeroQueueSizeFailoverSubscription() throws PulsarClientException {
        final String topicName = newTopicName();
        final String subscriptionName = "my-ex-subscription";
        final String messagePredicate = "my-message-";

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // 3. Create Consumer
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(0).subscriptionType(SubscriptionType.Failover)
                .consumerName("consumer-1").subscribe();


        // 4. Produce Messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            producer.send(message.getBytes());
        }

        // 5. Consume messages
        Message<byte[]> message;
        for (int i = 0; i < totalMessages / 2; i++) {
            assertEquals(consumer1.numMessagesInQueue(), 0);
            message = consumer1.receive();
            assertEquals(new String(message.getData()), messagePredicate + i);
            assertEquals(consumer1.numMessagesInQueue(), 0);
            log.info("Consumer received : " + new String(message.getData()));
        }

        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(0).subscriptionType(SubscriptionType.Failover)
                .consumerName("consumer-2").subscribe();

        // 6. Trigger redelivery
        consumer1.redeliverUnacknowledgedMessages();

        // 7. Trigger Failover
        consumer1.close();

        // 8. Receive messages on failed over consumer
        for (int i = 0; i < totalMessages / 2; i++) {
            assertEquals(consumer2.numMessagesInQueue(), 0);
            message = consumer2.receive();
            assertEquals(new String(message.getData()), messagePredicate + i);
            assertEquals(consumer2.numMessagesInQueue(), 0);
            log.info("Consumer received : " + new String(message.getData()));
        }
    }

    @Test
    public void testFailedZeroQueueSizeBatchMessage() throws PulsarClientException {

        int batchMessageDelayMs = 100;
        String topicName = newTopicName();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared).receiverQueueSize(0)
                .subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
            .topic(topicName)
            .messageRoutingMode(MessageRoutingMode.SinglePartition);

        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true).batchingMaxPublishDelay(batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        } else {
            producerBuilder.enableBatching(false);
        }

        Producer<byte[]> producer = producerBuilder.create();
        // send asynchronously so the messages are grouped into batches: a synchronous send would flush a batch
        // with a single message, which the producer publishes as a non-batched message
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }
        producer.flush();

        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();

        consumer.receiveAsync().handle((ok, e) -> {
            // as zero receiverQueueSize doesn't support batch message, must receive exception at callback.
            exceptionRef.set(e);
            return null;
        });
        Awaitility.await().untilAsserted(() -> assertNotNull(exceptionRef.get()));
        assertEquals(exceptionRef.get().getClass(), PulsarClientException.InvalidMessageException.class);
        assertEquals(exceptionRef.get().getMessage(),
                String.format("Unsupported Batch message with 0 size receiver queue for [%s]-[%s] ",
                        consumer.getSubscription(), consumer.getConsumerName()));
        // zeroConsumerImpl will close when received batch message.
        Awaitility.await().untilAsserted(()-> Assert.assertFalse(consumer.isConnected()));

        producer.close();
    }

    @Test
    public void testZeroQueueSizeMessageRedelivery() throws PulsarClientException {
        final String topic = newTopicName();
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .receiverQueueSize(0)
            .subscriptionName("sub")
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(1, TimeUnit.SECONDS)
            .subscribe();

        final int messages = 10;
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < messages; i++) {
            producer.send(i);
        }

        Set<Integer> receivedMessages = new HashSet<>();
        for (int i = 0; i < messages * 2; i++) {
            receivedMessages.add(consumer.receive().getValue());
        }

        Assert.assertEquals(receivedMessages.size(), messages);

        consumer.close();
        producer.close();
    }

    @Test
    public void testZeroQueueSizeMessageRedeliveryForListener() throws Exception {
        final String topic = newTopicName();
        final int messages = 10;
        final CountDownLatch latch = new CountDownLatch(messages * 2);
        Set<Integer> receivedMessages = new HashSet<>();
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .receiverQueueSize(0)
            .subscriptionName("sub")
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(1, TimeUnit.SECONDS)
            .messageListener((MessageListener<Integer>) (c, msg) -> {
                try {
                    receivedMessages.add(msg.getValue());
                } finally {
                    latch.countDown();
                }
            })
            .subscribe();

        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < messages; i++) {
            producer.send(i);
        }

        latch.await();
        Assert.assertEquals(receivedMessages.size(), messages);

        consumer.close();
        producer.close();
    }

    @Test
    public void testZeroQueueSizeMessageRedeliveryForAsyncReceive()
            throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = newTopicName();
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .receiverQueueSize(0)
            .subscriptionName("sub")
            .subscriptionType(SubscriptionType.Shared)
            .ackTimeout(1, TimeUnit.SECONDS)
            .subscribe();

        final int messages = 10;
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < messages; i++) {
            producer.send(i);
        }

        Set<Integer> receivedMessages = new HashSet<>();
        List<CompletableFuture<Message<Integer>>> futures = new ArrayList<>(20);
        for (int i = 0; i < messages * 2; i++) {
            futures.add(consumer.receiveAsync());
        }
        for (CompletableFuture<Message<Integer>> future : futures) {
            receivedMessages.add(future.get().getValue());
        }

        Assert.assertEquals(receivedMessages.size(), messages);

        consumer.close();
        producer.close();
    }

    @Test(timeOut = 30000)
    public void testZeroQueueGetExceptionWhenReceiveBatchMessage() throws PulsarClientException {

        final String topic = newTopicName();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Shared).receiverQueueSize(0)
                .subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic(topic)
                .messageRoutingMode(MessageRoutingMode.SinglePartition);

        producerBuilder.enableBatching(true)
                // set batchingMaxPublishDelay as 10 seconds to ensure all messages are sent as batch-messages
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS)
                .batchingMaxMessages(5);

        Producer<byte[]> producer = producerBuilder.create();

        // send a batch message to trigger zeroQueueConsumer to close
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }

        // when zeroQueueConsumer receive a batch message, it will close and receive method will throw exception
        assertThatThrownBy(
                consumer::receive
        )
                .isInstanceOf(PulsarClientException.class)
                .hasMessage("java.lang.InterruptedException: Queue is terminated")
                .hasCauseInstanceOf(InterruptedException.class);
    }

    @Test(timeOut = 30000)
    public void testPauseAndResume() throws Exception {
        final String topicName = newTopicName();
        final String subName = "sub";

        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
        AtomicInteger received = new AtomicInteger();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(0).messageListener((c1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    c1.acknowledgeAsync(msg);
                    received.incrementAndGet();
                    latch.get().countDown();
                }).subscribe();
        consumer.pause();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();

        for (int i = 0; i < 2; i++) {
            producer.send(("my-message-" + i).getBytes());
        }

        // Paused consumer receives only one message
        assertTrue(latch.get().await(2, TimeUnit.SECONDS), "Timed out waiting for message listener acks");
        Thread.sleep(2000);
        assertEquals(received.intValue(), 1, "Consumer received messages while paused");

        latch.set(new CountDownLatch(1));
        consumer.resume();
        assertTrue(latch.get().await(2, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        consumer.unsubscribe();
        producer.close();
    }

    @Test(timeOut = 30000)
    public void testPauseAndResumeWithUnloading() throws Exception {
        final String topicName = newTopicName();
        final String subName = "sub";

        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
        AtomicInteger received = new AtomicInteger();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .receiverQueueSize(0).messageListener((c1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    c1.acknowledgeAsync(msg);
                    received.incrementAndGet();
                    latch.get().countDown();
                }).subscribe();
        consumer.pause();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false).create();

        for (int i = 0; i < 2; i++) {
            producer.send(("my-message-" + i).getBytes());
        }

        // Paused consumer receives only one message
        assertTrue(latch.get().await(2, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        // Make sure no flow permits are sent when the consumer reconnects to the topic
        admin.topics().unload(topicName);
        Thread.sleep(2000);
        assertEquals(received.intValue(), 1, "Consumer received messages while paused");

        latch.set(new CountDownLatch(1));
        consumer.resume();
        assertTrue(latch.get().await(2, TimeUnit.SECONDS), "Timed out waiting for message listener acks");

        consumer.unsubscribe();
        producer.close();
    }

    @Test(timeOut = 30000)
    public void testReceiveAsyncNotStuckAfterTopicUnload() throws Exception {
        String topicName = newTopicName();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(0)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        try {
            ConsumerImpl<?> consumerImpl = (ConsumerImpl<?>) consumer;
            long lastDisconnectedTimestamp = consumerImpl.getLastDisconnectedTimestamp();
            CompletableFuture<Message<String>> receiveFuture = consumer.receiveAsync();

            admin.topics().unload(topicName);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> consumerImpl.getLastDisconnectedTimestamp() > lastDisconnectedTimestamp);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(consumerImpl::isConnected);

            producer.send("message");
            assertEquals(receiveFuture.get(10, TimeUnit.SECONDS).getValue(), "message");
        } finally {
            consumer.close();
            producer.close();
        }
    }

    @Test(timeOut = 30000)
    public void testMultiplePendingReceiveAsyncNotStuckAfterTopicUnload() throws Exception {
        String topicName = newTopicName();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(0)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        try {
            ConsumerImpl<?> consumerImpl = (ConsumerImpl<?>) consumer;
            long lastDisconnectedTimestamp = consumerImpl.getLastDisconnectedTimestamp();
            List<CompletableFuture<Message<String>>> receiveFutures = List.of(
                    consumer.receiveAsync(), consumer.receiveAsync(), consumer.receiveAsync());

            admin.topics().unload(topicName);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> consumerImpl.getLastDisconnectedTimestamp() > lastDisconnectedTimestamp);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(consumerImpl::isConnected);

            producer.send("message-0");
            producer.send("message-1");
            producer.send("message-2");

            CompletableFuture.allOf(receiveFutures.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
            Set<String> receivedMessages = new HashSet<>();
            for (CompletableFuture<Message<String>> receiveFuture : receiveFutures) {
                receivedMessages.add(receiveFuture.join().getValue());
            }
            assertEquals(receivedMessages, Set.of("message-0", "message-1", "message-2"));
        } finally {
            consumer.close();
            producer.close();
        }
    }

    private int getBrokerAvailablePermits(String topicName) throws Exception {
        List<? extends ConsumerStats> consumers = admin.topics().getStats(topicName)
                .getSubscriptions().get("sub").getConsumers();
        assertEquals(consumers.size(), 1);
        return consumers.get(0).getAvailablePermits();
    }

    private int getBrokerAvailablePermits(String topicName, String consumerName) throws Exception {
        return admin.topics().getStats(topicName).getSubscriptions().get("sub").getConsumers().stream()
                .filter(consumer -> consumerName.equals(consumer.getConsumerName()))
                .findFirst().orElseThrow().getAvailablePermits();
    }

    @DataProvider
    public Object[][] failedUnsubscribeListenerCompletionOrder() {
        return new Object[][] {{true, false, false}, {false, false, false},
                {true, true, false}, {true, true, true}};
    }

    @Test(timeOut = 60000, dataProvider = "failedUnsubscribeListenerCompletionOrder")
    public void testFailedUnsubscribePreservesListenerPermit(boolean listenerFinishesBeforeError,
                                                            boolean paused,
                                                            boolean resumeDuringUnsubscribe) throws Exception {
        String topic = newTopicName();
        CountDownLatch listenerEntered = new CountDownLatch(1);
        CountDownLatch releaseListener = new CountDownLatch(1);
        CompletableFuture<Void> listenerFinished = new CompletableFuture<>();
        CompletableFuture<Void> secondMessage = new CompletableFuture<>();
        List<String> received = new CopyOnWriteArrayList<>();
        try (PulsarClient client = newPulsarClient();
             ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING)
                     .topic(topic).subscriptionName("sub").consumerName("listener")
                     .subscriptionType(SubscriptionType.Shared).receiverQueueSize(0)
                     .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                     .messageListener((c, msg) -> {
                         c.acknowledgeAsync(msg);
                         received.add(msg.getValue());
                         if ("first".equals(msg.getValue())) {
                             listenerEntered.countDown();
                             try {
                                 assertThat(releaseListener.await(30, TimeUnit.SECONDS)).isTrue();
                                 listenerFinished.complete(null);
                             } catch (Throwable t) {
                                 listenerFinished.completeExceptionally(t);
                             }
                         } else {
                             secondMessage.complete(null);
                         }
                     }).subscribe();
             Consumer<String> otherConsumer = client.newConsumer(Schema.STRING).topic(topic)
                     .subscriptionName("sub").subscriptionType(SubscriptionType.Shared).receiverQueueSize(0)
                     .subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create()) {
            // The second consumer has no receive pending and therefore cannot take the listener's message.
            producer.send("first");
            assertThat(listenerEntered.await(10, TimeUnit.SECONDS)).isTrue();
            ClientCnx cnx = consumer.getClientCnx();
            long epoch = consumer.getConnectionHandler().getEpoch();
            if (paused) {
                consumer.pause();
            }
            try (ConsumerResponseGate gate = new ConsumerResponseGate(consumer, BaseCommand.Type.UNSUBSCRIBE)) {
                if (resumeDuringUnsubscribe) {
                    // Accumulate a permit while Ready and paused, then resume while unsubscribe is pending.
                    releaseListener.countDown();
                    listenerFinished.get(10, TimeUnit.SECONDS);
                    awaitListenerTasks(consumer);
                    assertThat(consumer.getAvailablePermits()).isEqualTo(1);
                }
                // The real broker rejects this request because another consumer shares the subscription.
                CompletableFuture<Void> unsubscribe = consumer.unsubscribeAsync(false);
                gate.responseHeld.get(10, TimeUnit.SECONDS);
                assertThat(consumer.getState()).isEqualTo(HandlerState.State.Closing);
                if (listenerFinishesBeforeError) {
                    releaseListener.countDown();
                    listenerFinished.get(10, TimeUnit.SECONDS);
                    awaitListenerTasks(consumer);
                }
                if (resumeDuringUnsubscribe) {
                    consumer.resume();
                }
                gate.awaitWrites();
                assertThat(gate.commands).as("permits remain deferred while unsubscribe is pending").isEmpty();

                gate.completeResponse();
                assertThatThrownBy(() -> unsubscribe.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(PulsarClientException.BrokerMetadataException.class);
                assertThat(consumer.isConnected()).isTrue();
                assertThat(consumer.getClientCnx()).isSameAs(cnx);
                assertThat(consumer.getConnectionHandler().getEpoch()).as("no reconnect replenishes permits")
                        .isEqualTo(epoch);
                if (!listenerFinishesBeforeError) {
                    releaseListener.countDown();
                    listenerFinished.get(10, TimeUnit.SECONDS);
                    awaitListenerTasks(consumer);
                }
                if (paused && !resumeDuringUnsubscribe) {
                    gate.awaitWrites();
                    assertThat(gate.commands).as("unsubscribe failure preserves pause").isEmpty();
                    assertThat(consumer.getAvailablePermits()).isEqualTo(1);
                    consumer.resume();
                }

                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(getBrokerAvailablePermits(topic, consumer.getConsumerName())).isEqualTo(1));
                gate.awaitWrites();
                assertThat(gate.commands).containsExactly(BaseCommand.Type.FLOW);
                assertThat(gate.permits.get()).isEqualTo(1);
                producer.send("second");
                secondMessage.get(10, TimeUnit.SECONDS);
                assertThat(received).containsExactly("first", "second");
            } finally {
                releaseListener.countDown();
            }
        } finally {
            releaseListener.countDown();
        }
    }

    @DataProvider
    public Object[][] listenerReconnectCompletionOrder() {
        return new Object[][] {{true, false}, {false, false}, {true, true}};
    }

    @Test(timeOut = 60000, dataProvider = "listenerReconnectCompletionOrder")
    public void testListenerReconnectSendsAckBeforeSinglePermit(boolean listenerFinishesBeforeSubscribe,
                                                              boolean resumeDuringSubscribe) throws Exception {
        String topic = newTopicName();
        CountDownLatch listenerEntered = new CountDownLatch(1);
        CountDownLatch releaseListener = new CountDownLatch(1);
        CompletableFuture<Void> listenerFinished = new CompletableFuture<>();
        CompletableFuture<Void> secondMessage = new CompletableFuture<>();
        List<String> received = new CopyOnWriteArrayList<>();
        try (PulsarClient client = newPulsarClient();
             ConsumerImpl<String> consumer = (ConsumerImpl<String>) client.newConsumer(Schema.STRING)
                     .topic(topic).subscriptionName("sub").receiverQueueSize(0)
                     .acknowledgmentGroupTime(1, TimeUnit.HOURS)
                     .messageListener((c, msg) -> {
                         c.acknowledgeAsync(msg);
                         received.add(msg.getValue());
                         if ("first".equals(msg.getValue())) {
                             listenerEntered.countDown();
                             try {
                                 assertThat(releaseListener.await(30, TimeUnit.SECONDS)).isTrue();
                                 listenerFinished.complete(null);
                             } catch (Throwable t) {
                                 listenerFinished.completeExceptionally(t);
                             }
                         } else {
                             secondMessage.complete(null);
                         }
                     }).subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic)
                     .enableBatching(false).create()) {
            producer.send("first");
            assertThat(listenerEntered.await(10, TimeUnit.SECONDS)).isTrue();
            try (ConsumerResponseGate gate = new ConsumerResponseGate(consumer, BaseCommand.Type.SUBSCRIBE)) {
                if (resumeDuringSubscribe) {
                    // Finish while paused on the old connection, leaving one permit for resume() to flush.
                    consumer.pause();
                    releaseListener.countDown();
                    listenerFinished.get(10, TimeUnit.SECONDS);
                    awaitListenerTasks(consumer);
                    assertThat(consumer.getAvailablePermits()).isEqualTo(1);
                }

                admin.topics().unload(topic);
                gate.responseHeld.get(10, TimeUnit.SECONDS);
                assertThat(consumer.getState()).isEqualTo(HandlerState.State.Connecting);

                if (listenerFinishesBeforeSubscribe) {
                    releaseListener.countDown();
                    listenerFinished.get(10, TimeUnit.SECONDS);
                    awaitListenerTasks(consumer);
                    if (resumeDuringSubscribe) {
                        consumer.resume();
                    }
                    gate.awaitWrites();
                    assertThat(gate.commands).as("commands before the SUBSCRIBE response").isEmpty();
                }

                gate.completeResponse();
                assertThat(consumer.isConnected()).isTrue();
                if (!listenerFinishesBeforeSubscribe) {
                    assertThat(gate.commands).as("the running listener still owns the permit")
                            .containsExactly(BaseCommand.Type.ACK);
                    releaseListener.countDown();
                    listenerFinished.get(10, TimeUnit.SECONDS);
                    awaitListenerTasks(consumer);
                }
                gate.awaitWrites();
                assertThat(gate.commands).containsExactly(BaseCommand.Type.ACK, BaseCommand.Type.FLOW);
                assertThat(gate.permits.get()).isEqualTo(1);
                Awaitility.await().atMost(10, TimeUnit.SECONDS)
                        .untilAsserted(() -> assertThat(getBrokerAvailablePermits(topic)).isEqualTo(1));

                producer.send("second");
                secondMessage.get(10, TimeUnit.SECONDS);
                assertThat(received).containsExactly("first", "second");
            } finally {
                releaseListener.countDown();
            }
        } finally {
            releaseListener.countDown();
        }
    }

    private static void awaitListenerTasks(ConsumerImpl<?> consumer) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>();
        consumer.externalPinnedExecutor.execute(() -> done.complete(null));
        done.get(10, TimeUnit.SECONDS);
    }

    // Delay a real broker response without blocking the connection's event loop or replacing the broker.
    private static final class ConsumerResponseGate extends ChannelDuplexHandler implements AutoCloseable {
        private final ClientCnx cnx;
        private final long consumerId;
        private final BaseCommand.Type requestType;
        private final CompletableFuture<Void> responseHeld = new CompletableFuture<>();
        private final List<BaseCommand.Type> commands = new CopyOnWriteArrayList<>();
        private final AtomicInteger permits = new AtomicInteger();
        private long requestId = -1;
        private ByteBuf response;
        private ChannelHandlerContext context;

        private ConsumerResponseGate(ConsumerImpl<?> consumer, BaseCommand.Type requestType) throws Exception {
            cnx = consumer.getClientCnx();
            consumerId = consumer.consumerId;
            this.requestType = requestType;
            cnx.channel().eventLoop().submit(() -> cnx.channel().pipeline()
                    .addBefore(cnx.ctx().name(), "consumer-response-gate", this)).get(10, TimeUnit.SECONDS);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof ByteBuf frame) {
                ByteBuf view = frame.duplicate();
                view.skipBytes(Integer.BYTES); // Outbound frames include the total length.
                BaseCommand command = readCommand(view);
                if (command.getType() == requestType) {
                    if (requestType == BaseCommand.Type.SUBSCRIBE
                            && command.getSubscribe().getConsumerId() == consumerId) {
                        requestId = command.getSubscribe().getRequestId();
                    } else if (requestType == BaseCommand.Type.UNSUBSCRIBE
                            && command.getUnsubscribe().getConsumerId() == consumerId) {
                        requestId = command.getUnsubscribe().getRequestId();
                    }
                } else if (requestId != -1) {
                    if (command.getType() == BaseCommand.Type.FLOW
                            && command.getFlow().getConsumerId() == consumerId) {
                        commands.add(command.getType());
                        permits.addAndGet(command.getFlow().getMessagePermits());
                    } else if (command.getType() == BaseCommand.Type.ACK
                            && command.getAck().getConsumerId() == consumerId) {
                        commands.add(command.getType());
                    }
                }
            }
            super.write(ctx, msg, promise);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            BaseCommand command = readCommand(((ByteBuf) msg).duplicate());
            long responseRequestId = switch (command.getType()) {
                case SUCCESS -> command.getSuccess().getRequestId();
                case ERROR -> command.getError().getRequestId();
                default -> -1;
            };
            if (requestId != -1 && responseRequestId == requestId) {
                response = (ByteBuf) msg;
                context = ctx;
                responseHeld.complete(null);
            } else {
                super.channelRead(ctx, msg);
            }
        }

        private static BaseCommand readCommand(ByteBuf view) {
            int commandSize = view.readInt();
            BaseCommand command = new BaseCommand();
            command.parseFrom(view, commandSize);
            return command;
        }

        private void awaitWrites() throws Exception {
            cnx.channel().eventLoop().submit(() -> { }).get(10, TimeUnit.SECONDS);
        }

        private void completeResponse() throws Exception {
            cnx.channel().eventLoop().submit(() -> {
                ByteBuf frame = response;
                response = null;
                context.fireChannelRead(frame);
            }).get(10, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            cnx.channel().eventLoop().submit(() -> {
                if (response != null) {
                    response.release();
                    response = null;
                }
                cnx.channel().pipeline().remove(this);
            }).get(10, TimeUnit.SECONDS);
        }
    }

    @DataProvider
    public Object[][] receiveCompletionBeforeRegistration() {
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 30000, dataProvider = "receiveCompletionBeforeRegistration")
    public void testCompletedReceiveBeforeRegistrationDoesNotSendPermit(boolean timeout) throws Exception {
        String topicName = newTopicName();
        @Cleanup PulsarClient client = newPulsarClient();
        @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(0)
                .subscribe();
        @Cleanup Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        ConsumerImpl<?> consumerImpl = (ConsumerImpl<?>) consumer;
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            // Hold registration until the application has cancelled or timed out the receive.
            consumerImpl.internalPinnedExecutor.execute(() -> {
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(blocked.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Message<String>> completedReceive = consumer.receiveAsync();
            if (timeout) {
                completedReceive.orTimeout(0, TimeUnit.MILLISECONDS);
                assertThatThrownBy(() -> completedReceive.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(TimeoutException.class);
            } else {
                assertThat(completedReceive.cancel(false)).isTrue();
            }
            release.countDown();
            consumerImpl.internalPinnedExecutor.submit(() -> { }).get(10, TimeUnit.SECONDS);
            // This request follows any FLOW on the same connection, so a zero-permit assertion
            // cannot pass merely because the broker has not processed the registration yet.
            consumer.getLastMessageIdsAsync().get(10, TimeUnit.SECONDS);
            assertThat(getBrokerAvailablePermits(topicName))
                    .as("a completed receive must not authorize prefetch").isZero();

            CompletableFuture<Message<String>> receive = consumer.receiveAsync();
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(getBrokerAvailablePermits(topicName)).isEqualTo(1));
            producer.send("message");
            Message<String> message = receive.get(10, TimeUnit.SECONDS);
            assertThat(message.getValue()).isEqualTo("message");
            consumer.acknowledge(message);
            message.release();
        } finally {
            release.countDown();
        }
    }

    @Test(timeOut = 30000)
    public void testReceiveAsyncRegisteredAfterReconnectionNotStuck() throws Exception {
        String topicName = newTopicName();
        @Cleanup PulsarClient client = newPulsarClient();
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(0)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            ConsumerImpl<?> consumerImpl = (ConsumerImpl<?>) consumer;
            // Block the consumer's internal executor so the receiveAsync registration task
            // cannot run until the reconnection has completed.
            consumerImpl.internalPinnedExecutor.execute(() -> {
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(blocked.await(10, TimeUnit.SECONDS));

            long lastDisconnectedTimestamp = consumerImpl.getLastDisconnectedTimestamp();
            CompletableFuture<Message<String>> receiveFuture = consumer.receiveAsync();

            admin.topics().unload(topicName);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> consumerImpl.getLastDisconnectedTimestamp() > lastDisconnectedTimestamp);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(consumerImpl::isConnected);

            release.countDown();

            // The pending receiveAsync must be counted by the reconnection path: exactly one permit.
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(getBrokerAvailablePermits(topicName), 1));

            producer.send("message");
            assertEquals(receiveFuture.get(10, TimeUnit.SECONDS).getValue(), "message");
        } finally {
            release.countDown();
            consumer.close();
            producer.close();
        }
    }

    @Test(timeOut = 30000)
    public void testMixedReceiveAndReceiveAsyncNotStuckAfterTopicUnload() throws Exception {
        String topicName = newTopicName();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(0)
                .subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();
        ExecutorService receiveExecutor = Executors.newSingleThreadExecutor();

        try {
            ConsumerImpl<?> consumerImpl = (ConsumerImpl<?>) consumer;
            Future<Message<String>> blockingReceive =
                    receiveExecutor.submit(() -> consumer.receive());
            List<CompletableFuture<Message<String>>> receiveFutures = List.of(
                    consumer.receiveAsync(), consumer.receiveAsync(), consumer.receiveAsync());

            // 1 permit for the blocking receive() plus 1 for each pending receiveAsync().
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(getBrokerAvailablePermits(topicName), 4));

            long lastDisconnectedTimestamp = consumerImpl.getLastDisconnectedTimestamp();
            admin.topics().unload(topicName);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> consumerImpl.getLastDisconnectedTimestamp() > lastDisconnectedTimestamp);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(consumerImpl::isConnected);

            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(getBrokerAvailablePermits(topicName), 4));

            for (int i = 0; i < 4; i++) {
                producer.send("message-" + i);
            }

            Set<String> receivedMessages = new HashSet<>();
            receivedMessages.add(blockingReceive.get(10, TimeUnit.SECONDS).getValue());
            for (CompletableFuture<Message<String>> receiveFuture : receiveFutures) {
                receivedMessages.add(receiveFuture.get(10, TimeUnit.SECONDS).getValue());
            }
            assertEquals(receivedMessages, Set.of("message-0", "message-1", "message-2", "message-3"));
        } finally {
            receiveExecutor.shutdownNow();
            consumer.close();
            producer.close();
        }
    }

    @Test(timeOut = 30000)
    public void testPauseAndResumeNoReconnection() throws Exception {
        final String topicName = newTopicName();
        final String subName = "sub";

        final Object object = new Object();
        AtomicBoolean running = new AtomicBoolean(false);

        final List<Integer> receivedMessages = Collections.synchronizedList(new ArrayList<>());
        final Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topicName)
                .subscriptionName(subName)
                .receiverQueueSize(0)
                .messageListener((MessageListener<Integer>) (consumer1, msg) -> {
                    assertNotNull(msg, "Message cannot be null");
                    receivedMessages.add(msg.getValue());
                    try {
                        consumer1.acknowledge(msg);
                    } catch (PulsarClientException ignored) {
                    }
                    synchronized (object) {
                        running.set(false);
                        object.notifyAll();
                    }
                }).subscribe();
        final Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .enableBatching(false)
                .create();
        final int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            consumer.resume();
            producer.newMessage().value(i).sendAsync();
            synchronized (object) {
                running.set(true);
                while (running.get()) {
                    object.wait();
                }
            }
            consumer.pause();
        }

        log.info().attr("messages", receivedMessages).log("Received messages");
        assertEquals(receivedMessages.size(), numMessages);
        for (int i = 0; i < numMessages; i++) {
            assertEquals(receivedMessages.get(i).intValue(), i);
        }
    }

    @Test(timeOut = 30000)
    public void testZeroQueueSizeConsumerWithPayloadProcessorReceiveBatchMessage() throws Exception {
        final String topicName = newTopicName();
        final String subscriptionName = "my-ex-subscription";
        final String messagePredicate = "my-message-";

        // 2. Create Producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxMessages(5)
                // set batchingMaxPublishDelay as 10 seconds to ensure all messages are sent as batch-messages
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS)
                .enableBatching(true)
                .create();

        // 3. Create Consumer
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topicName)
                .messagePayloadProcessor(new MessagePayloadProcessor() {
                                             @Override
                                             public <T> void process(MessagePayload payload,
                                                             MessagePayloadContext context, Schema<T> schema,
                                                             java.util.function.Consumer<Message<T>> messageConsumer) {
                                                 if (context.isBatch()) {
                                                     final int numMessages = context.getNumMessages();
                                                     for (int i = 0; i < numMessages; i++) {
                                                         messageConsumer.
                                                                 accept(context.getMessageAt(i, numMessages,
                                                                         payload, true, schema)
                                                                 );
                                                     }
                                                 } else {
                                                     messageConsumer.accept(context.asSingleMessage(payload, schema));
                                                 }
                                             }
                                         }
                )
                .subscriptionName(subscriptionName)
                .receiverQueueSize(0)
                .subscribe();

        ArrayList<CompletableFuture<MessageId>> futures = new ArrayList<>();
        // 3. producer publish batch-messages
        for (int i = 0; i < totalMessages; i++) {
            String message = messagePredicate + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        producer.flush();

        // ensure all messages are sent as batch messages
        for (CompletableFuture<MessageId> future : futures) {
            assertTrue(future.get() instanceof BatchMessageIdImpl);
        }

        // 4. consumer should throw PulsarClientException when call method receive()
        assertThatThrownBy(
                consumer::receive
        )
                .isInstanceOf(PulsarClientException.class)
                .hasMessage("java.lang.InterruptedException: Queue is terminated")
                .hasCauseInstanceOf(InterruptedException.class);
        producer.close();
    }
}
