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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.Level;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.utils.TestLogAppender;
import org.awaitility.Awaitility;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class MultiTopicsConsumerTest extends ProducerConsumerBase {
    private ScheduledExecutorService internalExecutorServiceDelegate;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
       clientBuilder.ioThreads(4).connectionsPerBroker(4);
    }

    // test that reproduces the issue https://github.com/apache/pulsar/issues/12024
    // where closing the consumer leads to an endless receive loop
    @Test
    public void testMultiTopicsConsumerCloses() throws Exception {
        String topicNameBase = "persistent://my-property/my-ns/my-topic-consumer-closes-";

        ClientConfigurationData conf = ((ClientBuilderImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString()))
                .getClientConfigurationData();

        @Cleanup
        PulsarClientImpl client = new PulsarClientImpl(conf) {
            {
                ScheduledExecutorService internalExecutorService =
                        (ScheduledExecutorService) super.getScheduledExecutorProvider().getExecutor();
                internalExecutorServiceDelegate = mock(ScheduledExecutorService.class,
                        // a spy isn't used since that doesn't work for private classes, instead
                        // the mock delegatesTo an existing instance. A delegate is sufficient for verifying
                        // method calls on the interface.
                        Mockito.withSettings().defaultAnswer(AdditionalAnswers.delegatesTo(internalExecutorService)));
            }

            @Override
            public ExecutorService getInternalExecutorService() {
                return internalExecutorServiceDelegate;
            }
        };
        @Cleanup
        Producer<byte[]> producer1 = client.newProducer()
                .topic(topicNameBase + "1")
                .enableBatching(false)
                .create();
        @Cleanup
        Producer<byte[]> producer2 = client.newProducer()
                .topic(topicNameBase + "2")
                .enableBatching(false)
                .create();
        @Cleanup
        Producer<byte[]> producer3 = client.newProducer()
                .topic(topicNameBase + "3")
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer = client
                .newConsumer()
                .topics(Lists.newArrayList(topicNameBase + "1", topicNameBase + "2", topicNameBase + "3"))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1)
                .subscriptionName(methodName)
                .subscribe();

        // wait for background tasks to start
        Thread.sleep(1000L);

        // when consumer is closed
        consumer.close();
        // give time for background tasks to execute
        Thread.sleep(1000L);

        // then verify that no scheduling operation has happened
        verify(internalExecutorServiceDelegate, times(0))
                .schedule(any(Runnable.class), anyLong(), any());
    }

    // test that reproduces the issue that PR https://github.com/apache/pulsar/pull/12456 fixes
    // where MultiTopicsConsumerImpl has a data race that causes out-of-order delivery of messages
    @Test
    public void testShouldMaintainOrderForIndividualTopicInMultiTopicsConsumer()
            throws PulsarAdminException, PulsarClientException, ExecutionException, InterruptedException,
            TimeoutException {
        String topicName = newTopicName();
        int numPartitions = 2;
        int numMessages = 100000;
        admin.topics().createPartitionedTopic(topicName, numPartitions);

        Producer<Long>[] producers = new Producer[numPartitions];

        for (int i = 0; i < numPartitions; i++) {
            producers[i] = pulsarClient.newProducer(Schema.INT64)
                    // produce to each partition directly so that order can be maintained in sending
                    .topic(topicName + "-partition-" + i)
                    .enableBatching(true)
                    .maxPendingMessages(30000)
                    .maxPendingMessagesAcrossPartitions(60000)
                    .batchingMaxMessages(10000)
                    .batchingMaxPublishDelay(5, TimeUnit.SECONDS)
                    .batchingMaxBytes(4 * 1024 * 1024)
                    .blockIfQueueFull(true)
                    .create();
        }

        @Cleanup
        Consumer<Long> consumer = pulsarClient
                .newConsumer(Schema.INT64)
                // consume on the partitioned topic
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(numMessages)
                .subscriptionName(methodName)
                .subscribe();

        // produce sequence numbers to each partition topic
        long sequenceNumber = 1L;
        for (int i = 0; i < numMessages; i++) {
            for (Producer<Long> producer : producers) {
                producer.newMessage()
                        .value(sequenceNumber)
                        .sendAsync();
            }
            sequenceNumber++;
        }
        for (Producer<Long> producer : producers) {
            producer.flush();
            producer.close();
        }

        // receive and validate sequences in the partitioned topic
        Map<String, AtomicLong> receivedSequences = new HashMap<>();
        int receivedCount = 0;
        while (receivedCount < numPartitions * numMessages) {
            Message<Long> message = consumer.receiveAsync().get(5, TimeUnit.SECONDS);
            consumer.acknowledge(message);
            receivedCount++;
            AtomicLong receivedSequenceCounter =
                    receivedSequences.computeIfAbsent(message.getTopicName(), k -> new AtomicLong(1L));
            Assert.assertEquals(message.getValue().longValue(), receivedSequenceCounter.getAndIncrement());
        }
        Assert.assertEquals(numPartitions * numMessages, receivedCount);
    }

    @Test
    public void testBatchReceiveAckTimeout()
            throws PulsarAdminException, PulsarClientException {
        String topicName = newTopicName();
        int numPartitions = 2;
        int numMessages = 100000;
        admin.topics().createPartitionedTopic(topicName, numPartitions);

        @Cleanup
        Producer<Long> producer = pulsarClient.newProducer(Schema.INT64)
                .topic(topicName)
                .enableBatching(false)
                .blockIfQueueFull(true)
                .create();

        @Cleanup
        Consumer<Long> consumer = pulsarClient
                .newConsumer(Schema.INT64)
                .topic(topicName)
                .receiverQueueSize(numMessages)
                .batchReceivePolicy(
                        BatchReceivePolicy.builder().maxNumMessages(1).timeout(2, TimeUnit.SECONDS).build()
                ).ackTimeout(1000, TimeUnit.MILLISECONDS)
                .subscriptionName(methodName)
                .subscribe();

        producer.newMessage()
                .value(1l)
                .send();

        // first batch receive
        Assert.assertEquals(consumer.batchReceive().size(), 1);
        // Not ack, trigger redelivery this message.
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(consumer.batchReceive().size(), 1);
        });
    }

    @Test(timeOut = 30000)
    public void testAcknowledgeWrongMessageId() throws Exception {
        final var topic1 = newTopicName();
        final var topic2 = newTopicName();

        @Cleanup final var singleTopicConsumer = pulsarClient.newConsumer()
                .topic(topic1)
                .subscriptionName("sub-1")
                .isAckReceiptEnabled(true)
                .subscribe();
        assertTrue(singleTopicConsumer instanceof ConsumerImpl);

        @Cleanup final var multiTopicsConsumer = pulsarClient.newConsumer()
                .topics(List.of(topic1, topic2))
                .subscriptionName("sub-2")
                .isAckReceiptEnabled(true)
                .subscribe();
        assertTrue(multiTopicsConsumer instanceof MultiTopicsConsumerImpl);

        @Cleanup final var producer = pulsarClient.newProducer().topic(topic1).create();
        final var nonTopicMessageIds = new ArrayList<MessageId>();
        nonTopicMessageIds.add(producer.send(new byte[]{ 0x00 }));
        nonTopicMessageIds.add(singleTopicConsumer.receive().getMessageId());

        // Multi-topics consumers can only acknowledge TopicMessageId, otherwise NotAllowedException will be thrown
        for (var msgId : nonTopicMessageIds) {
            assertFalse(msgId instanceof TopicMessageId);
            Assert.assertThrows(PulsarClientException.NotAllowedException.class,
                    () -> multiTopicsConsumer.acknowledge(msgId));
            Assert.assertThrows(PulsarClientException.NotAllowedException.class,
                    () -> multiTopicsConsumer.acknowledge(Collections.singletonList(msgId)));
            Assert.assertThrows(PulsarClientException.NotAllowedException.class,
                    () -> multiTopicsConsumer.acknowledgeCumulative(msgId));
        }

        // Single-topic consumer can acknowledge TopicMessageId
        final var topicMessageId = multiTopicsConsumer.receive().getMessageId();
        assertTrue(topicMessageId instanceof TopicMessageId);
        assertFalse(topicMessageId instanceof MessageIdImpl);
        singleTopicConsumer.acknowledge(topicMessageId);
    }

    @DataProvider
    public static Object[][] messageIdFromProducer() {
        return new Object[][] { { true }, { false } };
    }

    @Test(timeOut = 30000, dataProvider = "messageIdFromProducer")
    public void testSeekCustomTopicMessageId(boolean messageIdFromProducer) throws Exception {
        final var topic = TopicName.get(newTopicName()).toString();
        final var numPartitions = 3;
        admin.topics().createPartitionedTopic(topic, numPartitions);

        @Cleanup final var producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .messageRouter(new MessageRouter() {
                    int index = 0;
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return index++ % metadata.numPartitions();
                    }
                })
                .create();
        @Cleanup final var consumer = pulsarClient.newConsumer(Schema.INT32).topic(topic)
                .subscriptionName("sub").subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);

        final var msgIds = new HashMap<String, List<MessageId>>();
        final var numMessagesPerPartition = 10;
        final var numMessages = numPartitions * numMessagesPerPartition;
        for (int i = 0; i < numMessages; i++) {
            var msgId = (MessageIdImpl) producer.send(i);
            if (messageIdFromProducer) {
                msgIds.computeIfAbsent(topic + TopicName.PARTITIONED_TOPIC_SUFFIX + msgId.getPartitionIndex(),
                        __ -> new ArrayList<>()).add(msgId);
            } else {
                var topicMessageId = (TopicMessageId) consumer.receive().getMessageId();
                msgIds.computeIfAbsent(topicMessageId.getOwnerTopic(), __ -> new ArrayList<>()).add(topicMessageId);
            }
        }

        final var partitions = IntStream.range(0, numPartitions)
                .mapToObj(i -> topic + TopicName.PARTITIONED_TOPIC_SUFFIX + i)
                .collect(Collectors.toSet());
        assertEquals(msgIds.keySet(), partitions);

        for (var partition : partitions) {
            final var msgIdList = msgIds.get(partition);
            assertEquals(msgIdList.size(), numMessagesPerPartition);
            if (messageIdFromProducer) {
                consumer.seek(TopicMessageId.create(partition, msgIdList.get(numMessagesPerPartition / 2)));
            } else {
                consumer.seek(msgIdList.get(numMessagesPerPartition / 2));
            }
        }

        var topicMsgIds = new HashMap<String, List<TopicMessageId>>();
        for (int i = 0; i < ((numMessagesPerPartition / 2 - 1) * numPartitions); i++) {
            TopicMessageId topicMessageId = (TopicMessageId) consumer.receive().getMessageId();
            topicMsgIds.computeIfAbsent(topicMessageId.getOwnerTopic(), __ -> new ArrayList<>()).add(topicMessageId);
        }
        assertEquals(topicMsgIds.keySet(), partitions);
        for (var partition : partitions) {
            assertEquals(topicMsgIds.get(partition),
                    msgIds.get(partition).subList(numMessagesPerPartition / 2 + 1, numMessagesPerPartition));
        }
        consumer.close();
    }

    @Test(invocationCount = 10, timeOut = 30000)
    public void testMultipleIOThreads() throws PulsarAdminException, PulsarClientException {
        final var topic = TopicName.get(newTopicName()).toString();
        final var numPartitions = 100;
        admin.topics().createPartitionedTopic(topic, numPartitions);
        for (int i = 0; i < 100; i++) {
            admin.topics().createNonPartitionedTopic(topic + "-" + i);
        }
        @Cleanup
        final var consumer = pulsarClient.newConsumer(Schema.INT32).topicsPattern(topic + ".*")
                .subscriptionName("sub").subscribe();
        assertTrue(consumer instanceof MultiTopicsConsumerImpl);
        assertTrue(consumer.isConnected());
    }

    @Test
    public void testSameTopics() throws Exception {
        final String topic1 = BrokerTestUtil.newUniqueName("public/default/tp");
        final String topic2 = "persistent://" + topic1;
        admin.topics().createNonPartitionedTopic(topic2);
        // Create consumer with two same topics.
        try {
            pulsarClient.newConsumer(Schema.INT32).topics(Arrays.asList(topic1, topic2))
                    .subscriptionName("s1").subscribe();
            fail("Do not allow use two same topics.");
        } catch (Exception e) {
            if (e instanceof PulsarClientException && e.getCause() != null) {
                e = (Exception) e.getCause();
            }
            Throwable unwrapEx = FutureUtil.unwrapCompletionException(e);
            assertTrue(unwrapEx instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains( "Subscription topics include duplicate items"
                    + " or invalid names"));
        }
        // cleanup.
        admin.topics().delete(topic2);
    }

    @Test(timeOut = 30000)
    public void testSubscriptionNotFound() throws PulsarAdminException, PulsarClientException {
        final var topic1 = newTopicName();
        final var topic2 = newTopicName();

        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(false);

        try {
            final var singleTopicConsumer = pulsarClient.newConsumer()
                    .topic(topic1)
                    .subscriptionName("sub-1")
                    .isAckReceiptEnabled(true)
                    .subscribe();
            assertTrue(singleTopicConsumer instanceof ConsumerImpl);
        } catch (PulsarClientException.SubscriptionNotFoundException ignore) {
        } catch (Throwable t) {
            fail("Should throw PulsarClientException.SubscriptionNotFoundException instead");
        }

        try {
            final var multiTopicsConsumer = pulsarClient.newConsumer()
                    .topics(List.of(topic1, topic2))
                    .subscriptionName("sub-2")
                    .isAckReceiptEnabled(true)
                    .subscribe();
            assertTrue(multiTopicsConsumer instanceof MultiTopicsConsumerImpl);
        } catch (PulsarClientException.SubscriptionNotFoundException ignore) {
        } catch (Throwable t) {
            fail("Should throw PulsarClientException.SubscriptionNotFoundException instead");
        }

        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(true);
    }

    @Test(timeOut = 30000)
    public void testMessageListenerStopsProcessingAfterClosing() throws Exception {
        int numMessages = 100;
        String topic1 = newTopicName();
        String topic2 = newTopicName();
        final CountDownLatch consumerClosedLatch = new CountDownLatch(1);
        final CountDownLatch messageProcessedLatch = new CountDownLatch(1);
        AtomicInteger messageProcessedCount = new AtomicInteger(0);
        AtomicInteger messagesQueuedForExecutor = new AtomicInteger(0);
        AtomicInteger messagesCurrentlyInExecutor = new AtomicInteger(0);

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topics(List.of(topic1, topic2))
                .subscriptionName("my-subscriber-name")
                .messageListenerExecutor(new MessageListenerExecutor() {
                    @Override
                    public void execute(Message<?> message, Runnable runnable) {
                        messagesQueuedForExecutor.incrementAndGet();
                        messagesCurrentlyInExecutor.incrementAndGet();
                        executor.execute(() -> {
                            try {
                                runnable.run();
                            } finally {
                                messagesCurrentlyInExecutor.decrementAndGet();
                            }
                        });
                    }
                })
                .messageListener((c1, msg) -> {
                    messageProcessedCount.incrementAndGet();
                    c1.acknowledgeAsync(msg);
                    messageProcessedLatch.countDown();
                    try {
                        consumerClosedLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }).subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic1)
                .enableBatching(false)
                .create();

        for (int i = 0; i < numMessages; i++) {
            final String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        assertTrue(messageProcessedLatch.await(5, TimeUnit.SECONDS));
        // wait until all messages have been queued in the listener
        Awaitility.await().untilAsserted(() -> assertEquals(messagesQueuedForExecutor.get(), numMessages));
        @Cleanup
        TestLogAppender testLogAppender = TestLogAppender.create(Optional.empty());
        consumer.close();
        consumerClosedLatch.countDown();
        // only a single message should be processed
        assertEquals(messageProcessedCount.get(), 1);
        // wait until all messages have been drained from the executor
        Awaitility.await().untilAsserted(() -> assertEquals(messagesCurrentlyInExecutor.get(), 0));
        testLogAppender.getEvents().forEach(logEvent -> {
            if (logEvent.getLevel() == Level.ERROR) {
                org.apache.logging.log4j.message.Message logEventMessage = logEvent.getMessage();
                fail("No error should be logged when closing a consumer. Got: " + logEventMessage
                        .getFormattedMessage() + " throwable:" + logEventMessage.getThrowable());
            }
        });
    }
}
