/**
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

import lombok.Cleanup;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

@Test
public class ConsumerBatchReceiveTest extends ProducerConsumerBase {

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

    @DataProvider(name = "partitioned")
    public Object[][] partitionedTopicProvider() {
        return new Object[][] {
            { true },
            { false }
        };
    }

    @DataProvider(name = "batchReceivePolicy")
    public Object[][] batchReceivePolicyProvider() {
        return new Object[][] {

                // Default batch receive policy.
                { BatchReceivePolicy.DEFAULT_POLICY, true, 1000, false},
                // Only receive timeout limitation.
                { BatchReceivePolicy.builder()
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), true, 1000, false
                },
                // Only number of messages in a single batch receive limitation.
                { BatchReceivePolicy.builder()
                        .maxNumMessages(10)
                        .build(), true, 1000, false
                },
                // Number of messages and timeout limitation
                { BatchReceivePolicy.builder()
                        .maxNumMessages(13)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), true, 1000, false
                },
                // Size of messages and timeout limitation
                { BatchReceivePolicy.builder()
                        .maxNumBytes(64)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), true, 1000, false
                },
                // Default batch receive policy.
                { BatchReceivePolicy.DEFAULT_POLICY, false, 1000, false },
                // Only receive timeout limitation.
                { BatchReceivePolicy.builder()
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 1000, false
                },
                // Only number of messages in a single batch receive limitation.
                { BatchReceivePolicy.builder()
                        .maxNumMessages(10)
                        .build(), false, 1000, false
                },
                // Number of messages and timeout limitation
                { BatchReceivePolicy.builder()
                        .maxNumMessages(13)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 1000, false
                },
                // Size of messages and timeout limitation
                { BatchReceivePolicy.builder()
                        .maxNumBytes(64)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 1000, false
                },
                // Number of message limitation exceed receiverQueue size
                {
                    BatchReceivePolicy.builder()
                        .maxNumMessages(70)
                        .build(), true, 50, false
                },
                // Number of message limitation exceed receiverQueue size and timeout limitation
                {
                    BatchReceivePolicy.builder()
                        .maxNumMessages(50)
                        .timeout(10, TimeUnit.MILLISECONDS)
                        .build(), true, 30, false
                },
                // Number of message limitation is negative and timeout limitation
                {
                    BatchReceivePolicy.builder()
                        .maxNumMessages(-10)
                        .timeout(10, TimeUnit.MILLISECONDS)
                        .build(), true, 10, false
                },
                // Size of message limitation is negative and timeout limitation
                {
                    BatchReceivePolicy.builder()
                        .maxNumBytes(-100)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), true, 30, false
                },
                // Number of message limitation and size of message limitation are both negative and timeout limitation
                {
                    BatchReceivePolicy.builder()
                        .maxNumMessages(-10)
                        .maxNumBytes(-100)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), true, 30, false
                },
                // Number of message limitation exceed receiverQueue size
                {
                    BatchReceivePolicy.builder()
                        .maxNumMessages(70)
                        .build(), false, 50, false
                },
                // Number of message limitation exceed receiverQueue size and timeout limitation
                {
                    BatchReceivePolicy.builder()
                        .maxNumMessages(50)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 30, false
                },
                // Number of message limitation is negative and timeout limitation
                {
                    BatchReceivePolicy.builder()
                        .maxNumMessages(-10)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 30, false
                },
                // Size of message limitation is negative and timeout limitation
                {
                    BatchReceivePolicy.builder()
                        .maxNumBytes(-100)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 30, false
                },
                // Number of message limitation and size of message limitation are both negative and timeout limitation
                {
                    BatchReceivePolicy.builder()
                        .maxNumMessages(-10)
                        .maxNumBytes(-100)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 30, false
                },
                // Only timeout present
                {
                    BatchReceivePolicy.builder()
                            .maxNumMessages(0)
                            .maxNumBytes(0)
                            .timeout(50, TimeUnit.MILLISECONDS)
                            .build(), false, 30, false
                },
                // Only timeout present
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(-1)
                                .maxNumBytes(-1)
                                .timeout(50, TimeUnit.MILLISECONDS)
                                .build(), false, 30, false
                },

                // Ack receipt
                // Default batch receive policy.
                { BatchReceivePolicy.DEFAULT_POLICY, true, 1000, true },
                // Only receive timeout limitation.
                { BatchReceivePolicy.builder()
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), true, 1000, true
                },
                // Only number of messages in a single batch receive limitation.
                { BatchReceivePolicy.builder()
                        .maxNumMessages(10)
                        .build(), true, 1000, true
                },
                // Number of messages and timeout limitation
                { BatchReceivePolicy.builder()
                        .maxNumMessages(13)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), true, 1000, true
                },
                // Size of messages and timeout limitation
                { BatchReceivePolicy.builder()
                        .maxNumBytes(64)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), true, 1000, true
                },
                // Default batch receive policy.
                { BatchReceivePolicy.DEFAULT_POLICY, false, 1000, true },
                // Only receive timeout limitation.
                { BatchReceivePolicy.builder()
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 1000, true
                },
                // Only number of messages in a single batch receive limitation.
                { BatchReceivePolicy.builder()
                        .maxNumMessages(10)
                        .build(), false, 1000, true
                },
                // Number of messages and timeout limitation
                { BatchReceivePolicy.builder()
                        .maxNumMessages(13)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 1000, true
                },
                // Size of messages and timeout limitation
                { BatchReceivePolicy.builder()
                        .maxNumBytes(64)
                        .timeout(50, TimeUnit.MILLISECONDS)
                        .build(), false, 1000, true
                },
                // Number of message limitation exceed receiverQueue size
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(70)
                                .build(), true, 50, true
                },
                // Number of message limitation exceed receiverQueue size and timeout limitation
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(50)
                                .timeout(10, TimeUnit.MILLISECONDS)
                                .build(), true, 30, true
                },
                // Number of message limitation is negative and timeout limitation
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(-10)
                                .timeout(10, TimeUnit.MILLISECONDS)
                                .build(), true, 10, true
                },
                // Size of message limitation is negative and timeout limitation
                {
                        BatchReceivePolicy.builder()
                                .maxNumBytes(-100)
                                .timeout(50, TimeUnit.MILLISECONDS)
                                .build(), true, 30, true
                },
                // Number of message limitation and size of message limitation are both negative and timeout limitation
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(-10)
                                .maxNumBytes(-100)
                                .timeout(50, TimeUnit.MILLISECONDS)
                                .build(), true, 30, true
                },
                // Number of message limitation exceed receiverQueue size
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(70)
                                .build(), false, 50, true
                },
                // Number of message limitation exceed receiverQueue size and timeout limitation
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(50)
                                .timeout(50, TimeUnit.MILLISECONDS)
                                .build(), false, 30, true
                },
                // Number of message limitation is negative and timeout limitation
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(-10)
                                .timeout(50, TimeUnit.MILLISECONDS)
                                .build(), false, 30, true
                },
                // Size of message limitation is negative and timeout limitation
                {
                        BatchReceivePolicy.builder()
                                .maxNumBytes(-100)
                                .timeout(50, TimeUnit.MILLISECONDS)
                                .build(), false, 30, true
                },
                // Number of message limitation and size of message limitation are both negative and timeout limitation
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(-10)
                                .maxNumBytes(-100)
                                .timeout(50, TimeUnit.MILLISECONDS)
                                .build(), false, 30, true
                },
                // Only timeout present
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(0)
                                .maxNumBytes(0)
                                .timeout(50, TimeUnit.MILLISECONDS)
                                .build(), false, 30, true
                },
                // Only timeout present
                {
                        BatchReceivePolicy.builder()
                                .maxNumMessages(-1)
                                .maxNumBytes(-1)
                                .timeout(50, TimeUnit.MILLISECONDS)
                                .build(), false, 30, true
                }
        };
    }

    @Test(dataProvider = "batchReceivePolicy")
    public void testBatchReceiveNonPartitionedTopic(BatchReceivePolicy batchReceivePolicy,
                                                    boolean batchProduce,
                                                    int receiverQueueSize,
                                                    boolean isEnableAckReceipt) throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-receive-non-partition-" + UUID.randomUUID();
        testBatchReceive(topic, batchReceivePolicy, batchProduce, receiverQueueSize, isEnableAckReceipt);
    }

    @Test(dataProvider = "batchReceivePolicy")
    public void testBatchReceivePartitionedTopic(BatchReceivePolicy batchReceivePolicy,
                                                 boolean batchProduce,
                                                 int receiverQueueSize,
                                                 boolean isEnableAckReceipt) throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-receive-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        testBatchReceive(topic, batchReceivePolicy, batchProduce, receiverQueueSize, isEnableAckReceipt);
    }

    @Test(dataProvider = "batchReceivePolicy")
    public void testAsyncBatchReceiveNonPartitionedTopic(BatchReceivePolicy batchReceivePolicy,
                                                         boolean batchProduce,
                                                         int receiverQueueSize,
                                                         boolean isEnableAckReceipt) throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-receive-non-partition-async-" + UUID.randomUUID();
        testBatchReceiveAsync(topic, batchReceivePolicy, batchProduce, receiverQueueSize, isEnableAckReceipt);
    }

    @Test(dataProvider = "batchReceivePolicy")
    public void testAsyncBatchReceivePartitionedTopic(BatchReceivePolicy batchReceivePolicy,
                                                      boolean batchProduce,
                                                      int receiverQueueSize,
                                                      boolean isEnableAckReceipt) throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-receive-async-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        testBatchReceiveAsync(topic, batchReceivePolicy, batchProduce, receiverQueueSize, isEnableAckReceipt);
    }

    @Test(dataProvider = "batchReceivePolicy")
    public void testBatchReceiveAndRedeliveryNonPartitionedTopic(BatchReceivePolicy batchReceivePolicy,
                                                                 boolean batchProduce,
                                                                 int receiverQueueSize,
                                                                 boolean isEnableAckReceipt) throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-receive-and-redelivery-non-partition-" + UUID.randomUUID();
        testBatchReceiveAndRedelivery(topic, batchReceivePolicy, batchProduce, receiverQueueSize, isEnableAckReceipt);
    }

    @Test(dataProvider = "batchReceivePolicy")
    public void testBatchReceiveAndRedeliveryPartitionedTopic(BatchReceivePolicy batchReceivePolicy,
                                                              boolean batchProduce,
                                                              int receiverQueueSize,
                                                              boolean isEnableAckReceipt) throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-receive-and-redelivery-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        testBatchReceiveAndRedelivery(topic, batchReceivePolicy, batchProduce, receiverQueueSize, isEnableAckReceipt);
    }

    @Test
    public void verifyBatchSizeIsEqualToPolicyConfiguration() throws Exception {
        final int muxNumMessages = 100;
        final int messagesToSend = 500;

        final String topic = "persistent://my-property/my-ns/batch-receive-size" + UUID.randomUUID();
        BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder().maxNumMessages(muxNumMessages).build();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s2")
                .batchReceivePolicy(batchReceivePolicy)
                .subscribe();

        sendMessagesAsyncAndWait(producer, messagesToSend);
        receiveAllBatchesAndVerifyBatchSizeIsEqualToMaxNumMessages(consumer, batchReceivePolicy, messagesToSend / muxNumMessages);
    }

    @Test
    public void verifyNumBytesSmallerThanMessageSize() throws Exception {
        final int messagesToSend = 500;

        final String topic = "persistent://my-property/my-ns/batch-receive-" + UUID.randomUUID();
        BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder().maxNumBytes(10).build();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s2")
                .batchReceivePolicy(batchReceivePolicy)
                .subscribe();

        sendMessagesAsyncAndWait(producer, messagesToSend);
        CountDownLatch latch = new CountDownLatch(messagesToSend+1);
        receiveAsync(consumer, messagesToSend, latch);
        latch.await();
    }

    @Test(dataProvider = "partitioned")
    public void testBatchReceiveTimeoutTask(boolean partitioned) throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-receive-" + UUID.randomUUID();

        if (partitioned) {
            admin.topics().createPartitionedTopic(topic, 3);
        }

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .receiverQueueSize(1)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        .maxNumBytes(1024 * 1024)
                        .maxNumMessages(1)
                        .timeout(5, TimeUnit.SECONDS)
                        .build())
                .subscribe();
        Assert.assertFalse(((ConsumerBase<?>)consumer).hasBatchReceiveTimeout());
        final int messagesToSend = 500;
        sendMessagesAsyncAndWait(producer, messagesToSend);
        for (int i = 0; i < 100; i++) {
            Assert.assertNotNull(consumer.receive());
        }
        Assert.assertFalse(((ConsumerBase<?>)consumer).hasBatchReceiveTimeout());
        for (int i = 0; i < 400; i++) {
            Messages<String> batchReceived = consumer.batchReceive();
            Assert.assertEquals(batchReceived.size(), 1);
        }
        Awaitility.await().untilAsserted(() -> Assert.assertFalse(((ConsumerBase<?>)consumer).hasBatchReceiveTimeout()));
        Assert.assertEquals(consumer.batchReceive().size(), 0);
        Awaitility.await().untilAsserted(() -> Assert.assertFalse(((ConsumerBase<?>)consumer).hasBatchReceiveTimeout()));
    }


    private void receiveAllBatchesAndVerifyBatchSizeIsEqualToMaxNumMessages(Consumer<String> consumer,
                                                                            BatchReceivePolicy batchReceivePolicy,
                                                                            int numOfExpectedBatches) throws PulsarClientException {
        Messages<String> messages;
        for (int i = 0; i < numOfExpectedBatches; i++) {
            messages = consumer.batchReceive();
            log.info("Received {} messages in a single batch receive verifying batch size.", messages.size());
            Assert.assertEquals(messages.size(), batchReceivePolicy.getMaxNumMessages());
        }
    }

    private void testBatchReceive(String topic, BatchReceivePolicy batchReceivePolicy, boolean batchProduce,
                                  int receiverQueueSize, boolean enableAckReceipt) throws Exception {
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING).topic(topic);
        if (!batchProduce) {
            producerBuilder.enableBatching(false);
        }
        @Cleanup
        Producer<String> producer = producerBuilder.create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .receiverQueueSize(receiverQueueSize)
                .batchReceivePolicy(batchReceivePolicy)
                .isAckReceiptEnabled(enableAckReceipt)
                .enableBatchIndexAcknowledgment(enableAckReceipt)
                .subscribe();
        sendMessagesAsyncAndWait(producer, 100);
        batchReceiveAndCheck(consumer, 100);
    }

    private void testBatchReceiveAsync(String topic,
                                       BatchReceivePolicy batchReceivePolicy,
                                       boolean batchProduce,
                                       int receiverQueueSize,
                                       boolean isEnableAckReceipt) throws Exception {
        if (batchReceivePolicy.getTimeoutMs() <= 0) {
            return;
        }

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().ioThreads(10).serviceUrl(lookupUrl.toString()).build();
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING).topic(topic);
        if (!batchProduce) {
            producerBuilder.enableBatching(false);
        }

        @Cleanup
        Producer<String> producer = producerBuilder.create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .receiverQueueSize(receiverQueueSize)
                .batchReceivePolicy(batchReceivePolicy)
                .isAckReceiptEnabled(isEnableAckReceipt)
                .enableBatchIndexAcknowledgment(isEnableAckReceipt)
                .subscribe();

        sendMessagesAsyncAndWait(producer, 100);
        CountDownLatch latch = new CountDownLatch(101);
        receiveAsync(consumer, 100, latch);
        latch.await();
    }

    private void testBatchReceiveAndRedelivery(String topic,
                                               BatchReceivePolicy batchReceivePolicy,
                                               boolean batchProduce,
                                               int receiverQueueSize,
                                               boolean isEnableAckReceipt) throws Exception {
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING).topic(topic);
        if (!batchProduce) {
            producerBuilder.enableBatching(false);
        }
        @Cleanup
        Producer<String> producer = producerBuilder.create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .receiverQueueSize(receiverQueueSize)
                .batchReceivePolicy(batchReceivePolicy)
                .isAckReceiptEnabled(isEnableAckReceipt)
                .enableBatchIndexAcknowledgment(isEnableAckReceipt)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscribe();
        sendMessagesAsyncAndWait(producer, 100);
        batchReceiveAndRedelivery(consumer, 100);
    }

    private void receiveAsync(Consumer<String> consumer, int expected, CountDownLatch latch) {
        consumer.batchReceiveAsync().thenAccept(messages -> {
            if (messages != null) {
                log.info("Received {} messages in a single batch receive.", messages.size());
                for (Message<String> message : messages) {
                    Assert.assertNotNull(message.getValue());
                    log.info("Get message {} from batch", message.getValue());
                    latch.countDown();
                }
                consumer.acknowledgeAsync(messages);
                if (messages.size() < expected) {
                    ForkJoinPool.commonPool().execute(() -> receiveAsync(consumer, expected - messages.size(), latch));
                } else {
                    Assert.assertEquals(expected - messages.size(), 0);
                    latch.countDown();
                }
            }
        });
    }

    private void sendMessagesAsyncAndWait(Producer<String> producer, int messages) throws Exception {
        CountDownLatch latch = new CountDownLatch(messages);
        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message).thenAccept(messageId -> {
                log.info("Message {} published {}", message, messageId);
                if (messageId != null) {
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

    private void batchReceiveAndCheck(Consumer<String> consumer, int expected) throws Exception {
        Messages<String> messages;
        int messageReceived = 0;
        do {
            messages = consumer.batchReceive();
            if (messages != null) {
                messageReceived += messages.size();
                log.info("Received {} messages in a single batch receive.", messages.size());
                for (Message<String> message : messages) {
                    Assert.assertNotNull(message.getValue());
                    log.info("Get message {} from batch", message.getValue());
                }
                consumer.acknowledge(messages);
            }
        } while (messageReceived < expected);
        Assert.assertEquals(expected, messageReceived);
    }

    private void batchReceiveAndRedelivery(Consumer<String> consumer, int expected) throws Exception {
        Messages<String> messages;
        int messageReceived = 0;
        do {
            messages = consumer.batchReceive();
            if (messages != null) {
                messageReceived += messages.size();
                log.info("Received {} messages in a single batch receive.", messages.size());
                for (Message<String> message : messages) {
                    Assert.assertNotNull(message.getValue());
                    log.info("Get message {} from batch", message.getValue());
                    // don't ack, test message redelivery
                }
            }
        } while (messageReceived < expected);
        Assert.assertEquals(expected, messageReceived);

        do {
            messages = consumer.batchReceive();
            if (messages != null) {
                messageReceived += messages.size();
                log.info("Received {} messages in a single batch receive.", messages.size());
                for (Message<String> message : messages) {
                    Assert.assertNotNull(message.getValue());
                    log.info("Get message {} from batch", message.getValue());
                }
            }
            consumer.acknowledge(messages);
        } while (messageReceived < expected * 2);
        Assert.assertEquals(expected * 2, messageReceived);
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerBatchReceiveTest.class);
}
