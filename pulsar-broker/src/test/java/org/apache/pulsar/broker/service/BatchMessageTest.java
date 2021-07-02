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
package org.apache.pulsar.broker.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BatchMessageTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "codecAndContainerBuilder")
    public Object[][] codecAndContainerBuilderProvider() {
        return new Object[][] {
                { CompressionType.NONE, BatcherBuilder.DEFAULT },
                { CompressionType.LZ4, BatcherBuilder.DEFAULT },
                { CompressionType.ZLIB, BatcherBuilder.DEFAULT },
                { CompressionType.NONE, BatcherBuilder.KEY_BASED },
                { CompressionType.LZ4, BatcherBuilder.KEY_BASED },
                { CompressionType.ZLIB, BatcherBuilder.KEY_BASED }
        };
    }

    @DataProvider(name = "containerBuilder")
    public Object[][] containerBuilderProvider() {
        return new Object[][] {
                { BatcherBuilder.DEFAULT },
                { BatcherBuilder.KEY_BASED }
        };
    }

    @Test(dataProvider = "codecAndContainerBuilder")
    public void testSimpleBatchProducerWithFixedBatchSize(CompressionType compressionType, BatcherBuilder builder) throws Exception {
        int numMsgs = 50;
        int numMsgsInBatch = numMsgs / 2;
        final String topicName = "persistent://prop/ns-abc/testSimpleBatchProducerWithFixedBatchSize-" + UUID.randomUUID();
        final String subscriptionName = "sub-1" + compressionType.toString();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).compressionType(compressionType)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS).batchingMaxMessages(numMsgsInBatch).enableBatching(true)
                .batcherBuilder(builder)
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        // we expect 2 messages in the backlog since we sent 50 messages with the batch size set to 25. We have set the
        // batch time high enough for it to not affect the number of messages in the batch
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 2);
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();

        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            Assert.assertEquals(receivedMessage, expectedMessage,
                    "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);
        }
        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "codecAndContainerBuilder")
    public void testSimpleBatchProducerWithFixedBatchBytes(CompressionType compressionType, BatcherBuilder builder) throws Exception {
        int numMsgs = 50;
        int numBytesInBatch = 600;
        final String topicName = "persistent://prop/ns-abc/testSimpleBatchProducerWithFixedBatchSize-" + UUID.randomUUID();
        final String subscriptionName = "sub-1" + compressionType.toString();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .compressionType(compressionType)
            .batchingMaxPublishDelay(5, TimeUnit.SECONDS)
            .batchingMaxMessages(0)
            .batchingMaxBytes(numBytesInBatch)
            .enableBatching(true)
            .batcherBuilder(builder)
            .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        // we expect 2 messages in the backlog since we sent 50 messages with the batch size set to 25. We have set the
        // batch time high enough for it to not affect the number of messages in the batch
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 2);
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();

        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            Assert.assertEquals(receivedMessage, expectedMessage,
                    "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);
        }
        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "codecAndContainerBuilder")
    public void testSimpleBatchProducerWithFixedBatchTime(CompressionType compressionType, BatcherBuilder builder) throws Exception {
        int numMsgs = 100;
        final String topicName = "persistent://prop/ns-abc/testSimpleBatchProducerWithFixedBatchTime-" + UUID.randomUUID();
        final String subscriptionName = "time-sub-1" + compressionType.toString();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).compressionType(compressionType)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS).enableBatching(true)
                .batcherBuilder(builder)
                .create();

        Random random = new Random();
        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            // put a random sleep from 0 to 3 ms
            Thread.sleep(random.nextInt(4));
            byte[] message = ("msg-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        LOG.info("Sent {} messages, backlog is {} messages", numMsgs,
                topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false));
        assertTrue(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false) < numMsgs);

        producer.close();
    }

    @Test(dataProvider = "codecAndContainerBuilder")
    public void testSimpleBatchProducerWithFixedBatchSizeAndTime(CompressionType compressionType, BatcherBuilder builder) throws Exception {
        int numMsgs = 100;
        final String topicName = "persistent://prop/ns-abc/testSimpleBatchProducerWithFixedBatchSizeAndTime-" + UUID.randomUUID();
        final String subscriptionName = "time-size-sub-1" + compressionType.toString();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS).batchingMaxMessages(5)
                .batcherBuilder(builder)
                .compressionType(compressionType).enableBatching(true).create();

        Random random = new Random();
        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            // put a random sleep from 0 to 3 ms
            Thread.sleep(random.nextInt(4));
            byte[] message = ("msg-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        LOG.info("Sent {} messages, backlog is {} messages", numMsgs,
                topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false));
        assertTrue(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false) < numMsgs);

        producer.close();
    }

    @Test(dataProvider = "codecAndContainerBuilder")
    public void testBatchProducerWithLargeMessage(CompressionType compressionType, BatcherBuilder builder) throws Exception {
        int numMsgs = 50;
        int numMsgsInBatch = numMsgs / 2;
        final String topicName = "persistent://prop/ns-abc/testBatchProducerWithLargeMessage-" + UUID.randomUUID();
        final String subscriptionName = "large-message-sub-1" + compressionType.toString();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)

                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).compressionType(compressionType)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS).batchingMaxMessages(numMsgsInBatch).enableBatching(true)
                .batcherBuilder(builder)
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            if (i == 25) {
                // send a large message
                byte[] largeMessage = new byte[128 * 1024 + 4];
                sendFutureList.add(producer.sendAsync(largeMessage));
            } else {
                byte[] message = ("msg-" + i).getBytes();
                sendFutureList.add(producer.sendAsync(message));
            }
        }
        byte[] lastMsg = ("msg-" + "last").getBytes();
        sendFutureList.add(producer.sendAsync(lastMsg));

        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        // we expect 3 messages in the backlog since the large message in the middle should
        // close out the batch and be sent in a batch of its own
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 3);
        consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscribe();

        for (int i = 0; i <= numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            LOG.info("received msg - {}", Arrays.toString(msg.getData()));
            consumer.acknowledge(msg);
        }
        Thread.sleep(100);
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 0);
        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "codecAndContainerBuilder")
    public void testSimpleBatchProducerConsumer(CompressionType compressionType, BatcherBuilder builder) throws Exception {
        int numMsgs = 500;
        int numMsgsInBatch = numMsgs / 20;
        final String topicName = "persistent://prop/ns-abc/testSimpleBatchProducerConsumer-" + UUID.randomUUID();
        final String subscriptionName = "pc-sub-1" + compressionType.toString();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .compressionType(compressionType)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            // disabled time based batch by setting delay to a large enough value
            .batchingMaxPublishDelay(60, TimeUnit.HOURS)
            // disabled size based batch
            .batchingMaxMessages(2 * numMsgs)
            .enableBatching(true)
            .batcherBuilder(builder)
            .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("msg-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
            if ((i + 1) % numMsgsInBatch == 0) {
                producer.flush();
                LOG.info("Flush {} messages", (i + 1));
            }
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), numMsgs / numMsgsInBatch);
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();

        Message<byte[]> lastunackedMsg = null;
        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            if (i % 2 == 0) {
                consumer.acknowledgeCumulative(msg);
            } else {
                lastunackedMsg = msg;
            }
        }
        if (lastunackedMsg != null) {
            consumer.acknowledgeCumulative(lastunackedMsg);
        }
        Thread.sleep(100);
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 0);
        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "containerBuilder")
    public void testSimpleBatchSyncProducerWithFixedBatchSize(BatcherBuilder builder) throws Exception {
        int numMsgs = 10;
        int numMsgsInBatch = numMsgs / 2;
        final String topicName = "persistent://prop/ns-abc/testSimpleBatchSyncProducerWithFixedBatchSize-" + UUID.randomUUID();
        final String subscriptionName = "syncsub-1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS).batchingMaxMessages(numMsgsInBatch).enableBatching(true)
                .batcherBuilder(builder)
                .create();

        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            producer.send(message);
        }

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        // we expect 10 messages in the backlog since we sent 10 messages with the batch size set to 5.
        // However, we are using synchronous send and so each message will go as an individual message
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 10);
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();

        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            Assert.assertEquals(receivedMessage, expectedMessage,
                    "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);
        }
        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "containerBuilder")
    public void testSimpleBatchProducerConsumer1kMessages(BatcherBuilder builder) throws Exception {
        int numMsgs = 2000;
        int numMsgsInBatch = 4;
        final String topicName = "persistent://prop/ns-abc/testSimpleBatchProducerConsumer1kMessages-" + UUID.randomUUID();
        final String subscriptionName = "pc1k-sub-1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).maxPendingMessages(numMsgs + 1)
                .batchingMaxPublishDelay(30, TimeUnit.SECONDS).batchingMaxMessages(numMsgsInBatch).enableBatching(true)
                .batcherBuilder(builder)
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("msg-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();
        int sendError = 0;
        for (CompletableFuture<MessageId> sendFuture : sendFutureList) {
            if (sendFuture.isCompletedExceptionally()) {
                ++sendError;
            }
        }
        if (sendError != 0) {
            LOG.warn("[{}] Error sending {} messages", subscriptionName, sendError);
            numMsgs = numMsgs - sendError;
        }
        LOG.info("[{}] sent {} messages", subscriptionName, numMsgs);

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        // allow stats to be updated..
        LOG.info("[{}] checking backlog stats..", topic);
        rolloverPerIntervalStats();
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), numMsgs / numMsgsInBatch);
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();

        Message<byte[]> lastunackedMsg = null;
        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            assertNotNull(msg);
            lastunackedMsg = msg;
        }
        if (lastunackedMsg != null) {
            consumer.acknowledgeCumulative(lastunackedMsg);
        }

        consumer.close();
        producer.close();
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 0);
    }

    // test for ack holes
    /*
     * lid eid bid 0 0 1-10 ack type cumul till id 9 0 1 1-10 ack type cumul on batch id 5. (should remove 0,1, 10 also
     * on broker) individual ack on 6-10. (if ack type individual on bid 5, then hole remains which is ok) 0 2 1-10 0 3
     * 1-10
     */
    @Test(groups = "broker")
    public void testOutOfOrderAcksForBatchMessage() throws Exception {
        int numMsgs = 40;
        int numMsgsInBatch = numMsgs / 4;
        final String topicName = "persistent://prop/ns-abc/testOutOfOrderAcksForBatchMessage";
        final String subscriptionName = "oooack-sub-1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS).batchingMaxMessages(numMsgsInBatch).enableBatching(true)
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("msg-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), numMsgs / numMsgsInBatch);
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
        Set<Integer> individualAcks = new HashSet<>();
        for (int i = 15; i < 20; i++) {
            individualAcks.add(i);
        }
        Message<byte[]> lastunackedMsg = null;
        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            LOG.info("received message {}", new String(msg.getData(), UTF_8));
            assertNotNull(msg);
            if (i == 8) {
                consumer.acknowledgeCumulative(msg);
            } else if (i == 9) {
                // do not ack
            } else if (i == 14) {
                // should ack lid =0 eid = 1 on broker
                consumer.acknowledgeCumulative(msg);
                Thread.sleep(1000);
                rolloverPerIntervalStats();
                Thread.sleep(1000);
                assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 3);
            } else if (individualAcks.contains(i)) {
                consumer.acknowledge(msg);
            } else {
                lastunackedMsg = msg;
            }
        }
        Thread.sleep(1000);
        rolloverPerIntervalStats();
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 2);
        if (lastunackedMsg != null) {
            consumer.acknowledgeCumulative(lastunackedMsg);
        }
        Thread.sleep(100);
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 0);
        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "containerBuilder")
    public void testNonBatchCumulativeAckAfterBatchPublish(BatcherBuilder builder) throws Exception {
        int numMsgs = 10;
        int numMsgsInBatch = numMsgs;
        final String topicName = "persistent://prop/ns-abc/testNonBatchCumulativeAckAfterBatchPublish-" + UUID.randomUUID();
        final String subscriptionName = "nbcaabp-sub-1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS).batchingMaxMessages(numMsgsInBatch).enableBatching(true)
                .batcherBuilder(builder)
                .create();

        // create producer to publish non batch messages
        Producer<byte[]> noBatchProducer = pulsarClient.newProducer().topic(topicName).create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("msg-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));

        }
        FutureUtil.waitForAll(sendFutureList).get();
        sendFutureList.clear();
        byte[] nobatchmsg = ("nobatch").getBytes();
        noBatchProducer.sendAsync(nobatchmsg).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 2);
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();

        Message<byte[]> lastunackedMsg = null;
        for (int i = 0; i <= numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            lastunackedMsg = msg;
        }
        consumer.acknowledgeCumulative(lastunackedMsg);
        Thread.sleep(100);
        rolloverPerIntervalStats();
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 0);
        consumer.close();
        producer.close();
        noBatchProducer.close();
    }

    @Test(dataProvider = "containerBuilder")
    public void testBatchAndNonBatchCumulativeAcks(BatcherBuilder builder) throws Exception {
        int numMsgs = 50;
        int numMsgsInBatch = numMsgs / 10;
        final String topicName = "persistent://prop/ns-abc/testBatchAndNonBatchCumulativeAcks-" + UUID.randomUUID();
        final String subscriptionName = "bnb-sub-1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .batchingMaxPublishDelay(5, TimeUnit.SECONDS)
            .batchingMaxMessages(numMsgsInBatch)
            .enableBatching(true)
            .batcherBuilder(builder)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        // create producer to publish non batch messages
        Producer<byte[]> noBatchProducer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs / 2; i++) {
            byte[] message = ("msg-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
            byte[] nobatchmsg = ("nobatch-" + i).getBytes();
            sendFutureList.add(noBatchProducer.sendAsync(nobatchmsg));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false),
                (numMsgs / 2) / numMsgsInBatch + numMsgs / 2);
        consumer = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subscriptionName)
                    .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                    .subscribe();

        Message<byte[]> lastunackedMsg = null;
        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            LOG.info("[{}] got message position{} data {}", subscriptionName, msg.getMessageId(),
                    Arrays.toString(msg.getData()));
            if (i % 2 == 0) {
                lastunackedMsg = msg;
            } else {
                consumer.acknowledgeCumulative(msg);
                LOG.info("[{}] did cumulative ack on position{} ", subscriptionName, msg.getMessageId());
            }
        }
        consumer.acknowledgeCumulative(lastunackedMsg);

        retryStrategically(t -> topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false) == 0, 100, 100);

        consumer.close();
        producer.close();
        noBatchProducer.close();
    }

    /**
     * Verifies batch-message acking is thread-safe
     *
     * @throws Exception
     */
    @Test(dataProvider = "containerBuilder", timeOut = 3000)
    public void testConcurrentBatchMessageAck(BatcherBuilder builder) throws Exception {
        int numMsgs = 10;
        final String topicName = "persistent://prop/ns-abc/testConcurrentAck-" + UUID.randomUUID();
        final String subscriptionName = "sub-1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS).batchingMaxMessages(numMsgs).enableBatching(true)
                .batcherBuilder(builder)
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        final Consumer<byte[]> myConsumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).subscriptionType(SubscriptionType.Shared).subscribe();
        // assertEquals(dispatcher.getTotalUnackedMessages(), 1);
        ExecutorService executor = Executors.newFixedThreadPool(10);

        final CountDownLatch latch = new CountDownLatch(numMsgs);
        final AtomicBoolean failed = new AtomicBoolean(false);
        for (int i = 0; i < numMsgs; i++) {
            executor.submit(() -> {
                try {
                    Message<byte[]> msg = myConsumer.receive(1, TimeUnit.SECONDS);
                    myConsumer.acknowledge(msg);
                } catch (Exception e) {
                    failed.set(false);
                }
                latch.countDown();
            });
        }
        latch.await();

        PersistentDispatcherMultipleConsumers dispatcher = (PersistentDispatcherMultipleConsumers) topic
                .getSubscription(subscriptionName).getDispatcher();
        // check strategically to let ack-message receive by broker
        retryStrategically((test) -> dispatcher.getConsumers().get(0).getUnackedMessages() == 0, 50, 150);
        assertEquals(dispatcher.getConsumers().get(0).getUnackedMessages(), 0);

        executor.shutdownNow();
        myConsumer.close();
        producer.close();
    }

    @Test
    public void testOrderingOfKeyBasedBatchMessageContainer() throws PulsarClientException, ExecutionException, InterruptedException {
        final String topicName = "persistent://prop/ns-abc/testKeyBased";
        final String subscriptionName = "sub-1";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS)
                .batchingMaxMessages(30)
                .enableBatching(true)
                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();
        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        String[] keys = new String[]{"key-1", "key-2", "key-3"};
        for (int i = 0; i < 10; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            for (String key : keys) {
                sendFutureList.add(producer.newMessage().key(key).value(message).sendAsync());
            }
        }
        FutureUtil.waitForAll(sendFutureList).get();

        String receivedKey = "";
        int receivedMessageIndex = 0;
        for (int i = 0; i < 30; i++) {
            Message<byte[]> received = consumer.receive();
            if (!received.getKey().equals(receivedKey)) {
                receivedKey = received.getKey();
                receivedMessageIndex = 0;
            }
            assertEquals(new String(received.getValue()), "my-message-" + receivedMessageIndex % 10);
            consumer.acknowledge(received);
            receivedMessageIndex++;
        }

        for (int i = 0; i < 10; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            for (String key : keys) {
                sendFutureList.add(producer.newMessage()
                        .key(UUID.randomUUID().toString())
                        .orderingKey(key.getBytes())
                        .value(message)
                        .sendAsync());
            }
        }
        FutureUtil.waitForAll(sendFutureList).get();

        receivedKey = "";
        receivedMessageIndex = 0;
        for (int i = 0; i < 30; i++) {
            Message<byte[]> received = consumer.receive();
            if (!new String(received.getOrderingKey()).equals(receivedKey)) {
                receivedKey = new String(received.getOrderingKey());
                receivedMessageIndex = 0;
            }
            assertEquals(new String(received.getValue()), "my-message-" + receivedMessageIndex % 10);
            consumer.acknowledge(received);
            receivedMessageIndex++;
        }

        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "containerBuilder")
    public void testRetrieveSequenceIdGenerated(BatcherBuilder builder) throws Exception {

        int numMsgs = 10;
        final String topicName = "persistent://prop/ns-abc/testRetrieveSequenceIdGenerated-" + UUID.randomUUID();
        final String subscriptionName = "sub-1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS).batchingMaxMessages(numMsgs).enableBatching(true)
                .batcherBuilder(builder)
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            sendFutureList.add(producer.sendAsync(message));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> received = consumer.receive();
            Assert.assertEquals(received.getSequenceId(), i);
            consumer.acknowledge(received);
        }

        producer.close();
        consumer.close();
    }

    @Test(dataProvider = "containerBuilder")
    public void testRetrieveSequenceIdSpecify(BatcherBuilder builder) throws Exception {

        int numMsgs = 10;
        final String topicName = "persistent://prop/ns-abc/testRetrieveSequenceIdSpecify-" + UUID.randomUUID();
        final String subscriptionName = "sub-1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS).batchingMaxMessages(numMsgs).enableBatching(true)
                .batcherBuilder(builder)
                .create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            sendFutureList.add(producer.newMessage().sequenceId(i + 100).value(message).sendAsync());
        }
        FutureUtil.waitForAll(sendFutureList).get();

        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> received = consumer.receive();
            Assert.assertEquals(received.getSequenceId(), i + 100);
            consumer.acknowledge(received);
        }

        producer.close();
        consumer.close();
    }

    @Test(dataProvider = "codecAndContainerBuilder")
    public void testSendOverSizeMessage(CompressionType compressionType, BatcherBuilder builder) throws Exception {

        final int numMsgs = 10;
        final String topicName = "persistent://prop/ns-abc/testSendOverSizeMessage-" + UUID.randomUUID();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(2)
                .enableBatching(true)
                .compressionType(compressionType)
                .batcherBuilder(builder)
                .create();

        try {
            producer.send(new byte[1024 * 1024 * 10]);
        } catch (PulsarClientException e) {
            assertTrue(e instanceof PulsarClientException.InvalidMessageException);
        }

        for (int i = 0; i < numMsgs; i++) {
            producer.send(new byte[1024]);
        }

        producer.close();

    }

    @Test
    public void testBatchMessageDispatchingAccordingToPermits() throws Exception {

        int numMsgs = 1000;
        int batchMessages = 10;
        final String topicName = "persistent://prop/ns-abc/testRetrieveSequenceIdSpecify-" + UUID.randomUUID();
        final String subscriptionName = "sub-1";

        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(10).subscriptionType(SubscriptionType.Shared)
                .subscribe();

        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subscriptionName).receiverQueueSize(10).subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).batchingMaxMessages(batchMessages)
                .batchingMaxPublishDelay(500, TimeUnit.MILLISECONDS).enableBatching(true).create();

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            sendFutureList.add(producer.newMessage().value(message).sendAsync());
        }
        FutureUtil.waitForAll(sendFutureList).get();

        assertEquals(consumer1.numMessagesInQueue(), batchMessages, batchMessages);
        assertEquals(consumer2.numMessagesInQueue(), batchMessages, batchMessages);

        producer.close();
        consumer1.close();
    }

    private static final Logger LOG = LoggerFactory.getLogger(BatchMessageTest.class);
}
