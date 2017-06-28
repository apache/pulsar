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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class BatchMessageTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "codec")
    public Object[][] codecProvider() {
        return new Object[][] { { CompressionType.NONE }, { CompressionType.LZ4 }, { CompressionType.ZLIB }, };
    }

    @Test(dataProvider = "codec")
    public void testSimpleBatchProducerWithFixedBatchSize(CompressionType compressionType) throws Exception {
        int numMsgs = 50;
        int numMsgsInBatch = numMsgs / 2;
        final String topicName = "persistent://prop/use/ns-abc/testSimpleBatchProducerWithFixedBatchSize";
        final String subscriptionName = "sub-1" + compressionType.toString();

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setCompressionType(compressionType);
        producerConf.setBatchingMaxPublishDelay(5000, TimeUnit.MILLISECONDS);
        producerConf.setBatchingMaxMessages(numMsgsInBatch);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            Message msg = MessageBuilder.create().setContent(message).build();
            sendFutureList.add(producer.sendAsync(msg));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        // we expect 2 messages in the backlog since we sent 50 messages with the batch size set to 25. We have set the
        // batch time high enough for it to not affect the number of messages in the batch
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 2);
        consumer = pulsarClient.subscribe(topicName, subscriptionName);

        for (int i = 0; i < numMsgs; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            Assert.assertEquals(receivedMessage, expectedMessage,
                    "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);
        }
        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "codec")
    public void testSimpleBatchProducerWithFixedBatchTime(CompressionType compressionType) throws Exception {
        int numMsgs = 100;
        final String topicName = "persistent://prop/use/ns-abc/testSimpleBatchProducerWithFixedBatchTime";
        final String subscriptionName = "time-sub-1" + compressionType.toString();

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setCompressionType(compressionType);
        producerConf.setBatchingMaxPublishDelay(10, TimeUnit.MILLISECONDS);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);

        Random random = new Random();
        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            // put a random sleep from 0 to 3 ms
            Thread.sleep(random.nextInt(4));
            byte[] message = ("msg-" + i).getBytes();
            Message msg = MessageBuilder.create().setContent(message).build();
            sendFutureList.add(producer.sendAsync(msg));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        LOG.info("Sent {} messages, backlog is {} messages", numMsgs,
                topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog());
        assertTrue(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog() < numMsgs);

        producer.close();
    }

    @Test(dataProvider = "codec")
    public void testSimpleBatchProducerWithFixedBatchSizeAndTime(CompressionType compressionType) throws Exception {
        int numMsgs = 100;
        final String topicName = "persistent://prop/use/ns-abc/testSimpleBatchProducerWithFixedBatchSizeAndTime";
        final String subscriptionName = "time-size-sub-1" + compressionType.toString();

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setBatchingMaxPublishDelay(10, TimeUnit.MILLISECONDS);
        producerConf.setBatchingMaxMessages(5);
        producerConf.setCompressionType(compressionType);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);

        Random random = new Random();
        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            // put a random sleep from 0 to 3 ms
            Thread.sleep(random.nextInt(4));
            byte[] message = ("msg-" + i).getBytes();
            Message msg = MessageBuilder.create().setContent(message).build();
            sendFutureList.add(producer.sendAsync(msg));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        LOG.info("Sent {} messages, backlog is {} messages", numMsgs,
                topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog());
        assertTrue(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog() < numMsgs);

        producer.close();
    }

    @Test(dataProvider = "codec")
    public void testBatchProducerWithLargeMessage(CompressionType compressionType) throws Exception {
        int numMsgs = 50;
        int numMsgsInBatch = numMsgs / 2;
        final String topicName = "persistent://prop/use/finance/testBatchProducerWithLargeMessage";
        final String subscriptionName = "large-message-sub-1" + compressionType.toString();

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setCompressionType(compressionType);
        producerConf.setBatchingMaxPublishDelay(5000, TimeUnit.MILLISECONDS);
        producerConf.setBatchingMaxMessages(numMsgsInBatch);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            if (i == 25) {
                // send a large message
                byte[] largeMessage = new byte[128 * 1024 + 4];
                Message msg = MessageBuilder.create().setContent(largeMessage).build();
                sendFutureList.add(producer.sendAsync(msg));
            } else {
                byte[] message = ("msg-" + i).getBytes();
                Message msg = MessageBuilder.create().setContent(message).build();
                sendFutureList.add(producer.sendAsync(msg));
            }
        }
        byte[] message = ("msg-" + "last").getBytes();
        Message lastMsg = MessageBuilder.create().setContent(message).build();
        sendFutureList.add(producer.sendAsync(lastMsg));

        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        // we expect 3 messages in the backlog since the large message in the middle should
        // close out the batch and be sent in a batch of its own
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 3);
        consumer = pulsarClient.subscribe(topicName, subscriptionName);

        for (int i = 0; i <= numMsgs; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            LOG.info("received msg - {}", msg.getData().toString());
            consumer.acknowledge(msg);
        }
        Thread.sleep(100);
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 0);
        consumer.close();
        producer.close();
    }

    @Test(dataProvider = "codec")
    public void testSimpleBatchProducerConsumer(CompressionType compressionType) throws Exception {
        int numMsgs = 500;
        int numMsgsInBatch = numMsgs / 20;
        final String topicName = "persistent://prop/use/ns-abc/testSimpleBatchProducerConsumer";
        final String subscriptionName = "pc-sub-1" + compressionType.toString();

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setCompressionType(compressionType);
        producerConf.setBatchingMaxPublishDelay(5000, TimeUnit.MILLISECONDS);
        producerConf.setBatchingMaxMessages(numMsgsInBatch);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("msg-" + i).getBytes();
            Message msg = MessageBuilder.create().setContent(message).build();
            sendFutureList.add(producer.sendAsync(msg));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(),
                numMsgs / numMsgsInBatch);
        consumer = pulsarClient.subscribe(topicName, subscriptionName);

        Message lastunackedMsg = null;
        for (int i = 0; i < numMsgs; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
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
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 0);
        consumer.close();
        producer.close();
    }

    @Test
    public void testSimpleBatchSyncProducerWithFixedBatchSize() throws Exception {
        int numMsgs = 10;
        int numMsgsInBatch = numMsgs / 2;
        final String topicName = "persistent://prop/use/ns-abc/testSimpleBatchSyncProducerWithFixedBatchSize";
        final String subscriptionName = "syncsub-1";

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setBatchingMaxPublishDelay(1000, TimeUnit.MILLISECONDS);
        producerConf.setBatchingMaxMessages(numMsgsInBatch);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);

        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            Message msg = MessageBuilder.create().setContent(message).build();
            producer.send(msg);
        }

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        // we expect 10 messages in the backlog since we sent 10 messages with the batch size set to 5.
        // However, we are using synchronous send and so each message will go as an individual message
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 10);
        consumer = pulsarClient.subscribe(topicName, subscriptionName);

        for (int i = 0; i < numMsgs; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            Assert.assertEquals(receivedMessage, expectedMessage,
                    "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);
        }
        consumer.close();
        producer.close();

    }

    @Test
    public void testSimpleBatchProducerConsumer1kMessages() throws Exception {
        int numMsgs = 2000;
        int numMsgsInBatch = 4;
        final String topicName = "persistent://prop/use/ns-abc/testSimpleBatchProducerConsumer1kMessages";
        final String subscriptionName = "pc1k-sub-1";

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMaxPendingMessages(numMsgs + 1);
        producerConf.setBatchingMaxPublishDelay(30000, TimeUnit.MILLISECONDS);
        producerConf.setBatchingMaxMessages(numMsgsInBatch);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("msg-" + i).getBytes();
            Message msg = MessageBuilder.create().setContent(message).build();
            sendFutureList.add(producer.sendAsync(msg));
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

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        // allow stats to be updated..
        Thread.sleep(5000);
        LOG.info("[{}] checking backlog stats..");
        rolloverPerIntervalStats();
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(),
                numMsgs / numMsgsInBatch);
        consumer = pulsarClient.subscribe(topicName, subscriptionName);

        Message lastunackedMsg = null;
        for (int i = 0; i < numMsgs; i++) {
            Message msg = consumer.receive(1, TimeUnit.SECONDS);
            assertNotNull(msg);
            lastunackedMsg = msg;
        }
        if (lastunackedMsg != null) {
            consumer.acknowledgeCumulative(lastunackedMsg);
        }
        Thread.sleep(100);
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 0);
        consumer.close();
        producer.close();
    }

    // test for ack holes
    /*
     * lid eid bid 0 0 1-10 ack type cumul till id 9 0 1 1-10 ack type cumul on batch id 5. (should remove 0,1, 10 also
     * on broker) individual ack on 6-10. (if ack type individual on bid 5, then hole remains which is ok) 0 2 1-10 0 3
     * 1-10
     */
    @Test
    public void testOutOfOrderAcksForBatchMessage() throws Exception {
        int numMsgs = 40;
        int numMsgsInBatch = numMsgs / 4;
        final String topicName = "persistent://prop/use/ns-abc/testOutOfOrderAcksForBatchMessage";
        final String subscriptionName = "oooack-sub-1";

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setBatchingMaxPublishDelay(5000, TimeUnit.MILLISECONDS);
        producerConf.setBatchingMaxMessages(numMsgsInBatch);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("msg-" + i).getBytes();
            Message msg = MessageBuilder.create().setContent(message).build();
            sendFutureList.add(producer.sendAsync(msg));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        rolloverPerIntervalStats();
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(),
                numMsgs / numMsgsInBatch);
        consumer = pulsarClient.subscribe(topicName, subscriptionName);
        Set<Integer> individualAcks = new HashSet<>();
        for (int i = 15; i < 20; i++) {
            individualAcks.add(i);
        }
        Message lastunackedMsg = null;
        for (int i = 0; i < numMsgs; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
            LOG.info("received message {}", String.valueOf(msg.getData()));
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
                assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 3);
            } else if (individualAcks.contains(i)) {
                consumer.acknowledge(msg);
            } else {
                lastunackedMsg = msg;
            }
        }
        Thread.sleep(1000);
        rolloverPerIntervalStats();
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 2);
        if (lastunackedMsg != null) {
            consumer.acknowledgeCumulative(lastunackedMsg);
        }
        Thread.sleep(100);
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 0);
        consumer.close();
        producer.close();
    }

    @Test
    public void testNonBatchCumulativeAckAfterBatchPublish() throws Exception {
        int numMsgs = 10;
        int numMsgsInBatch = numMsgs;
        final String topicName = "persistent://prop/use/ns-abc/testNonBatchCumulativeAckAfterBatchPublish";
        final String subscriptionName = "nbcaabp-sub-1";

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setBatchingMaxPublishDelay(5000, TimeUnit.MILLISECONDS);
        producerConf.setBatchingMaxMessages(numMsgsInBatch);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);
        // create producer to publish non batch messages
        Producer noBatchProducer = pulsarClient.createProducer(topicName);

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("msg-" + i).getBytes();
            Message msg = MessageBuilder.create().setContent(message).build();
            sendFutureList.add(producer.sendAsync(msg));

        }
        FutureUtil.waitForAll(sendFutureList).get();
        sendFutureList.clear();
        byte[] nobatchmsg = ("nobatch").getBytes();
        Message nmsg = MessageBuilder.create().setContent(nobatchmsg).build();
        noBatchProducer.sendAsync(nmsg).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 2);
        consumer = pulsarClient.subscribe(topicName, subscriptionName);

        Message lastunackedMsg = null;
        for (int i = 0; i <= numMsgs; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            lastunackedMsg = msg;
        }
        if (lastunackedMsg != null) {
            consumer.acknowledgeCumulative(lastunackedMsg);
        }
        Thread.sleep(100);
        rolloverPerIntervalStats();
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 0);
        assertTrue(((ConsumerImpl) consumer).isBatchingAckTrackerEmpty());
        consumer.close();
        producer.close();
        noBatchProducer.close();
    }

    @Test
    public void testBatchAndNonBatchCumulativeAcks() throws Exception {
        int numMsgs = 50;
        int numMsgsInBatch = numMsgs / 10;
        final String topicName = "persistent://prop/use/ns-abc/testBatchAndNonBatchCumulativeAcks";
        final String subscriptionName = "bnb-sub-1";

        Consumer consumer = pulsarClient.subscribe(topicName, subscriptionName);
        consumer.close();

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setBatchingMaxPublishDelay(5000, TimeUnit.MILLISECONDS);
        producerConf.setBatchingMaxMessages(numMsgsInBatch);
        producerConf.setBatchingEnabled(true);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);
        // create producer to publish non batch messages
        Producer noBatchProducer = pulsarClient.createProducer(topicName);

        List<CompletableFuture<MessageId>> sendFutureList = Lists.newArrayList();
        for (int i = 0; i < numMsgs / 2; i++) {
            byte[] message = ("msg-" + i).getBytes();
            Message msg = MessageBuilder.create().setContent(message).build();
            sendFutureList.add(producer.sendAsync(msg));
            byte[] nobatchmsg = ("nobatch-" + i).getBytes();
            msg = MessageBuilder.create().setContent(nobatchmsg).build();
            sendFutureList.add(noBatchProducer.sendAsync(msg));
        }
        FutureUtil.waitForAll(sendFutureList).get();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);

        rolloverPerIntervalStats();
        assertTrue(topic.getProducers().values().iterator().next().getStats().msgRateIn > 0.0);
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(),
                (numMsgs / 2) / numMsgsInBatch + numMsgs / 2);
        consumer = pulsarClient.subscribe(topicName, subscriptionName);

        Message lastunackedMsg = null;
        for (int i = 0; i < numMsgs; i++) {
            Message msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            LOG.info("[{}] got message position{} data {}", subscriptionName, msg.getMessageId(),
                    String.valueOf(msg.getData()));
            if (i % 2 == 0) {
                lastunackedMsg = msg;
            } else {
                consumer.acknowledgeCumulative(msg);
                LOG.info("[{}] did cumulative ack on position{} ", subscriptionName, msg.getMessageId());
            }
        }
        if (lastunackedMsg != null) {
            consumer.acknowledgeCumulative(lastunackedMsg);
        }
        Thread.sleep(100);
        assertEquals(topic.getPersistentSubscription(subscriptionName).getNumberOfEntriesInBacklog(), 0);
        assertTrue(((ConsumerImpl) consumer).isBatchingAckTrackerEmpty());
        consumer.close();
        producer.close();
        noBatchProducer.close();
    }

    private static final Logger LOG = LoggerFactory.getLogger(BatchMessageTest.class);
}
