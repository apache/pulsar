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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.RelativeTimeUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class SubscriptionSeekTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        conf.setAcknowledgmentAtBatchIndexLevelEnabled(true);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSeek() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testSeek";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        // Disable pre-fetch in consumer to track the messages received
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscription").receiverQueueSize(0).subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);
        assertEquals(topicRef.getSubscriptions().size(), 1);

        List<MessageId> messageIds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            MessageId msgId = producer.send(message.getBytes());
            messageIds.add(msgId);
        }

        PersistentSubscription sub = topicRef.getSubscription("my-subscription");
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);

        consumer.seek(MessageId.latest);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 0);

        // Wait for consumer to reconnect
        Awaitility.await().until(consumer::isConnected);
        consumer.seek(MessageId.earliest);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);

        Awaitility.await().until(consumer::isConnected);
        consumer.seek(messageIds.get(5));
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 5);

        MessageIdImpl messageId = (MessageIdImpl) messageIds.get(5);
        MessageIdImpl beforeEarliest = new MessageIdImpl(
                messageId.getLedgerId() - 1, messageId.getEntryId(), messageId.getPartitionIndex());
        MessageIdImpl afterLatest = new MessageIdImpl(
                messageId.getLedgerId() + 1, messageId.getEntryId(), messageId.getPartitionIndex());

        log.info("MessageId {}: beforeEarliest: {}, afterLatest: {}", messageId, beforeEarliest, afterLatest);

        Awaitility.await().until(consumer::isConnected);
        consumer.seek(beforeEarliest);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);

        Awaitility.await().until(consumer::isConnected);
        consumer.seek(afterLatest);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 0);
    }

    @Test
    public void testSeekForBatch() throws Exception {
        final String topicName = "persistent://prop/use/ns-abcd/testSeekForBatch";
        String subscriptionName = "my-subscription-batch";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .topic(topicName).create();


        List<MessageId> messageIds = new ArrayList<>();
        List<CompletableFuture<MessageId>> futureMessageIds = new ArrayList<>();

        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            messages.add(message);
            CompletableFuture<MessageId> messageIdCompletableFuture = producer.sendAsync(message);
            futureMessageIds.add(messageIdCompletableFuture);
        }

        for (CompletableFuture<MessageId> futureMessageId : futureMessageIds) {
            MessageId messageId = futureMessageId.get();
            messageIds.add(messageId);
        }

        producer.close();


        org.apache.pulsar.client.api.Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .startMessageIdInclusive()
                .subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);

        assertEquals(topicRef.getSubscriptions().size(), 1);

        consumer.seek(MessageId.earliest);
        Message<String> receiveBeforEarliest = consumer.receive();
        assertEquals(receiveBeforEarliest.getValue(), messages.get(0));
        consumer.seek(MessageId.latest);
        Message<String> receiveAfterLatest = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(receiveAfterLatest);

        for (MessageId messageId : messageIds) {
            consumer.seek(messageId);
            MessageId receiveId = consumer.receive().getMessageId();
            assertEquals(receiveId, messageId);
        }
    }

    @Test
    public void testSeekForBatchMessageAndSpecifiedBatchIndex() throws Exception {
        final String topicName = "persistent://prop/use/ns-abcd/testSeekForBatchMessageAndSpecifiedBatchIndex";
        String subscriptionName = "my-subscription-batch";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(true)
                .batchingMaxMessages(3)
                // set batch max publish delay big enough to make sure entry has 3 messages
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS)
                .topic(topicName).create();


        List<MessageId> messageIds = new ArrayList<>();
        List<CompletableFuture<MessageId>> futureMessageIds = new ArrayList<>();

        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String message = "my-message-" + i;
            messages.add(message);
            CompletableFuture<MessageId> messageIdCompletableFuture = producer.sendAsync(message);
            futureMessageIds.add(messageIdCompletableFuture);
        }

        for (CompletableFuture<MessageId> futureMessageId : futureMessageIds) {
            MessageId messageId = futureMessageId.get();
            messageIds.add(messageId);
        }

        producer.close();

        assertTrue(messageIds.get(0) instanceof  BatchMessageIdImpl);
        assertTrue(messageIds.get(1) instanceof  BatchMessageIdImpl);
        assertTrue(messageIds.get(2) instanceof  BatchMessageIdImpl);

        BatchMessageIdImpl batchMsgId0 = (BatchMessageIdImpl) messageIds.get(0);
        BatchMessageIdImpl batchMsgId1 = (BatchMessageIdImpl) messageIds.get(1);
        BatchMessageIdImpl msgIdToSeekFirst = (BatchMessageIdImpl) messageIds.get(2);

        assertEquals(batchMsgId0.getEntryId(), batchMsgId1.getEntryId());
        assertEquals(batchMsgId1.getEntryId(), msgIdToSeekFirst.getEntryId());

        PulsarClient newPulsarClient = PulsarClient.builder()
                // set start backoff interval short enough to make sure client will re-connect quickly
                .startingBackoffInterval(1, TimeUnit.MICROSECONDS)
                .serviceUrl(lookupUrl.toString())
                .build();

        org.apache.pulsar.client.api.Consumer<String> consumer = newPulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .startMessageIdInclusive()
                .subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getSubscriptions().size(), 1);

        consumer.seek(msgIdToSeekFirst);
        MessageId msgId = consumer.receive().getMessageId();
        assertTrue(msgId instanceof BatchMessageIdImpl);
        BatchMessageIdImpl batchMsgId = (BatchMessageIdImpl) msgId;
        assertEquals(batchMsgId, msgIdToSeekFirst);


        consumer.seek(MessageId.earliest);
        Message<String> receiveBeforEarliest = consumer.receive();
        assertEquals(receiveBeforEarliest.getValue(), messages.get(0));
        consumer.seek(MessageId.latest);
        Message<String> receiveAfterLatest = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(receiveAfterLatest);

        for (MessageId messageId : messageIds) {
            consumer.seek(messageId);
            MessageId receiveId = consumer.receive().getMessageId();
            assertEquals(receiveId, messageId);
        }

        newPulsarClient.close();
    }

    @Test
    public void testSeekForBatchByAdmin() throws PulsarClientException, ExecutionException, InterruptedException, PulsarAdminException {
        final String topicName = "persistent://prop/use/ns-abcd/testSeekForBatchByAdmin-" + UUID.randomUUID().toString();
        String subscriptionName = "my-subscription-batch";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .topic(topicName).create();


        List<MessageId> messageIds = new ArrayList<>();
        List<CompletableFuture<MessageId>> futureMessageIds = new ArrayList<>();

        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            messages.add(message);
            CompletableFuture<MessageId> messageIdCompletableFuture = producer.sendAsync(message);
            futureMessageIds.add(messageIdCompletableFuture);
        }

        for (CompletableFuture<MessageId> futureMessageId : futureMessageIds) {
            MessageId messageId = futureMessageId.get();
            messageIds.add(messageId);
        }

        producer.close();


        org.apache.pulsar.client.api.Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscribe();

        admin.topics().resetCursor(topicName, subscriptionName, MessageId.earliest);

        // Wait consumer reconnect
        Awaitility.await().until(consumer::isConnected);
        Message<String> receiveBeforeEarliest = consumer.receive();
        assertEquals(receiveBeforeEarliest.getValue(), messages.get(0));

        admin.topics().resetCursor(topicName, subscriptionName, MessageId.latest);
        // Wait consumer reconnect
        Awaitility.await().until(consumer::isConnected);
        Message<String> receiveAfterLatest = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(receiveAfterLatest);

        admin.topics().resetCursor(topicName, subscriptionName, messageIds.get(0), true);
        // Wait consumer reconnect
        Awaitility.await().until(consumer::isConnected);
        Message<String> received = consumer.receive();
        assertEquals(received.getMessageId(), messageIds.get(1));

        admin.topics().resetCursor(topicName, subscriptionName, messageIds.get(0), false);
        // Wait consumer reconnect
        Awaitility.await().until(consumer::isConnected);
        received = consumer.receive();
        assertEquals(received.getMessageId(), messageIds.get(0));

        admin.topics().resetCursor(topicName, subscriptionName, messageIds.get(messageIds.size() - 1), true);
        // Wait consumer reconnect
        Awaitility.await().until(consumer::isConnected);
        received = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(received);

        admin.topics().resetCursor(topicName, subscriptionName, messageIds.get(messageIds.size() - 1), false);
        // Wait consumer reconnect
        Awaitility.await().until(consumer::isConnected);
        received = consumer.receive();
        assertEquals(received.getMessageId(), messageIds.get(messageIds.size() - 1));

        admin.topics().resetCursor(topicName, subscriptionName, new BatchMessageIdImpl(-1, -1, -1,10), true);
        // Wait consumer reconnect
        Awaitility.await().until(consumer::isConnected);
        received = consumer.receive();
        assertEquals(received.getMessageId(), messageIds.get(0));
    }


    @Test
    public void testConcurrentResetCursor() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testConcurrentReset_" + System.currentTimeMillis();
        final String subscriptionName = "test-sub-name";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);

        List<MessageId> messageIds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            MessageId msgId = producer.send(message.getBytes());
            messageIds.add(msgId);
        }

        List<PulsarAdminException> exceptions = Lists.newLinkedList();
        class ResetCursorThread extends Thread {
            public void run() {
                try {
                    admin.topics().resetCursor(topicName, subscriptionName, messageIds.get(3));
                } catch (PulsarAdminException e) {
                    exceptions.add(e);
                }
            }
        }

        List<ResetCursorThread> resetCursorThreads = Lists.newLinkedList();
        for (int i = 0; i < 4; i ++) {
            ResetCursorThread thread = new ResetCursorThread();
            resetCursorThreads.add(thread);
        }
        for (int i = 0; i < 4; i ++) {
            resetCursorThreads.get(i).start();
        }
        for (int i = 0; i < 4; i ++) {
            resetCursorThreads.get(i).join();
        }

        for (PulsarAdminException exception : exceptions) {
            log.error("Meet Exception", exception);
            assertTrue(exception.getMessage().contains("Failed to fence subscription"));
        }
    }

    @Test
    public void testSeekOnPartitionedTopic() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testSeekPartitions";

        admin.topics().createPartitionedTopic(topicName, 2);
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscription").subscribe();

        try {
            consumer.seek(MessageId.latest);
        } catch (PulsarClientException e) {
            fail("Should not have exception");
        }
    }

    @Test
    public void testSeekTime() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testSeekTime";
        String resetTimeStr = "100s";
        long resetTimeInMillis = TimeUnit.SECONDS
                .toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(resetTimeStr));

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        // Disable pre-fetch in consumer to track the messages received
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscription").receiverQueueSize(0).subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getProducers().size(), 1);
        assertEquals(topicRef.getSubscriptions().size(), 1);
        PersistentSubscription sub = topicRef.getSubscription("my-subscription");

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);

        long currentTimestamp = System.currentTimeMillis();
        consumer.seek(currentTimestamp);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 0);

        // Wait for consumer to reconnect
        Awaitility.await().until(consumer::isConnected);
        consumer.seek(currentTimestamp - resetTimeInMillis);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);
    }

    @Test
    public void testSeekTimeByFunction() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/test" + UUID.randomUUID();
        int partitionNum = 4;
        int msgNum = 20;
        admin.topics().createPartitionedTopic(topicName, partitionNum);
        creatProducerAndSendMsg(topicName, msgNum);
        org.apache.pulsar.client.api.Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING).startMessageIdInclusive()
                .topic(topicName).subscriptionName("my-sub").subscribe();
        long now = System.currentTimeMillis();
        consumer.seek((topic) -> now);
        assertNull(consumer.receive(1, TimeUnit.SECONDS));

        consumer.seek((topic) -> {
            TopicName name = TopicName.get(topic);
            switch (name.getPartitionIndex()) {
                case 0:
                    return MessageId.latest;
                case 1:
                    return MessageId.earliest;
                case 2:
                    return now;
                case 3:
                    return now - 999999;
                default:
                    return null;
            }
        });
        int count = 0;
        while (true) {
            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count++;
        }
        int msgNumInPartition0 = 0;
        int msgNumInPartition1 = msgNum / partitionNum;
        int msgNumInPartition2 = 0;
        int msgNumInPartition3 = msgNum / partitionNum;

        assertEquals(count, msgNumInPartition0 + msgNumInPartition1 + msgNumInPartition2 + msgNumInPartition3);

    }

    @Test
    public void testSeekTimeOnPartitionedTopic() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testSeekTimePartitions";
        final String resetTimeStr = "100s";
        final int partitions = 2;
        long resetTimeInMillis = TimeUnit.SECONDS
                .toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(resetTimeStr));
        admin.topics().createPartitionedTopic(topicName, partitions);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        // Disable pre-fetch in consumer to track the messages received
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscription").subscribe();

        List<PersistentSubscription> subs = new ArrayList<>();

        for (int i = 0; i < partitions; i++) {
            PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService()
                    .getTopicReference(topicName + TopicName.PARTITIONED_TOPIC_SUFFIX + i).get();
            assertNotNull(topicRef);
            assertEquals(topicRef.getProducers().size(), 1);
            assertEquals(topicRef.getSubscriptions().size(), 1);
            PersistentSubscription sub = topicRef.getSubscription("my-subscription");
            assertNotNull(sub);
            subs.add(sub);
        }

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        long backlogs = 0;
        for (PersistentSubscription sub : subs) {
            backlogs += sub.getNumberOfEntriesInBacklog(false);
        }

        assertEquals(backlogs, 10);

        backlogs = 0;
        long currentTimestamp = System.currentTimeMillis();
        consumer.seek(currentTimestamp);
        for (PersistentSubscription sub : subs) {
            backlogs += sub.getNumberOfEntriesInBacklog(false);
        }
        assertEquals(backlogs, 0);

        // Wait for consumer to reconnect
        Awaitility.await().until(consumer::isConnected);
        consumer.seek(currentTimestamp - resetTimeInMillis);
        backlogs = 0;

        for (PersistentSubscription sub : subs) {
            backlogs += sub.getNumberOfEntriesInBacklog(false);
        }
        assertEquals(backlogs, 10);
    }

    @Test
    public void testShouldCloseAllConsumersForMultipleConsumerDispatcherWhenSeek() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testShouldCloseAllConsumersForMultipleConsumerDispatcherWhenSeek";
        // Disable pre-fetch in consumer to track the messages received
        org.apache.pulsar.client.api.Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-subscription")
                .subscribe();

        org.apache.pulsar.client.api.Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-subscription")
                .subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getSubscriptions().size(), 1);
        List<Consumer> consumers = topicRef.getSubscriptions().get("my-subscription").getConsumers();
        assertEquals(consumers.size(), 2);
        Set<String> connectedSinceSet = new HashSet<>();
        for (Consumer consumer : consumers) {
            connectedSinceSet.add(consumer.getStats().getConnectedSince());
        }
        assertEquals(connectedSinceSet.size(), 2);
        consumer1.seek(MessageId.earliest);
        // Wait for consumer to reconnect
        Awaitility.await().until(consumer1::isConnected);
        Awaitility.await().until(consumer2::isConnected);

        consumers = topicRef.getSubscriptions().get("my-subscription").getConsumers();
        assertEquals(consumers.size(), 2);
        for (Consumer consumer : consumers) {
            assertFalse(connectedSinceSet.contains(consumer.getStats().getConnectedSince()));
        }
        consumer1.close();
        consumer2.close();
    }

    @Test
    public void testOnlyCloseActiveConsumerForSingleActiveConsumerDispatcherWhenSeek() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testOnlyCloseActiveConsumerForSingleActiveConsumerDispatcherWhenSeek";
        // Disable pre-fetch in consumer to track the messages received
        org.apache.pulsar.client.api.Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionName("my-subscription")
                .subscribe();

        pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionName("my-subscription")
                .subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);
        assertEquals(topicRef.getSubscriptions().size(), 1);
        List<Consumer> consumers = topicRef.getSubscriptions().get("my-subscription").getConsumers();
        assertEquals(consumers.size(), 2);
        Set<String> connectedSinceSet = new HashSet<>();
        for (Consumer consumer : consumers) {
            connectedSinceSet.add(consumer.getStats().getConnectedSince());
        }
        assertEquals(connectedSinceSet.size(), 2);
        consumer1.seek(MessageId.earliest);
        // Wait for consumer to reconnect
        Awaitility.await().until(consumer1::isConnected);

        consumers = topicRef.getSubscriptions().get("my-subscription").getConsumers();
        assertEquals(consumers.size(), 2);

        boolean hasConsumerNotDisconnected = false;
        for (Consumer consumer : consumers) {
            if (connectedSinceSet.contains(consumer.getStats().getConnectedSince())) {
                hasConsumerNotDisconnected = true;
            }
        }
        assertTrue(hasConsumerNotDisconnected);
    }

    @Test
    public void testSeekByFunction() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/test" + UUID.randomUUID();
        int partitionNum = 4;
        int msgNum = 160;
        admin.topics().createPartitionedTopic(topicName, partitionNum);
        creatProducerAndSendMsg(topicName, msgNum);
        org.apache.pulsar.client.api.Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING).startMessageIdInclusive()
                .topic(topicName).subscriptionName("my-sub").subscribe();

        TopicName partitionedTopic = TopicName.get(topicName);
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .startMessageId(MessageId.earliest)
                .topic(partitionedTopic.getPartition(0).toString()).create();
        List<MessageId> list = new ArrayList<>();
        while (reader.hasMessageAvailable()) {
            list.add(reader.readNext().getMessageId());
        }
        // get middle msg from partition-0
        MessageId middleMsgIdInPartition0 = list.get(list.size() / 2);
        List<MessageId> msgNotIn = list.subList(0, list.size() / 2 - 1);
        // get last msg from partition-1
        MessageId lastMsgInPartition1 = admin.topics().getLastMessageId(partitionedTopic.getPartition(1).toString());
        reader.close();
        reader = pulsarClient.newReader(Schema.STRING)
                .startMessageId(MessageId.earliest)
                .topic(partitionedTopic.getPartition(2).toString()).create();
        // get first msg from partition-2
        MessageId firstMsgInPartition2 = reader.readNext().getMessageId();

        consumer.seek((topic) -> {
            int index = TopicName.get(topic).getPartitionIndex();
            if (index == 0) {
                return middleMsgIdInPartition0;
            } else if (index == 1) {
                return lastMsgInPartition1;
            } else if (index == 2) {
                return firstMsgInPartition2;
            }
            return null;
        });
        Set<MessageId> received = new HashSet<>();
        while (true) {
            Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            TopicMessageIdImpl topicMessageId = (TopicMessageIdImpl) message.getMessageId();
            received.add(topicMessageId.getInnerMessageId());
        }
        int msgNumFromPartition1 = list.size() / 2;
        int msgNumFromPartition2 = 1;
        int msgNumFromPartition3 = msgNum / partitionNum;
        assertEquals(received.size(), msgNumFromPartition1 + msgNumFromPartition2 + msgNumFromPartition3);
        assertTrue(received.contains(middleMsgIdInPartition0));
        assertTrue(received.contains(lastMsgInPartition1));
        assertTrue(received.contains(firstMsgInPartition2));
        for (MessageId messageId : msgNotIn) {
            assertFalse(received.contains(messageId));
        }
        reader.close();
        consumer.close();
    }

    private List<MessageId> creatProducerAndSendMsg(String topic, int msgNum) throws Exception {
        List<MessageId> messageIds = new ArrayList<>();
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .topic(topic).create();
        for (int i = 0; i < msgNum; i++) {
            messageIds.add(producer.send("msg" + i));
        }
        producer.close();
        return messageIds;
    }

    @Test
    public void testSeekByFunctionAndMultiTopic() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/test" + UUID.randomUUID();
        final String topicName2 = "persistent://prop/use/ns-abc/test" + UUID.randomUUID();
        int partitionNum = 3;
        int msgNum = 15;
        admin.topics().createPartitionedTopic(topicName, partitionNum);
        admin.topics().createPartitionedTopic(topicName2, partitionNum);
        creatProducerAndSendMsg(topicName, msgNum);
        creatProducerAndSendMsg(topicName2, msgNum);
        TopicName topic = TopicName.get(topicName);
        TopicName topic2 = TopicName.get(topicName2);
        MessageId msgIdInTopic1Partition0 = admin.topics().getLastMessageId(topic.getPartition(0).toString());
        MessageId msgIdInTopic1Partition2 = admin.topics().getLastMessageId(topic.getPartition(2).toString());
        MessageId msgIdInTopic2Partition0 = admin.topics().getLastMessageId(topic2.getPartition(0).toString());
        MessageId msgIdInTopic2Partition2 = admin.topics().getLastMessageId(topic2.getPartition(2).toString());

        org.apache.pulsar.client.api.Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING).startMessageIdInclusive()
                .topics(Arrays.asList(topicName, topicName2)).subscriptionName("my-sub").subscribe();
        consumer.seek((partitionedTopic) -> {
            if (partitionedTopic.equals(topic.getPartition(0).toString())) {
                return msgIdInTopic1Partition0;
            }
            if (partitionedTopic.equals(topic.getPartition(2).toString())) {
                return msgIdInTopic1Partition2;
            }
            if (partitionedTopic.equals(topic2.getPartition(0).toString())) {
                return msgIdInTopic2Partition0;
            }
            if (partitionedTopic.equals(topic2.getPartition(2).toString())) {
                return msgIdInTopic2Partition2;
            }
            return MessageId.earliest;
        });
        int count = 0;
        while (true) {
            Message message = consumer.receive(2, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            count++;
        }
        int msgInTopic1Partition0 = 1;
        int msgInTopic1Partition1 = msgNum / partitionNum;
        int msgInTopic1Partition2 = 1;
        assertEquals(count, (msgInTopic1Partition0 + msgInTopic1Partition1 + msgInTopic1Partition2) * 2);
    }

    @Test
    public void testExceptionBySeekFunction() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/test" + UUID.randomUUID();
        creatProducerAndSendMsg(topicName,10);
        org.apache.pulsar.client.api.Consumer consumer = pulsarClient
                .newConsumer()
                .topic(topicName).subscriptionName("my-sub").subscribe();
        try {
            consumer.seek((Function<String, MessageId>) null);
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException);
            assertTrue(e.getMessage().contains("Function must be set"));
        }
        assertNull(consumer.seekAsync((topic)-> null).get());
        try {
            assertNull(consumer.seekAsync((topic)-> new Object()).get());
            fail("should fail");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException);
            assertTrue(e.getCause().getMessage().contains("Only support seek by messageId or timestamp"));
        }
    }
}
