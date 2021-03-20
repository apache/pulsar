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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.RelativeTimeUtil;
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
        Thread.sleep(500);
        consumer.seek(MessageId.earliest);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);

        Thread.sleep(500);
        consumer.seek(messageIds.get(5));
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 5);

        MessageIdImpl messageId = (MessageIdImpl) messageIds.get(5);
        MessageIdImpl beforeEarliest = new MessageIdImpl(
                messageId.getLedgerId() - 1, messageId.getEntryId(), messageId.getPartitionIndex());
        MessageIdImpl afterLatest = new MessageIdImpl(
                messageId.getLedgerId() + 1, messageId.getEntryId(), messageId.getPartitionIndex());

        log.info("MessageId {}: beforeEarliest: {}, afterLatest: {}", messageId, beforeEarliest, afterLatest);

        Thread.sleep(500);
        consumer.seek(beforeEarliest);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);

        Thread.sleep(500);
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
        Thread.sleep(1000);
        Message<String> receiveBeforeEarliest = consumer.receive();
        assertEquals(receiveBeforeEarliest.getValue(), messages.get(0));

        admin.topics().resetCursor(topicName, subscriptionName, MessageId.latest);
        // Wait consumer reconnect
        Thread.sleep(1000);
        Message<String> receiveAfterLatest = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(receiveAfterLatest);

        admin.topics().resetCursor(topicName, subscriptionName, messageIds.get(0), true);
        // Wait consumer reconnect
        Thread.sleep(1000);
        Message<String> received = consumer.receive();
        assertEquals(received.getMessageId(), messageIds.get(1));

        admin.topics().resetCursor(topicName, subscriptionName, messageIds.get(0), false);
        // Wait consumer reconnect
        Thread.sleep(1000);
        received = consumer.receive();
        assertEquals(received.getMessageId(), messageIds.get(0));

        admin.topics().resetCursor(topicName, subscriptionName, messageIds.get(messageIds.size() - 1), true);
        // Wait consumer reconnect
        Thread.sleep(1000);
        received = consumer.receive(1, TimeUnit.SECONDS);
        assertNull(received);

        admin.topics().resetCursor(topicName, subscriptionName, messageIds.get(messageIds.size() - 1), false);
        // Wait consumer reconnect
        Thread.sleep(1000);
        received = consumer.receive();
        assertEquals(received.getMessageId(), messageIds.get(messageIds.size() - 1));

        admin.topics().resetCursor(topicName, subscriptionName, new BatchMessageIdImpl(-1, -1, -1 ,10), true);
        // Wait consumer reconnect
        Thread.sleep(1000);
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
        Thread.sleep(1000);
        consumer.seek(currentTimestamp - resetTimeInMillis);
        assertEquals(sub.getNumberOfEntriesInBacklog(false), 10);
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
        Thread.sleep(1000);
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

        pulsarClient.newConsumer()
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
        Thread.sleep(1000);

        consumers = topicRef.getSubscriptions().get("my-subscription").getConsumers();
        assertEquals(consumers.size(), 2);
        for (Consumer consumer : consumers) {
            assertFalse(connectedSinceSet.contains(consumer.getStats().getConnectedSince()));
        }
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
        Thread.sleep(1000);

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
}
