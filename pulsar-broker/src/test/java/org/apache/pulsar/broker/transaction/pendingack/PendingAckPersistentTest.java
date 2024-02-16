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
package org.apache.pulsar.broker.transaction.pendingack;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertNotNull;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for consuming transaction messages.
 */
@Slf4j
@Test(groups = "broker")
public class PendingAckPersistentTest extends TransactionTestBase {

    private static final String PENDING_ACK_REPLAY_TOPIC = NAMESPACE1 + "/pending-ack-replay";

    private static final int NUM_PARTITIONS = 16;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        setUpBase(1, NUM_PARTITIONS, PENDING_ACK_REPLAY_TOPIC, 0);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void individualPendingAckReplayTest() throws Exception {
        int messageCount = 1000;
        String subName = "individual-test";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .enableBatching(true)
                .batchingMaxMessages(200)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        Transaction abortTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        List<MessageId> pendingAckMessageIds = new ArrayList<>();
        List<MessageId> normalAckMessageIds = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            producer.send("Hello Pulsar!".getBytes());
            Message<byte[]> message = consumer.receive();
            if (i % 2 == 0) {
                consumer.acknowledgeAsync(message.getMessageId(), abortTxn).get();
                pendingAckMessageIds.add(message.getMessageId());
            } else {
                normalAckMessageIds.add(message.getMessageId());
            }
        }

        //in order to test pending ack replay
        admin.topics().unload(PENDING_ACK_REPLAY_TOPIC);
        Awaitility.await().until(consumer::isConnected);
        Transaction commitTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        // this messageIds are ack by transaction
        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            try {
                consumer.acknowledgeAsync(pendingAckMessageIds.get(i), txn).get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }
        }
        // this messageIds are not ack by transaction
        for (int i = 0; i < normalAckMessageIds.size(); i++) {
            consumer.acknowledgeAsync(normalAckMessageIds.get(i), commitTxn).get();
        }

        txn.abort().get();
        // commit this txn, normalAckMessageIds are in pending ack state
        commitTxn.commit().get();
        // abort this txn, pendingAckMessageIds are delete from pending ack state
        abortTxn.abort().get();

        // replay this pending ack
        admin.topics().unload(PENDING_ACK_REPLAY_TOPIC);
        Awaitility.await().until(consumer::isConnected);

        abortTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        commitTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();
        // normalAckMessageIds are ack and then commit, so ack fail
        for (int i = 0; i < normalAckMessageIds.size(); i++) {
            try {
                consumer.acknowledgeAsync(normalAckMessageIds.get(i), abortTxn).get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }
        }

        // pendingAckMessageIds are all abort, so can ack again
        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            consumer.acknowledgeAsync(pendingAckMessageIds.get(i), commitTxn).get();
        }

        abortTxn.abort().get();
        commitTxn.commit().get();

        PersistentTopic topic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(TopicName.get(PENDING_ACK_REPLAY_TOPIC).toString(), false).get().get();
        Field field = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
        field.setAccessible(true);
        PendingAckHandleImpl pendingAckHandle =
                (PendingAckHandleImpl) field.get(topic.getSubscription(subName));
        field = PendingAckHandleImpl.class.getDeclaredField("pendingAckStoreFuture");
        field.setAccessible(true);
        CompletableFuture<PendingAckStore> pendingAckStoreCompletableFuture =
                (CompletableFuture<PendingAckStore>) field.get(pendingAckHandle);
        pendingAckStoreCompletableFuture.get();

        field = MLPendingAckStore.class.getDeclaredField("cursor");
        field.setAccessible(true);

        ManagedCursor managedCursor = (ManagedCursor) field.get(pendingAckStoreCompletableFuture.get());

        // in order to check out the pending ack cursor is clear whether or not.
        Awaitility.await()
                .until(() -> ((PositionImpl) managedCursor.getMarkDeletedPosition())
                        .compareTo((PositionImpl) managedCursor.getManagedLedger().getLastConfirmedEntry()) == -1);
    }

    @Test
    public void cumulativePendingAckReplayTest() throws Exception {
        int messageCount = 1000;
        getPulsarServiceList().get(0).getConfig().setTransactionPendingAckLogIndexMinLag(4 * messageCount + 2);
        getPulsarServiceList().get(0).getConfiguration().setManagedLedgerDefaultMarkDeleteRateLimit(10);
        String subName = "cumulative-test";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .enableBatching(true)
                .batchingMaxMessages(200)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(PENDING_ACK_REPLAY_TOPIC)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        Transaction abortTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        List<MessageId> pendingAckMessageIds = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            producer.send("Hello Pulsar!".getBytes());
        }

        for (int i = 0; i < messageCount; i++) {
            Message<byte[]> message = consumer.receive();
            pendingAckMessageIds.add(message.getMessageId());
            consumer.acknowledgeCumulativeAsync(message.getMessageId(), abortTxn).get();
        }

        admin.topics().unload(PENDING_ACK_REPLAY_TOPIC);
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        Awaitility.await().until(consumer::isConnected);

        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            try {
                consumer.acknowledgeCumulativeAsync(pendingAckMessageIds.get(i), txn).get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }
        }
        Transaction commitTxn = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();
        abortTxn.abort().get();

        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            consumer.acknowledgeCumulativeAsync(pendingAckMessageIds.get(i), commitTxn).get();
        }
        commitTxn.commit().get();

        admin.topics().unload(PENDING_ACK_REPLAY_TOPIC);
        Awaitility.await().until(consumer::isConnected);

        for (int i = 0; i < pendingAckMessageIds.size(); i++) {
            try {
                consumer.acknowledgeCumulativeAsync(pendingAckMessageIds.get(i), txn).get();
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
            }
        }

        PersistentTopic topic = (PersistentTopic) getPulsarServiceList().get(0).getBrokerService()
                .getTopic(TopicName.get(PENDING_ACK_REPLAY_TOPIC).toString(), false).get().get();
        Field field = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
        field.setAccessible(true);
        PendingAckHandleImpl pendingAckHandle =
                (PendingAckHandleImpl) field.get(topic.getSubscription(subName));
        field = PendingAckHandleImpl.class.getDeclaredField("pendingAckStoreFuture");
        field.setAccessible(true);
        CompletableFuture<PendingAckStore> pendingAckStoreCompletableFuture =
                (CompletableFuture<PendingAckStore>) field.get(pendingAckHandle);
        pendingAckStoreCompletableFuture.get();

        field = MLPendingAckStore.class.getDeclaredField("cursor");
        field.setAccessible(true);

        ManagedCursor managedCursor = (ManagedCursor) field.get(pendingAckStoreCompletableFuture.get());

        // in order to check out the pending ack cursor is clear whether or not.
        Awaitility.await()
                .until(() -> ((PositionImpl) managedCursor.getMarkDeletedPosition())
                        .compareTo((PositionImpl) managedCursor.getManagedLedger().getLastConfirmedEntry()) == 0);
    }

    @Test
    private void testDeleteSubThenDeletePendingAckManagedLedger() throws Exception {

        String subName = "test-delete";

        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test-delete").toString();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        consumer.close();

        admin.topics().deleteSubscription(topic, subName);

        List<String> topics = admin.namespaces().getTopics(NAMESPACE1);

        TopicStats topicStats = admin.topics().getStats(topic, false);

        assertFalse(topics.contains(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic, subName)));

        assertTrue(topics.contains(topic));
    }

    @Test
    private void testDeleteTopicThenDeletePendingAckManagedLedger() throws Exception {

        String subName1 = "test-delete";
        String subName2 = "test-delete";

        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test-delete").toString();
        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName1)
                .subscriptionType(SubscriptionType.Failover)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        consumer1.close();

        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName2)
                .subscriptionType(SubscriptionType.Failover)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        consumer2.close();

        admin.topics().delete(topic);

        List<String> topics = admin.namespaces().getTopics(NAMESPACE1);

        assertFalse(topics.contains(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic, subName1)));
        assertFalse(topics.contains(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic, subName2)));
        assertFalse(topics.contains(topic));
    }

    @Test
    public void testDeleteUselessLogDataWhenSubCursorMoved() throws Exception {
        getPulsarServiceList().get(0).getConfig().setTransactionPendingAckLogIndexMinLag(5);
        getPulsarServiceList().get(0).getConfiguration().setManagedLedgerDefaultMarkDeleteRateLimit(5);
        String subName = "test-log-delete";
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test-log-delete").toString();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        for (int i = 0; i < 20; i++) {
            producer.newMessage().send();
        }
        // init
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        consumer.acknowledgeAsync(message.getMessageId(), transaction).get();

        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(topic, false).get().get();

        PersistentSubscription persistentSubscription = persistentTopic.getSubscription(subName);
        Field field = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
        field.setAccessible(true);
        PendingAckHandleImpl pendingAckHandle = (PendingAckHandleImpl) field.get(persistentSubscription);
        Field field1 = PendingAckHandleImpl.class.getDeclaredField("pendingAckStoreFuture");
        field1.setAccessible(true);
        PendingAckStore pendingAckStore = ((CompletableFuture<PendingAckStore>) field1.get(pendingAckHandle)).get();

        Field field3 = MLPendingAckStore.class.getDeclaredField("pendingAckLogIndex");
        Field field4 = MLPendingAckStore.class.getDeclaredField("maxIndexLag");

        field3.setAccessible(true);
        field4.setAccessible(true);

        ConcurrentSkipListMap<PositionImpl, PositionImpl> pendingAckLogIndex =
                (ConcurrentSkipListMap<PositionImpl, PositionImpl>) field3.get(pendingAckStore);
        long maxIndexLag = (long) field4.get(pendingAckStore);
        Assert.assertEquals(pendingAckLogIndex.size(), 0);
        Assert.assertEquals(maxIndexLag, 5);
        transaction.commit().get();

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(persistentSubscription.getCursor().getPersistentMarkDeletedPosition().getEntryId(),
                        ((MessageIdImpl)message.getMessageId()).getEntryId()));
        // 7 more acks. Will find that there are still only two records in the map.
        Transaction transaction1 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        Message<byte[]> message0 = null;
        //remove previous index
        for (int i = 0; i < 4; i++) {
            message0 = consumer.receive(5, TimeUnit.SECONDS);
            consumer.acknowledgeAsync(message0.getMessageId(), transaction1).get();
        }
        Assert.assertEquals(pendingAckLogIndex.size(), 1);
        maxIndexLag = (long) field4.get(pendingAckStore);
        Assert.assertEquals(maxIndexLag, 5);
        //add new index
        for (int i = 0; i < 9; i++) {
            message0= consumer.receive(5, TimeUnit.SECONDS);
            consumer.acknowledgeAsync(message0.getMessageId(), transaction1).get();
        }

        Assert.assertEquals(pendingAckLogIndex.size(), 2);
        maxIndexLag = (long) field4.get(pendingAckStore);
        Assert.assertEquals(maxIndexLag, 10);

        transaction1.commit().get();
        Message<byte[]> message1 = message0;
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(persistentSubscription.getCursor().getPersistentMarkDeletedPosition().getEntryId(),
                        ((MessageIdImpl)message1.getMessageId()).getEntryId()));

        Transaction transaction2 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        Message<byte[]> message2 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message2.getMessageId(), transaction2).get();

        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(pendingAckLogIndex.size(), 0);
        });
        maxIndexLag = (long) field4.get(pendingAckStore);
        Assert.assertEquals(maxIndexLag, 5);
    }

    @Test
    public void testPendingAckLowWaterMarkRemoveFirstTxn() throws Exception {
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test").toString();

        String subName = "subName";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        for (int i = 0; i < 5; i++) {
            producer.newMessage().send();
        }

        Transaction transaction1 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();

        Message<byte[]> message1 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message1.getMessageId(), transaction1);
        transaction1.commit().get();


        Transaction transaction2 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        while (transaction1.getTxnID().getMostSigBits() != transaction2.getTxnID().getMostSigBits()) {
            transaction2 = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.SECONDS)
                    .build()
                    .get();
        }

        Transaction transaction3 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();
        while (transaction1.getTxnID().getMostSigBits() != transaction3.getTxnID().getMostSigBits()) {
            transaction3 = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.SECONDS)
                    .build()
                    .get();
        }

        Message<byte[]> message3 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message3.getMessageId(), transaction2);
        transaction2.commit().get();

        Message<byte[]> message2 = consumer.receive(5, TimeUnit.SECONDS);

        Field field = TransactionImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(transaction1, TransactionImpl.State.OPEN);

        consumer.acknowledgeAsync(message2.getMessageId(), transaction1).get();
        Message<byte[]> message4 = consumer.receive(5, TimeUnit.SECONDS);
        field.set(transaction2, TransactionImpl.State.OPEN);
        consumer.acknowledgeAsync(message4.getMessageId(), transaction2).get();

        Message<byte[]> message5 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message5.getMessageId(), transaction3);
        transaction3.commit().get();


        PersistentTopic persistentTopic =
                (PersistentTopic) getPulsarServiceList()
                        .get(0)
                        .getBrokerService()
                        .getTopic(topic, false)
                        .get()
                        .get();

        PersistentSubscription persistentSubscription = persistentTopic.getSubscription(subName);
        Field field1 = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
        field1.setAccessible(true);
        PendingAckHandleImpl oldPendingAckHandle = (PendingAckHandleImpl) field1.get(persistentSubscription);
        Field field2 = PendingAckHandleImpl.class.getDeclaredField("individualAckOfTransaction");
        field2.setAccessible(true);
        LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>> oldIndividualAckOfTransaction =
                (LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>>) field2.get(oldPendingAckHandle);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(oldIndividualAckOfTransaction.size(), 0));

        PendingAckHandleImpl pendingAckHandle = new PendingAckHandleImpl(persistentSubscription);

        Method method = PendingAckHandleImpl.class.getDeclaredMethod("initPendingAckStore");
        method.setAccessible(true);
        method.invoke(pendingAckHandle);

        Field field3 = PendingAckHandleImpl.class.getDeclaredField("pendingAckStoreFuture");
        field3.setAccessible(true);

        Awaitility.await().until(() -> {
            CompletableFuture<PendingAckStore> completableFuture =
                    (CompletableFuture<PendingAckStore>) field3.get(pendingAckHandle);
            completableFuture.get();
            return true;
        });


        LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>> individualAckOfTransaction =
                (LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>>) field2.get(pendingAckHandle);

        assertFalse(individualAckOfTransaction.containsKey(transaction1.getTxnID()));
        assertFalse(individualAckOfTransaction.containsKey(transaction2.getTxnID()));

    }

    @Test
    public void testTransactionConflictExceptionWhenAckBatchMessage() throws Exception {
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "test").toString();

        String subscriptionName = "my-subscription-batch";
        pulsarServiceList.get(0).getBrokerService()
                .getManagedLedgerConfig(TopicName.get(topic)).get()
                .setDeletionAtBatchIndexLevelEnabled(true);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(true)
                .batchingMaxMessages(3)
                // set batch max publish delay big enough to make sure entry has 3 messages
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS)
                .topic(topic).create();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscriptionName)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Exclusive)
                .isAckReceiptEnabled(true)
                .topic(topic)
                .subscribe();

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

        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.DAYS)
                .build()
                .get();

        Message<String> message1 = consumer.receive();
        Message<String> message2 = consumer.receive();

        BatchMessageIdImpl messageId = (BatchMessageIdImpl) message2.getMessageId();
        consumer.acknowledgeAsync(messageId, transaction).get();

        Transaction transaction2 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.DAYS)
                .build()
                .get();
        transaction.commit().get();

        try {
            consumer.acknowledgeAsync(messageId, transaction2).get();
            fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof PulsarClientException.TransactionConflictException);
        }
    }

    @Test
    public void testGetSubPatternTopicFilterTxnInternalTopic() throws Exception {
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "testGetSubPatternTopicFilterTxnInternalTopic").toString();

        int partition = 3;
        admin.topics().createPartitionedTopic(topic, partition);

        String subscriptionName = "sub";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topic).create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .topic(topic)
                .subscribe();

        for (int i = 0; i < partition; i++) {
            producer.send("test");
        }

        // creat pending ack managedLedger
        for (int i = 0; i < partition; i++) {
            Transaction transaction = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.SECONDS)
                    .build()
                    .get();
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), transaction);
            transaction.commit().get();
        }

        consumer.close();

        @Cleanup
        Consumer<String> patternConsumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName("patternSub")
                .subscriptionType(SubscriptionType.Shared)
                .topicsPattern("persistent://" + NAMESPACE1 + "/.*")
                .subscribe();

        for (int i = 0; i < partition; i++) {
            producer.send("test" + i);
        }

        // can use pattern sub consume
        for (int i = 0; i < partition; i++) {
            patternConsumer.acknowledgeAsync(patternConsumer.receive().getMessageId());
        }
        patternConsumer.close();
        producer.close();
    }

    @Test
    public void testGetManagedLegerConfigFailThenUnload() throws Exception {
        String topic = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(NAMESPACE1), "testGetManagedLegerConfigFailThenUnload").toString();

        String subscriptionName = "sub";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topic).create();

        PersistentTopic persistentTopic =
                (PersistentTopic) getPulsarServiceList()
                        .get(0)
                        .getBrokerService()
                        .getTopic(topic, false)
                        .get().orElse(null);

        assertNotNull(persistentTopic);
        BrokerService brokerService = spy(persistentTopic.getBrokerService());
        doReturn(FutureUtil.failedFuture(new BrokerServiceException.ServiceUnitNotReadyException("test")))
                .when(brokerService).getManagedLedgerConfig(any());
        Field field = AbstractTopic.class.getDeclaredField("brokerService");
        field.setAccessible(true);
        field.set(persistentTopic, brokerService);

        // init pending ack store
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .topic(topic)
                .subscribe();

        producer.send("test");
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(30, TimeUnit.SECONDS).build().get();

        // pending ack init fail, so the ack will throw exception
        try {
            consumer.acknowledgeAsync(consumer.receive().getMessageId(), transaction).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.LookupException);
        }

        // can unload success
        admin.topics().unload(topic);
    }
}
