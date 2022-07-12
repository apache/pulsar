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
package org.apache.pulsar.broker.transaction.buffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Pulsar client transaction test.
 */
@Slf4j
@Test(groups = "broker")
public class TransactionLowWaterMarkTest extends TransactionTestBase {

    private static final String TOPIC = "persistent://" + NAMESPACE1 + "/test-topic";

    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        setUpBase(1, 16, TOPIC, 0);

        Map<TransactionCoordinatorID, TransactionMetadataStore> stores =
                getPulsarServiceList().get(0).getTransactionMetadataStoreService().getStores();
        Awaitility.await().until(() -> {
            if (stores.size() == 16) {
                for (TransactionCoordinatorID transactionCoordinatorID : stores.keySet()) {
                    if (((MLTransactionMetadataStore) stores.get(transactionCoordinatorID)).getState()
                            != TransactionMetadataStoreState.State.Ready) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        });
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTransactionBufferLowWaterMark() throws Exception {
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        final String TEST1 = "test1";
        final String TEST2 = "test2";
        final String TEST3 = "test3";

        producer.newMessage(txn).value(TEST1.getBytes()).send();
        txn.commit().get();

        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST1);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);

        Field field = TransactionImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(txn, TransactionImpl.State.OPEN);
        producer.newMessage(txn).value(TEST2.getBytes()).send();
        try {
            txn.commit().get();
            Assert.fail("The commit operation should be failed.");
        } catch (Exception e){
            Assert.assertTrue(e.getCause() instanceof TransactionCoordinatorClientException.TransactionNotFoundException);
        }

        PartitionedTopicMetadata partitionedTopicMetadata =
                ((PulsarClientImpl) pulsarClient).getLookup()
                        .getPartitionedTopicMetadata(TopicName.TRANSACTION_COORDINATOR_ASSIGN).get();
        Transaction lowWaterMarkTxn = null;
        for (int i = 0; i < partitionedTopicMetadata.partitions; i++) {
            lowWaterMarkTxn = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.SECONDS)
                    .build().get();
            if (((TransactionImpl) lowWaterMarkTxn).getTxnIdMostBits() == ((TransactionImpl) txn).getTxnIdMostBits()) {
                break;
            }
        }

        if (lowWaterMarkTxn != null &&
                ((TransactionImpl) lowWaterMarkTxn).getTxnIdMostBits() == ((TransactionImpl) txn).getTxnIdMostBits()) {
            producer.newMessage(lowWaterMarkTxn).value(TEST3.getBytes()).send();

            message = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(message);

            lowWaterMarkTxn.commit().get();

            message = consumer.receive();
            assertEquals(new String(message.getData()), TEST3);

        } else {
            fail();
        }

    }

    @Test
    public void testPendingAckLowWaterMark() throws Exception {
        String subName = "test";
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC)
                .subscriptionName(subName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        final String TEST1 = "test1";
        final String TEST2 = "test2";
        final String TEST3 = "test3";

        producer.send(TEST1.getBytes());
        producer.send(TEST2.getBytes());
        producer.send(TEST3.getBytes());

        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST1);
        consumer.acknowledgeAsync(message.getMessageId(), txn).get();
        LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>> individualAckOfTransaction = null;

        for (int i = 0; i < getPulsarServiceList().size(); i++) {
            Field field = BrokerService.class.getDeclaredField("topics");
            field.setAccessible(true);
            ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics =
                    (ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>>) field
                            .get(getPulsarServiceList().get(i).getBrokerService());
            CompletableFuture<Optional<Topic>> completableFuture = topics.get(TOPIC);
            if (completableFuture != null) {
                Optional<Topic> topic = completableFuture.get();
                if (topic.isPresent()) {
                    PersistentSubscription persistentSubscription = (PersistentSubscription) topic.get()
                            .getSubscription(subName);
                    field = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
                    field.setAccessible(true);
                    PendingAckHandleImpl pendingAckHandle = (PendingAckHandleImpl) field.get(persistentSubscription);
                    field = PendingAckHandleImpl.class.getDeclaredField("individualAckOfTransaction");
                    field.setAccessible(true);
                    individualAckOfTransaction =
                            (LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>>) field.get(pendingAckHandle);
                }
            }
        }

        assertTrue(individualAckOfTransaction.containsKey(new TxnID(((TransactionImpl) txn).getTxnIdMostBits(),
                ((TransactionImpl) txn).getTxnIdLeastBits())));
        txn.commit().get();
        Field field = TransactionImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(txn, TransactionImpl.State.OPEN);
        assertFalse(individualAckOfTransaction.containsKey(new TxnID(((TransactionImpl) txn).getTxnIdMostBits(),
                ((TransactionImpl) txn).getTxnIdLeastBits())));

        message = consumer.receive();
        assertEquals(new String(message.getData()), TEST2);
        consumer.acknowledgeAsync(message.getMessageId(), txn).get();
        assertTrue(individualAckOfTransaction.containsKey(new TxnID(((TransactionImpl) txn).getTxnIdMostBits(),
                ((TransactionImpl) txn).getTxnIdLeastBits())));

        PartitionedTopicMetadata partitionedTopicMetadata =
                ((PulsarClientImpl) pulsarClient).getLookup()
                        .getPartitionedTopicMetadata(TopicName.TRANSACTION_COORDINATOR_ASSIGN).get();
        Transaction lowWaterMarkTxn = null;
        for (int i = 0; i < partitionedTopicMetadata.partitions; i++) {
            lowWaterMarkTxn = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.SECONDS)
                    .build().get();
            if (((TransactionImpl) lowWaterMarkTxn).getTxnIdMostBits() == ((TransactionImpl) txn).getTxnIdMostBits()) {
                break;
            }
        }

        if (lowWaterMarkTxn != null &&
                ((TransactionImpl) lowWaterMarkTxn).getTxnIdMostBits() == ((TransactionImpl) txn).getTxnIdMostBits()) {
            producer.newMessage(lowWaterMarkTxn).value(TEST3.getBytes()).send();

            message = consumer.receive(2, TimeUnit.SECONDS);
            assertEquals(new String(message.getData()), TEST3);
            consumer.acknowledgeAsync(message.getMessageId(), lowWaterMarkTxn).get();

            assertTrue(individualAckOfTransaction.containsKey(new TxnID(((TransactionImpl) txn).getTxnIdMostBits(),
                    ((TransactionImpl) txn).getTxnIdLeastBits())));

            assertTrue(individualAckOfTransaction
                    .containsKey(new TxnID(((TransactionImpl) lowWaterMarkTxn).getTxnIdMostBits(),
                            ((TransactionImpl) lowWaterMarkTxn).getTxnIdLeastBits())));
            lowWaterMarkTxn.commit().get();

            assertFalse(individualAckOfTransaction.containsKey(new TxnID(((TransactionImpl) txn).getTxnIdMostBits(),
                    ((TransactionImpl) txn).getTxnIdLeastBits())));

            assertFalse(individualAckOfTransaction
                    .containsKey(new TxnID(((TransactionImpl) lowWaterMarkTxn).getTxnIdMostBits(),
                            ((TransactionImpl) lowWaterMarkTxn).getTxnIdLeastBits())));

        } else {
            fail();
        }
    }

    @Test
    public void testTBLowWaterMarkEndToEnd() throws Exception {
        Transaction txn1 = pulsarClient.newTransaction()
                .withTransactionTimeout(500, TimeUnit.SECONDS)
                .build().get();
        Transaction txn2 = pulsarClient.newTransaction()
                .withTransactionTimeout(500, TimeUnit.SECONDS)
                .build().get();
        while (txn2.getTxnID().getMostSigBits() != txn1.getTxnID().getMostSigBits()) {
            txn2 = pulsarClient.newTransaction()
                    .withTransactionTimeout(500, TimeUnit.SECONDS)
                    .build().get();
        }

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        producer.newMessage(txn1).send();
        producer.newMessage(txn2).send();

        txn1.commit().get();
        txn2.commit().get();

        Field field = TransactionImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(txn1, TransactionImpl.State.OPEN);

        AtomicLong pendingWriteOps = Whitebox.getInternalState(getPulsarServiceList().get(0)
                .getBrokerService().getTopic(TopicName.get(TOPIC).toString(),
                        false).get().get(), "pendingWriteOps");
        try {
            producer.newMessage(txn1).send();
            fail();
        } catch (PulsarClientException.NotAllowedException ignore) {
            // no-op
        }

        assertEquals(pendingWriteOps.get(), 0);
    }

    @Test
    public void testLowWaterMarkForDifferentTC() throws Exception {
        String subName = "sub";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC)
                .subscriptionName(subName)
                .subscribe();

        Transaction txn1 = pulsarClient.newTransaction()
                .withTransactionTimeout(500, TimeUnit.SECONDS)
                .build().get();
        Transaction txn2 = pulsarClient.newTransaction()
                .withTransactionTimeout(500, TimeUnit.SECONDS)
                .build().get();
        while (txn2.getTxnID().getMostSigBits() == txn1.getTxnID().getMostSigBits()) {
            txn2 = pulsarClient.newTransaction()
                    .withTransactionTimeout(500, TimeUnit.SECONDS)
                    .build().get();
        }
        Transaction txn3 = pulsarClient.newTransaction()
                .withTransactionTimeout(500, TimeUnit.SECONDS)
                .build().get();
        while (txn3.getTxnID().getMostSigBits() != txn2.getTxnID().getMostSigBits()) {
            txn3 = pulsarClient.newTransaction()
                    .withTransactionTimeout(500, TimeUnit.SECONDS)
                    .build().get();
        }

        Transaction txn4 = pulsarClient.newTransaction()
                .withTransactionTimeout(500, TimeUnit.SECONDS)
                .build().get();
        while (txn4.getTxnID().getMostSigBits() != txn1.getTxnID().getMostSigBits()) {
            txn4 = pulsarClient.newTransaction()
                    .withTransactionTimeout(500, TimeUnit.SECONDS)
                    .build().get();
        }

        for (int i = 0; i < 10; i++) {
            producer.newMessage().send();
        }

        producer.newMessage(txn1).send();
        producer.newMessage(txn2).send();
        producer.newMessage(txn3).send();
        producer.newMessage(txn4).send();

        Message<byte[]> message1 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message1.getMessageId(), txn1);
        Message<byte[]> message2 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message2.getMessageId(), txn2);
        Message<byte[]> message3 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message3.getMessageId(), txn3);
        Message<byte[]> message4 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message4.getMessageId(), txn4);

        txn1.commit().get();
        txn2.commit().get();

        Field field = TransactionImpl.class.getDeclaredField("state");
        field.setAccessible(true);
        field.set(txn1, TransactionImpl.State.OPEN);
        field.set(txn2, TransactionImpl.State.OPEN);

        producer.newMessage(txn1).send();
        producer.newMessage(txn2).send();

        Message<byte[]> message5 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message5.getMessageId(), txn1);
        Message<byte[]> message6 = consumer.receive(5, TimeUnit.SECONDS);
        consumer.acknowledgeAsync(message6.getMessageId(), txn2);

        txn3.commit().get();
        TxnID txnID1 = txn1.getTxnID();
        TxnID txnID2 = txn2.getTxnID();
        Awaitility.await().untilAsserted(() -> {
            assertTrue(checkTxnIsOngoingInTP(txnID1, subName));
            assertTrue(checkTxnIsOngoingInTP(txnID2, subName));
            assertTrue(checkTxnIsOngoingInTB(txnID1));
            assertTrue(checkTxnIsOngoingInTB(txnID2));
        });

        txn4.commit().get();

        Awaitility.await().untilAsserted(() -> {
            assertFalse(checkTxnIsOngoingInTP(txnID1, subName));
            assertFalse(checkTxnIsOngoingInTP(txnID2, subName));
            assertFalse(checkTxnIsOngoingInTB(txnID1));
            assertFalse(checkTxnIsOngoingInTB(txnID2));
        });
    }

    private boolean checkTxnIsOngoingInTP(TxnID txnID, String subName) throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService()
                .getTopic(TopicName.get(TOPIC).toString(), false)
                .get().get();

        PersistentSubscription persistentSubscription = persistentTopic.getSubscription(subName);

        Field field1 = PersistentSubscription.class.getDeclaredField("pendingAckHandle");
        field1.setAccessible(true);
        PendingAckHandleImpl pendingAckHandle = (PendingAckHandleImpl) field1.get(persistentSubscription);

        Field field2 = PendingAckHandleImpl.class.getDeclaredField("individualAckOfTransaction");
        field2.setAccessible(true);
        LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>> individualAckOfTransaction =
                (LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>>) field2.get(pendingAckHandle);
        return individualAckOfTransaction.containsKey(txnID);
    }

    private boolean checkTxnIsOngoingInTB(TxnID txnID) throws Exception {
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService()
                .getTopic(TopicName.get(TOPIC).toString(), false)
                .get().get();

        TopicTransactionBuffer topicTransactionBuffer =
                (TopicTransactionBuffer) persistentTopic.getTransactionBuffer();
        Field field3 = TopicTransactionBuffer.class.getDeclaredField("ongoingTxns");
        field3.setAccessible(true);
        LinkedMap<TxnID, PositionImpl> ongoingTxns =
                (LinkedMap<TxnID, PositionImpl>) field3.get(topicTransactionBuffer);
        return ongoingTxns.containsKey(txnID);

    }


}
