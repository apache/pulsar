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
package org.apache.pulsar.broker.transaction.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Field;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TopicTransactionBufferTest extends TransactionTestBase {


    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        setBrokerCount(1);
        setUpBase(1, 16, "persistent://" + NAMESPACE1 + "/test", 0);

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
    public void testTransactionBufferAppendMarkerWriteFailState() throws Exception {
        final String topic = "persistent://" + NAMESPACE1 + "/testPendingAckManageLedgerWriteFailState";
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        producer.newMessage(txn).value("test".getBytes()).send();
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(TopicName.get(topic).toString(), false).get().get();
        FieldUtils.writeField(persistentTopic.getManagedLedger(), "state", ManagedLedgerImpl.State.WriteFailed, true);
        txn.commit().get();
    }

    @Test
    public void testCheckDeduplicationFailedWhenCreatePersistentTopic() throws Exception {
        String topic = "persistent://" + NAMESPACE1 + "/test_" + UUID.randomUUID();
        PulsarService pulsar = pulsarServiceList.get(0);
        BrokerService brokerService0 = pulsar.getBrokerService();
        BrokerService brokerService = Mockito.spy(brokerService0);
        AtomicReference<PersistentTopic> reference = new AtomicReference<>();

        Mockito
                .doAnswer(inv -> {
                    String topic1 = inv.getArgument(0);
                    ManagedLedger ledger = inv.getArgument(1);
                    BrokerService service = inv.getArgument(2);
                    Class<?> topicKlass = inv.getArgument(3);
                    if (topicKlass.equals(PersistentTopic.class)) {
                        PersistentTopic pt = Mockito.spy(new PersistentTopic(topic1, ledger, service));
                        CompletableFuture<Void> f = CompletableFuture
                                .failedFuture(new ManagedLedgerException("This is an exception"));
                        Mockito.doReturn(f).when(pt).checkDeduplicationStatus();
                        reference.set(pt);
                        return pt;
                    } else {
                        return new NonPersistentTopic(topic1, service);
                    }
                })
                .when(brokerService)
                .newTopic(Mockito.eq(topic), Mockito.any(), Mockito.eq(brokerService),
                        Mockito.eq(PersistentTopic.class));

        brokerService.createPersistentTopic0(topic, true, new CompletableFuture<>(), Collections.emptyMap());

        Awaitility.waitAtMost(1, TimeUnit.MINUTES).until(() -> reference.get() != null);
        PersistentTopic persistentTopic = reference.get();
        TransactionBuffer buffer = persistentTopic.getTransactionBuffer();
        Assert.assertTrue(buffer instanceof TopicTransactionBuffer);
        TopicTransactionBuffer ttb = (TopicTransactionBuffer) buffer;
        TopicTransactionBufferState.State expectState = TopicTransactionBufferState.State.Close;
        Assert.assertEquals(ttb.getState(), expectState);
    }


    @Test
    public void testCloseTransactionBufferWhenTimeout() throws Exception {
        String topic = "persistent://" + NAMESPACE1 + "/testCloseTransactionBufferWhenTimeout";
        PulsarService pulsar = pulsarServiceList.get(0);
        BrokerService brokerService0 = pulsar.getBrokerService();
        BrokerService brokerService = Mockito.spy(brokerService0);
        AtomicReference<PersistentTopic> reference = new AtomicReference<>();
        pulsar.getConfiguration().setTopicLoadTimeoutSeconds(5);
        long topicLoadTimeout = TimeUnit.SECONDS.toMillis(pulsar.getConfiguration().getTopicLoadTimeoutSeconds() + 3);

        Mockito
                .doAnswer(inv -> {
                    Thread.sleep(topicLoadTimeout);
                    PersistentTopic persistentTopic = (PersistentTopic) inv.callRealMethod();
                    reference.set(persistentTopic);
                    return persistentTopic;
                })
                .when(brokerService)
                .newTopic(Mockito.eq(topic), Mockito.any(), Mockito.eq(brokerService),
                        Mockito.eq(PersistentTopic.class));

        CompletableFuture<Optional<Topic>> f = brokerService.getTopic(topic, true);

        Awaitility.waitAtMost(20, TimeUnit.SECONDS)
                .pollInterval(Duration.ofSeconds(2)).until(() -> reference.get() != null);
        PersistentTopic persistentTopic = reference.get();
        TransactionBuffer buffer = persistentTopic.getTransactionBuffer();
        Assert.assertTrue(buffer instanceof TopicTransactionBuffer);
        TopicTransactionBuffer ttb = (TopicTransactionBuffer) buffer;
        TopicTransactionBufferState.State expectState = TopicTransactionBufferState.State.Close;
        Assert.assertEquals(ttb.getState(), expectState);
        Assert.assertTrue(f.isCompletedExceptionally());
    }

    /**
     * This test verifies the state changes of a TransactionBuffer within a topic under different conditions.
     * Initially, the TransactionBuffer is in a NoSnapshot state upon topic creation.
     * It remains in the NoSnapshot state even after a normal message is sent.
     * The state changes to Ready only after a transactional message is sent.
     * The test also ensures that the TransactionBuffer can be correctly recovered after the topic is unloaded.
     */
    @Test
    public void testWriteSnapshotWhenFirstTxnMessageSend() throws Exception {
        // 1. Prepare test environment.
        String topic = "persistent://" + NAMESPACE1 + "/testWriteSnapshotWhenFirstTxnMessageSend";
        String txnMsg = "transaction message";
        String normalMsg = "normal message";
        admin.topics().createNonPartitionedTopic(topic);
        PersistentTopic persistentTopic = (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                .getTopic(topic, false)
                .get()
                .get();
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("my-sub")
                .subscribe();
        // 2. Test the state of transaction buffer after building producer with no new messages.
        // The TransactionBuffer should be in NoSnapshot state before transaction message sent.
        TopicTransactionBuffer topicTransactionBuffer = (TopicTransactionBuffer) persistentTopic.getTransactionBuffer();
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(topicTransactionBuffer.getState(), TopicTransactionBufferState.State.NoSnapshot);
        });
        // 3. Test the state of transaction buffer after sending normal messages.
        // The TransactionBuffer should still be in NoSnapshot state after a normal message is sent.
        producer.newMessage().value(normalMsg).send();
        Assert.assertEquals(topicTransactionBuffer.getState(), TopicTransactionBufferState.State.NoSnapshot);
        // 4. Test the state of transaction buffer after sending transaction messages.
        // The transaction buffer should be in Ready state at this time.
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.HOURS)
                .build()
                .get();
        producer.newMessage(transaction).value(txnMsg).send();
        Assert.assertEquals(topicTransactionBuffer.getState(), TopicTransactionBufferState.State.Ready);
        // 5. Test transaction buffer can be recovered correctly.
        // There are 4 message sent to this topic, 2 normal message and 2 transaction message |m1|m2-txn1|m3-txn1|m4|.
        // Aborting the transaction and unload the topic and then redelivering unacked messages,
        // only normal messages can be received.
        transaction.abort().get(5, TimeUnit.SECONDS);
        producer.newMessage().value(normalMsg).send();
        admin.topics().unload(topic);
        PersistentTopic persistentTopic2 = (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                .getTopic(topic, false)
                .get()
                .get();
        TopicTransactionBuffer topicTransactionBuffer2 = (TopicTransactionBuffer) persistentTopic2
                .getTransactionBuffer();
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(topicTransactionBuffer2.getState(), TopicTransactionBufferState.State.Ready);
        });
        consumer.redeliverUnacknowledgedMessages();
        for (int i = 0; i < 2; i++) {
            Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
            Assert.assertEquals(message.getValue(), normalMsg);
        }
        Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNull(message);
    }

    /**
     * Send some messages before transaction buffer ready and then send some messages after transaction buffer ready,
     * these messages should be received in order.
     */
    @Test
    public void testMessagePublishInOrder() throws Exception {
        // 1. Prepare test environment.
        String topic = "persistent://" + NAMESPACE1 + "/testMessagePublishInOrder" + RandomUtils.nextLong();
        admin.topics().createNonPartitionedTopic(topic);
        PersistentTopic persistentTopic = (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                .getTopic(topic, false)
                .get()
                .get();
        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .create();
        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.HOURS)
                .build().get();
        // 2. Set a new future in transaction buffer as `transactionBufferFuture` to stimulate whether the
        // transaction buffer recover completely.
        TopicTransactionBuffer topicTransactionBuffer = (TopicTransactionBuffer) persistentTopic.getTransactionBuffer();
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        Whitebox.setInternalState(topicTransactionBuffer, "transactionBufferFuture", completableFuture);
        Field stateField = TopicTransactionBufferState.class.getDeclaredField("state");
        stateField.setAccessible(true);
        stateField.set(topicTransactionBuffer, TopicTransactionBufferState.State.Ready);
        // Register this topic to the transaction in advance to avoid the sending request pending here.
        ((TransactionImpl) transaction).registerProducedTopic(topic).get(5, TimeUnit.SECONDS);
        // 3. Test the messages sent before transaction buffer ready is in order.
        for (int i = 0; i < 50; i++) {
            producer.newMessage(transaction).value(i).sendAsync();
        }
        // 4. Test the messages sent after transaction buffer ready is in order.
        completableFuture.complete(null);
        for (int i = 50; i < 100; i++) {
            producer.newMessage(transaction).value(i).sendAsync();
        }
        transaction.commit().get();
        for (int i = 0; i < 100; i++) {
            Message<Integer> message = consumer.receive(5, TimeUnit.SECONDS);
            Assert.assertEquals(message.getValue(), i);
        }
    }

    /**
     * Test `testMessagePublishInOrder` will test the ref count work as expected with no exception.
     * And this test is used to test the memory leak due to ref count.
     */
    @Test
    public void testRefCountWhenAppendBufferToTxn() throws Exception {
        // 1. Prepare test resource
        String topic = "persistent://" + NAMESPACE1 + "/testRefCountWhenAppendBufferToTxn";
        admin.topics().createNonPartitionedTopic(topic);
        PersistentTopic persistentTopic = (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                .getTopic(topic, false)
                .get()
                .get();
        TopicTransactionBuffer topicTransactionBuffer = (TopicTransactionBuffer) persistentTopic.getTransactionBuffer();
        // 2. Test reference count does not change in the method `appendBufferToTxn`.
        ByteBuf byteBuf = Unpooled.buffer();
        topicTransactionBuffer.appendBufferToTxn(new TxnID(1, 1), 1L, byteBuf)
                .get(5, TimeUnit.SECONDS);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(byteBuf.refCnt(), 1));
        // 3. release resource
        byteBuf.release();
    }
}
