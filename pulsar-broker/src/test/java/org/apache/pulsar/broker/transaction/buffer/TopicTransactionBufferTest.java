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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Cleanup;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertTrue;
import io.opentelemetry.api.common.Attributes;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.OpenTelemetryTopicStats;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;
import org.apache.pulsar.broker.transaction.buffer.utils.TransactionBufferTestImpl;
import org.apache.pulsar.broker.transaction.buffer.utils.TransactionBufferTestProvider;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_TENANT, "tnx")
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "tnx/ns1")
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic)
                .putAll(OpenTelemetryAttributes.TransactionStatus.COMMITTED.attributes)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.FAILURE.attributes)
                .build();

        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        assertMetricLongSumValue(
                pulsarTestContexts.get(0).getOpenTelemetryMetricReader().collectAllMetrics(),
                OpenTelemetryTopicStats.TRANSACTION_BUFFER_CLIENT_OPERATION_COUNTER, attributes, 0);

        producer.newMessage(txn).value("test".getBytes()).send();
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(TopicName.get(topic).toString(), false).get().get();
        FieldUtils.writeField(persistentTopic.getManagedLedger(), "state", ManagedLedgerImpl.State.WriteFailed, true);
        txn.commit().get();

        assertMetricLongSumValue(
                pulsarTestContexts.get(0).getOpenTelemetryMetricReader().collectAllMetrics(),
                OpenTelemetryTopicStats.TRANSACTION_BUFFER_CLIENT_OPERATION_COUNTER, attributes, 1);
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
     * This test mainly test the following two point:
     *      1. `getLastMessageIds` will get max read position.
     *      Send two message |1:0|1:1|; mock max read position as |1:0|; `getLastMessageIds` will get |1:0|.
     *      2. `getLastMessageIds` will wait Transaction buffer recover completely.
     *      Mock `checkIfTBRecoverCompletely` return an exception, `getLastMessageIds` will fail too.
     *      Mock `checkIfTBRecoverCompletely` return null, `getLastMessageIds` will get correct result.
     */
    @Test
    public void testGetMaxPositionAfterTBReady() throws Exception {
        // 1. Prepare test environment.
        String topic = "persistent://" + NAMESPACE1 + "/testGetMaxReadyPositionAfterTBReady";
        // 1.1 Mock component.
        TransactionBuffer transactionBuffer = Mockito.spy(TransactionBuffer.class);
        when(transactionBuffer.checkIfTBRecoverCompletely())
                // If the Transaction buffer failed to recover, we can not get the correct last max read id.
                .thenReturn(CompletableFuture.failedFuture(new Throwable("Mock fail")))
                // If the transaction buffer recover successfully, the max read position can be acquired successfully.
                .thenReturn(CompletableFuture.completedFuture(null));
        TransactionBufferProvider transactionBufferProvider = Mockito.spy(TransactionBufferProvider.class);
        Mockito.doReturn(transactionBuffer).when(transactionBufferProvider).newTransactionBuffer(any());
        TransactionBufferProvider originalTBProvider = getPulsarServiceList().get(0).getTransactionBufferProvider();
        Mockito.doReturn(transactionBufferProvider).when(getPulsarServiceList().get(0)).getTransactionBufferProvider();
        // 2. Building producer and consumer.
        admin.topics().createNonPartitionedTopic(topic);
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        // 3. Send message and test the exception can be handled as expected.
        MessageIdImpl messageId = (MessageIdImpl) producer.newMessage().send();
        producer.newMessage().send();
        Mockito.doReturn(PositionFactory.create(messageId.getLedgerId(), messageId.getEntryId()))
                .when(transactionBuffer).getMaxReadPosition();
        try {
            consumer.getLastMessageIds();
            fail();
        } catch (PulsarClientException exception) {
            assertTrue(exception.getMessage().contains("Failed to recover Transaction Buffer."));
        }
        List<TopicMessageId> messageIdList = consumer.getLastMessageIds();
        assertEquals(messageIdList.size(), 1);
        TopicMessageIdImpl actualMessageID = (TopicMessageIdImpl) messageIdList.get(0);
        assertEquals(messageId.getLedgerId(), actualMessageID.getLedgerId());
        assertEquals(messageId.getEntryId(), actualMessageID.getEntryId());
        // 4. Clean resource
        Mockito.doReturn(originalTBProvider).when(getPulsarServiceList().get(0)).getTransactionBufferProvider();
    }

    /**
     * Add a E2E test for the get last message ID. It tests 4 cases.
     *     <p>
     *         1. Only normal messages in the topic.
     *         2. There are ongoing transactions, last message ID will not be updated until transaction end.
     *         3. Aborted transaction will make the last message ID be updated as expected.
     *         4. Committed transaction will make the last message ID be updated as expected.
     *     </p>
     */
    @Test
    public void testGetLastMessageIdsWithOngoingTransactions() throws Exception {
        // 1. Prepare environment
        String topic = "persistent://" + NAMESPACE1 + "/testGetLastMessageIdsWithOngoingTransactions";
        String subName = "my-subscription";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscribe();

        // 2. Test last max read position can be required correctly.
        // 2.1 Case1: send 3 original messages. |1:0|1:1|1:2|
        MessageIdImpl expectedLastMessageID = null;
        for (int i = 0; i < 3; i++) {
            expectedLastMessageID = (MessageIdImpl) producer.newMessage().send();
        }
        assertGetLastMessageId(consumer, expectedLastMessageID);
        // 2.2 Case2: send 2 ongoing transactional messages and 2 original messages.
        // |1:0|1:1|1:2|txn1:start->1:3|1:4|txn2:start->1:5.
        Transaction txn1 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.HOURS)
                .build()
                .get();
        Transaction txn2 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.HOURS)
                .build()
                .get();

        // |1:0|1:1|1:2|txn1:1:3|
        producer.newMessage(txn1).send();

        // |1:0|1:1|1:2|txn1:1:3|1:4|
        MessageIdImpl expectedLastMessageID1 = (MessageIdImpl) producer.newMessage().send();

        // |1:0|1:1|1:2|txn1:1:3|1:4|txn2:1:5|
        producer.newMessage(txn2).send();

        // 2.2.1 Last message ID will not change when txn1 and txn2 do not end.
        assertGetLastMessageId(consumer, expectedLastMessageID);

        // 2.2.2 Last message ID will update to 1:4 when txn1 committed.
        // |1:0|1:1|1:2|txn1:1:3|1:4|txn2:1:5|tx1:commit->1:6|
        txn1.commit().get(5, TimeUnit.SECONDS);
        assertGetLastMessageId(consumer, expectedLastMessageID1);

        // 2.2.3 Last message ID will still to 1:4 when txn2 aborted.
        // |1:0|1:1|1:2|txn1:1:3|1:4|txn2:1:5|tx1:commit->1:6|tx2:abort->1:7|
        txn2.abort().get(5, TimeUnit.SECONDS);
        assertGetLastMessageId(consumer, expectedLastMessageID1);

        // Handle the case of the maxReadPosition < lastPosition, but it's an aborted transactional message.
        Transaction txn3 = pulsarClient.newTransaction()
                .build()
                .get();
        producer.newMessage(txn3).send();
        assertGetLastMessageId(consumer, expectedLastMessageID1);
        txn3.abort().get(5, TimeUnit.SECONDS);
        assertGetLastMessageId(consumer, expectedLastMessageID1);
    }

    /**
     * produce 3 messages and then trigger a ledger switch,
     * then create a transaction and send a transactional message.
     * As there are messages in the new ledger, the reader should be able to read the messages.
     * But reader.hasMessageAvailable() returns false if the entry id of  max read position is -1.
     * @throws Exception
     */
    @Test
    public void testGetLastMessageIdsWithOpenTransactionAtLedgerHead() throws Exception {
        String topic = "persistent://" + NAMESPACE1 + "/testGetLastMessageIdsWithOpenTransactionAtLedgerHead";
        String subName = "my-subscription";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscribe();
        MessageId expectedLastMessageID = null;
        for (int i = 0; i < 3; i++) {
            expectedLastMessageID = producer.newMessage().value(String.valueOf(i).getBytes()).send();
            System.out.println("expectedLastMessageID: " + expectedLastMessageID);
        }
        triggerLedgerSwitch(topic);
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.HOURS)
                .build()
                .get();
        producer.newMessage(txn).send();

        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .create();
        assertTrue(reader.hasMessageAvailable());
    }

    private void triggerLedgerSwitch(String topicName) throws Exception{
        admin.topics().unload(topicName);
        Awaitility.await().until(() -> {
            CompletableFuture<Optional<Topic>> topicFuture =
                    getPulsarServiceList().get(0).getBrokerService().getTopic(topicName, false);
            if (!topicFuture.isDone() || topicFuture.isCompletedExceptionally()){
                return false;
            }
            Optional<Topic> topicOptional = topicFuture.join();
            if (!topicOptional.isPresent()){
                return false;
            }
            PersistentTopic persistentTopic = (PersistentTopic) topicOptional.get();
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            return managedLedger.getState() == ManagedLedgerImpl.State.LedgerOpened;
        });
    }

    private void assertGetLastMessageId(Consumer<?> consumer, MessageIdImpl expected) throws Exception {
        TopicMessageIdImpl actual = (TopicMessageIdImpl) consumer.getLastMessageIds().get(0);
        assertEquals(expected.getEntryId(), actual.getEntryId());
        assertEquals(expected.getLedgerId(), actual.getLedgerId());
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
        this.pulsarServiceList.forEach(pulsarService ->  {
            pulsarService.setTransactionBufferProvider(new TransactionBufferTestProvider());
        });
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

        // 2. Set a new future in transaction buffer as `transactionBufferFuture` to simulate whether the
        // transaction buffer recover completely.
        TransactionBufferTestImpl topicTransactionBuffer = (TransactionBufferTestImpl) persistentTopic
                .getTransactionBuffer();
        CompletableFuture<Position> completableFuture = new CompletableFuture<>();
        CompletableFuture<Position> originalFuture = topicTransactionBuffer.getPublishFuture();
        topicTransactionBuffer.setPublishFuture(completableFuture);
        topicTransactionBuffer.setState(TopicTransactionBufferState.State.Ready);
        // Register this topic to the transaction in advance to avoid the sending request pending here.
        ((TransactionImpl) transaction).registerProducedTopic(topic).get(5, TimeUnit.SECONDS);
        // 3. Test the messages sent before transaction buffer ready is in order.
        for (int i = 0; i < 50; i++) {
            producer.newMessage(transaction).value(i).sendAsync();
        }
        // 4. Test the messages sent after transaction buffer ready is in order.
        completableFuture.complete(originalFuture.get());
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
        this.pulsarServiceList.forEach(pulsarService ->  {
            pulsarService.setTransactionBufferProvider(new TransactionBufferTestProvider());
        });
        String topic = "persistent://" + NAMESPACE1 + "/testRefCountWhenAppendBufferToTxn";
        admin.topics().createNonPartitionedTopic(topic);
        PersistentTopic persistentTopic = (PersistentTopic) pulsarServiceList.get(0).getBrokerService()
                .getTopic(topic, false)
                .get()
                .get();
        TransactionBufferTestImpl topicTransactionBuffer = (TransactionBufferTestImpl) persistentTopic
                .getTransactionBuffer();
        // 2. Test reference count does not change in the method `appendBufferToTxn`.
        // 2.1 Test sending first transaction message, this will take a snapshot.
        ByteBuf byteBuf1 = Unpooled.buffer();
        topicTransactionBuffer.appendBufferToTxn(new TxnID(1, 1), 1L, byteBuf1)
                .get(5, TimeUnit.SECONDS);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(byteBuf1.refCnt(), 1));
        // 2.2 Test send the second transaction message, this will not take snapshots.
        ByteBuf byteBuf2 = Unpooled.buffer();
        topicTransactionBuffer.appendBufferToTxn(new TxnID(1, 1), 1L, byteBuf1)
                .get(5, TimeUnit.SECONDS);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(byteBuf2.refCnt(), 1));
        // 2.3 Test sending message failed.
        topicTransactionBuffer.setPublishFuture(FutureUtil.failedFuture(new Exception("fail")));
        ByteBuf byteBuf3 = Unpooled.buffer();
        try {
            topicTransactionBuffer.appendBufferToTxn(new TxnID(1, 1), 1L, byteBuf1)
                    .get(5, TimeUnit.SECONDS);
            fail();
        } catch (Exception e) {
            assertEquals(e.getCause().getMessage(), "fail");
        }
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(byteBuf3.refCnt(), 1));
        // 3. release resource
        byteBuf1.release();
        byteBuf2.release();
        byteBuf3.release();
    }
}
