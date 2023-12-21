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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;
import java.util.List;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
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
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
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
        when(transactionBuffer.checkIfTBRecoverCompletely(anyBoolean()))
                // Handle producer will check transaction buffer recover completely.
                .thenReturn(CompletableFuture.completedFuture(null))
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
        Mockito.doReturn(new PositionImpl(messageId.getLedgerId(), messageId.getEntryId()))
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
        assertMessageId(consumer, expectedLastMessageID, 0);
        // 2.2 Case2: send 2 ongoing transactional messages and 2 original messages.
        // |1:0|1:1|1:2|txn1->1:3|1:4|txn2->1:5|1:6|.
        Transaction txn1 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.HOURS)
                .build()
                .get();
        Transaction txn2 = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.HOURS)
                .build()
                .get();
        producer.newMessage(txn1).send();
        MessageIdImpl expectedLastMessageID1 = (MessageIdImpl) producer.newMessage().send();
        producer.newMessage(txn2).send();
        MessageIdImpl expectedLastMessageID2 = (MessageIdImpl) producer.newMessage().send();
        // 2.2.1 Last message ID will not change when txn1 and txn2 do not end.
        assertMessageId(consumer, expectedLastMessageID, 0);
        // 2.2.2 Last message ID will update to 1:4 when txn1 committed.
        txn1.commit().get(5, TimeUnit.SECONDS);
        assertMessageId(consumer, expectedLastMessageID1, 0);
        // 2.2.3 Last message ID will update to 1:6 when txn2 aborted.
        txn2.abort().get(5, TimeUnit.SECONDS);
        // Todo: We can not ignore the marker's position in this fix.
        assertMessageId(consumer, expectedLastMessageID2, 2);
    }

    private void assertMessageId(Consumer<?> consumer, MessageIdImpl expected, int entryOffset) throws Exception {
        TopicMessageIdImpl actual = (TopicMessageIdImpl) consumer.getLastMessageIds().get(0);
        assertEquals(expected.getEntryId(), actual.getEntryId() - entryOffset);
        assertEquals(expected.getLedgerId(), actual.getLedgerId());
    }

}
