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

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferState;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.powermock.reflect.Whitebox;
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
        Whitebox.setInternalState(persistentTopic.getManagedLedger(), "state", ManagedLedgerImpl.State.WriteFailed);
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
                    if (TopicName.get(topic1).isPersistent()) {
                        PersistentTopic pt = Mockito.spy(new PersistentTopic(topic1, ledger, service));
                        CompletableFuture<Void> f = new CompletableFuture<>();
                        f.completeExceptionally(new ManagedLedgerException("This is an exception"));
                        Mockito.doReturn(f).when(pt).checkDeduplicationStatus();
                        reference.set(pt);
                        return pt;
                    } else {
                        return new NonPersistentTopic(topic1, service);
                    }
                })
                .when(brokerService)
                .newPersistentTopic(Mockito.eq(topic), Mockito.any(), Mockito.eq(brokerService));

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
        String topic = "persistent://" + NAMESPACE1 + "/test_" + UUID.randomUUID();
        PulsarService pulsar = pulsarServiceList.get(0);
        BrokerService brokerService0 = pulsar.getBrokerService();
        BrokerService brokerService = Mockito.spy(brokerService0);
        AtomicReference<PersistentTopic> reference = new AtomicReference<>();
        pulsar.getConfiguration().setTopicLoadTimeoutSeconds(10);
        long topicLoadTimeout = TimeUnit.SECONDS.toMillis(pulsar.getConfiguration().getTopicLoadTimeoutSeconds() + 1);

        Mockito
                .doAnswer(inv -> {
                    Thread.sleep(topicLoadTimeout);
                    PersistentTopic persistentTopic = (PersistentTopic) inv.callRealMethod();
                    reference.set(persistentTopic);
                    return persistentTopic;
                })
                .when(brokerService)
                .newPersistentTopic(Mockito.eq(topic), Mockito.any(), Mockito.eq(brokerService));

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

}
