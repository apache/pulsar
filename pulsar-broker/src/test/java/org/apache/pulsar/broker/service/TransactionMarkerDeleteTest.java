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

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionMarkerDeleteTest extends TransactionTestBase {

    private static final int TOPIC_PARTITION = 3;
    private static final String TOPIC_OUTPUT = NAMESPACE1 + "/output";
    private static final int NUM_PARTITIONS = 16;
    @BeforeMethod
    protected void setup() throws Exception {
        setUpBase(1, NUM_PARTITIONS, TOPIC_OUTPUT, TOPIC_PARTITION);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMarkerDeleteTimes() throws Exception {
        ManagedLedgerImpl managedLedger =
                spy((ManagedLedgerImpl) getPulsarServiceList().get(0).getManagedLedgerFactory().open("test"));
        PersistentTopic topic = mock(PersistentTopic.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration configuration = mock(ServiceConfiguration.class);
        doReturn(brokerService).when(topic).getBrokerService();
        doReturn(pulsarService).when(brokerService).getPulsar();
        doReturn(configuration).when(pulsarService).getConfig();
        doReturn(false).when(configuration).isTransactionCoordinatorEnabled();
        doReturn(managedLedger).when(topic).getManagedLedger();
        ManagedCursor cursor = managedLedger.openCursor("test");
        PersistentSubscription persistentSubscription =
                spyWithClassAndConstructorArgs(PersistentSubscription.class, topic, "test", cursor, false);
        Position position = managedLedger.addEntry("test".getBytes());
        persistentSubscription.acknowledgeMessage(Collections.singletonList(position),
                AckType.Individual, Collections.emptyMap());
        verify(managedLedger, times(0)).asyncReadEntry(any(), any(), any());
    }


    @Test
    public void testMarkerDelete() throws Exception {
        final String subName = "testMarkerDelete";
        final String topicName = NAMESPACE1 + "/testMarkerDelete";
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient
                .newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topicName)
                .create();

        Transaction txn1 = getTxn();
        Transaction txn2 = getTxn();
        Transaction txn3 = getTxn();
        Transaction txn4 = getTxn();

        MessageIdImpl msgId1 = (MessageIdImpl) producer.newMessage(txn1).send();
        MessageIdImpl msgId2 = (MessageIdImpl) producer.newMessage(txn2).send();
        assertNull(consumer.receive(1, TimeUnit.SECONDS));
        txn1.commit().get();

        consumer.acknowledgeAsync(consumer.receive()).get();
        assertNull(consumer.receive(1, TimeUnit.SECONDS));

        // maxReadPosition move to msgId1, msgId2 have not be committed
        assertEquals(admin.topics().getInternalStats(topicName).cursors.get(subName).markDeletePosition,
                PositionImpl.get(msgId1.getLedgerId(), msgId1.getEntryId()).toString());

        MessageIdImpl msgId3 = (MessageIdImpl) producer.newMessage(txn3).send();
        txn2.commit().get();

        consumer.acknowledgeAsync(consumer.receive()).get();
        assertNull(consumer.receive(1, TimeUnit.SECONDS));

        // maxReadPosition move to txn1 marker, so entryId is msgId2.getEntryId() + 1,
        // because send msgId2 before commit txn1
        assertEquals(admin.topics().getInternalStats(topicName).cursors.get(subName).markDeletePosition,
                PositionImpl.get(msgId2.getLedgerId(), msgId2.getEntryId() + 1).toString());

        MessageIdImpl msgId4 = (MessageIdImpl) producer.newMessage(txn4).send();
        txn3.commit().get();

        consumer.acknowledgeAsync(consumer.receive()).get();
        assertNull(consumer.receive(1, TimeUnit.SECONDS));

        // maxReadPosition move to txn2 marker, because msgId4 have not be committed
        assertEquals(admin.topics().getInternalStats(topicName).cursors.get(subName).markDeletePosition,
                PositionImpl.get(msgId3.getLedgerId(), msgId3.getEntryId() + 1).toString());

        txn4.abort().get();

        // maxReadPosition move to txn4 abort marker, so entryId is msgId4.getEntryId() + 2
        Awaitility.await().untilAsserted(() -> assertEquals(admin.topics().getInternalStats(topicName)
                .cursors.get(subName).markDeletePosition, PositionImpl.get(msgId4.getLedgerId(),
                msgId4.getEntryId() + 2).toString()));
    }

    private Transaction getTxn() throws Exception {
        return pulsarClient
                .newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS)
                .build()
                .get();
    }
}
