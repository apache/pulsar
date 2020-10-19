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
package org.apache.pulsar.client.impl;

import com.google.common.collect.Sets;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStoreProvider;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.io.core.KeyValue;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TransactionPendingAckReplyAndDeleteTest extends TransactionTestBase {

    private final static int TOPIC_PARTITION = 3;

    private final static String TENANT = "tnx";
    private final static String NAMESPACE1 = TENANT + "/ns1";
    private final static String TOPIC_OUTPUT = NAMESPACE1 + "/output";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData("http://localhost:" + webServicePort));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT, TOPIC_PARTITION);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Thread.sleep(1000 * 3);
    }

    @AfterMethod
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void testCumulativePendingAckStoreAndReply() throws Exception {
        String topicName = "test";
        String subName = "test";
        MLPendingAckStoreProvider mlPendingAckStoreProvider = new MLPendingAckStoreProvider();
        PersistentTopic persistentTopic =  mock(PersistentTopic.class);
        PersistentSubscription persistentSubscription = mock(PersistentSubscription.class);
        ManagedCursorImpl managedCursor = mock(ManagedCursorImpl.class);
        PositionImpl position = PositionImpl.get(-1, -1);
        doReturn(getPulsarServiceList().get(1).getBrokerService()).when(persistentTopic).getBrokerService();
        doReturn(topicName).when(persistentTopic).getName();
        doReturn(managedCursor).when(persistentSubscription).getCursor();
        doReturn(position).when(managedCursor).getMarkDeletedPosition();
        doReturn(false).when(managedCursor).isMessageDeleted(any());
        doNothing().when(persistentSubscription).acknowledgeMessage((List<Position>) any(), any(), any());
        doReturn(CompletableFuture.completedFuture(null)).when(persistentSubscription)
                .acknowledgeMessage(any(), (List<Position>) any(), any());
        mlPendingAckStoreProvider.newPendingAckStore(persistentTopic, subName);
        CompletableFuture<PendingAckStore> completableFuture = mlPendingAckStoreProvider
                .newPendingAckStore(persistentTopic, subName);
        MLPendingAckStore mlPendingAckStore = (MLPendingAckStore) completableFuture.get();
        PendingAckHandleImpl pendingAckHandle = new PendingAckHandleImpl(topicName, subName, completableFuture);
        pendingAckHandle.setPersistentSubscription(persistentSubscription);
        Thread.sleep(1000L);

        //compare current whether write to memory
        TxnID txnID = new TxnID(1, 1);
        position = PositionImpl.get(1, 1);
        pendingAckHandle.acknowledgeMessage(txnID, Collections.singletonList(position), PulsarApi.CommandAck.AckType.Cumulative).get();
        Field field = mlPendingAckStore.getClass().getDeclaredField("pendingCumulativeAckPosition");
        field.setAccessible(true);
        KeyValue<TxnID, Position> keyValue = (KeyValue<TxnID, Position>) field.get(mlPendingAckStore);
        assertEquals(txnID, keyValue.getKey());
        field = pendingAckHandle.getClass().getDeclaredField("pendingCumulativeAckMessage");
        field.setAccessible(true);
        assertEquals(position, field.get(pendingAckHandle));
        field = pendingAckHandle.getClass().getDeclaredField("pendingCumulativeAckTxnId");
        field.setAccessible(true);
        assertEquals(txnID, field.get(pendingAckHandle));
        txnID = new TxnID(1, 1);
        position = PositionImpl.get(2, 2);
        pendingAckHandle.acknowledgeMessage(txnID, Collections.singletonList(position), PulsarApi.CommandAck.AckType.Cumulative).get();

        //compare pendingAckHandle replay whether recover success
        completableFuture = mlPendingAckStoreProvider
                .newPendingAckStore(persistentTopic, subName);
        mlPendingAckStore = (MLPendingAckStore) completableFuture.get();
        pendingAckHandle = new PendingAckHandleImpl(topicName, subName, completableFuture);
        pendingAckHandle.setPersistentSubscription(persistentSubscription);
        Thread.sleep(1000L);
        field = mlPendingAckStore.getClass().getDeclaredField("pendingCumulativeAckPosition");
        field.setAccessible(true);
        keyValue = (KeyValue<TxnID, Position>) field.get(mlPendingAckStore);
        assertEquals(txnID, keyValue.getKey());
        field = pendingAckHandle.getClass().getDeclaredField("pendingCumulativeAckMessage");
        field.setAccessible(true);
        assertEquals(position, field.get(pendingAckHandle));
        field = pendingAckHandle.getClass().getDeclaredField("pendingCumulativeAckTxnId");
        field.setAccessible(true);
        assertEquals(txnID, field.get(pendingAckHandle));

        //test PendingAckHandle commit cumulativeTxn
        pendingAckHandle.commitTxn(txnID, Collections.emptyMap()).get();
        completableFuture = mlPendingAckStoreProvider
                .newPendingAckStore(persistentTopic, subName);
        mlPendingAckStore = (MLPendingAckStore) completableFuture.get();
        pendingAckHandle = new PendingAckHandleImpl(topicName, subName, completableFuture);
        pendingAckHandle.setPersistentSubscription(persistentSubscription);
        Thread.sleep(1000L);
        field = mlPendingAckStore.getClass().getDeclaredField("pendingCumulativeAckPosition");
        field.setAccessible(true);
        keyValue = (KeyValue<TxnID, Position>) field.get(mlPendingAckStore);
        assertNull(keyValue);
        field = pendingAckHandle.getClass().getDeclaredField("pendingCumulativeAckMessage");
        field.setAccessible(true);
        assertNull(field.get(pendingAckHandle));
        field = pendingAckHandle.getClass().getDeclaredField("pendingCumulativeAckTxnId");
        field.setAccessible(true);
        assertNull(field.get(pendingAckHandle));

        //test PendingAckHandle abort cumulativeTxn
        txnID = new TxnID(2, 2);
        position = PositionImpl.get(2, 3);
        Dispatcher dispatcher = mock(Dispatcher.class);
        Consumer consumer = mock(Consumer.class);
        doReturn(dispatcher).when(persistentSubscription).getDispatcher();
        doNothing().when(dispatcher).redeliverUnacknowledgedMessages(any());
        doNothing().when(dispatcher).redeliverUnacknowledgedMessages(any(), any());
        doReturn(null).when(consumer).getPendingAcks();
        completableFuture = mlPendingAckStoreProvider
                .newPendingAckStore(persistentTopic, subName);
        mlPendingAckStore = (MLPendingAckStore) completableFuture.get();
        pendingAckHandle = new PendingAckHandleImpl(topicName, subName, completableFuture);
        pendingAckHandle.setPersistentSubscription(persistentSubscription);
        Thread.sleep(1000L);
        pendingAckHandle.acknowledgeMessage(txnID, Collections.singletonList(position),
                PulsarApi.CommandAck.AckType.Cumulative).get();
        pendingAckHandle.abortTxn(txnID, consumer).get();
        field = mlPendingAckStore.getClass().getDeclaredField("pendingCumulativeAckPosition");
        field.setAccessible(true);
        keyValue = (KeyValue<TxnID, Position>) field.get(mlPendingAckStore);
        assertNull(keyValue);
        field = pendingAckHandle.getClass().getDeclaredField("pendingCumulativeAckMessage");
        field.setAccessible(true);
        assertNull(field.get(pendingAckHandle));
        field = pendingAckHandle.getClass().getDeclaredField("pendingCumulativeAckTxnId");
        field.setAccessible(true);
        assertNull(field.get(pendingAckHandle));
    }

    @Test
    public void testIndividualPendingAckStoreAndReply() throws Exception {
        String topicName = "test";
        String subName = "test";
        MLPendingAckStoreProvider mlPendingAckStoreProvider = new MLPendingAckStoreProvider();
        PersistentTopic persistentTopic =  mock(PersistentTopic.class);
        PersistentSubscription persistentSubscription = mock(PersistentSubscription.class);
        ManagedCursorImpl managedCursor = mock(ManagedCursorImpl.class);
        PositionImpl position = PositionImpl.get(-1, -1);
        doReturn(getPulsarServiceList().get(1).getBrokerService()).when(persistentTopic).getBrokerService();
        doReturn(topicName).when(persistentTopic).getName();
        doReturn(managedCursor).when(persistentSubscription).getCursor();
        doReturn(position).when(managedCursor).getMarkDeletedPosition();
        doReturn(false).when(managedCursor).isMessageDeleted(any());
        doNothing().when(persistentSubscription).acknowledgeMessage((List<Position>) any(), any(), any());
        doReturn(CompletableFuture.completedFuture(null)).when(persistentSubscription)
                .acknowledgeMessage(any(), (List<Position>) any(), any());
        mlPendingAckStoreProvider.newPendingAckStore(persistentTopic, subName);
        CompletableFuture<PendingAckStore> completableFuture = mlPendingAckStoreProvider
                .newPendingAckStore(persistentTopic, subName);
        MLPendingAckStore mlPendingAckStore = (MLPendingAckStore) completableFuture.get();
        PendingAckHandleImpl pendingAckHandle = new PendingAckHandleImpl(topicName, subName, completableFuture);
        pendingAckHandle.setPersistentSubscription(persistentSubscription);
        Thread.sleep(1000L);

        //compare current whether write to memory
        TxnID txnID = new TxnID(1, 1);
        position = PositionImpl.get(1, 1);
        pendingAckHandle.acknowledgeMessage(txnID, Collections.singletonList(position),
                PulsarApi.CommandAck.AckType.Individual).get();
        Field field = mlPendingAckStore.getClass().getDeclaredField("pendingIndividualAckPersistentMap");
        field.setAccessible(true);
        ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<Position>> pendingIndividualAckPersistentMap =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<Position>>) field.get(mlPendingAckStore);
        assertTrue(pendingIndividualAckPersistentMap.containsKey(txnID));
        assertEquals(pendingIndividualAckPersistentMap.get(txnID).size(), 1);
        field = pendingAckHandle.getClass().getDeclaredField("pendingAckMessages");
        field.setAccessible(true);
        assertTrue(((ConcurrentOpenHashMap<Position, Position>) field.get(pendingAckHandle)).containsKey(position));

        field = pendingAckHandle.getClass().getDeclaredField("pendingIndividualAckMessagesMap");
        field.setAccessible(true);
        ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashMap<Position, Position>> pendingIndividualAckMessagesMap =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashMap<Position, Position>>) field.get(pendingAckHandle);
        assertTrue(pendingIndividualAckMessagesMap.containsKey(txnID));
        assertEquals(pendingIndividualAckMessagesMap.get(txnID).size(), 1);
        assertTrue(pendingIndividualAckMessagesMap.get(txnID).containsKey(position));

        //compare pendingAckHandle replay whether recover success
        completableFuture = mlPendingAckStoreProvider
                .newPendingAckStore(persistentTopic, subName);
        mlPendingAckStore = (MLPendingAckStore) completableFuture.get();
        pendingAckHandle = new PendingAckHandleImpl(topicName, subName, completableFuture);
        pendingAckHandle.setPersistentSubscription(persistentSubscription);
        Thread.sleep(1000L);
        field = mlPendingAckStore.getClass().getDeclaredField("pendingIndividualAckPersistentMap");
        field.setAccessible(true);
        pendingIndividualAckPersistentMap =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<Position>>) field.get(mlPendingAckStore);
        assertTrue(pendingIndividualAckPersistentMap.containsKey(txnID));
        assertEquals(pendingIndividualAckPersistentMap.get(txnID).size(), 1);
        field = pendingAckHandle.getClass().getDeclaredField("pendingAckMessages");
        field.setAccessible(true);
        assertTrue(((ConcurrentOpenHashMap<Position, Position>) field.get(pendingAckHandle)).containsKey(position));

        field = pendingAckHandle.getClass().getDeclaredField("pendingIndividualAckMessagesMap");
        field.setAccessible(true);
        pendingIndividualAckMessagesMap =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashMap<Position, Position>>) field.get(pendingAckHandle);
        assertTrue(pendingIndividualAckMessagesMap.containsKey(txnID));
        assertEquals(pendingIndividualAckMessagesMap.get(txnID).size(), 1);
        assertTrue(pendingIndividualAckMessagesMap.get(txnID).containsKey(position));

        //test PendingAckHandle commit individualTxn
        pendingAckHandle.commitTxn(txnID, Collections.emptyMap()).get();
        TxnID txnID1 = new TxnID(2, 2);
        Position position1 = PositionImpl.get(2, 2);
        pendingAckHandle.acknowledgeMessage(txnID1, Collections.singletonList(position1),
                PulsarApi.CommandAck.AckType.Individual).get();
        completableFuture = mlPendingAckStoreProvider
                .newPendingAckStore(persistentTopic, subName);
        mlPendingAckStore = (MLPendingAckStore) completableFuture.get();
        pendingAckHandle = new PendingAckHandleImpl(topicName, subName, completableFuture);
        pendingAckHandle.setPersistentSubscription(persistentSubscription);
        Thread.sleep(1000L);
        field = mlPendingAckStore.getClass().getDeclaredField("pendingIndividualAckPersistentMap");
        field.setAccessible(true);
        pendingIndividualAckPersistentMap =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<Position>>) field.get(mlPendingAckStore);
        assertTrue(pendingIndividualAckPersistentMap.containsKey(txnID1));
        assertFalse(pendingIndividualAckPersistentMap.containsKey(txnID));
        assertEquals(pendingIndividualAckPersistentMap.get(txnID1).size(), 1);
        field = pendingAckHandle.getClass().getDeclaredField("pendingAckMessages");
        field.setAccessible(true);
        ConcurrentOpenHashMap<Position, Position> pendingAckMessages =
                (ConcurrentOpenHashMap<Position, Position>) field.get(pendingAckHandle);
        assertTrue(pendingAckMessages.containsKey(position1));
        assertFalse(pendingAckMessages.containsKey(position));

        field = pendingAckHandle.getClass().getDeclaredField("pendingIndividualAckMessagesMap");
        field.setAccessible(true);
        pendingIndividualAckMessagesMap =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashMap<Position, Position>>) field.get(pendingAckHandle);
        assertTrue(pendingIndividualAckMessagesMap.containsKey(txnID1));
        assertFalse(pendingIndividualAckMessagesMap.containsKey(txnID));
        assertFalse(pendingIndividualAckMessagesMap.get(txnID1).containsKey(position));

        //test PendingAckHandle abort individualTxn
        pendingAckHandle.acknowledgeMessage(txnID1, Collections.singletonList(PositionImpl.get(3, 3)),
                PulsarApi.CommandAck.AckType.Individual).get();
        TxnID txnID2 = new TxnID(3, 3);
        Position position2 = PositionImpl.get(4, 4);
        pendingAckHandle.acknowledgeMessage(txnID2, Collections.singletonList(position2),
                PulsarApi.CommandAck.AckType.Individual).get();
        Dispatcher dispatcher = mock(Dispatcher.class);
        Consumer consumer = mock(Consumer.class);
        doReturn(dispatcher).when(persistentSubscription).getDispatcher();
        doNothing().when(dispatcher).redeliverUnacknowledgedMessages(any());
        doNothing().when(dispatcher).redeliverUnacknowledgedMessages(any(), any());
        doReturn(null).when(consumer).getPendingAcks();
        pendingAckHandle.abortTxn(txnID1, consumer).get();
        completableFuture = mlPendingAckStoreProvider
                .newPendingAckStore(persistentTopic, subName);
        mlPendingAckStore = (MLPendingAckStore) completableFuture.get();
        pendingAckHandle = new PendingAckHandleImpl(topicName, subName, completableFuture);
        pendingAckHandle.setPersistentSubscription(persistentSubscription);
        Thread.sleep(1000L);
        field = mlPendingAckStore.getClass().getDeclaredField("pendingIndividualAckPersistentMap");
        field.setAccessible(true);
        pendingIndividualAckPersistentMap =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashSet<Position>>) field.get(mlPendingAckStore);
        assertTrue(pendingIndividualAckPersistentMap.containsKey(txnID2));
        assertFalse(pendingIndividualAckPersistentMap.containsKey(txnID1));
        assertEquals(pendingIndividualAckPersistentMap.get(txnID2).size(), 1);
        field = pendingAckHandle.getClass().getDeclaredField("pendingAckMessages");
        field.setAccessible(true);
        pendingAckMessages = (ConcurrentOpenHashMap<Position, Position>) field.get(pendingAckHandle);
        assertTrue(pendingAckMessages.containsKey(position2));
        assertFalse(pendingAckMessages.containsKey(position1));
        assertFalse(pendingAckMessages.containsKey(PositionImpl.get(3, 3)));

        field = pendingAckHandle.getClass().getDeclaredField("pendingIndividualAckMessagesMap");
        field.setAccessible(true);
        pendingIndividualAckMessagesMap =
                (ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashMap<Position, Position>>) field.get(pendingAckHandle);
        assertTrue(pendingIndividualAckMessagesMap.containsKey(txnID2));
        assertTrue(pendingIndividualAckMessagesMap.get(txnID2).containsKey(position2));
        assertFalse(pendingIndividualAckMessagesMap.containsKey(txnID1));
    }
}
