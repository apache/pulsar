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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockBookKeeper;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.SubscriptionPendingAckMessages;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.SubscriptionPendingAckMessages.PendingAckMessageEntry;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.PersistentTopicTest;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@PrepareForTest({ ZooKeeperDataCache.class, BrokerService.class })
public class PersistentSubscriptionTest {

    private PulsarService pulsarMock;
    private BrokerService brokerMock;
    private ManagedLedgerFactory mlFactoryMock;
    private ManagedLedger ledgerMock;
    private ManagedCursorImpl cursorMock;
    private ConfigurationCacheService configCacheServiceMock;
    private PersistentTopic topic;
    private PersistentSubscription persistentSubscription;
    private Consumer consumerMock;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    final String subName = "subscriptionName";

    final TxnID txnID1 = new TxnID(1,1);
    final TxnID txnID2 = new TxnID(1,2);

    private static final Logger log = LoggerFactory.getLogger(PersistentTopicTest.class);

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        pulsarMock = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsarMock).getConfiguration();
        doReturn(mock(Compactor.class)).when(pulsarMock).getCompactor();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsarMock).getManagedLedgerFactory();

        ZooKeeper zkMock = createMockZooKeeper();
        doReturn(zkMock).when(pulsarMock).getZkClient();
        doReturn(createMockBookKeeper(zkMock, pulsarMock.getOrderedExecutor().chooseThread(0)))
                .when(pulsarMock).getBookKeeperClient();

        ZooKeeperCache cache = mock(ZooKeeperCache.class);
        doReturn(30).when(cache).getZkOperationTimeoutSeconds();
        CompletableFuture getDataFuture = new CompletableFuture();
        getDataFuture.complete(Optional.empty());
        doReturn(getDataFuture).when(cache).getDataAsync(anyString(), any(), any());
        doReturn(cache).when(pulsarMock).getLocalZkCache();

        configCacheServiceMock = mock(ConfigurationCacheService.class);
        @SuppressWarnings("unchecked")
        ZooKeeperDataCache<Policies> zkPoliciesDataCacheMock = mock(ZooKeeperDataCache.class);
        doReturn(zkPoliciesDataCacheMock).when(configCacheServiceMock).policiesCache();
        doReturn(configCacheServiceMock).when(pulsarMock).getConfigurationCache();
        doReturn(Optional.empty()).when(zkPoliciesDataCacheMock).get(anyString());

        LocalZooKeeperCacheService zkCacheMock = mock(LocalZooKeeperCacheService.class);
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkPoliciesDataCacheMock).getAsync(any());
        doReturn(zkPoliciesDataCacheMock).when(zkCacheMock).policiesCache();
        doReturn(zkCacheMock).when(pulsarMock).getLocalZkCacheService();

        brokerMock = spy(new BrokerService(pulsarMock));
        doNothing().when(brokerMock).unloadNamespaceBundlesGracefully();
        doReturn(brokerMock).when(pulsarMock).getBrokerService();

        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursorImpl.class);
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();
        doReturn(new PositionImpl(1, 50)).when(cursorMock).getMarkDeletedPosition();

        topic = new PersistentTopic(successTopicName, ledgerMock, brokerMock);

        consumerMock = mock(Consumer.class);

        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false);
    }

    @AfterMethod
    public void teardown() throws Exception {
        brokerMock.close(); //to clear pulsarStats
        try {
            pulsarMock.close();
        } catch (Exception e) {
            log.warn("Failed to close pulsar service", e);
            throw e;
        }
    }

    @Test
    public void testCanRecoverPendingAckMessages() throws TransactionConflictException {
        clearInvocations(cursorMock);
        // Faking pending ack messages read from meta store.
        PositionInfo.Builder positionBuilder = PositionInfo.newBuilder()
                .setLedgerId(1)
                .setEntryId(2);
        SubscriptionPendingAckMessages.PositionList.Builder positionListBuilder =
                SubscriptionPendingAckMessages.PositionList.newBuilder()
                .addPositions(positionBuilder.build());
        SubscriptionPendingAckMessages.PendingAckMessageEntry.Builder messageEntryBuilder =
                SubscriptionPendingAckMessages.PendingAckMessageEntry.newBuilder()
                .setTxnId("3,4")
                .setPositionList(positionListBuilder.build());
        SubscriptionPendingAckMessages subscriptionPendingAckMessages =
                SubscriptionPendingAckMessages.newBuilder()
                .addAllPendingAckMessages(Arrays.asList(messageEntryBuilder.build()))
                .setPendingCumulativeAckMessagePosition(positionBuilder.setLedgerId(-1).setEntryId(-1).build())
                .build();
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            MetaStoreCallback<SubscriptionPendingAckMessages> callback = (MetaStoreCallback)args[1];
            callback.operationComplete(subscriptionPendingAckMessages, null);
            return null;
        }).when(cursorMock).getPendingAckPositionMetaInfo(any(String.class), any(MetaStoreCallback.class));

        // Recover pending ack messages
        PersistentSubscription recoveredSubscription = new PersistentSubscription(topic, subName, cursorMock, false);

        verify(cursorMock, times(1)).getPendingAckPositionMetaInfo(any(String.class), any(MetaStoreCallback.class));

        TxnID txnID = new TxnID(3,4);
        List<Position> positions = new ArrayList<>();
        positions.add(new PositionImpl(1, 2));

        // Can not single ack message already acked before recover for same txnID.
        try {
            recoveredSubscription.acknowledgeMessage(txnID, positions, AckType.Individual);
            fail("Single acknowledge for transaction should fail. ");
        } catch (TransactionConflictException e) {
            assertEquals(e.getMessage(),"[persistent://prop/use/ns-abc/successTopic][subscriptionName] " +
                    "Transaction:3,4 try to ack message:1:2 in pending ack status.");
        }

        positions.clear();
        positions.add(new PositionImpl(1, 100));

        // Cumulative ack should succeed as recovered meta data doesn't have cumulative ack message.
        recoveredSubscription.acknowledgeMessage(txnID, positions, AckType.Cumulative);
    }

    @Test
    public void testCanAcknowledgeAndCommitForTransaction() throws TransactionConflictException {
        List<Position> expectedSinglePositions = new ArrayList<>();
        expectedSinglePositions.add(new PositionImpl(1, 1));
        expectedSinglePositions.add(new PositionImpl(1, 3));
        expectedSinglePositions.add(new PositionImpl(1, 5));

        doAnswer((invocationOnMock) -> {
            assertTrue(((List)invocationOnMock.getArguments()[0]).containsAll(expectedSinglePositions));
            ((AsyncCallbacks.DeleteCallback) invocationOnMock.getArguments()[1])
                    .deleteComplete(invocationOnMock.getArguments()[2]);
            return null;
        }).when(cursorMock).asyncDelete(any(List.class), any(AsyncCallbacks.DeleteCallback.class), any());

        doAnswer((invocationOnMock) -> {
            assertTrue(((PositionImpl)invocationOnMock.getArguments()[0]).compareTo(new PositionImpl(3, 100)) == 0);
            ((AsyncCallbacks.MarkDeleteCallback) invocationOnMock.getArguments()[2])
                    .markDeleteComplete(invocationOnMock.getArguments()[3]);
            return null;
        }).when(cursorMock).asyncMarkDelete(any(), any(), any(AsyncCallbacks.MarkDeleteCallback.class), any());

        List<Position> positions = new ArrayList<>();
        positions.add(new PositionImpl(1, 1));
        positions.add(new PositionImpl(1, 3));
        positions.add(new PositionImpl(1, 5));

        // Assert what will be persist is what txn has acknowledged.
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            PositionInfo.Builder pendingCumulativeAckPositionInfoBuilder = (PositionInfo.Builder)args[0];
            List<PendingAckMessageEntry> pendingAckMessageEntryBuilderList = (List<PendingAckMessageEntry>)args[1];
            assertEquals(-1, pendingCumulativeAckPositionInfoBuilder.getLedgerId());
            assertEquals(-1, pendingCumulativeAckPositionInfoBuilder.getLedgerId());
            assertEquals(1,pendingAckMessageEntryBuilderList.size());
            assertEquals("1,1", pendingAckMessageEntryBuilderList.get(0).getTxnId());
            List<PositionInfo> positionInfoList = pendingAckMessageEntryBuilderList.get(0).getPositionList().getPositionsList();
            Position[] expectedPositions = {new PositionImpl(1, 1),
                    new PositionImpl(1, 3), new PositionImpl(1, 5)};
            List<Position> positionList = new ArrayList<>();
            positionInfoList.forEach(positionInfo -> positionList.add(new PositionImpl(positionInfo.getLedgerId(),
                                                                                        positionInfo.getEntryId())));
            assertTrue(positionList.containsAll(Arrays.asList(expectedPositions)));
            return null;
        }).when(cursorMock).persistPendingAckPositionMetaInfo(any(), any(), any(), any());

        // Single ack for txn
        persistentSubscription.acknowledgeMessage(txnID1, positions, AckType.Individual);

        positions.clear();
        positions.add(new PositionImpl(3, 100));

        // Assert what will be persist is what txn has acknowledged.
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            PositionInfo.Builder pendingCumulativeAckPositionInfoBuilder = (PositionInfo.Builder)args[0];
            List<PendingAckMessageEntry> pendingAckMessageEntryBuilderList = (List<PendingAckMessageEntry>)args[1];
            assertEquals(3, pendingCumulativeAckPositionInfoBuilder.getLedgerId());
            assertEquals(100, pendingCumulativeAckPositionInfoBuilder.getEntryId());
            assertEquals(1,pendingAckMessageEntryBuilderList.size());
            assertEquals("1,1", pendingAckMessageEntryBuilderList.get(0).getTxnId());
            List<PositionInfo> positionInfoList = pendingAckMessageEntryBuilderList.get(0).getPositionList().getPositionsList();
            Position[] expectedPositions = {new PositionImpl(1, 1),
                    new PositionImpl(1, 3), new PositionImpl(1, 5)};
            List<Position> positionList = new ArrayList<>();
            positionInfoList.forEach(positionInfo -> positionList.add(new PositionImpl(positionInfo.getLedgerId(),
                    positionInfo.getEntryId())));
            assertTrue(positionList.containsAll(Arrays.asList(expectedPositions)));
            return null;
        }).when(cursorMock).persistPendingAckPositionMetaInfo(any(), any(), any(), any());

        // Cumulative ack for txn
        persistentSubscription.acknowledgeMessage(txnID1, positions, AckType.Cumulative);

        // Commit txn
        persistentSubscription.commitTxn(txnID1, Collections.emptyMap());

        // Verify corresponding ledger method was called with expected args.
        verify(cursorMock, times(1)).asyncDelete(any(List.class), any(), any());
        verify(cursorMock, times(1)).asyncMarkDelete(any(), any(Map.class), any(), any());
        verify(cursorMock, times(2)).persistPendingAckPositionMetaInfo(any(), any(), any(), any());
    }

    @Test
    public void testCanAcknowledgeAndAbortForTransaction() throws TransactionConflictException, BrokerServiceException {
        List<Position> positions = new ArrayList<>();
        positions.add(new PositionImpl(2, 1));
        positions.add(new PositionImpl(2, 3));
        positions.add(new PositionImpl(2, 5));

        Position[] expectedSinglePositions = {new PositionImpl(3, 1),
                                        new PositionImpl(3, 3), new PositionImpl(3, 5)};

        doAnswer((invocationOnMock) -> {
            assertTrue(Arrays.deepEquals(((List)invocationOnMock.getArguments()[0]).toArray(), expectedSinglePositions));
            ((AsyncCallbacks.DeleteCallback) invocationOnMock.getArguments()[1])
                    .deleteComplete(invocationOnMock.getArguments()[2]);
            return null;
        }).when(cursorMock).asyncDelete(any(List.class), any(AsyncCallbacks.DeleteCallback.class), any());

        doReturn(PulsarApi.CommandSubscribe.SubType.Exclusive).when(consumerMock).subType();

        persistentSubscription.addConsumer(consumerMock);

        // Single ack for txn1
        persistentSubscription.acknowledgeMessage(txnID1, positions, AckType.Individual);

        positions.clear();
        positions.add(new PositionImpl(1, 100));

        // Cumulative ack for txn1
        persistentSubscription.acknowledgeMessage(txnID1, positions, AckType.Cumulative);

        positions.clear();
        positions.add(new PositionImpl(2, 1));

        // Can not single ack message already acked.
        try {
            persistentSubscription.acknowledgeMessage(txnID2, positions, AckType.Individual);
            fail("Single acknowledge for transaction2 should fail. ");
        } catch (TransactionConflictException e) {
            assertEquals(e.getMessage(),"[persistent://prop/use/ns-abc/successTopic][subscriptionName] " +
                    "Transaction:1,2 try to ack message:2:1 in pending ack status.");
        }

        positions.clear();
        positions.add(new PositionImpl(2, 50));

        // Can not cumulative ack message for another txn.
        try {
            persistentSubscription.acknowledgeMessage(txnID2, positions, AckType.Cumulative);
            fail("Cumulative acknowledge for transaction2 should fail. ");
        } catch (TransactionConflictException e) {
            System.out.println(e.getMessage());
            assertEquals(e.getMessage(),"[persistent://prop/use/ns-abc/successTopic][subscriptionName] " +
                "Transaction:1,2 try to cumulative ack message while transaction:1,1 already cumulative acked messages.");
        }

        positions.clear();
        positions.add(new PositionImpl(1, 1));
        positions.add(new PositionImpl(1, 3));
        positions.add(new PositionImpl(1, 5));
        positions.add(new PositionImpl(3, 1));
        positions.add(new PositionImpl(3, 3));
        positions.add(new PositionImpl(3, 5));

        // Acknowledge from normal consumer will succeed ignoring message acked by ongoing transaction.
        persistentSubscription.acknowledgeMessage(positions, AckType.Individual, Collections.emptyMap());

        //Abort txn.
        persistentSubscription.abortTxn(txnID1, consumerMock);

        positions.clear();
        positions.add(new PositionImpl(2, 50));

        // Retry above ack, will succeed. As abort has clear pending_ack for those messages.
        persistentSubscription.acknowledgeMessage(txnID2, positions, AckType.Cumulative);

        positions.clear();
        positions.add(new PositionImpl(2, 1));

        persistentSubscription.acknowledgeMessage(txnID2, positions, AckType.Individual);
    }
}
