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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.transaction.buffer.impl.InMemTransactionBufferProvider;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleState;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@PrepareForTest({ ZooKeeperDataCache.class, BrokerService.class })
@PowerMockIgnore({"org.apache.logging.log4j.*"})
@Test(groups = "broker")
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
    private ManagedLedgerConfig managedLedgerConfigMock;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    final String subName = "subscriptionName";

    final TxnID txnID1 = new TxnID(1,1);
    final TxnID txnID2 = new TxnID(1,2);

    private static final Logger log = LoggerFactory.getLogger(PersistentTopicTest.class);

    private OrderedExecutor executor;
    private EventLoopGroup eventLoopGroup;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("persistent-subscription-test").build();
        eventLoopGroup = new NioEventLoopGroup();

        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setTransactionCoordinatorEnabled(true);
        pulsarMock = spy(new PulsarService(svcConfig));
        doReturn(new InMemTransactionBufferProvider()).when(pulsarMock).getTransactionBufferProvider();
        doReturn(new TransactionPendingAckStoreProvider() {
            @Override
            public CompletableFuture<PendingAckStore> newPendingAckStore(PersistentSubscription subscription) {
                return CompletableFuture.completedFuture(new PendingAckStore() {
                    @Override
                    public void replayAsync(PendingAckHandleImpl pendingAckHandle, ScheduledExecutorService executorService) {
                        try {
                            Field field = PendingAckHandleState.class.getDeclaredField("state");
                            field.setAccessible(true);
                            field.set(pendingAckHandle, PendingAckHandleState.State.Ready);
                        } catch (NoSuchFieldException | IllegalAccessException e) {
                            fail();
                        }
                    }

                    @Override
                    public CompletableFuture<Void> closeAsync() {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> appendIndividualAck(TxnID txnID, List<MutablePair<PositionImpl, Integer>> positions) {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> appendCumulativeAck(TxnID txnID, PositionImpl position) {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> appendCommitMark(TxnID txnID, AckType ackType) {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> appendAbortMark(TxnID txnID, AckType ackType) {
                        return CompletableFuture.completedFuture(null);
                    }
                });
            }
        }).when(pulsarMock).getTransactionPendingAckStoreProvider();
        doReturn(svcConfig).when(pulsarMock).getConfiguration();
        doReturn(mock(Compactor.class)).when(pulsarMock).getCompactor();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsarMock).getManagedLedgerFactory();

        ZooKeeper zkMock = createMockZooKeeper();
        doReturn(zkMock).when(pulsarMock).getZkClient();
        doReturn(createMockBookKeeper(executor))
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

        brokerMock = spy(new BrokerService(pulsarMock, eventLoopGroup));
        doNothing().when(brokerMock).unloadNamespaceBundlesGracefully();
        doReturn(brokerMock).when(pulsarMock).getBrokerService();

        ledgerMock = mock(ManagedLedgerImpl.class);
        cursorMock = mock(ManagedCursorImpl.class);
        managedLedgerConfigMock = mock(ManagedLedgerConfig.class);
        doReturn(new ManagedCursorContainer()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();
        doReturn(new PositionImpl(1, 50)).when(cursorMock).getMarkDeletedPosition();
        doReturn(ledgerMock).when(cursorMock).getManagedLedger();
        doReturn(managedLedgerConfigMock).when(ledgerMock).getConfig();
        doReturn(false).when(managedLedgerConfigMock).isAutoSkipNonRecoverableData();

        topic = new PersistentTopic(successTopicName, ledgerMock, brokerMock);

        consumerMock = mock(Consumer.class);

        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        brokerMock.close(); //to clear pulsarStats
        try {
            pulsarMock.close();
        } catch (Exception e) {
            log.warn("Failed to close pulsar service", e);
            throw e;
        }

        executor.shutdownNow();
        eventLoopGroup.shutdownGracefully().get();
    }

    @Test
    public void testCanAcknowledgeAndCommitForTransaction() throws ExecutionException, InterruptedException {
        doAnswer((invocationOnMock) -> {
            ((AsyncCallbacks.DeleteCallback) invocationOnMock.getArguments()[1])
                    .deleteComplete(invocationOnMock.getArguments()[2]);
            return null;
        }).when(cursorMock).asyncDelete(any(List.class), any(AsyncCallbacks.DeleteCallback.class), any());

        List<MutablePair<PositionImpl, Integer>> positionsPair = new ArrayList<>();
        positionsPair.add(new MutablePair<>(new PositionImpl(1, 1), 0));
        positionsPair.add(new MutablePair<>(new PositionImpl(1, 3), 0));
        positionsPair.add(new MutablePair<>(new PositionImpl(1, 5), 0));

        doAnswer((invocationOnMock) -> {
            assertTrue(Arrays.deepEquals(((List)invocationOnMock.getArguments()[0]).toArray(),
                    positionsPair.toArray()));
            ((AsyncCallbacks.MarkDeleteCallback) invocationOnMock.getArguments()[2])
                    .markDeleteComplete(invocationOnMock.getArguments()[3]);
            return null;
        }).when(cursorMock).asyncMarkDelete(any(), any(), any(AsyncCallbacks.MarkDeleteCallback.class), any());

        // Single ack for txn
        persistentSubscription.transactionIndividualAcknowledge(txnID1, positionsPair);

        // Commit txn
        persistentSubscription.endTxn(txnID1.getMostSigBits(), txnID1.getLeastSigBits(), TxnAction.COMMIT_VALUE, -1).get();

        List<PositionImpl> positions = new ArrayList<>();
        positions.add(new PositionImpl(3, 100));

        // Cumulative ack for txn
        persistentSubscription.transactionCumulativeAcknowledge(txnID1, positions);

        doAnswer((invocationOnMock) -> {
            assertEquals(((PositionImpl) invocationOnMock.getArguments()[0]).compareTo(new PositionImpl(3, 100)), 0);
            ((AsyncCallbacks.MarkDeleteCallback) invocationOnMock.getArguments()[2])
                    .markDeleteComplete(invocationOnMock.getArguments()[3]);
            return null;
        }).when(cursorMock).asyncMarkDelete(any(), any(), any(AsyncCallbacks.MarkDeleteCallback.class), any());

        // Commit txn
        persistentSubscription.endTxn(txnID1.getMostSigBits(), txnID1.getLeastSigBits(), TxnAction.COMMIT_VALUE, -1).get();
    }

    @Test
    public void testCanAcknowledgeAndAbortForTransaction() throws BrokerServiceException, InterruptedException {
        List<MutablePair<PositionImpl, Integer>> positionsPair = new ArrayList<>();
        positionsPair.add(new MutablePair<>(new PositionImpl(2, 1), 0));
        positionsPair.add(new MutablePair<>(new PositionImpl(2, 3), 0));
        positionsPair.add(new MutablePair<>(new PositionImpl(2, 5), 0));

        doAnswer((invocationOnMock) -> {
            ((AsyncCallbacks.DeleteCallback) invocationOnMock.getArguments()[1])
                    .deleteComplete(invocationOnMock.getArguments()[2]);
            return null;
        }).when(cursorMock).asyncDelete(any(List.class), any(AsyncCallbacks.DeleteCallback.class), any());

        doReturn(CommandSubscribe.SubType.Exclusive).when(consumerMock).subType();
        Awaitility.await().until(() -> {
            try {
                persistentSubscription.addConsumer(consumerMock);
                return true;
            } catch (Exception e) {
                return false;
            }
        });

        // Single ack for txn1
        persistentSubscription.transactionIndividualAcknowledge(txnID1, positionsPair);

        List<PositionImpl> positions = new ArrayList<>();
        positions.add(new PositionImpl(1, 100));

        // Cumulative ack for txn1
        persistentSubscription.transactionCumulativeAcknowledge(txnID1, positions);

        positions.clear();
        positions.add(new PositionImpl(2, 1));

        // Can not single ack message already acked.
        try {
            persistentSubscription.transactionIndividualAcknowledge(txnID2, positionsPair).get();
            fail("Single acknowledge for transaction2 should fail. ");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getMessage(),"[persistent://prop/use/ns-abc/successTopic][subscriptionName] " +
                    "Transaction:(1,2) try to ack message:2:1 in pending ack status.");
        }

        positions.clear();
        positions.add(new PositionImpl(2, 50));

        // Can not cumulative ack message for another txn.
        try {
            persistentSubscription.transactionCumulativeAcknowledge(txnID2, positions).get();
            fail("Cumulative acknowledge for transaction2 should fail. ");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionConflictException);
            assertEquals(e.getCause().getMessage(),"[persistent://prop/use/ns-abc/successTopic]" +
                    "[subscriptionName] Transaction:(1,2) try to cumulative batch ack position: " +
                    "2:50 within range of current currentPosition: 1:100");
        }

        List<Position> positionList = new ArrayList<>();
        positionList.add(new PositionImpl(1, 1));
        positionList.add(new PositionImpl(1, 3));
        positionList.add(new PositionImpl(1, 5));
        positionList.add(new PositionImpl(3, 1));
        positionList.add(new PositionImpl(3, 3));
        positionList.add(new PositionImpl(3, 5));

        // Acknowledge from normal consumer will succeed ignoring message acked by ongoing transaction.
        persistentSubscription.acknowledgeMessage(positionList, AckType.Individual, Collections.emptyMap());

        //Abort txn.
        persistentSubscription.endTxn(txnID1.getMostSigBits(), txnID2.getLeastSigBits(), TxnAction.ABORT_VALUE, -1);

        positions.clear();
        positions.add(new PositionImpl(2, 50));

        // Retry above ack, will succeed. As abort has clear pending_ack for those messages.
        persistentSubscription.transactionCumulativeAcknowledge(txnID2, positions);

        positionsPair.clear();
        positionsPair.add(new MutablePair(new PositionImpl(2, 1), 0));

        persistentSubscription.transactionIndividualAcknowledge(txnID2, positionsPair);
    }
}
