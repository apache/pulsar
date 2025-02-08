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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.channel.EventLoopGroup;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
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
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentSubscriptionTest {

    private PulsarTestContext pulsarTestContext;
    private ManagedLedger ledgerMock;
    private ManagedCursorImpl cursorMock;
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
        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .spyByDefault()
                .configCustomizer(config -> {
                    config.setTransactionCoordinatorEnabled(true);
                    config.setTransactionPendingAckStoreProviderClassName(
                            CustomTransactionPendingAckStoreProvider.class.getName());
                    config.setTransactionBufferProviderClassName(InMemTransactionBufferProvider.class.getName());
                })
                .useTestPulsarResources()
                .build();

        NamespaceResources namespaceResources = pulsarTestContext.getPulsarResources().getNamespaceResources();
        doReturn(Optional.of(new Policies())).when(namespaceResources)
                .getPoliciesIfCached(any());

        ledgerMock = mock(ManagedLedgerImpl.class);
        cursorMock = mock(ManagedCursorImpl.class);
        managedLedgerConfigMock = mock(ManagedLedgerConfig.class);
        doReturn(new ManagedCursorContainer()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();
        doReturn(new PositionImpl(1, 50)).when(cursorMock).getMarkDeletedPosition();
        doReturn(ledgerMock).when(cursorMock).getManagedLedger();
        doReturn(managedLedgerConfigMock).when(ledgerMock).getConfig();
        doReturn(false).when(managedLedgerConfigMock).isAutoSkipNonRecoverableData();

        topic = new PersistentTopic(successTopicName, ledgerMock, pulsarTestContext.getBrokerService());

        consumerMock = mock(Consumer.class);

        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock, false);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
            pulsarTestContext = null;
        }
    }

    @Test
    public void testCanAcknowledgeAndAbortForTransaction() throws Exception {
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
        persistentSubscription.transactionCumulativeAcknowledge(txnID1, positions).get();

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

    @Test
    public void testAcknowledgeUpdateCursorLastActive() throws Exception {
        doAnswer((invocationOnMock) -> {
            ((AsyncCallbacks.DeleteCallback) invocationOnMock.getArguments()[1])
                    .deleteComplete(invocationOnMock.getArguments()[2]);
            return null;
        }).when(cursorMock).asyncDelete(any(List.class), any(AsyncCallbacks.DeleteCallback.class), any());

        doCallRealMethod().when(cursorMock).updateLastActive();
        doCallRealMethod().when(cursorMock).getLastActive();

        List<Position> positionList = new ArrayList<>();
        positionList.add(new PositionImpl(1, 1));
        long beforeAcknowledgeTimestamp = System.currentTimeMillis();
        Thread.sleep(1);
        persistentSubscription.acknowledgeMessage(positionList, AckType.Individual, Collections.emptyMap());

        // `acknowledgeMessage` should update cursor last active
        assertTrue(persistentSubscription.cursor.getLastActive() > beforeAcknowledgeTimestamp);
    }

    public static class CustomTransactionPendingAckStoreProvider implements TransactionPendingAckStoreProvider {
        @Override
        public CompletableFuture<PendingAckStore> newPendingAckStore(PersistentSubscription subscription) {
            return CompletableFuture.completedFuture(new PendingAckStore() {
                @Override
                public void replayAsync(PendingAckHandleImpl pendingAckHandle, ExecutorService executorService) {
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
                public CompletableFuture<Void> appendIndividualAck(TxnID txnID,
                                                                   List<MutablePair<PositionImpl, Integer>> positions) {
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

        @Override
        public CompletableFuture<Boolean> checkInitializedBefore(PersistentSubscription subscription) {
            return CompletableFuture.completedFuture(true);
        }
    }

    @Test
    public void testGetReplicatedSubscriptionConfiguration() {
        Map<String, Long> properties = PersistentSubscription.getBaseCursorProperties(true);
        assertThat(properties).containsEntry(PersistentSubscription.REPLICATED_SUBSCRIPTION_PROPERTY, 1L);
        ManagedCursor cursor = mock(ManagedCursor.class);
        doReturn(properties).when(cursor).getProperties();
        assertThat(PersistentSubscription.isCursorFromReplicatedSubscription(cursor)).isTrue();

        properties = new HashMap<>();
        properties.put(PersistentSubscription.REPLICATED_SUBSCRIPTION_PROPERTY, 10L);
        doReturn(properties).when(cursor).getProperties();
        assertThat(PersistentSubscription.isCursorFromReplicatedSubscription(cursor)).isFalse();

        properties = new HashMap<>();
        properties.put(PersistentSubscription.REPLICATED_SUBSCRIPTION_PROPERTY, -1L);
        doReturn(properties).when(cursor).getProperties();
        assertThat(PersistentSubscription.isCursorFromReplicatedSubscription(cursor)).isFalse();

        properties = PersistentSubscription.getBaseCursorProperties(false);
        assertThat(properties).doesNotContainKey(PersistentSubscription.REPLICATED_SUBSCRIPTION_PROPERTY);

        properties = PersistentSubscription.getBaseCursorProperties(null);
        assertThat(properties).doesNotContainKey(PersistentSubscription.REPLICATED_SUBSCRIPTION_PROPERTY);
    }
}
