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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that {@link PendingAckHandleImpl} only changes its in-memory pending-ack state on its pinned executor,
 * even when the pending-ack store or the cursor completes on another thread.
 */
@Test(groups = "broker")
public class PendingAckHandleImplThreadingTest {

    private ExecutorService pinnedExecutor;
    private PersistentSubscription subscription;
    private PendingAckStore store;
    private PendingAckHandleImpl handle;

    @BeforeMethod
    public void setUp() throws Exception {
        pinnedExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("pending-ack-pinned"));
        subscription = mock(PersistentSubscription.class, RETURNS_DEEP_STUBS);
        when(subscription.getTopicName()).thenReturn("persistent://public/default/pending-ack-threading");
        when(subscription.getName()).thenReturn("sub");
        var pulsar = subscription.getTopic().getBrokerService().getPulsar();
        when(pulsar.getTransactionExecutorProvider().getExecutor(any(PendingAckHandleImpl.class)))
                .thenReturn(pinnedExecutor);
        TransactionPendingAckStoreProvider provider = mock(TransactionPendingAckStoreProvider.class);
        when(pulsar.getTransactionPendingAckStoreProvider()).thenReturn(provider);
        when(provider.checkInitializedBefore(subscription)).thenReturn(CompletableFuture.completedFuture(true));
        store = mock(PendingAckStore.class);
        when(provider.newPendingAckStore(subscription)).thenReturn(CompletableFuture.completedFuture(store));
        doAnswer(invocation -> {
            PendingAckHandleImpl h = invocation.getArgument(0);
            h.changeToReadyState();
            h.completeHandleFuture();
            return null;
        }).when(store).replayAsync(any(), any());
        when(store.appendCommitMark(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(store.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        handle = new PendingAckHandleImpl(subscription);
        handle.pendingAckHandleFuture().get(10, TimeUnit.SECONDS);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (handle != null) {
            handle.closeAsync().get(10, TimeUnit.SECONDS);
        }
        pinnedExecutor.shutdownNow();
    }

    @Test
    public void testAppendCompletedOnAnotherThreadIsAppliedOnPinnedExecutor() throws Exception {
        CompletableFuture<Void> appended = new CompletableFuture<>();
        when(store.appendIndividualAck(any(), anyList())).thenReturn(appended);
        TxnID txnID = new TxnID(1, 1);
        CompletableFuture<Void> ackFuture = handle.individualAcknowledgeMessage(txnID, positions(5, 1));
        verify(store, timeout(10000)).appendIndividualAck(eq(txnID), anyList());

        CountDownLatch release = blockPinnedExecutor();
        try {
            // Completed here, as the store's own thread would.
            appended.complete(null);
            assertThat(handle.getStats(false).ongoingTxnSize)
                    .as("The ack must not be applied on the store's thread").isEqualTo(0);
            assertThat(ackFuture).isNotDone();
        } finally {
            release.countDown();
        }
        ackFuture.get(10, TimeUnit.SECONDS);
        assertThat(handle.getStats(false).ongoingTxnSize).isEqualTo(1);
    }

    @Test
    public void testCommitCompletedByCursorOnAnotherThreadIsAppliedOnPinnedExecutor() throws Exception {
        when(store.appendIndividualAck(any(), anyList())).thenReturn(CompletableFuture.completedFuture(null));
        TxnID txnID = new TxnID(1, 2);
        handle.individualAcknowledgeMessage(txnID, positions(5, 2)).get(10, TimeUnit.SECONDS);
        assertThat(handle.getStats(false).ongoingTxnSize).isEqualTo(1);

        CompletableFuture<Void> cursorAck = new CompletableFuture<>();
        when(subscription.acknowledgeMessageAsync(anyList(), eq(AckType.Individual), any())).thenReturn(cursorAck);
        CompletableFuture<Void> commitFuture = handle.commitTxn(txnID, Map.of(), 0L);
        verify(subscription, timeout(10000)).acknowledgeMessageAsync(anyList(), eq(AckType.Individual), any());

        CountDownLatch release = blockPinnedExecutor();
        try {
            // Completed here, as the cursor's mark-delete callback thread would.
            cursorAck.complete(null);
            assertThat(handle.getStats(false).ongoingTxnSize)
                    .as("The committed transaction must not be removed on the cursor's thread").isEqualTo(1);
            assertThat(commitFuture).isNotDone();
        } finally {
            release.countDown();
        }
        commitFuture.get(10, TimeUnit.SECONDS);
        assertThat(handle.getStats(false).ongoingTxnSize).isEqualTo(0);
    }

    private static List<MutablePair<Position, Integer>> positions(long ledgerId, long entryId) {
        return List.of(MutablePair.of(PositionFactory.create(ledgerId, entryId), 1));
    }

    /**
     * Occupies the pinned executor until the returned latch is released, so that any work handed to it stays queued.
     */
    private CountDownLatch blockPinnedExecutor() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        pinnedExecutor.execute(() -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        return release;
    }
}
