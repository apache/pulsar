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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** A final failed snapshot must be retried even when no more messages are published. */
public class TopicTransactionBufferSnapshotRetryTest {

    @DataProvider(name = "snapshotFailures")
    public Object[][] snapshotFailures() {
        return new Object[][]{{"storage-timeout"}, {"busy-cancellation"}};
    }

    @Test(dataProvider = "snapshotFailures", timeOut = 30_000)
    public void testRetriesFailedFinalSnapshotWithoutNewPublication(String failure) throws Exception {
        try (TestContext context = new TestContext()) {
            Position published = PositionFactory.create(7, 1);
            CompletableFuture<Void> firstSnapshot = context.nextSnapshot();
            context.publish(published);
            context.tick();
            assertThat(context.snapshotPositions).containsExactly(published);

            firstSnapshot.completeExceptionally(failure.equals("storage-timeout")
                    ? new PulsarClientException.TimeoutException("Snapshot index write timed out")
                    : new BrokerServiceException.ServiceUnitNotReadyException("Snapshot index update was canceled"));
            context.awaitExecutorsIdle();
            context.nextSnapshot().complete(null);

            // This is the next scheduled timer callback. No publication or max-read-position change occurred.
            context.tick();
            assertThat(context.snapshotPositions)
                    .as("a failed final snapshot must remain dirty and retry without a later publication")
                    .containsExactly(published, published);
            context.awaitExecutorsIdle();
            context.tick();
            assertThat(context.snapshotPositions)
                    .as("a successfully retried snapshot must not remain dirty forever")
                    .containsExactly(published, published);
        }
    }

    @Test(timeOut = 30_000)
    public void testDelayedFailureAndSuccessPreserveNewerPublication() throws Exception {
        try (TestContext context = new TestContext()) {
            Position first = PositionFactory.create(7, 1);
            Position second = PositionFactory.create(7, 2);
            Position third = PositionFactory.create(7, 3);
            CompletableFuture<Void> firstSnapshot = context.nextSnapshot();
            context.publish(first);
            context.tick();

            // A later append is dirty while the older snapshot is still in flight.
            context.publish(second);
            firstSnapshot.completeExceptionally(new PulsarClientException.TimeoutException("Delayed snapshot failure"));
            context.awaitExecutorsIdle();
            CompletableFuture<Void> secondSnapshot = context.nextSnapshot();
            context.tick();
            assertThat(context.snapshotPositions).containsExactly(first, second);

            // Completing the snapshot of 'second' must not acknowledge the newer 'third' publication.
            context.publish(third);
            secondSnapshot.complete(null);
            context.awaitExecutorsIdle();
            context.nextSnapshot().complete(null);
            context.tick();
            assertThat(context.snapshotPositions).containsExactly(first, second, third);
            context.awaitExecutorsIdle();
            context.tick();
            assertThat(context.snapshotPositions).containsExactly(first, second, third);
        }
    }

    private static final class TestContext implements AutoCloseable {
        private final ExecutorService transactionExecutor = Executors.newSingleThreadExecutor();
        private final ListeningScheduledExecutorService recoveryExecutor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
        private final AtomicReference<Position> lastConfirmed = new AtomicReference<>(PositionFactory.create(7, 0));
        private final LinkedBlockingQueue<CompletableFuture<Void>> snapshots = new LinkedBlockingQueue<>();
        private final List<Position> snapshotPositions = new CopyOnWriteArrayList<>();
        private final TopicTransactionBuffer buffer;

        TestContext() throws Exception {
            PersistentTopic topic = mock(PersistentTopic.class);
            BrokerService broker = mock(BrokerService.class);
            PulsarService pulsar = mock(PulsarService.class);
            ServiceConfiguration configuration = mock(ServiceConfiguration.class);
            ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
            ManagedCursor cursor = mock(ManagedCursor.class);
            ExecutorProvider transactionExecutors = mock(ExecutorProvider.class);
            OrderedScheduler recoveryExecutors = mock(OrderedScheduler.class);
            AbortedTxnProcessor processor = mock(AbortedTxnProcessor.class);
            when(topic.getName()).thenReturn("persistent://public/default/snapshot-retry");
            when(topic.getBrokerService()).thenReturn(broker);
            when(topic.getManagedLedger()).thenReturn(ledger);
            when(topic.getMaxReadPositionCallBack())
                    .thenReturn(mock(TopicTransactionBuffer.MaxReadPositionCallBack.class));
            when(broker.getPulsar()).thenReturn(pulsar);
            when(pulsar.getConfiguration()).thenReturn(configuration);
            when(configuration.getTransactionBufferSnapshotMaxTransactionCount()).thenReturn(1000);
            when(configuration.getTransactionBufferSnapshotMinTimeInMillis()).thenReturn(1000);
            // Timer callbacks are driven explicitly; no wall-clock timer can race the asserted sequence.
            when(pulsar.getTransactionTimer()).thenReturn(mock(HashedWheelTimer.class));
            when(pulsar.getTransactionExecutorProvider()).thenReturn(transactionExecutors);
            when(transactionExecutors.getExecutor(any(Object.class))).thenReturn(transactionExecutor);
            when(pulsar.getTransactionSnapshotRecoverExecutorProvider()).thenReturn(recoveryExecutors);
            when(recoveryExecutors.chooseThread(any(Object.class))).thenReturn(recoveryExecutor);
            when(ledger.getLastConfirmedEntry()).thenAnswer(ignored -> lastConfirmed.get());
            when(ledger.getConfig()).thenReturn(new ManagedLedgerConfig());
            when(ledger.newNonDurableCursor(any(), anyString())).thenReturn(cursor);
            when(processor.recoverFromSnapshot()).thenReturn(CompletableFuture.completedFuture(lastConfirmed.get()));
            when(processor.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            when(processor.takeAbortedTxnsSnapshot(any())).thenAnswer(invocation -> {
                snapshotPositions.add(invocation.getArgument(0));
                CompletableFuture<Void> future = snapshots.poll();
                return future == null ? CompletableFuture.completedFuture(null) : future;
            });
            buffer = new TopicTransactionBuffer(topic, processor, AbortedTxnProcessor.SnapshotType.Segment);
            buffer.checkIfTBRecoverCompletely().get(5, TimeUnit.SECONDS);
        }

        CompletableFuture<Void> nextSnapshot() {
            CompletableFuture<Void> future = new CompletableFuture<>();
            snapshots.add(future);
            return future;
        }

        void publish(Position receipt) {
            lastConfirmed.set(receipt);
            // This is the normal broker callback after an append completes, not a private-state override.
            buffer.syncMaxReadPositionForNormalPublish(receipt, false);
        }

        void tick() {
            buffer.run(mock(Timeout.class));
        }

        void awaitExecutorsIdle() throws Exception {
            transactionExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            recoveryExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            try {
                buffer.closeAsync().get(5, TimeUnit.SECONDS);
            } finally {
                transactionExecutor.shutdownNow();
                recoveryExecutor.shutdownNow();
                assertThat(transactionExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThat(recoveryExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }
}
