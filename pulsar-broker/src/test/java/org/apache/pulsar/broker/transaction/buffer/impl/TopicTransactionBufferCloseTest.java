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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
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
import org.apache.pulsar.client.util.ExecutorProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicTransactionBufferCloseTest {

    @Test(timeOut = 10_000)
    public void testCloseInducedRecoveryFailureDoesNotCloseTopicAgain() throws Exception {
        ContinuationBlockingRecoveryFuture recoveryFuture = new ContinuationBlockingRecoveryFuture();
        recoveryFuture.allowContinuationRegistration();
        try (TestContext context = new TestContext(recoveryFuture, PositionFactory.EARLIEST)) {
            recoveryFuture.awaitContinuationRegistrationStarted();
            when(context.processor.closeAsync()).thenAnswer(__ -> {
                recoveryFuture.completeExceptionally(
                        new BrokerServiceException.ServiceUnitNotReadyException("processor closed"));
                return CompletableFuture.completedFuture(null);
            });

            context.transactionBuffer.closeAsync().get(5, TimeUnit.SECONDS);
            context.awaitExecutorIdle();

            verify(context.topic, never()).close(true);
            assertTrue(context.transactionBuffer.getTransactionBufferFuture().isCompletedExceptionally());
        }
    }

    @Test(timeOut = 10_000)
    public void testRecoveryContinuationDoesNotStartAfterClose() throws Exception {
        ContinuationBlockingRecoveryFuture recoveryFuture = new ContinuationBlockingRecoveryFuture();
        try (TestContext context = new TestContext(recoveryFuture, PositionFactory.EARLIEST)) {
            recoveryFuture.awaitContinuationRegistrationStarted();
            recoveryFuture.complete(PositionFactory.EARLIEST);

            context.transactionBuffer.closeAsync().get(5, TimeUnit.SECONDS);
            recoveryFuture.allowContinuationRegistration();
            context.awaitExecutorIdle();

            verify(context.managedLedger, never()).newNonDurableCursor(any(), anyString());
            assertTrue(context.transactionBuffer.getTransactionBufferFuture().isCompletedExceptionally());
        } finally {
            recoveryFuture.allowContinuationRegistration();
        }
    }

    @Test(timeOut = 10_000)
    public void testLateRecoveryReadIsReleasedAfterClose() throws Exception {
        Position startPosition = PositionFactory.create(1, 0);
        Position lastPosition = PositionFactory.create(1, 3);
        CompletableFuture<Position> recoveryFuture = CompletableFuture.completedFuture(startPosition);
        CountDownLatch readStarted = new CountDownLatch(1);
        AtomicReference<AsyncCallbacks.ReadEntriesCallback> readCallback = new AtomicReference<>();
        try (TestContext context = new TestContext(recoveryFuture, lastPosition)) {
            when(context.managedCursor.hasMoreEntries()).thenReturn(true);
            doAnswer(invocation -> {
                readCallback.set(invocation.getArgument(1));
                readStarted.countDown();
                return null;
            }).when(context.managedCursor).asyncReadEntries(anyInt(), any(), anyLong(), any());

            assertTrue(readStarted.await(5, TimeUnit.SECONDS));
            context.transactionBuffer.closeAsync().get(5, TimeUnit.SECONDS);
            context.awaitExecutorIdle();

            Entry lateEntry = mock(Entry.class);
            readCallback.get().readEntriesComplete(List.of(lateEntry), null);

            verify(lateEntry).release();
            verify(context.managedCursor).asyncReadEntries(anyInt(), any(), anyLong(), any());
        }
    }

    private static final class TestContext implements AutoCloseable {
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final AbortedTxnProcessor processor = mock(AbortedTxnProcessor.class);
        private final PersistentTopic topic = mock(PersistentTopic.class);
        private final ManagedLedgerImpl managedLedger = mock(ManagedLedgerImpl.class);
        private final ManagedCursor managedCursor = mock(ManagedCursor.class);
        private final TopicTransactionBuffer transactionBuffer;

        private TestContext(CompletableFuture<Position> recoveryFuture, Position lastConfirmedEntry) throws Exception {
            BrokerService brokerService = mock(BrokerService.class);
            PulsarService pulsar = mock(PulsarService.class);
            ServiceConfiguration configuration = mock(ServiceConfiguration.class);
            ExecutorProvider executorProvider = mock(ExecutorProvider.class);

            when(topic.getName()).thenReturn("persistent://public/default/test-close-during-recovery");
            when(topic.getBrokerService()).thenReturn(brokerService);
            when(topic.getManagedLedger()).thenReturn(managedLedger);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            when(pulsar.getConfiguration()).thenReturn(configuration);
            when(pulsar.getTransactionExecutorProvider()).thenReturn(executorProvider);
            when(executorProvider.getExecutor(any(Object.class))).thenReturn(executor);
            when(managedLedger.getLastConfirmedEntry()).thenReturn(lastConfirmedEntry);
            when(managedLedger.getConfig()).thenReturn(new ManagedLedgerConfig());
            when(managedLedger.newNonDurableCursor(any(), anyString())).thenReturn(managedCursor);
            when(processor.recoverFromSnapshot()).thenReturn(recoveryFuture);
            when(processor.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

            transactionBuffer = new TopicTransactionBuffer(topic, processor, AbortedTxnProcessor.SnapshotType.Single);
        }

        private void awaitExecutorIdle() throws Exception {
            executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            transactionBuffer.closeAsync().get(5, TimeUnit.SECONDS);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /** Blocks both continuation APIs so tests can deterministically control the registration race. */
    private static final class ContinuationBlockingRecoveryFuture extends CompletableFuture<Position> {
        private final CountDownLatch registrationStarted = new CountDownLatch(1);
        private final CountDownLatch allowRegistration = new CountDownLatch(1);

        @Override
        public CompletableFuture<Void> thenAccept(Consumer<? super Position> action) {
            awaitRegistration();
            return super.thenAccept(action);
        }

        @Override
        public CompletableFuture<Void> thenAcceptAsync(Consumer<? super Position> action, Executor executor) {
            awaitRegistration();
            return super.thenAcceptAsync(action, executor);
        }

        private void awaitRegistration() {
            registrationStarted.countDown();
            try {
                assertTrue(allowRegistration.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        private void awaitContinuationRegistrationStarted() throws Exception {
            assertTrue(registrationStarted.await(5, TimeUnit.SECONDS));
        }

        private void allowContinuationRegistration() {
            allowRegistration.countDown();
        }
    }
}
