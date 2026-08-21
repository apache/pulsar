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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AbstractSnapshotAbortedTxnProcessorTest {

    @Test(timeOut = 10_000)
    public void testCloseCancelsQueuedRecovery() throws Exception {
        try (RecoveryTestContext context = RecoveryTestContext.queued()) {
            CompletableFuture<?> recoveryFuture = context.recoverFromSnapshot();
            CompletableFuture<?> repeatedRecoveryFuture = context.processor.recoverFromSnapshot();
            context.awaitRecoveryQueued();

            context.processor.closeAsync().get(5, TimeUnit.SECONDS);

            assertTrue(context.submittedRecoveryTask().isCancelled());
            context.verifyRecoveryFailedAfterClose(recoveryFuture);
            context.verifyRecoveryFailedAfterClose(repeatedRecoveryFuture);
            assertFalse(context.processor.recoveryStarted());
            assertTrue(context.processor.resourcesClosed());
        }
    }

    @Test(timeOut = 10_000)
    public void testCloseWaitsForRunningRecovery() throws Exception {
        try (RecoveryTestContext context = RecoveryTestContext.running()) {
            CompletableFuture<?> recoveryFuture = context.recoverFromSnapshot();
            context.awaitRecoveryStarted();

            Future<?> recoveryTask = context.submittedRecoveryTask();
            CompletableFuture<Void> closeFuture = context.processor.closeAsync();

            assertFalse(recoveryTask.isCancelled());
            assertFalse(recoveryFuture.isDone(), "Running recovery must not complete until its task stops");
            assertFalse(closeFuture.isDone(), "Closing must wait for running recovery before closing resources");
            assertFalse(context.processor.resourcesClosed());

            context.finishRecovery();
            context.verifyRecoveryFailedAfterClose(recoveryFuture);
            closeFuture.get(5, TimeUnit.SECONDS);
            assertTrue(context.processor.resourcesClosed());
        }
    }

    @Test(timeOut = 10_000)
    public void testCloseWaitsForSynchronousRecoveryCallback() throws Exception {
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch finishCallback = new CountDownLatch(1);
        try (RecoveryTestContext context = RecoveryTestContext.running()) {
            CompletableFuture<?> recoveryFuture = context.recoverFromSnapshot();
            CompletableFuture<Void> callbackFuture = recoveryFuture.thenRun(() -> {
                callbackStarted.countDown();
                try {
                    assertTrue(finishCallback.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            context.awaitRecoveryStarted();

            context.finishRecovery();
            assertTrue(callbackStarted.await(5, TimeUnit.SECONDS));
            CompletableFuture<Void> closeFuture = context.processor.closeAsync();

            assertFalse(closeFuture.isDone(), "Closing must wait for the recovery callback to return");
            assertFalse(context.processor.resourcesClosed());

            finishCallback.countDown();
            callbackFuture.get(5, TimeUnit.SECONDS);
            closeFuture.get(5, TimeUnit.SECONDS);
            assertTrue(context.processor.resourcesClosed());
        } finally {
            finishCallback.countDown();
        }
    }

    @Test(timeOut = 10_000)
    public void testCloseAfterRecoveryCompleted() throws Exception {
        try (RecoveryTestContext context = RecoveryTestContext.running()) {
            CompletableFuture<?> recoveryFuture = context.recoverFromSnapshot();
            context.awaitRecoveryStarted();

            context.finishRecovery();
            context.verifyRecoverySucceeded(recoveryFuture);
            context.processor.closeAsync().get(5, TimeUnit.SECONDS);

            assertTrue(context.processor.resourcesClosed());
            context.verifyRecoveryFailedAfterClose(context.recoverFromSnapshot());
        }
    }

    @Test(timeOut = 10_000)
    public void testRetryAfterRecoveryFailed() throws Exception {
        try (RecoveryTestContext context = RecoveryTestContext.running()) {
            RuntimeException failure = new RuntimeException("recovery failed");
            context.processor.failNextRecovery(failure);
            CompletableFuture<?> recoveryFuture = context.recoverFromSnapshot();
            context.awaitRecoveryStarted();

            context.finishRecovery();
            context.verifyRecoveryFailed(recoveryFuture, failure);
            context.verifyRecoverySucceeded(context.recoverFromSnapshot());

            context.processor.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeOut = 10_000)
    public void testRecoverySubmissionRejected() throws Exception {
        TrackingScheduledExecutor recoveryExecutor = new TrackingScheduledExecutor();
        recoveryExecutor.shutdown();
        TestSnapshotProcessor processor = new TestSnapshotProcessor(recoveryExecutor);

        CompletableFuture<Position> recoveryFuture = processor.recoverFromSnapshot();

        ExecutionException exception = expectThrows(ExecutionException.class, recoveryFuture::get);
        assertTrue(exception.getCause() instanceof RejectedExecutionException);
        processor.closeAsync().get(5, TimeUnit.SECONDS);
        assertTrue(processor.resourcesClosed());
    }

    private static final class RecoveryTestContext implements AutoCloseable {

        private final TrackingScheduledExecutor recoveryExecutor = new TrackingScheduledExecutor();
        private final CountDownLatch releaseBlocker = new CountDownLatch(1);
        private final TestSnapshotProcessor processor;

        private CompletableFuture<Boolean> recoveryCallbackHeldProcessorLock;

        static RecoveryTestContext queued() throws Exception {
            return new RecoveryTestContext(true);
        }

        static RecoveryTestContext running() throws Exception {
            return new RecoveryTestContext(false);
        }

        private RecoveryTestContext(boolean queueRecovery) throws Exception {
            if (queueRecovery) {
                CountDownLatch blockerStarted = new CountDownLatch(1);
                recoveryExecutor.execute(() -> {
                    blockerStarted.countDown();
                    try {
                        releaseBlocker.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
            }

            processor = new TestSnapshotProcessor(recoveryExecutor);
        }

        CompletableFuture<?> recoverFromSnapshot() {
            CompletableFuture<?> recoveryFuture = processor.recoverFromSnapshot();
            recoveryCallbackHeldProcessorLock = new CompletableFuture<>();
            recoveryFuture.whenComplete((__, ___) ->
                    recoveryCallbackHeldProcessorLock.complete(Thread.holdsLock(processor)));
            return recoveryFuture;
        }

        void awaitRecoveryQueued() {
            assertEquals(recoveryExecutor.getQueue().size(), 1);
            assertFalse(submittedRecoveryTask().isCancelled());
        }

        void awaitRecoveryStarted() throws InterruptedException {
            assertTrue(processor.awaitRecoveryStarted());
        }

        Future<?> submittedRecoveryTask() {
            Future<?> recoveryTask = recoveryExecutor.submittedTask();
            assertNotNull(recoveryTask);
            return recoveryTask;
        }

        void finishRecovery() {
            processor.finishRecovery();
        }

        void verifyRecoveryFailedAfterClose(CompletableFuture<?> recoveryFuture) throws Exception {
            ExecutionException exception = expectThrows(ExecutionException.class,
                    () -> recoveryFuture.get(5, TimeUnit.SECONDS));
            assertTrue(exception.getCause() instanceof BrokerServiceException.ServiceUnitNotReadyException,
                    "Closing the processor must fail the recovery future");
            verifyRecoveryCallbackDidNotHoldProcessorLock();
        }

        void verifyRecoverySucceeded(CompletableFuture<?> recoveryFuture) throws Exception {
            assertNull(recoveryFuture.get(5, TimeUnit.SECONDS));
            verifyRecoveryCallbackDidNotHoldProcessorLock();
        }

        void verifyRecoveryFailed(CompletableFuture<?> recoveryFuture, Throwable expected) {
            ExecutionException exception = expectThrows(ExecutionException.class,
                    () -> recoveryFuture.get(5, TimeUnit.SECONDS));
            assertSame(exception.getCause(), expected);
        }

        private void verifyRecoveryCallbackDidNotHoldProcessorLock() throws Exception {
            assertFalse(recoveryCallbackHeldProcessorLock.get(1, TimeUnit.SECONDS),
                    "Recovery callbacks must run without holding the processor lock");
        }

        @Override
        public void close() throws Exception {
            releaseBlocker.countDown();
            processor.finishRecovery();
            recoveryExecutor.shutdownNow();
            assertTrue(recoveryExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static final class TrackingScheduledExecutor extends ScheduledThreadPoolExecutor {

        private final AtomicReference<Future<?>> submittedTask = new AtomicReference<>();

        private TrackingScheduledExecutor() {
            super(1);
        }

        @Override
        public Future<?> submit(Runnable task) {
            Future<?> future = super.submit(task);
            submittedTask.set(future);
            return future;
        }

        private Future<?> submittedTask() {
            return submittedTask.get();
        }
    }

    private static final class TestSnapshotProcessor extends AbstractSnapshotAbortedTxnProcessor {

        private final CountDownLatch recoveryStarted = new CountDownLatch(1);
        private final CountDownLatch finishRecovery = new CountDownLatch(1);
        private final AtomicBoolean resourcesClosed = new AtomicBoolean();
        private final AtomicReference<RuntimeException> nextRecoveryFailure = new AtomicReference<>();

        private TestSnapshotProcessor(ScheduledExecutorService recoveryExecutor) {
            super(recoveryExecutor);
        }

        @Override
        protected Position doRecoverFromSnapshot(ScheduledExecutorService executor) throws Exception {
            recoveryStarted.countDown();
            assertTrue(finishRecovery.await(5, TimeUnit.SECONDS));
            RuntimeException failure = nextRecoveryFailure.getAndSet(null);
            if (failure != null) {
                throw failure;
            }
            return null;
        }

        @Override
        protected CompletableFuture<Void> closeResources() {
            resourcesClosed.set(true);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void putAbortedTxnAndPosition(TxnID txnID, Position position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void trimExpiredAbortedTxns() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean checkAbortedTransaction(TxnID txnID) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> clearAbortedTxnSnapshot() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> takeAbortedTxnsSnapshot(Position maxReadPosition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TransactionBufferStats generateSnapshotStats(boolean segmentStats) {
            throw new UnsupportedOperationException();
        }

        boolean awaitRecoveryStarted() throws InterruptedException {
            return recoveryStarted.await(5, TimeUnit.SECONDS);
        }

        boolean recoveryStarted() {
            return recoveryStarted.getCount() == 0;
        }

        void finishRecovery() {
            finishRecovery.countDown();
        }

        void failNextRecovery(RuntimeException failure) {
            nextRecoveryFailure.set(failure);
        }

        boolean resourcesClosed() {
            return resourcesClosed.get();
        }
    }
}
