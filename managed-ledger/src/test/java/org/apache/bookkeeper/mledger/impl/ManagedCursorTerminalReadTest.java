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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/** Terminal reads own their wait slot only until their one terminal callback. */
public class ManagedCursorTerminalReadTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20000)
    public void consecutiveTerminalReadsReleaseWaitOwnership() throws Exception {
        try (Fixture fixture = fixture("terminal-read-sequential", false)) {
            Probe first = new Probe();
            read(fixture.cursor, first);
            ManagedLedgerException firstError = first.failure.get(5, TimeUnit.SECONDS);
            Probe second = new Probe();
            read(fixture.cursor, second);
            ManagedLedgerException secondError = second.failure.get(5, TimeUnit.SECONDS);
            assertTrue(firstError instanceof NoMoreEntriesToReadException);
            assertTrue(secondError instanceof NoMoreEntriesToReadException,
                    "A completed terminal read must not leave a concurrent-wait reservation: " + secondError);
            assertEquals(first.failed.get(), 1);
            assertEquals(second.failed.get(), 1);
            assertFalse(fixture.cursor.hasPendingReadRequest());
            assertEquals(fixture.cursor.getPendingReadOpsCount(), 0);
            assertTrue(fixture.ledger.waitingCursors.isEmpty());
        }
    }

    @Test(timeOut = 20000)
    public void terminalCallbackCanReenterWithoutCompletingTheNewReadTwice() throws Exception {
        try (Fixture fixture = fixture("terminal-read-reentry", false)) {
            Probe second = new Probe();
            Probe first = new Probe(() -> read(fixture.cursor, second));
            read(fixture.cursor, first);
            assertTrue(first.failure.get(5, TimeUnit.SECONDS) instanceof NoMoreEntriesToReadException);
            assertTrue(second.failure.get(5, TimeUnit.SECONDS) instanceof NoMoreEntriesToReadException);
            assertEquals(first.failed.get(), 1);
            assertEquals(second.failed.get(), 1);
            assertEquals(first.completed.get() + second.completed.get(), 0);
            assertFalse(fixture.cursor.hasPendingReadRequest());
            assertEquals(fixture.cursor.getPendingReadOpsCount(), 0);
            assertTrue(fixture.ledger.waitingCursors.isEmpty());
        }
    }

    @Test(timeOut = 20000)
    public void terminalObserverFailureIsNotDeliveredAsAnotherReadFailure() throws Exception {
        try (Fixture fixture = fixture("terminal-read-observer-failure", false)) {
            Probe first = new Probe(() -> {
                throw new IllegalStateException("injected terminal observer failure");
            });
            read(fixture.cursor, first);
            assertTrue(first.failure.get(5, TimeUnit.SECONDS) instanceof NoMoreEntriesToReadException);
            assertEquals(first.failed.get(), 1, "Observer failure cannot invoke the same callback a second time");
            Probe next = new Probe();
            read(fixture.cursor, next);
            assertTrue(next.failure.get(5, TimeUnit.SECONDS) instanceof NoMoreEntriesToReadException);
            assertFalse(fixture.cursor.hasPendingReadRequest());
            assertEquals(fixture.cursor.getPendingReadOpsCount(), 0);
        }
    }

    @Test(timeOut = 20000)
    public void cancelledDelayedTerminalCheckCannotClaimAReplacementRead() throws Exception {
        try (Fixture fixture = fixture("terminal-read-cancel-replace", true)) {
            Probe cancelled = new Probe();
            read(fixture.cursor, cancelled);
            assertTrue(fixture.cursor.hasPendingReadRequest());
            assertTrue(fixture.cursor.cancelPendingReadRequest());
            Probe replacement = new Probe();
            read(fixture.cursor, replacement);
            assertEquals(fixture.checks.size(), 2);
            fixture.checks.get(0).run();
            assertEquals(cancelled.failed.get(), 0, "Cancelled read cannot be completed by its old delayed task");
            assertEquals(replacement.failed.get(), 0);
            assertTrue(fixture.cursor.hasPendingReadRequest());
            fixture.checks.get(1).run();
            assertTrue(replacement.failure.get(5, TimeUnit.SECONDS) instanceof NoMoreEntriesToReadException);
            assertEquals(replacement.failed.get(), 1);
            assertFalse(fixture.cursor.hasPendingReadRequest());
            assertEquals(fixture.cursor.getPendingReadOpsCount(), 0);
            assertTrue(fixture.ledger.waitingCursors.isEmpty());
        }
    }

    @Test(timeOut = 20000)
    public void staleTerminalCheckCannotClaimAReplacementThatReusesItsOperation() throws Exception {
        try (Fixture fixture = fixture("terminal-read-reused-operation", true)) {
            // OpReadEntry pooling is per thread: register on a Netty thread, like the broker's read threads.
            ReusedOperation reused = CompletableFuture.supplyAsync(() -> cancelUntilOperationIsReused(fixture),
                    task -> new FastThreadLocalThread(task).start()).get(10, TimeUnit.SECONDS);
            assertSame(reused.replacement.op, reused.stale.op);
            assertNotEquals(reused.replacement.opReadId, reused.stale.opReadId);
            assertEquals(fixture.checks.size(), reused.cancelled.size() + 1);

            // The stale check targets the same OpReadEntry instance that the replacement now waits with.
            fixture.checks.get(reused.staleCheck).run();
            for (int i = 0; i < fixture.checks.size() - 1; i++) {
                if (i != reused.staleCheck) {
                    fixture.checks.get(i).run();
                }
            }
            assertSame(ManagedCursorImpl.WAITING_READ_OP_UPDATER.get(fixture.cursor), reused.replacement,
                    "A stale check cannot clear a later registration of the same OpReadEntry");
            assertEquals(reused.replacementProbe.failed.get(), 0);
            for (Probe probe : reused.cancelled) {
                assertEquals(probe.failed.get(), 0, "Cancelled read cannot be completed by its old delayed task");
            }

            fixture.checks.get(fixture.checks.size() - 1).run();
            assertTrue(reused.replacementProbe.failure.get(5, TimeUnit.SECONDS)
                    instanceof NoMoreEntriesToReadException);
            assertEquals(reused.replacementProbe.failed.get(), 1);
            assertFalse(fixture.cursor.hasPendingReadRequest());
            assertEquals(fixture.cursor.getPendingReadOpsCount(), 0);
            assertTrue(fixture.ledger.waitingCursors.isEmpty());
        }
    }

    @Test(timeOut = 20000)
    public void notificationWinningBeforeDelayedTerminalCheckKeepsOneCompletion() throws Exception {
        try (Fixture fixture = fixture("terminal-read-notification-wins", true)) {
            doAnswer(invocation -> {
                OpReadEntry operation = invocation.getArgument(0);
                operation.readEntriesFailed(new NoMoreEntriesToReadException("controlled terminal notification"),
                        operation.ctx);
                return null;
            }).when(fixture.ledger).asyncReadEntries(any(OpReadEntry.class));
            Probe probe = new Probe();
            read(fixture.cursor, probe);
            assertEquals(fixture.checks.size(), 1);
            fixture.cursor.notifyEntriesAvailable();
            assertTrue(probe.failure.get(5, TimeUnit.SECONDS) instanceof NoMoreEntriesToReadException);
            assertEquals(probe.failed.get(), 1);
            fixture.checks.get(0).run();
            assertEquals(probe.failed.get(), 1, "A stale terminal check cannot complete an already-notified read");
            assertFalse(fixture.cursor.hasPendingReadRequest());
            assertEquals(fixture.cursor.getPendingReadOpsCount(), 0);
        }
    }

    @Test(timeOut = 20000)
    public void oldTerminalUnregistrationCannotEraseConcurrentReadRegistration() throws Exception {
        try (Fixture fixture = fixture("terminal-read-concurrent-registration", false)) {
            CountDownLatch oldRemovalEntered = new CountDownLatch(1);
            CountDownLatch releaseOldRemoval = new CountDownLatch(1);
            CountDownLatch oldRemovalFinished = new CountDownLatch(1);
            CountDownLatch replacementRegistrationEntered = new CountDownLatch(1);
            CountDownLatch replacementRegistrationReturned = new CountDownLatch(1);
            AtomicInteger registrations = new AtomicInteger();
            AtomicInteger removals = new AtomicInteger();
            AtomicBoolean replacementWasRegistered = new AtomicBoolean();
            AtomicReference<Thread> replacementThread = new AtomicReference<>();
            doAnswer(invocation -> {
                boolean replacement = registrations.incrementAndGet() == 2;
                if (replacement) {
                    replacementRegistrationEntered.countDown();
                }
                Object result = invocation.callRealMethod();
                if (replacement) {
                    replacementWasRegistered.set(fixture.ledger.waitingCursors.contains(fixture.cursor));
                    replacementRegistrationReturned.countDown();
                }
                return result;
            }).when(fixture.ledger).addWaitingCursor(fixture.cursor);
            doAnswer(invocation -> {
                if (removals.incrementAndGet() == 1) {
                    oldRemovalEntered.countDown();
                    assertTrue(releaseOldRemoval.await(5, TimeUnit.SECONDS), "Old removal was not released");
                    try {
                        return invocation.callRealMethod();
                    } finally {
                        oldRemovalFinished.countDown();
                    }
                }
                return invocation.callRealMethod();
            }).when(fixture.ledger).removeWaitingCursor(fixture.cursor);
            ExecutorService threads = Executors.newFixedThreadPool(2);
            try {
                Probe first = new Probe();
                Probe replacement = new Probe();
                CompletableFuture<Void> firstRead = CompletableFuture.runAsync(
                        () -> read(fixture.cursor, first), threads);
                assertTrue(oldRemovalEntered.await(5, TimeUnit.SECONDS));
                // The terminal task claimed and cleared its wait slot, but still owns registration removal.
                assertFalse(fixture.cursor.hasPendingReadRequest());
                CompletableFuture<Void> replacementRead = CompletableFuture.runAsync(() -> {
                    replacementThread.set(Thread.currentThread());
                    read(fixture.cursor, replacement);
                }, threads);
                assertTrue(replacementRegistrationEntered.await(5, TimeUnit.SECONDS));
                assertTrue(fixture.cursor.hasPendingReadRequest(), "The replacement owns the new wait slot");
                Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                        replacementRegistrationReturned.getCount() == 0
                                || replacementThread.get().getState() == Thread.State.BLOCKED);
                assertEquals(replacementRegistrationReturned.getCount(), 1L,
                        "New registration must wait until the old registration is removed");
                assertEquals(first.failed.get(), 0);
                assertEquals(replacement.failed.get(), 0);
                releaseOldRemoval.countDown();
                firstRead.get(5, TimeUnit.SECONDS);
                replacementRead.get(5, TimeUnit.SECONDS);
                assertEquals(oldRemovalFinished.getCount(), 0L);
                assertTrue(replacementWasRegistered.get(), "Old completion cannot erase the replacement registration");
                assertTrue(first.failure.get(5, TimeUnit.SECONDS) instanceof NoMoreEntriesToReadException);
                assertTrue(replacement.failure.get(5, TimeUnit.SECONDS) instanceof NoMoreEntriesToReadException);
                assertEquals(first.failed.get(), 1);
                assertEquals(replacement.failed.get(), 1);
                assertEquals(first.completed.get() + replacement.completed.get(), 0);
                assertFalse(fixture.cursor.hasPendingReadRequest());
                assertEquals(fixture.cursor.getPendingReadOpsCount(), 0);
                assertTrue(fixture.ledger.waitingCursors.isEmpty());
            } finally {
                releaseOldRemoval.countDown();
                threads.shutdownNow();
                assertTrue(threads.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private Fixture fixture(String name, boolean delayed) throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setNewEntriesCheckDelayInMillis(delayed ? 1 : 0);
        ManagedLedgerImpl ledger = spy((ManagedLedgerImpl) factory.open(name, config));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor", InitialPosition.Earliest);
        ledger.terminate();
        List<Runnable> checks = new ArrayList<>();
        if (delayed) {
            OrderedScheduler scheduler = mock(OrderedScheduler.class);
            doReturn(scheduler).when(ledger).getScheduledExecutor();
            doAnswer(invocation -> {
                checks.add(invocation.getArgument(0));
                return mock(ScheduledFuture.class);
            }).when(scheduler).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        }
        return new Fixture(ledger, cursor, checks);
    }

    private static void read(ManagedCursorImpl cursor, Probe probe) {
        cursor.asyncReadEntriesOrWait(1, probe, null, PositionFactory.LATEST);
    }

    /** Cancels and replaces the waiting read until the pool hands the cancelled OpReadEntry to the replacement. */
    private static ReusedOperation cancelUntilOperationIsReused(Fixture fixture) {
        List<Probe> cancelled = new ArrayList<>();
        ManagedCursorImpl.WaitingRead stale = null;
        int staleCheck = -1;
        for (int attempt = 0; attempt < 64; attempt++) {
            Probe probe = new Probe();
            read(fixture.cursor, probe);
            ManagedCursorImpl.WaitingRead registered = ManagedCursorImpl.WAITING_READ_OP_UPDATER.get(fixture.cursor);
            if (stale != null && registered.op == stale.op) {
                return new ReusedOperation(stale, staleCheck, registered, probe, cancelled);
            }
            assertTrue(fixture.cursor.cancelPendingReadRequest());
            cancelled.add(probe);
            stale = registered;
            staleCheck = fixture.checks.size() - 1;
        }
        throw new AssertionError("The replacement read never reused the cancelled read's OpReadEntry");
    }

    private record ReusedOperation(ManagedCursorImpl.WaitingRead stale, int staleCheck,
                                   ManagedCursorImpl.WaitingRead replacement, Probe replacementProbe,
                                   List<Probe> cancelled) {
    }

    private record Fixture(ManagedLedgerImpl ledger, ManagedCursorImpl cursor, List<Runnable> checks)
            implements AutoCloseable {
        @Override
        public void close() throws Exception {
            try {
                cursor.close();
            } finally {
                ledger.close();
            }
        }
    }

    private static final class Probe implements ReadEntriesCallback {
        private final CompletableFuture<ManagedLedgerException> failure = new CompletableFuture<>();
        private final AtomicInteger completed = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();
        private final Runnable observer;
        private final List<ManagedLedgerException> errors = new CopyOnWriteArrayList<>();

        private Probe() {
            this(() -> { });
        }

        private Probe(Runnable observer) {
            this.observer = observer;
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            entries.forEach(Entry::release);
            completed.incrementAndGet();
            failure.completeExceptionally(new AssertionError("Unexpected successful terminal read"));
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            errors.add(exception);
            failed.incrementAndGet();
            failure.complete(exception);
            observer.run();
        }
    }
}
