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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class ManagedLedgerTerminationTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20000)
    public void terminateSimple() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        Position p0 = ledger.addEntry("entry-0".getBytes());

        Position lastPosition = ledger.terminate();

        assertEquals(lastPosition, p0);

        try {
            ledger.addEntry("entry-1".getBytes());
        } catch (ManagedLedgerTerminatedException e) {
            // Expected
        }
    }

    @Test(timeOut = 30000)
    public void terminateDuringLedgerSwitchKeepsTerminatedState() throws Exception {
        // Regression for terminate racing with a ledger rollover. The new ledger create callback is held until after
        // terminate wins. The late create callback must not reopen the managed ledger, and the late-created ledger
        // must be closed/deleted because terminate will not use a future ledger for pending writes.
        BookKeeper spyBookKeeper = spy(bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(1);

        AtomicBoolean holdNextCreate = new AtomicBoolean(false);
        CountDownLatch createRequested = new CountDownLatch(1);
        AtomicReference<CreateCallback> createCallback = new AtomicReference<>();
        AtomicReference<Object> createCtx = new AtomicReference<>();
        AtomicReference<LedgerHandle> createdLedger = new AtomicReference<>();

        doAnswer(invocation -> {
            if (holdNextCreate.compareAndSet(true, false)) {
                LedgerHandle lh = bkc.createLedger(invocation.getArgument(0), invocation.getArgument(1),
                        invocation.getArgument(2), invocation.getArgument(3), invocation.getArgument(4));
                createdLedger.set(lh);
                createCallback.set(invocation.getArgument(5));
                createCtx.set(invocation.getArgument(6));
                createRequested.countDown();
                return null;
            }
            return invocation.callRealMethod();
        }).when(spyBookKeeper).asyncCreateLedger(anyInt(), anyInt(), anyInt(), any(), any(), any(), any(), any());

        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, spyBookKeeper);
        try {
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) localFactory.open("terminate_during_ledger_switch", config);
            holdNextCreate.set(true);

            Position p0 = ledger.addEntry("entry-0".getBytes());
            assertTrue(createRequested.await(5, TimeUnit.SECONDS));
            assertEquals(ledger.getState(), ManagedLedgerImpl.State.CreatingLedger);

            CountDownLatch addFailed = new CountDownLatch(1);
            AtomicReference<ManagedLedgerException> addFailure = new AtomicReference<>();
            AtomicReference<Position> addSuccess = new AtomicReference<>();
            ledger.asyncAddEntry("entry-1".getBytes(), new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    addSuccess.set(position);
                    addFailed.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    addFailure.set(exception);
                    addFailed.countDown();
                }
            }, null);
            Awaitility.await().untilAsserted(() -> assertEquals(ledger.pendingAddEntries.size(), 1));

            Position lastPosition = ledger.terminate();
            assertEquals(lastPosition, p0);
            assertEquals(ledger.getState(), ManagedLedgerImpl.State.Terminated);

            assertTrue(addFailed.await(5, TimeUnit.SECONDS));
            assertTrue(addSuccess.get() == null);
            assertTrue(addFailure.get() instanceof ManagedLedgerTerminatedException);
            assertEquals(ledger.pendingAddEntries.size(), 0);

            long lateCreatedLedgerId = createdLedger.get().getId();
            assertTrue(bkc.getLedgers().contains(lateCreatedLedgerId));
            createCallback.get().createComplete(BKException.Code.OK, createdLedger.get(), createCtx.get());
            assertTrue(((CompletableFuture<?>) createCtx.get()).isDone());
            assertEquals(ledger.mbean.getPendingBookieOpsStats().dataLedgerCreateOp, 0);
            Awaitility.await().untilAsserted(() -> assertFalse(bkc.getLedgers().contains(lateCreatedLedgerId)));
            assertEquals(ledger.getState(), ManagedLedgerImpl.State.Terminated);

            try {
                ledger.addEntry("entry-2".getBytes());
                fail("Should have thrown exception");
            } catch (ManagedLedgerTerminatedException e) {
                // Expected
            }

            ledger.close();
            ManagedLedger reopened = localFactory.open("terminate_during_ledger_switch", config);
            assertTrue(reopened.isTerminated());
            try {
                reopened.addEntry("entry-3".getBytes());
                fail("Should have thrown exception");
            } catch (ManagedLedgerTerminatedException e) {
                // Expected
            }
        } finally {
            localFactory.shutdown();
        }
    }

    @Test(timeOut = 30000)
    public void terminatePositionIncludesAddAlreadyAckedByBookKeeper() throws Exception {
        // BK has already acked the add and advanced LAC, but the ML client callback is still queued behind the
        // managed-ledger executor. terminate must use the BK LAC as its boundary, so the terminated position still
        // includes this add even though the client callback is delivered after terminate() returns.
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("terminate_includes_acked_add");
        LedgerHandle originalLedger = ledger.currentLedger;
        LedgerHandle spyLedger = spy(originalLedger);
        long ledgerId = originalLedger.getId();

        CountDownLatch addIssued = new CountDownLatch(1);
        CountDownLatch executorBlocked = new CountDownLatch(1);
        CountDownLatch releaseExecutor = new CountDownLatch(1);
        CountDownLatch addCompleted = new CountDownLatch(1);
        AtomicLong lac = new AtomicLong(-1);
        AtomicReference<AddCallback> bkAddCallback = new AtomicReference<>();
        AtomicReference<Object> bkAddCtx = new AtomicReference<>();
        AtomicReference<Position> addSuccess = new AtomicReference<>();
        AtomicReference<ManagedLedgerException> addFailure = new AtomicReference<>();

        doAnswer(invocation -> {
            bkAddCallback.set(invocation.getArgument(1));
            bkAddCtx.set(invocation.getArgument(2));
            addIssued.countDown();
            return null;
        }).when(spyLedger).asyncAddEntry(any(ByteBuf.class), any(AddCallback.class), any());
        doAnswer(invocation -> lac.get()).when(spyLedger).getLastAddConfirmed();
        doAnswer(invocation -> {
            CloseCallback closeCallback = invocation.getArgument(0);
            Object closeCtx = invocation.getArgument(1);
            closeCallback.closeComplete(BKException.Code.OK, spyLedger, closeCtx);
            return null;
        }).when(spyLedger).asyncClose(any(CloseCallback.class), any());

        ledger.currentLedger = spyLedger;

        ledger.asyncAddEntry("entry-0".getBytes(), new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                addSuccess.set(position);
                addCompleted.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                addFailure.set(exception);
                addCompleted.countDown();
            }
        }, null);

        assertTrue(addIssued.await(5, TimeUnit.SECONDS));
        ledger.getExecutor().execute(() -> {
            executorBlocked.countDown();
            try {
                releaseExecutor.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(executorBlocked.await(5, TimeUnit.SECONDS));

        lac.set(0);
        bkAddCallback.get().addComplete(BKException.Code.OK, spyLedger, 0, bkAddCtx.get());

        try {
            Position terminatedPosition = ledger.terminate();
            assertEquals(terminatedPosition, PositionFactory.create(ledgerId, 0));
            assertFalse(addCompleted.await(100, TimeUnit.MILLISECONDS));
        } finally {
            releaseExecutor.countDown();
        }

        assertTrue(addCompleted.await(5, TimeUnit.SECONDS));
        assertEquals(addSuccess.get(), PositionFactory.create(ledgerId, 0));
        assertTrue(addFailure.get() == null);
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.Terminated);
    }

    @Test(timeOut = 30000)
    public void terminateFailsInflightAddDrainedByLedgerClose() throws Exception {
        // BK close drains outstanding adds that have not reached LAC. Those entries are outside the terminated
        // position, so ML must fail their callbacks as terminated instead of entering the normal write-failure path,
        // which would return from ledgerClosed() in Terminated state and leave the add callback hanging.
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("terminate_inflight_add_close");
        LedgerHandle originalLedger = ledger.currentLedger;
        LedgerHandle spyLedger = spy(originalLedger);
        long ledgerId = originalLedger.getId();

        CountDownLatch addIssued = new CountDownLatch(1);
        AtomicReference<AddCallback> bkAddCallback = new AtomicReference<>();
        AtomicReference<Object> bkAddCtx = new AtomicReference<>();

        doAnswer(invocation -> {
            bkAddCallback.set(invocation.getArgument(1));
            bkAddCtx.set(invocation.getArgument(2));
            addIssued.countDown();
            return null;
        }).when(spyLedger).asyncAddEntry(any(ByteBuf.class), any(AddCallback.class), any());

        doAnswer(invocation -> {
            CloseCallback closeCallback = invocation.getArgument(0);
            Object closeCtx = invocation.getArgument(1);
            bkAddCallback.get().addComplete(BKException.Code.LedgerClosedException, spyLedger, -1, bkAddCtx.get());
            closeCallback.closeComplete(BKException.Code.OK, spyLedger, closeCtx);
            return null;
        }).when(spyLedger).asyncClose(any(CloseCallback.class), any());

        ledger.currentLedger = spyLedger;

        CountDownLatch addFailed = new CountDownLatch(1);
        AtomicReference<ManagedLedgerException> addFailure = new AtomicReference<>();
        AtomicReference<Position> addSuccess = new AtomicReference<>();
        ledger.asyncAddEntry("entry-0".getBytes(), new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                addSuccess.set(position);
                addFailed.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                addFailure.set(exception);
                addFailed.countDown();
            }
        }, null);

        assertTrue(addIssued.await(5, TimeUnit.SECONDS));
        Awaitility.await().untilAsserted(() -> assertEquals(ledger.pendingAddEntries.size(), 1));

        Position lastPosition = ledger.terminate();

        assertEquals(lastPosition, PositionFactory.create(ledgerId, -1));
        assertTrue(addFailed.await(5, TimeUnit.SECONDS));
        assertTrue(addSuccess.get() == null);
        assertTrue(addFailure.get() instanceof ManagedLedgerTerminatedException);
        Awaitility.await().untilAsserted(() -> assertEquals(ledger.pendingAddEntries.size(), 0));
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.Terminated);
    }

    @Test(timeOut = 20000)
    public void ledgerSwitchCompletionDoesNotReopenTerminatedLedger() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("terminate_state_not_overwritten");
        ledger.addEntry("entry-0".getBytes());

        // Regression for a ledger-switch completion callback arriving after terminate().
        ManagedLedgerImpl.STATE_UPDATER.set(ledger, ManagedLedgerImpl.State.Terminated);
        ledger.updateLedgersIdsComplete(null);

        assertEquals(ledger.getState(), ManagedLedgerImpl.State.Terminated);
    }

    @Test(timeOut = 20000)
    public void terminateReopen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        Position p0 = ledger.addEntry("entry-0".getBytes());

        Position lastPosition = ledger.terminate();

        assertEquals(lastPosition, p0);

        ledger.close();

        ledger = factory.open("my_test_ledger");

        try {
            ledger.addEntry("entry-1".getBytes());
            fail("Should have thrown exception");
        } catch (ManagedLedgerTerminatedException e) {
            // Expected
        }
    }

    @Test(timeOut = 20000)
    public void terminateWithCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p0 = ledger.addEntry("entry-0".getBytes());
        Position p1 = ledger.addEntry("entry-1".getBytes());

        List<Entry> entries = c1.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getPosition(), p0);
        entries.forEach(Entry::release);

        Position lastPosition = ledger.terminate();
        assertEquals(lastPosition, p1);

        // Cursor can keep reading
        entries = c1.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getPosition(), p1);
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    public void terminateWithCursorReadOrWait() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p0 = ledger.addEntry("entry-0".getBytes());
        Position p1 = ledger.addEntry("entry-1".getBytes());
        assertFalse(ledger.isTerminated());

        Position lastPosition = ledger.terminate();
        assertTrue(ledger.isTerminated());
        assertEquals(lastPosition, p1);

        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getPosition(), p0);
        assertEquals(entries.get(1).getPosition(), p1);
        entries.forEach(Entry::release);

        // Normal read will just return no entries
        assertEquals(c1.readEntries(10), Collections.emptyList());

        // Read or wait will fail
        try {
            c1.readEntriesOrWait(10);
            fail("Should have thrown exception");
        } catch (NoMoreEntriesToReadException e) {
            // Expected
        }
    }

    @Test(timeOut = 20000)
    public void terminateWithNonDurableCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        Position p0 = ledger.addEntry("entry-0".getBytes());
        Position p1 = ledger.addEntry("entry-1".getBytes());
        assertFalse(ledger.isTerminated());

        Position lastPosition = ledger.terminate();
        assertTrue(ledger.isTerminated());
        assertEquals(lastPosition, p1);

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionFactory.EARLIEST);

        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getPosition(), p0);
        assertEquals(entries.get(1).getPosition(), p1);
        entries.forEach(Entry::release);

        // Normal read will just return no entries
        assertEquals(c1.readEntries(10), Collections.emptyList());

        // Read or wait will fail
        try {
            c1.readEntriesOrWait(10);
            fail("Should have thrown exception");
        } catch (NoMoreEntriesToReadException e) {
            // Expected
        }
    }

}
