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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.mockito.invocation.InvocationOnMock;
import org.testng.annotations.Test;

/**
 * Tests for {@link ManagedLedger#asyncReadEntries(Position, int, Position)} and the
 * {@link OpReadEntries} state machine.
 */
public class OpReadEntriesTest extends MockedBookKeeperTestCase {

    private static final Charset ENCODING = StandardCharsets.UTF_8;

    private static Throwable expectFutureFailure(CompletableFuture<?> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
            throw new AssertionError("Expected future to fail");
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    @Test(timeOut = 20000)
    public void nullMaxPositionTreatedAsLatest() throws Exception {
        @Cleanup("close")
        ManagedLedger ledger = factory.open("nullMaxPositionTreatedAsLatest");
        Position p0 = ledger.addEntry("entry-0".getBytes(ENCODING));
        ledger.addEntry("entry-1".getBytes(ENCODING));

        List<Entry> withNull = ledger.asyncReadEntries(p0, 10, null).get(5, TimeUnit.SECONDS);
        List<Entry> withLatest = ledger.asyncReadEntries(p0, 10, PositionFactory.LATEST)
                .get(5, TimeUnit.SECONDS);

        assertEquals(withNull.size(), withLatest.size());
        assertEquals(withNull.size(), 2);
        for (int i = 0; i < withNull.size(); i++) {
            assertEquals(withNull.get(i).getPosition(), withLatest.get(i).getPosition());
        }
        withNull.forEach(Entry::release);
        withLatest.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    public void partialEntriesReturnedOnMidReadFailure() throws Exception {
        @Cleanup("close")
        ManagedLedger ledger = factory.open("partialEntriesReturnedOnMidReadFailure",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        Position p0 = ledger.addEntry("entry-0".getBytes(ENCODING));
        Position p1 = ledger.addEntry("entry-1".getBytes(ENCODING));
        ledger.addEntry("entry-2".getBytes(ENCODING));
        assertNotEquals(p0.getLedgerId(), p1.getLedgerId());

        bkc.deleteLedger(p1.getLedgerId());

        // After p0 is read, the next ledger fails to open. The future still completes successfully
        // with the partial [p0]; callers detect truncation by comparing size against the request.
        List<Entry> entries = ledger.asyncReadEntries(p0, 3).get(5, TimeUnit.SECONDS);
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getPosition(), p0);
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    public void autoSkipNonRecoverableDataContinuesPastDeletedLedger() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setMaxEntriesPerLedger(1);
        config.setAutoSkipNonRecoverableData(true);
        @Cleanup("close")
        ManagedLedger ledger = factory.open("autoSkipNonRecoverableDataContinuesPastDeletedLedger", config);

        Position p1 = ledger.addEntry("entry-0".getBytes(ENCODING));
        Position p2 = ledger.addEntry("entry-1".getBytes(ENCODING));
        Position p3 = ledger.addEntry("entry-2".getBytes(ENCODING));
        assertNotEquals(p1.getLedgerId(), p2.getLedgerId());

        bkc.deleteLedger(p1.getLedgerId());

        List<Entry> entries = ledger.asyncReadEntries(p1, 10).get(5, TimeUnit.SECONDS);
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getPosition(), p2);
        assertEquals(entries.get(1).getPosition(), p3);
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    public void completionRunsOnMlExecutor() throws Exception {
        @Cleanup("close")
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completionRunsOnMlExecutor");
        Position p0 = ledger.addEntry("entry-0".getBytes(ENCODING));

        AtomicReference<String> completionThread = new AtomicReference<>();
        ledger.asyncReadEntries(p0, 1).whenComplete((entries, ex) -> {
            completionThread.set(Thread.currentThread().getName());
            if (entries != null) {
                entries.forEach(Entry::release);
            }
        }).get(5, TimeUnit.SECONDS);

        String threadName = completionThread.get();
        assertNotNull(threadName);
        assertTrue(threadName.startsWith("test-OrderedScheduler-"),
                "expected ML executor thread, got " + threadName);
    }

    @Test(timeOut = 15000)
    public void emptyCacheResultDoesNotLivelock() throws Exception {
        @Cleanup("close")
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("emptyCacheResultDoesNotLivelock");
        Position p0 = ledger.addEntry("entry-0".getBytes(ENCODING));

        EntryCache originalCache = ledger.entryCache;
        EntryCache spyCache = spy(originalCache);
        AtomicInteger invocations = new AtomicInteger();
        doAnswer((InvocationOnMock invocation) -> {
            invocations.incrementAndGet();
            ReadEntriesCallback callback = invocation.getArgument(4);
            callback.readEntriesComplete(Collections.emptyList(), invocation.getArgument(5));
            return null;
        }).when(spyCache).asyncReadEntry(any(ReadHandle.class), anyLong(), anyLong(),
                any(), any(ReadEntriesCallback.class), any());

        Field f = ManagedLedgerImpl.class.getDeclaredField("entryCache");
        f.setAccessible(true);
        f.set(ledger, spyCache);

        try {
            List<Entry> entries = ledger.asyncReadEntries(p0, 5).get(3, TimeUnit.SECONDS);
            assertEquals(entries.size(), 0);
            assertTrue(invocations.get() <= 5,
                    "expected <=5 cache invocations, got " + invocations.get());
        } finally {
            f.set(ledger, originalCache);
        }
    }

    @Test(timeOut = 20000)
    public void fencedMlRejectsReadAtApiEntry() throws Exception {
        @Cleanup("close")
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("fencedMlRejectsReadAtApiEntry");
        Position p0 = ledger.addEntry("entry-0".getBytes(ENCODING));
        ledger.close();

        Throwable failure = expectFutureFailure(ledger.asyncReadEntries(p0, 1));
        assertTrue(failure instanceof ManagedLedgerException.ManagedLedgerFencedException);

        // The exception is constructed in ManagedLedgerImpl.asyncReadEntries (API entry), not in
        // OpReadEntries.readEntries. Confirms the state check happens before any metadata work.
        Throwable cause = failure.getCause() != null ? failure.getCause() : failure;
        boolean atApiEntry = false;
        for (StackTraceElement frame : cause.getStackTrace()) {
            if (ManagedLedgerImpl.class.getName().equals(frame.getClassName())
                    && "asyncReadEntries".equals(frame.getMethodName())) {
                atApiEntry = true;
                break;
            }
            if (OpReadEntries.class.getName().equals(frame.getClassName())) {
                break;
            }
        }
        assertTrue(atApiEntry,
                "expected exception at API entry. stack: " + Arrays.toString(cause.getStackTrace()));
    }

    @Test(timeOut = 20000)
    public void countAboveHundredRejected() throws Exception {
        @Cleanup("close")
        ManagedLedger ledger = factory.open("countAboveHundredRejected");
        Position p0 = ledger.addEntry("entry-0".getBytes(ENCODING));

        Throwable failure = expectFutureFailure(ledger.asyncReadEntries(p0, 101));
        assertTrue(failure instanceof IllegalArgumentException);

        // MAX_VALUE used to throw OOM synchronously from `new ArrayList<>(count)`; the cap prevents that.
        Throwable failure2 = expectFutureFailure(ledger.asyncReadEntries(p0, Integer.MAX_VALUE));
        assertTrue(failure2 instanceof IllegalArgumentException);
    }

    @Test(timeOut = 20000)
    public void readFromRemovedLedgerAdvancesToNextValid() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "readFromRemovedLedgerAdvancesToNextValid",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(10));

        ledger.addEntry("entry-0".getBytes(ENCODING));
        // Force a roll, then discard the new empty ledger; the next addEntry lands in a 3rd ledger.
        ledger.ledgerClosed(ledger.currentLedger, 0L);
        Awaitility.await().untilAsserted(() ->
                assertEquals(ManagedLedgerImpl.STATE_UPDATER.get(ledger),
                        ManagedLedgerImpl.State.LedgerOpened));
        long removedLedgerId = ledger.currentLedger.getId();
        ledger.ledgerClosed(ledger.currentLedger, -1L);
        Awaitility.await().untilAsserted(() ->
                assertEquals(ManagedLedgerImpl.STATE_UPDATER.get(ledger),
                        ManagedLedgerImpl.State.LedgerOpened));
        Position p1 = ledger.addEntry("entry-1".getBytes(ENCODING));

        assertFalse(ledger.getLedgersInfo().containsKey(removedLedgerId));

        List<Entry> entries = ledger.asyncReadEntries(
                PositionFactory.create(removedLedgerId, 0), 1).get(5, TimeUnit.SECONDS);
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getPosition(), p1);
        entries.forEach(Entry::release);

        ledger.close();
    }
}
