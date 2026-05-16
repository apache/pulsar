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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

            createCallback.get().createComplete(BKException.Code.OK, createdLedger.get(), createCtx.get());
            assertTrue(addFailed.await(5, TimeUnit.SECONDS));
            assertTrue(addSuccess.get() == null);
            assertTrue(addFailure.get() instanceof ManagedLedgerTerminatedException);
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
