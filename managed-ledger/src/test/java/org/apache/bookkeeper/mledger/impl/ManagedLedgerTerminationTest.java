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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerTerminationTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20000)
    public void terminateSimple() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", initManagedLedgerConfig(defaultConfig()));

        Position p0 = ledger.addEntry("entry-0".getBytes());

        Position lastPosition = ledger.terminate();

        assertEquals(lastPosition, p0);

        try {
            ledger.addEntry("entry-1".getBytes());
        } catch (ManagedLedgerTerminatedException e) {
            // Expected
        }
    }

    @Test(timeOut = 20000)
    public void terminateReopen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", initManagedLedgerConfig(defaultConfig()));

        Position p0 = ledger.addEntry("entry-0".getBytes());

        Position lastPosition = ledger.terminate();

        assertEquals(lastPosition, p0);

        ledger.close();

        ledger = factory.open("my_test_ledger", initManagedLedgerConfig(defaultConfig()));

        try {
            ledger.addEntry("entry-1".getBytes());
            fail("Should have thrown exception");
        } catch (ManagedLedgerTerminatedException e) {
            // Expected
        }
    }

    @Test(timeOut = 20000)
    public void terminateWithCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", initManagedLedgerConfig(defaultConfig()));
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
        ManagedLedger ledger = factory.open("my_test_ledger", initManagedLedgerConfig(defaultConfig()));
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
        ManagedLedger ledger = factory.open("my_test_ledger", initManagedLedgerConfig(defaultConfig()));

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

    @Test(timeOut = 20000)
    public void terminateWhileCreatingLedger() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);
        ManagedCursor c1 = ledger.openCursor("c1");

        // The first entry fills the ledger and triggers a rollover. The add and the close of the full ledger take the
        // first 2 steps of the mock BookKeeper client: hold the 3rd one, which is the creation of the next ledger
        CompletableFuture<Void> createLedgerGate = bkc.promiseAfter(2);
        Position p0 = ledger.addEntry("entry-0".getBytes());
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.CreatingLedger);

        // These adds are queued, waiting for the new ledger
        CompletableFuture<Position> add1 = addEntryAsync(ledger, "entry-1");
        CompletableFuture<Position> add2 = addEntryAsync(ledger, "entry-2");
        Awaitility.await().untilAsserted(() -> assertEquals(ledger.getPendingAddEntriesCount(), 2));

        assertEquals(ledger.terminate(), p0);

        // The ledger creation completes after the managed ledger was terminated
        createLedgerGate.complete(null);

        assertFailedWithTerminated(add1);
        assertFailedWithTerminated(add2);
        assertTerminatedAt(factory, ledger, p0);

        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getPosition(), p0);
        entries.forEach(Entry::release);
        assertEquals(c1.readEntries(10), Collections.emptyList());

        // The terminated state is what gets recovered
        ledger.close();
        ManagedLedger reopened = factory.open("my_test_ledger", config);
        assertTrue(reopened.isTerminated());
        assertEquals(reopened.getLastConfirmedEntry(), p0);
    }

    @Test(timeOut = 20000)
    public void terminateWhileCreatingLedgerFails() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        // Hold the creation of the next ledger, as in terminateWhileCreatingLedger()
        CompletableFuture<Void> createLedgerGate = bkc.promiseAfter(2);
        Position p0 = ledger.addEntry("entry-0".getBytes());
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.CreatingLedger);

        CompletableFuture<Position> add1 = addEntryAsync(ledger, "entry-1");
        Awaitility.await().untilAsserted(() -> assertEquals(ledger.getPendingAddEntriesCount(), 1));

        assertEquals(ledger.terminate(), p0);

        // The ledger creation fails after the managed ledger was terminated
        createLedgerGate.completeExceptionally(new BKException.BKNotEnoughBookiesException());

        assertFailedWithTerminated(add1);
        assertTerminatedAt(factory, ledger, p0);
    }

    @Test(timeOut = 20000)
    public void terminateWhileCreatingLedgerTimesOut() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setMaxEntriesPerLedger(1);
        config.setMetadataOperationsTimeoutSeconds(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        // Hold the creation of the next ledger, as in terminateWhileCreatingLedger()
        CompletableFuture<Void> createLedgerGate = bkc.promiseAfter(2);
        Position p0 = ledger.addEntry("entry-0".getBytes());
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.CreatingLedger);

        CompletableFuture<Position> add1 = addEntryAsync(ledger, "entry-1");
        Awaitility.await().untilAsserted(() -> assertEquals(ledger.getPendingAddEntriesCount(), 1));

        assertEquals(ledger.terminate(), p0);

        // The ledger creation times out after the managed ledger was terminated
        assertFailedWithTerminated(add1);
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.Terminated);

        // ... and the ledger still gets created later on. Hold its deletion, which is the next step of the mock
        // BookKeeper client, to observe that the ledger was created
        CompletableFuture<Void> deleteLedgerGate = bkc.promiseAfter(0);
        createLedgerGate.complete(null);
        Awaitility.await().untilAsserted(() -> assertEquals(bkc.getLedgers().size(), 2));

        deleteLedgerGate.complete(null);
        assertTerminatedAt(factory, ledger, p0);
    }

    @Test(timeOut = 20000)
    public void terminateWhileLedgersListUpdateIsDeferred() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        // Another metadata operation is in progress: after the rollover, the new ledger is created but the update of
        // the ledgers list keeps being deferred
        assertTrue(ledger.metadataMutex.tryLock());
        Position p0 = ledger.addEntry("entry-0".getBytes());
        Awaitility.await().untilAsserted(
                () -> assertEquals(ledger.getStats().getPendingBookieOpsStats().dataLedgerCreateOp, 0));
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.CreatingLedger);

        CompletableFuture<Position> add1 = addEntryAsync(ledger, "entry-1");
        Awaitility.await().untilAsserted(() -> assertEquals(ledger.getPendingAddEntriesCount(), 1));

        assertEquals(ledger.terminate(), p0);

        // The deferred update of the ledgers list gets its turn after the managed ledger was terminated
        ledger.metadataMutex.unlock();

        assertFailedWithTerminated(add1);
        assertTerminatedAt(factory, ledger, p0);
    }

    @DataProvider(name = "ledgersListUpdateFails")
    public Object[][] ledgersListUpdateFails() {
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 20000, dataProvider = "ledgersListUpdateFails")
    @SuppressWarnings("unchecked")
    public void terminateWhileUpdatingLedgersList(boolean updateFails) throws Exception {
        String mlPath = "/managed-ledgers/my_test_ledger";

        // Holds the response of a ledgers list update, which either is applied right away or fails. The spy does not
        // block, since the update is triggered while holding the managed ledger monitor
        CompletableFuture<Void> updateResponseGate = new CompletableFuture<>();
        AtomicBoolean interceptNextPut = new AtomicBoolean(false);
        CountDownLatch putIntercepted = new CountDownLatch(1);
        FaultInjectionMetadataStore spyStore = spy(metadataStore);
        doAnswer(inv -> {
            if (!interceptNextPut.compareAndSet(true, false)) {
                return inv.callRealMethod();
            }
            putIntercepted.countDown();
            CompletableFuture<Stat> response = updateFails
                    ? CompletableFuture.failedFuture(new MetadataStoreException("injected failure"))
                    : (CompletableFuture<Stat>) inv.callRealMethod();
            return updateResponseGate.thenCompose(ignore -> response);
        }).when(spyStore).put(eq(mlPath), any(byte[].class), any());

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl spyStoreFactory = new ManagedLedgerFactoryImpl(spyStore, bkc);
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) spyStoreFactory.open("my_test_ledger", config);

        // The first entry fills the ledger and triggers a rollover: the new ledger is created and the update of the
        // ledgers list is sent to the metadata store, though the response is still in flight
        interceptNextPut.set(true);
        Position p0 = ledger.addEntry("entry-0".getBytes());
        assertTrue(putIntercepted.await(10, TimeUnit.SECONDS));
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.CreatingLedger);

        CompletableFuture<Position> add1 = addEntryAsync(ledger, "entry-1");
        Awaitility.await().untilAsserted(() -> assertEquals(ledger.getPendingAddEntriesCount(), 1));

        // Terminate, holding its ledger close, which is the next step of the mock BookKeeper client
        CompletableFuture<Void> closeLedgerGate = bkc.promiseAfter(0);
        CompletableFuture<Position> terminated = new CompletableFuture<>();
        ledger.asyncTerminate(new TerminateCallback() {
            @Override
            public void terminateComplete(Position lastCommittedPosition, Object ctx) {
                terminated.complete(lastCommittedPosition);
            }

            @Override
            public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                terminated.completeExceptionally(exception);
            }
        }, null);
        assertTrue(ledger.isTerminated());

        // The response of the ledgers list update arrives after the managed ledger was terminated
        updateResponseGate.complete(null);
        assertFailedWithTerminated(add1);

        closeLedgerGate.complete(null);
        assertEquals(terminated.get(), p0);
        assertTerminatedAt(spyStoreFactory, ledger, p0);
    }

    private static CompletableFuture<Position> addEntryAsync(ManagedLedger ledger, String data) {
        CompletableFuture<Position> future = new CompletableFuture<>();
        ledger.asyncAddEntry(data.getBytes(), new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                future.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future;
    }

    private static void assertFailedWithTerminated(CompletableFuture<Position> add) throws Exception {
        try {
            Position position = add.get();
            fail("Add should have failed, it was written at " + position);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ManagedLedgerTerminatedException, "Unexpected failure: " + e.getCause());
        }
    }

    /**
     * Asserts that the managed ledger stayed terminated at the given position, after a ledger rollover that was in
     * progress during the terminate: nothing was written past that position, in memory, in the metadata store or in
     * BookKeeper, where the ledger created by the rollover must not be leaked.
     */
    private void assertTerminatedAt(ManagedLedgerFactoryImpl factory, ManagedLedgerImpl ledger, Position lastPosition)
            throws Exception {
        assertEquals(ledger.getState(), ManagedLedgerImpl.State.Terminated);
        assertEquals(ledger.getPendingAddEntriesCount(), 0);
        assertEquals(ledger.getLastConfirmedEntry(), lastPosition);
        assertEquals(ledger.getLedgersInfoAsList().size(), 1);

        ManagedLedgerInfo info = factory.getManagedLedgerInfo(ledger.getName());
        assertEquals(info.ledgers.size(), 1);
        assertEquals(info.ledgers.get(0).ledgerId, lastPosition.getLedgerId());
        assertEquals(info.terminatedPosition.ledgerId, lastPosition.getLedgerId());
        assertEquals(info.terminatedPosition.entryId, lastPosition.getEntryId());

        Awaitility.await().untilAsserted(() -> assertEquals(bkc.getLedgers(), Set.of(lastPosition.getLedgerId())));

        try {
            ledger.addEntry("entry-after-terminate".getBytes());
            fail("Should have thrown exception");
        } catch (ManagedLedgerTerminatedException e) {
            // Expected
        }
    }

}
