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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.UpdatePropertiesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
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
    public void terminateRemainsPersistedWhenClosedDuringMetadataUpdate() throws Exception {
        String ledgerName = "my_test_ledger";
        ManagedLedger ledger = factory.open(ledgerName);
        Position p0 = ledger.addEntry("entry-0".getBytes());

        CountDownLatch propertyMetadataPutStarted = new CountDownLatch(1);
        CountDownLatch allowPropertyMetadataPut = new CountDownLatch(1);
        String metadataPath = "/managed-ledgers/" + ledgerName;
        metadataStore.failConditional(new MetadataStoreException.BadVersionException("metadata update blocked"),
                (operation, path) -> {
                    if (operation != FaultInjectionMetadataStore.OperationType.PUT || !metadataPath.equals(path)) {
                        return false;
                    }
                    propertyMetadataPutStarted.countDown();
                    try {
                        return !allowPropertyMetadataPut.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return true;
                    }
                });

        CompletableFuture<Void> propertyUpdateResult = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> ledger.asyncSetProperty("key", "value", new UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                propertyUpdateResult.complete(null);
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                propertyUpdateResult.completeExceptionally(exception);
            }
        }, null));
        assertTrue(propertyMetadataPutStarted.await(5, TimeUnit.SECONDS));

        CompletableFuture<Position> terminationResult = new CompletableFuture<>();
        ledger.asyncTerminate(new TerminateCallback() {
            @Override
            public void terminateComplete(Position lastCommittedPosition, Object ctx) {
                terminationResult.complete(lastCommittedPosition);
            }

            @Override
            public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                terminationResult.completeExceptionally(exception);
            }
        }, null);

        CompletableFuture<Void> closeResult = new CompletableFuture<>();
        ledger.asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                closeResult.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                closeResult.completeExceptionally(exception);
            }
        }, null);

        try {
            closeResult.get(5, TimeUnit.SECONDS);
        } finally {
            allowPropertyMetadataPut.countDown();
        }

        propertyUpdateResult.get(5, TimeUnit.SECONDS);
        assertEquals(terminationResult.get(5, TimeUnit.SECONDS), p0);

        ManagedLedger reopenedLedger = factory.open(ledgerName);
        try {
            reopenedLedger.addEntry("entry-1".getBytes());
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
