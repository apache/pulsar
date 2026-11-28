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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ReadOnlyCursorCloseTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 30000)
    public void testFactoryCursorCloseReleasesCachedReadHandles() throws Exception {
        String name = "readonly-cursor-close";
        ManagedLedger writer = factory.open(name, rolloverConfig());
        Position first = writer.addEntry("first".getBytes(StandardCharsets.UTF_8));
        Position second = writer.addEntry("second".getBytes(StandardCharsets.UTF_8));
        assertThat(first.getLedgerId()).isNotEqualTo(second.getLedgerId());

        ReadOnlyCursor cursor = factory.openReadOnlyCursor(name, PositionFactory.EARLIEST, defaultConfig());
        readAllEntries(cursor);

        ManagedLedgerImpl readOnlyLedger = readOnlyLedgerOf(cursor);
        assertThat(readOnlyLedger.ledgerCache.size()).isEqualTo(2);
        List<LedgerHandle> handles = replaceCachedReadHandlesWithSpies(readOnlyLedger);

        cursor.close();
        // A second close must not release the handles twice.
        cursor.close();

        for (LedgerHandle handle : handles) {
            verify(handle, times(1)).closeAsync();
        }
        assertThat(readOnlyLedger.ledgerCache.size()).isZero();
        assertThat(readOnlyLedger.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
        // The writable managed ledger with the same name stays open.
        writer.addEntry("after-close".getBytes(StandardCharsets.UTF_8));
    }

    @Test(timeOut = 30000)
    public void testAsyncFactoryCursorCloseReleasesCachedReadHandles() throws Exception {
        String name = "readonly-cursor-async-close";
        ManagedLedger writer = factory.open(name, rolloverConfig());
        writer.addEntry("first".getBytes(StandardCharsets.UTF_8));
        writer.addEntry("second".getBytes(StandardCharsets.UTF_8));

        ReadOnlyCursor cursor = factory.openReadOnlyCursor(name, PositionFactory.EARLIEST, defaultConfig());
        readAllEntries(cursor);

        ManagedLedgerImpl readOnlyLedger = readOnlyLedgerOf(cursor);
        assertThat(readOnlyLedger.ledgerCache.size()).isEqualTo(2);
        List<LedgerHandle> handles = replaceCachedReadHandlesWithSpies(readOnlyLedger);

        closeAsync(cursor);

        for (LedgerHandle handle : handles) {
            verify(handle).closeAsync();
        }
        assertThat(readOnlyLedger.ledgerCache.size()).isZero();
    }

    @Test(timeOut = 30000)
    public void testWritableLedgerReopenedAfterFactoryCursorCloseCanRead() throws Exception {
        String name = "readonly-cursor-then-writer";
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger writer = factory.open(name, config);
        Position first = writer.addEntry("first".getBytes(StandardCharsets.UTF_8));
        writer.close();

        ReadOnlyCursor cursor = factory.openReadOnlyCursor(name, PositionFactory.EARLIEST, config);
        readAllEntries(cursor);
        cursor.close();

        ManagedLedger reopened = factory.open(name, config);
        CompletableFuture<Entry> reading = new CompletableFuture<>();
        reopened.asyncReadEntry(first, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                reading.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                reading.completeExceptionally(exception);
            }
        }, null);
        Entry entry = reading.get(5, TimeUnit.SECONDS);
        try {
            assertThat(entry.getData()).containsExactly("first".getBytes(StandardCharsets.UTF_8));
        } finally {
            entry.release();
        }
    }

    @Test(timeOut = 30000)
    public void testFactoryCursorCloseWithoutReadsClosesLedger() throws Exception {
        String name = "readonly-cursor-close-without-reads";
        ManagedLedger writer = factory.open(name, defaultConfig().setRetentionTime(1, TimeUnit.HOURS));
        writer.addEntry("entry".getBytes(StandardCharsets.UTF_8));

        ReadOnlyCursor cursor = factory.openReadOnlyCursor(name, PositionFactory.EARLIEST, defaultConfig());
        ManagedLedgerImpl readOnlyLedger = readOnlyLedgerOf(cursor);

        cursor.close();

        assertThat(readOnlyLedger.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
        assertThat(readOnlyLedger.ledgerCache.size()).isZero();
    }

    @Test(timeOut = 30000)
    public void testCursorOnCallerOwnedLedgerKeepsItOpen() throws Exception {
        String name = "readonly-cursor-borrowed-ledger";
        ManagedLedger writer = factory.open(name, defaultConfig().setRetentionTime(1, TimeUnit.HOURS));
        Position position = writer.addEntry("entry".getBytes(StandardCharsets.UTF_8));
        ReadOnlyManagedLedger reader = openReadOnly(name);
        ReadOnlyCursor cursor = reader.createReadOnlyCursor(PositionFactory.EARLIEST);
        readAllEntries(cursor);

        ManagedLedgerImpl readOnlyLedger = readOnlyLedgerOf(cursor);
        assertThat(readOnlyLedger.ledgerCache.size()).isEqualTo(1);
        List<LedgerHandle> handles = replaceCachedReadHandlesWithSpies(readOnlyLedger);
        try {
            cursor.close();

            for (LedgerHandle handle : handles) {
                verify(handle, never()).closeAsync();
            }
            assertThat(readOnlyLedger.getState()).isNotEqualTo(ManagedLedgerImpl.State.Closed);

            // The caller-owned ledger is still usable after the cursor is closed.
            CompletableFuture<Entry> reading = new CompletableFuture<>();
            reader.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(Entry entry, Object ctx) {
                    reading.complete(entry);
                }

                @Override
                public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                    reading.completeExceptionally(exception);
                }
            }, null);
            Entry entry = reading.get(5, TimeUnit.SECONDS);
            entry.release();
        } finally {
            closeLedger(readOnlyLedger);
        }
    }

    private static ManagedLedgerConfig rolloverConfig() {
        ManagedLedgerConfig config = defaultConfig()
                .setRetentionTime(1, TimeUnit.HOURS)
                .setRetentionSizeInMB(-1)
                .setMaxEntriesPerLedger(1);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        return config;
    }

    private static void readAllEntries(ReadOnlyCursor cursor) throws Exception {
        while (cursor.hasMoreEntries()) {
            List<Entry> entries = cursor.readEntries(10);
            assertThat(entries).isNotEmpty();
            entries.forEach(Entry::release);
        }
    }

    private static ManagedLedgerImpl readOnlyLedgerOf(ReadOnlyCursor cursor) {
        return (ManagedLedgerImpl) ((ManagedCursor) cursor).getManagedLedger();
    }

    private static List<LedgerHandle> replaceCachedReadHandlesWithSpies(ManagedLedgerImpl ledger) throws Exception {
        List<LedgerHandle> handles = new ArrayList<>();
        for (Long ledgerId : ledger.ledgerCache.keys()) {
            ReadHandle readHandle = ledger.ledgerCache.get(ledgerId).get(5, TimeUnit.SECONDS);
            LedgerHandle spyHandle = spy((LedgerHandle) readHandle);
            ledger.ledgerCache.put(ledgerId, CompletableFuture.completedFuture(spyHandle));
            handles.add(spyHandle);
        }
        return handles;
    }

    private static void closeAsync(ReadOnlyCursor cursor) throws Exception {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                closed.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                closed.completeExceptionally(exception);
            }
        }, null);
        closed.get(5, TimeUnit.SECONDS);
    }

    private static void closeLedger(ManagedLedgerImpl ledger) throws Exception {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        ledger.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                closed.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                closed.completeExceptionally(exception);
            }
        }, null);
        closed.get(5, TimeUnit.SECONDS);
    }

    private ReadOnlyManagedLedger openReadOnly(String name) throws Exception {
        CompletableFuture<ReadOnlyManagedLedger> result = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(name, new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
            @Override
            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger, Object ctx) {
                result.complete(managedLedger);
            }

            @Override
            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, defaultConfig(), null);
        return result.get(10, TimeUnit.SECONDS);
    }
}
