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
package org.apache.bookkeeper.mledger.impl.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

class ReadEntryUtils {

    static CompletableFuture<LedgerEntries> readAsync(ManagedLedger ml, ReadHandle handle, long firstEntry,
                                                      long lastEntry) {
        return readAsync(ml, handle, firstEntry, lastEntry, false, 0);
    }

    static CompletableFuture<LedgerEntries> readAsync(ManagedLedger ml, ReadHandle handle, long firstEntry,
                                                      long lastEntry, boolean batchReadEnabled, int batchReadMaxSize) {
        if (ml.getOptionalLedgerInfo(handle.getId()).isEmpty()) {
            // The read handle comes from another managed ledger, in this case, we can only compare the entry range with
            // the LAC of that read handle. Specifically, it happens when this method is called by a
            // ReadOnlyManagedLedgerImpl object.
            return handle.readAsync(firstEntry, lastEntry);
        }
        // Compare the entry range with the lastConfirmedEntry maintained by the managed ledger because the entry cache
        // of `ShadowManagedLedgerImpl` reads entries via `ReadOnlyLedgerHandle`, which never updates `lastAddConfirmed`
        final var lastConfirmedEntry = ml.getLastConfirmedEntry();
        if (lastConfirmedEntry == null) {
            return CompletableFuture.failedFuture(new ManagedLedgerException(
                    "LastConfirmedEntry is null when reading ledger " + handle.getId()));
        }
        if (handle.getId() > lastConfirmedEntry.getLedgerId()) {
            return CompletableFuture.failedFuture(new ManagedLedgerException("LastConfirmedEntry is "
                    + lastConfirmedEntry + " when reading ledger " + handle.getId()));
        }
        if (handle.getId() == lastConfirmedEntry.getLedgerId() && lastEntry > lastConfirmedEntry.getEntryId()) {
            return CompletableFuture.failedFuture(new ManagedLedgerException("LastConfirmedEntry is "
                    + lastConfirmedEntry + " when reading entry " + lastEntry));
        }

        int numberOfEntries = (int) (lastEntry - firstEntry + 1);

        // Use batch read for multiple entries when enabled.
        if (batchReadEnabled && numberOfEntries > 1 && batchReadMaxSize > 0) {
            return batchReadUnconfirmed(handle, firstEntry, numberOfEntries, batchReadMaxSize);
        }
        return handle.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    private static CompletableFuture<LedgerEntries> batchReadUnconfirmed(
            ReadHandle handle, long firstEntry, int maxCount, int maxSize) {
        CompletableFuture<LedgerEntries> future = new CompletableFuture<>();
        List<LedgerEntry> receivedEntries = new ArrayList<>(maxCount);
        List<LedgerEntries> ledgerEntries = new ArrayList<>(4);
        doBatchRead(handle, firstEntry, maxCount, maxSize, receivedEntries, ledgerEntries, future);
        return future;
    }

    private static void doBatchRead(ReadHandle handle, long firstEntry, int maxCount, int maxSize,
                                    List<LedgerEntry> receivedEntries, List<LedgerEntries> ledgerEntries,
                                    CompletableFuture<LedgerEntries> future) {
        handle.batchReadUnconfirmedAsync(firstEntry, maxCount - receivedEntries.size(), maxSize)
                .whenComplete((entries, throwable) -> {
                    if (throwable != null) {
                        onBatchReadComplete(handle, firstEntry, maxCount, receivedEntries, ledgerEntries, future,
                                throwable);
                        return;
                    }
                    long lastReceivedEntry = -1;
                    int prevReceivedCount = receivedEntries.size();
                    for (LedgerEntry entry : entries) {
                        receivedEntries.add(entry);
                        lastReceivedEntry = entry.getEntryId();
                    }
                    ledgerEntries.add(entries);
                    if (receivedEntries.size() >= maxCount || prevReceivedCount == receivedEntries.size()) {
                        onBatchReadComplete(handle, firstEntry, maxCount, receivedEntries, ledgerEntries, future, null);
                        return;
                    }
                    doBatchRead(handle, lastReceivedEntry + 1, maxCount, maxSize,
                            receivedEntries, ledgerEntries, future);
                });
    }

    private static void onBatchReadComplete(ReadHandle handle, long firstEntry, int maxCount,
                                            List<LedgerEntry> receivedEntries, List<LedgerEntries> ledgerEntries,
                                            CompletableFuture<LedgerEntries> future, Throwable error) {
        if (error != null) {
            ledgerEntries.forEach(LedgerEntries::close);
            future.completeExceptionally(error);
            return;
        }
        if (receivedEntries.isEmpty()) {
            ledgerEntries.forEach(LedgerEntries::close);
            future.completeExceptionally(new ManagedLedgerException(
                    "Batch read returned no entries for ledger " + handle.getId()
                            + " starting from entry " + firstEntry));
            return;
        }
        future.complete(CompositeLedgerEntriesImpl.create(receivedEntries, ledgerEntries));
    }
}
