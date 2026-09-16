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
                                                      long lastEntry, boolean batchReadEnabled, long maxSizeBytes) {
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

        // Use batch read for multiple entries when enabled. The size limit of the read bounds each batch read
        // request; without a limit (0), the BookKeeper client caps a request at its netty max frame size.
        if (batchReadEnabled && numberOfEntries > 1) {
            return batchReadUnconfirmed(handle, firstEntry, numberOfEntries, Math.max(maxSizeBytes, 0));
        }
        return handle.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    private static CompletableFuture<LedgerEntries> batchReadUnconfirmed(
            ReadHandle handle, long firstEntry, int maxCount, long maxSize) {
        CompletableFuture<LedgerEntries> future = new CompletableFuture<>();
        List<LedgerEntry> receivedEntries = new ArrayList<>(maxCount);
        List<LedgerEntries> ledgerEntries = new ArrayList<>(4);
        doBatchRead(handle, firstEntry, maxCount, maxSize, receivedEntries, ledgerEntries, future);
        return future;
    }

    private static void doBatchRead(ReadHandle handle, long firstEntry, int maxCount, long maxSize,
                                    List<LedgerEntry> receivedEntries, List<LedgerEntries> ledgerEntries,
                                    CompletableFuture<LedgerEntries> future) {
        if (future.isDone()) {
            ledgerEntries.forEach(LedgerEntries::close);
            return;
        }
        int remainingCount = maxCount - receivedEntries.size();
        CompletableFuture<LedgerEntries> readFuture;
        try {
            readFuture = handle.batchReadUnconfirmedAsync(firstEntry, remainingCount, maxSize);
        } catch (UnsupportedOperationException e) {
            // The BookKeeper client cannot issue batch reads (v3 wire protocol): read the remaining range with a
            // regular read, whose result is handled below like a batch read result
            readFuture = handle.readUnconfirmedAsync(firstEntry, firstEntry + remainingCount - 1);
        } catch (Throwable error) {
            onBatchReadComplete(handle, maxCount, receivedEntries, ledgerEntries, future, error);
            return;
        }
        readFuture.whenComplete((entries, error) -> {
            if (error != null) {
                onBatchReadComplete(handle, maxCount, receivedEntries, ledgerEntries, future, error);
                return;
            }
            if (receivedEntries.isEmpty() && !entries.iterator().hasNext()) {
                // Nothing was read at all: hand the empty result over as a regular read would, the callers already
                // cope with a read that returned no entries
                if (!future.complete(entries)) {
                    entries.close();
                }
                return;
            }
            ledgerEntries.add(entries);
            int previousCount = receivedEntries.size();
            long nextEntryId = firstEntry;
            for (LedgerEntry entry : entries) {
                if (entry.getEntryId() != nextEntryId) {
                    onBatchReadComplete(handle, maxCount, receivedEntries, ledgerEntries, future,
                            new ManagedLedgerException("Invalid batch read result for ledger " + handle.getId()
                                    + ": returned non-contiguous entry " + entry.getEntryId()
                                    + " while expecting " + nextEntryId));
                    return;
                }
                receivedEntries.add(entry);
                nextEntryId++;
            }
            if (receivedEntries.size() >= maxCount || previousCount == receivedEntries.size()) {
                onBatchReadComplete(handle, maxCount, receivedEntries, ledgerEntries, future, null);
                return;
            }
            doBatchRead(handle, nextEntryId, maxCount, maxSize, receivedEntries, ledgerEntries, future);
        });
    }

    private static void onBatchReadComplete(ReadHandle handle, int maxCount,
                                            List<LedgerEntry> receivedEntries, List<LedgerEntries> ledgerEntries,
                                            CompletableFuture<LedgerEntries> future, Throwable error) {
        if (error != null) {
            ledgerEntries.forEach(LedgerEntries::close);
            future.completeExceptionally(error);
            return;
        }
        if (receivedEntries.size() != maxCount) {
            ledgerEntries.forEach(LedgerEntries::close);
            future.completeExceptionally(new ManagedLedgerException(
                    "Batch read returned " + receivedEntries.size() + " entries for ledger " + handle.getId()
                            + " while " + maxCount + " entries were expected"));
            return;
        }
        LedgerEntries result = CompositeLedgerEntriesImpl.create(receivedEntries, ledgerEntries);
        if (!future.complete(result)) {
            result.close();
        }
    }
}
