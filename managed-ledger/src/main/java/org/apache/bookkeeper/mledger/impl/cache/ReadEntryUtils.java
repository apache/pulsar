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

        // Use batch read for multiple entries when enabled. The size limit of the read bounds each batch read
        // request; without a limit (0), the BookKeeper client caps a request at its netty max frame size.
        if (batchReadEnabled && lastEntry > firstEntry) {
            CompletableFuture<LedgerEntries> future = new CompletableFuture<>();
            batchRead(handle, firstEntry, lastEntry, Math.max(maxSizeBytes, 0), null, null, future);
            return future;
        }
        return handle.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    /**
     * Issues a batch read for the entries from {@code firstEntry} to {@code lastEntry}. A bookie returns at least the
     * first entry but may stop before {@code lastEntry} once the size limit is reached, in which case the next request
     * continues from the first missing entry and the results are merged once the range is complete. {@code received}
     * and {@code batches} accumulate the entries and the results of the previous requests; they are null until a
     * partial result makes them necessary, so that the common single-request case hands the result over as-is.
     */
    private static void batchRead(ReadHandle handle, long firstEntry, long lastEntry, long maxSize,
                                  List<LedgerEntry> received, List<LedgerEntries> batches,
                                  CompletableFuture<LedgerEntries> future) {
        if (future.isDone()) {
            // The read was cancelled: release what was received so far
            closeAll(batches);
            return;
        }
        int maxCount = (int) (lastEntry - firstEntry + 1);
        CompletableFuture<LedgerEntries> readFuture;
        try {
            readFuture = handle.batchReadUnconfirmedAsync(firstEntry, maxCount, maxSize);
        } catch (Throwable t) {
            failBatchRead(batches, future, t);
            return;
        }
        readFuture.whenComplete((entries, error) -> {
            if (error != null) {
                failBatchRead(batches, future, error);
                return;
            }
            // Validate the result in a single pass: the entries must be contiguous from firstEntry and within the
            // requested range. They are collected only when merging with the previous results.
            long nextEntryId = firstEntry;
            for (LedgerEntry entry : entries) {
                if (nextEntryId > lastEntry || entry.getEntryId() != nextEntryId) {
                    String problem = nextEntryId > lastEntry
                            ? "entry " + entry.getEntryId() + " beyond the last requested entry " + lastEntry
                            : "non-contiguous entry " + entry.getEntryId() + " while expecting " + nextEntryId;
                    entries.close();
                    failBatchRead(batches, future, new ManagedLedgerException(
                            "Invalid batch read result for ledger " + handle.getId() + ": returned " + problem));
                    return;
                }
                nextEntryId++;
                if (received != null) {
                    received.add(entry);
                }
            }
            int count = (int) (nextEntryId - firstEntry);
            if (received == null) {
                if (count == maxCount || count == 0) {
                    // The whole range in a single request, the common case, or nothing at all (handed over as a
                    // regular read would, the callers cope with a read that returned no entries): no merge needed
                    if (!future.complete(entries)) {
                        entries.close();
                    }
                    return;
                }
            } else if (count == 0) {
                // No progress after a partial result: the range cannot be completed
                entries.close();
                failBatchRead(batches, future, new ManagedLedgerException("Batch read returned " + received.size()
                        + " entries for ledger " + handle.getId() + " while " + (received.size() + maxCount)
                        + " entries were expected"));
                return;
            }
            List<LedgerEntry> allEntries = received;
            List<LedgerEntries> allBatches = batches;
            if (allEntries == null) {
                // The first request returned a prefix of the range: start merging
                allEntries = new ArrayList<>(maxCount);
                for (LedgerEntry entry : entries) {
                    allEntries.add(entry);
                }
                allBatches = new ArrayList<>(4);
            }
            allBatches.add(entries);
            if (nextEntryId > lastEntry) {
                LedgerEntries result = CompositeLedgerEntriesImpl.create(allEntries, allBatches);
                if (!future.complete(result)) {
                    result.close();
                }
                return;
            }
            batchRead(handle, nextEntryId, lastEntry, maxSize, allEntries, allBatches, future);
        });
    }

    private static void failBatchRead(List<LedgerEntries> batches, CompletableFuture<LedgerEntries> future,
                                      Throwable error) {
        closeAll(batches);
        future.completeExceptionally(error);
    }

    private static void closeAll(List<LedgerEntries> batches) {
        if (batches != null) {
            batches.forEach(LedgerEntries::close);
        }
    }
}
