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
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadEntryUtils {
    private static final Logger log = LoggerFactory.getLogger(ReadEntryUtils.class);

    static CompletableFuture<LedgerEntries> readAsync(ManagedLedger ml, ReadHandle handle, long firstEntry,
                                                      long lastEntry) {
        return readAsync(ml, handle, firstEntry, lastEntry, false, 0);
    }

    static CompletableFuture<LedgerEntries> readAsync(ManagedLedger ml, ReadHandle handle, long firstEntry,
                                                      long lastEntry, boolean batchReadEnabled, long batchReadMaxSize) {
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

        // Use batch read for multiple entries when enabled
        if (batchReadEnabled && numberOfEntries > 1) {
            if (log.isDebugEnabled()) {
                log.debug("Using batch read for ledger {} entries {}-{}, maxCount={}, maxSize={}",
                        handle.getId(), firstEntry, lastEntry, numberOfEntries, batchReadMaxSize);
            }
            return batchReadWithAutoRefill(handle, firstEntry, lastEntry, numberOfEntries, batchReadMaxSize);
        }

        return handle.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    private static CompletableFuture<LedgerEntries> batchReadWithAutoRefill(
            ReadHandle handle, long firstEntry, long lastEntry,
            int maxCount, long maxSize) {

        return handle.batchReadAsync(firstEntry, maxCount, maxSize)
                .exceptionallyCompose(ex -> {
                    // Fallback to readUnconfirmedAsync if batch read fails
                    log.warn("Batch read failed for ledger {} entries {}-{}, falling back to regular read: {}",
                            handle.getId(), firstEntry, lastEntry, ex.getMessage());
                    return handle.readUnconfirmedAsync(firstEntry, lastEntry);
                })
                .thenCompose(entries -> {
                    // Collect entries and find the last received entry id in a single pass
                    List<LedgerEntry> receivedList = new ArrayList<>();
                    long lastReceivedEntryId = -1;
                    for (LedgerEntry e : entries) {
                        receivedList.add(e);
                        lastReceivedEntryId = e.getEntryId();
                    }
                    int receivedCount = receivedList.size();

                    // All entries received, return as-is
                    if (receivedCount >= maxCount) {
                        return CompletableFuture.completedFuture(entries);
                    }

                    // Partial result: need to read remaining entries
                    if (receivedCount == 0) {
                        // Edge case: no entries returned, use regular read
                        entries.close();
                        log.warn("Batch read returned 0 entries for ledger {} entries {}-{}, falling back to regular read",
                                handle.getId(), firstEntry, lastEntry);
                        return handle.readUnconfirmedAsync(firstEntry, lastEntry);
                    }

                    // Close the original entries since we've collected them into receivedList
                    entries.close();

                    long nextEntryId = lastReceivedEntryId + 1;
                    int remainingCount = (int) (lastEntry - nextEntryId + 1);

                    if (log.isDebugEnabled()) {
                        log.debug("Batch read partial result for ledger {}: received {}/{}, reading remaining {}-{}",
                                handle.getId(), receivedCount, maxCount, nextEntryId, lastEntry);
                    }

                    // Recursively read remaining entries
                    return batchReadWithAutoRefill(handle, nextEntryId, lastEntry, remainingCount, maxSize)
                            .thenApply(remainingEntries -> {
                                // Combine received and remaining entries
                                List<LedgerEntry> combined = new ArrayList<>(receivedCount + remainingCount);
                                combined.addAll(receivedList);
                                for (LedgerEntry e : remainingEntries) {
                                    combined.add(e);
                                }
                                remainingEntries.close();
                                return LedgerEntriesImpl.create(combined);
                            });
                });
    }
}
