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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@VisibleForTesting
public class ReadEntryUtils {

    private static final Logger log = LoggerFactory.getLogger(ReadEntryUtils.class);

    @VisibleForTesting
    public static CompletableFuture<LedgerEntries> readAsync(ManagedLedger ml, ReadHandle handle, long firstEntry,
                                                      long lastEntry, boolean useBookkeeperV2WireProtocol) {
        int entriesToRead = (int) (lastEntry - firstEntry + 1);
        boolean useBatchRead = useBatchRead(entriesToRead, handle, useBookkeeperV2WireProtocol);
        if (ml.getOptionalLedgerInfo(handle.getId()).isEmpty()) {
            // The read handle comes from another managed ledger, in this case, we can only compare the entry range with
            // the LAC of that read handle. Specifically, it happens when this method is called by a
            // ReadOnlyManagedLedgerImpl object.
            if (!useBatchRead) {
                return handle.readAsync(firstEntry, lastEntry);
            }
            return handle.batchReadAsync(firstEntry, entriesToRead, 0);
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

        if (useBatchRead && handle instanceof LedgerHandle lh) {
            return asyncBatchReadUnconfirmedEntries(lh, firstEntry, entriesToRead);
        }
        return handle.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    private static boolean useBatchRead(int entriesToRead, ReadHandle handle, boolean useBookkeeperV2WireProtocol) {
        // Batch read is not supported for striped ledgers.
        LedgerMetadata m = handle.getLedgerMetadata();
        boolean isStriped = m.getEnsembleSize() != m.getWriteQuorumSize();
        return entriesToRead > 1 && useBookkeeperV2WireProtocol && !isStriped;
    }

    private static CompletableFuture<LedgerEntries> asyncBatchReadUnconfirmedEntries(LedgerHandle lh, long firstEntry,
            int numEntries) {
        CompletableFuture<LedgerEntries> f = new CompletableFuture<>();
        lh.asyncBatchReadUnconfirmedEntries(firstEntry, numEntries, 0, (rc, lh1, seq, ctx) -> {
            if (rc != BKException.Code.OK) {
                log.error("Failed to batch read entries from ledger {} : {}", lh1.getId(), BKException.getMessage(rc));
                f.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            List<org.apache.bookkeeper.client.api.LedgerEntry> entries = new ArrayList<>(numEntries);
            while (seq.hasMoreElements()) {
                LedgerEntry entry = seq.nextElement();
                LedgerEntryImpl entryImpl = LedgerEntryImpl.create(entry.getLedgerId(), entry.getEntryId(),
                        entry.getLength(), entry.getEntryBuffer());
                entries.add(entryImpl);
            }
            f.complete(LedgerEntriesImpl.create(entries));
        }, null);
        return f;
    }
}
