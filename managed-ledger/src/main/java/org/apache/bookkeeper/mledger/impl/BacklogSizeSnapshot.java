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

import java.util.Arrays;
import java.util.Map;
import java.util.function.ToLongFunction;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;

/** Creates a request-scoped estimator for subscription backlog statistics. */
@InterfaceAudience.Private
public final class BacklogSizeSnapshot {
    private BacklogSizeSnapshot() {
    }

    /**
     * Create an approximate estimator shared by a short-lived batch of positions.
     */
    public static ToLongFunction<Position> create(ManagedLedger ledger) {
        // Subclasses can change ledger metadata independently of this implementation's synchronization.
        if (ledger.getClass() != ManagedLedgerImpl.class) {
            return ledger::getEstimatedBacklogSize;
        }

        ManagedLedgerImpl impl = (ManagedLedgerImpl) ledger;
        final long[] ledgerIds;
        final long[] prefixSizes;
        final long[] entries;
        final long totalSize;
        final Position confirmedPosition;
        final long currentLedgerId;
        final long currentSize;
        final long currentEntries;
        synchronized (impl) {
            if (impl.currentLedger == null || impl.ledgers.isEmpty()) {
                return ledger::getEstimatedBacklogSize;
            }
            int count = impl.ledgers.size();
            ledgerIds = new long[count];
            prefixSizes = new long[count];
            entries = new long[count];
            int i = 0;
            for (Map.Entry<Long, LedgerInfo> entry : impl.ledgers.entrySet()) {
                // The exposed ledger map can also be modified by callers that do not hold this monitor.
                if (i == count) {
                    return ledger::getEstimatedBacklogSize;
                }
                LedgerInfo info = entry.getValue();
                ledgerIds[i] = entry.getKey();
                prefixSizes[i] = info.getSize();
                entries[i] = info.getEntries();
                i++;
            }
            if (i != count) {
                return ledger::getEstimatedBacklogSize;
            }
            // The write callback updates the total before publishing lastConfirmedEntry. Reading the
            // position first ensures the total includes its bytes, although the counters are not atomic.
            confirmedPosition = impl.lastConfirmedEntry;
            totalSize = impl.getTotalSize();
            currentLedgerId = impl.currentLedger.getId();
            currentSize = impl.currentLedgerSize;
            currentEntries = impl.currentLedgerEntries;
        }
        for (int i = 1; i < prefixSizes.length; i++) {
            prefixSizes[i] += prefixSizes[i - 1];
        }
        return pos -> {
            if (pos == null) {
                return 0;
            }
            // A later cursor position can refer to an entry whose bytes were not in the captured total.
            if (confirmedPosition == null || pos.compareTo(confirmedPosition) > 0
                    || pos.getLedgerId() > ledgerIds[ledgerIds.length - 1]) {
                return impl.getEstimatedBacklogSize(pos);
            }
            if (pos.getLedgerId() == currentLedgerId && pos.getEntryId() >= currentEntries
                    && pos.getEntryId() >= 0) {
                return impl.getEstimatedBacklogSize(pos);
            }
            int index = Arrays.binarySearch(ledgerIds, pos.getLedgerId());
            int before = index >= 0 ? index : -index - 1;
            long size = totalSize - (before == 0 ? 0 : prefixSizes[before - 1]);
            if (index < 0) {
                return size;
            }
            long ledgerSize = prefixSizes[index] - (index == 0 ? 0 : prefixSizes[index - 1]);
            if (pos.getLedgerId() == currentLedgerId) {
                return size - ManagedLedgerImpl.consumedLedgerSize(currentSize, currentEntries, pos.getEntryId());
            }
            return size - ManagedLedgerImpl.consumedLedgerSize(ledgerSize, entries[index], pos.getEntryId());
        };
    }
}
