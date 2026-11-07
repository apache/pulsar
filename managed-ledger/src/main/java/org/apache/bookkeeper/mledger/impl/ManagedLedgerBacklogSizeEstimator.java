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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;

/** Estimates subscription backlog sizes together using one copy of their ledger metadata prefix. */
@InterfaceAudience.Private
public final class ManagedLedgerBacklogSizeEstimator {
    private ManagedLedgerBacklogSizeEstimator() {
    }

    /**
     * Estimate a batch of positions captured before calling this method.
     * The results have the same approximate semantics as {@link ManagedLedger#getEstimatedBacklogSize(Position)}.
     */
    public static Map<Position, Long> estimate(ManagedLedger ledger, Collection<Position> positions) {
        Map<Position, Long> estimates = new HashMap<>();
        // Subclasses can change ledger metadata independently of this implementation's synchronization.
        if (ledger.getClass() != ManagedLedgerImpl.class || positions.size() <= 1) {
            for (Position position : positions) {
                estimates.put(position, ledger.getEstimatedBacklogSize(position));
            }
            return estimates;
        }

        long lastLedgerId = Long.MIN_VALUE;
        boolean hasPosition = false;
        for (Position position : positions) {
            if (position != null) {
                lastLedgerId = Math.max(lastLedgerId, position.getLedgerId());
                hasPosition = true;
            }
        }
        if (!hasPosition) {
            estimates.put(null, 0L);
            return estimates;
        }

        ManagedLedgerImpl impl = (ManagedLedgerImpl) ledger;
        long[] ledgerIds = new long[8];
        long[] prefixSizes = new long[8];
        long[] entries = new long[8];
        int count = 0;
        final long totalSize;
        final long currentLedgerId;
        final boolean hasCurrentLedger;
        final long currentSize;
        final long currentEntries;
        synchronized (impl) {
            for (Map.Entry<Long, LedgerInfo> entry : impl.ledgers.headMap(lastLedgerId, true).entrySet()) {
                if (count == ledgerIds.length) {
                    int capacity = count * 2;
                    ledgerIds = Arrays.copyOf(ledgerIds, capacity);
                    prefixSizes = Arrays.copyOf(prefixSizes, capacity);
                    entries = Arrays.copyOf(entries, capacity);
                }
                LedgerInfo info = entry.getValue();
                ledgerIds[count] = entry.getKey();
                prefixSizes[count] = info.getSize();
                entries[count] = info.getEntries();
                count++;
            }
            totalSize = impl.getTotalSize();
            hasCurrentLedger = impl.currentLedger != null;
            currentLedgerId = hasCurrentLedger ? impl.currentLedger.getId() : 0;
            currentSize = impl.currentLedgerSize;
            currentEntries = impl.currentLedgerEntries;
        }

        for (int i = 1; i < count; i++) {
            prefixSizes[i] += prefixSizes[i - 1];
        }
        for (Position position : positions) {
            if (position == null) {
                estimates.put(null, 0L);
                continue;
            }
            int index = Arrays.binarySearch(ledgerIds, 0, count, position.getLedgerId());
            int before = index >= 0 ? index : -index - 1;
            long sizeBefore = before == 0 ? 0 : prefixSizes[before - 1];
            long size = totalSize - sizeBefore;
            if (index >= 0) {
                long ledgerSize = prefixSizes[index] - sizeBefore;
                if (hasCurrentLedger && position.getLedgerId() == currentLedgerId) {
                    size -= ManagedLedgerImpl.consumedLedgerSize(currentSize, currentEntries, position.getEntryId());
                } else {
                    size -= ManagedLedgerImpl.consumedLedgerSize(ledgerSize, entries[index], position.getEntryId());
                }
            }
            estimates.put(position, size);
        }
        return estimates;
    }
}
