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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
     * Results are keyed by their input positions; duplicate positions share one result.
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
        List<LedgerMetadata> metadata = new ArrayList<>();
        final long totalSize;
        final long currentLedgerId;
        final boolean hasCurrentLedger;
        final long currentSize;
        final long currentEntries;
        synchronized (impl) {
            for (Map.Entry<Long, LedgerInfo> entry : impl.ledgers.headMap(lastLedgerId, true).entrySet()) {
                LedgerInfo info = entry.getValue();
                metadata.add(new LedgerMetadata(entry.getKey(), info.getSize(), info.getEntries()));
            }
            totalSize = impl.getTotalSize();
            hasCurrentLedger = impl.currentLedger != null;
            currentLedgerId = hasCurrentLedger ? impl.currentLedger.getId() : 0;
            currentSize = impl.currentLedgerSize;
            currentEntries = impl.currentLedgerEntries;
        }

        long[] prefixSizes = new long[metadata.size() + 1];
        for (int i = 0; i < metadata.size(); i++) {
            prefixSizes[i + 1] = prefixSizes[i] + metadata.get(i).size();
        }
        for (Position position : positions) {
            if (position == null) {
                estimates.put(null, 0L);
                continue;
            }
            int index = lowerBound(metadata, position.getLedgerId());
            long size = totalSize - prefixSizes[index];
            if (index < metadata.size() && metadata.get(index).ledgerId() == position.getLedgerId()) {
                LedgerMetadata info = metadata.get(index);
                if (hasCurrentLedger && position.getLedgerId() == currentLedgerId) {
                    size -= ManagedLedgerImpl.consumedLedgerSize(currentSize, currentEntries, position.getEntryId());
                } else {
                    size -= ManagedLedgerImpl.consumedLedgerSize(info.size(), info.entries(), position.getEntryId());
                }
            }
            estimates.put(position, size);
        }
        return estimates;
    }

    private static int lowerBound(List<LedgerMetadata> metadata, long ledgerId) {
        int low = 0;
        int high = metadata.size();
        while (low < high) {
            int mid = low + (high - low) / 2;
            if (metadata.get(mid).ledgerId() < ledgerId) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private record LedgerMetadata(long ledgerId, long size, long entries) {
    }
}
