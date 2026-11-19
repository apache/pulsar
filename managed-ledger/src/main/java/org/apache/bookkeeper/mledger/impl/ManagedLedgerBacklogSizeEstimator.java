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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;

/** Estimates subscription backlog sizes together using one scan of their ledger metadata prefix. */
@InterfaceAudience.Private
public final class ManagedLedgerBacklogSizeEstimator {
    private ManagedLedgerBacklogSizeEstimator() {
    }

    /**
     * Estimate a batch of positions captured before calling this method.
     * The results have the same approximate semantics as {@link ManagedLedger#getEstimatedBacklogSize(Position)}.
     * Results are keyed by their input positions; duplicate positions share one result.
     * An empty position collection produces an empty result.
     *
     * @throws NullPointerException if the ledger, position collection, or any position is null
     */
    public static Map<Position, Long> estimate(ManagedLedger ledger, Collection<Position> positions) {
        List<Position> sortedPositions = new ArrayList<>(positions.size());
        for (Position position : positions) {
            sortedPositions.add(Objects.requireNonNull(position, "position"));
        }
        Map<Position, Long> estimates = new HashMap<>();
        // Subclasses can change ledger metadata independently of this implementation's synchronization.
        if (ledger.getClass() != ManagedLedgerImpl.class || sortedPositions.size() <= 1) {
            for (Position position : sortedPositions) {
                estimates.put(position, ledger.getEstimatedBacklogSize(position));
            }
            return estimates;
        }

        sortedPositions.sort(Comparator.comparingLong(Position::getLedgerId));
        int positionCount = sortedPositions.size();
        long[] ledgerIds = new long[positionCount];
        int[] positionLedgerIndexes = new int[positionCount];
        int queriedLedgerCount = 0;
        for (int i = 0; i < positionCount; i++) {
            long ledgerId = sortedPositions.get(i).getLedgerId();
            if (queriedLedgerCount == 0 || ledgerIds[queriedLedgerCount - 1] != ledgerId) {
                ledgerIds[queriedLedgerCount++] = ledgerId;
            }
            positionLedgerIndexes[i] = queriedLedgerCount - 1;
        }
        // Allocate by query count before acquiring the writer monitor. Only the strict prefix sum
        // and the queried ledger's counters are needed, not a copy of every ledger in that prefix.
        long[] prefixSizes = new long[queriedLedgerCount];
        long[] ledgerSizes = new long[queriedLedgerCount];
        long[] ledgerEntries = new long[queriedLedgerCount];
        boolean[] ledgerPresent = new boolean[queriedLedgerCount];

        ManagedLedgerImpl impl = (ManagedLedgerImpl) ledger;
        final long totalSize;
        final long currentLedgerId;
        final boolean hasCurrentLedger;
        final long currentSize;
        final long currentEntries;
        synchronized (impl) {
            // LedgerInfo IDs match the map keys. Iterating values avoids allocating an entry wrapper
            // per ledger in ConcurrentSkipListMap's submap iterator.
            Iterator<LedgerInfo> metadata = impl.ledgers
                    .headMap(ledgerIds[queriedLedgerCount - 1], true).values().iterator();
            LedgerInfo info = metadata.hasNext() ? metadata.next() : null;
            long prefixSize = 0;
            for (int i = 0; i < queriedLedgerCount; i++) {
                while (info != null && info.getLedgerId() < ledgerIds[i]) {
                    prefixSize += info.getSize();
                    info = metadata.hasNext() ? metadata.next() : null;
                }
                prefixSizes[i] = prefixSize;
                if (info != null && info.getLedgerId() == ledgerIds[i]) {
                    ledgerPresent[i] = true;
                    ledgerSizes[i] = info.getSize();
                    ledgerEntries[i] = info.getEntries();
                }
            }
            totalSize = impl.getTotalSize();
            hasCurrentLedger = impl.currentLedger != null;
            currentLedgerId = hasCurrentLedger ? impl.currentLedger.getId() : 0;
            currentSize = impl.currentLedgerSize;
            currentEntries = impl.currentLedgerEntries;
        }

        for (int i = 0; i < positionCount; i++) {
            Position position = sortedPositions.get(i);
            int ledgerIndex = positionLedgerIndexes[i];
            long size = totalSize - prefixSizes[ledgerIndex];
            if (ledgerPresent[ledgerIndex]) {
                if (hasCurrentLedger && ledgerIds[ledgerIndex] == currentLedgerId) {
                    size -= ManagedLedgerImpl.consumedLedgerSize(currentSize, currentEntries, position.getEntryId());
                } else {
                    size -= ManagedLedgerImpl.consumedLedgerSize(
                            ledgerSizes[ledgerIndex], ledgerEntries[ledgerIndex], position.getEntryId());
                }
            }
            estimates.put(position, size);
        }
        return estimates;
    }
}
