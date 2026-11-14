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
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        long lastLedgerId = sortedPositions.get(sortedPositions.size() - 1).getLedgerId();

        ManagedLedgerImpl impl = (ManagedLedgerImpl) ledger;
        List<LedgerInfo> metadata = new ArrayList<>();
        final long totalSize;
        final long currentLedgerId;
        final boolean hasCurrentLedger;
        final long currentSize;
        final long currentEntries;
        synchronized (impl) {
            for (Map.Entry<Long, LedgerInfo> entry : impl.ledgers.headMap(lastLedgerId, true).entrySet()) {
                LedgerInfo info = entry.getValue();
                metadata.add(new LedgerInfo().setLedgerId(entry.getKey())
                        .setSize(info.getSize()).setEntries(info.getEntries()));
            }
            totalSize = impl.getTotalSize();
            hasCurrentLedger = impl.currentLedger != null;
            currentLedgerId = hasCurrentLedger ? impl.currentLedger.getId() : 0;
            currentSize = impl.currentLedgerSize;
            currentEntries = impl.currentLedgerEntries;
        }

        int metadataIndex = 0;
        long sizeBeforeLedger = 0;
        for (Position position : sortedPositions) {
            while (metadataIndex < metadata.size()
                    && metadata.get(metadataIndex).getLedgerId() < position.getLedgerId()) {
                sizeBeforeLedger += metadata.get(metadataIndex).getSize();
                metadataIndex++;
            }
            long size = totalSize - sizeBeforeLedger;
            if (metadataIndex < metadata.size()
                    && metadata.get(metadataIndex).getLedgerId() == position.getLedgerId()) {
                LedgerInfo info = metadata.get(metadataIndex);
                if (hasCurrentLedger && position.getLedgerId() == currentLedgerId) {
                    size -= ManagedLedgerImpl.consumedLedgerSize(currentSize, currentEntries, position.getEntryId());
                } else {
                    size -= ManagedLedgerImpl.consumedLedgerSize(
                            info.getSize(), info.getEntries(), position.getEntryId());
                }
            }
            estimates.put(position, size);
        }
        return estimates;
    }
}
