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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.PositionInfo;

/**
 * Strategy interface for persisting individual (non-cumulative) ack state to the cursor ledger
 * and recovering it.
 *
 * <h3>Implementations</h3>
 * <ul>
 *   <li>Built-in legacy path — ManagedCursorImpl handles flush/recovery inline when no
 *       AckPersistence is configured (default behavior).</li>
 *   <li>{@link IndividualAckStatePersistence} — per-msgLedger: per-msgLedger Data Entry + Checkpoint
 *       Marker.</li>
 * </ul>
 */
public interface AckPersistence {

    CompletableFuture<PersistResult> persist(
            LedgerHandle lh,
            Position mdPos,
            Map<String, Long> properties,
            BitmapAckState ackState);

    CompletableFuture<RecoveredState> recover(LedgerHandle lh);

    default void onLedgerRollover() { }

    default void setZkCmHint(long cursorLedgerId, long entryId) { }

    /**
     * Called when the mark-delete position advances past {@code mdLedgerId}.
     * Implementations that track per-ledger state (e.g. DE+CM) should clean up stale entries.
     */
    default void onMarkDeleteAdvance(long mdLedgerId) { }

    /**
     * Returns {@code true} if old cursor ledgers should be deleted immediately on rollover
     * (legacy path). Returns {@code false} when old ledgers must be kept (DE+CM cross-ledger refs).
     */
    default boolean shouldGcOldCursorLedgerOnRollover() { return true; }

    /**
     * Returns the minimum cursor ledger ID still referenced by this persistence instance,
     * or {@code -1} if there are no tracked references.
     */
    default long getOldestReferencedCursorLedgerId() { return -1; }

    /**
     * Returns the serialized byte size of the last persisted unacked ranges (monitoring metric).
     */
    default int getSerializedSize() { return 0; }

    /**
     * Builds the legacy {@link MessageRange} list for MetaStore (ZK close / fallback writes).
     * Truncates to {@code maxRanges} and emits truncation telemetry.
     * Returns an empty list for persistence strategies that do not use ZK ranges.
     */
    default List<MessageRange> buildIndividualDeletedMessageRanges(BitmapAckState ackState, int maxRanges) {
        return Collections.emptyList();
    }

    /**
     * Builds the {@link BatchedEntryDeletionIndexInfo} list for MetaStore writes.
     * Truncates to {@code maxIndexes} and emits truncation telemetry.
     * Returns an empty list for persistence strategies that do not use ZK batch ranges.
     */
    default List<BatchedEntryDeletionIndexInfo> buildBatchEntryDeletionIndexInfoList(
            BitmapAckState ackState, int maxIndexes) {
        return Collections.emptyList();
    }

    final class PersistResult {
        private final long totalBytes;
        private final long commitEntryId;

        public PersistResult(long totalBytes, long commitEntryId) {
            this.totalBytes = totalBytes;
            this.commitEntryId = commitEntryId;
        }

        public long totalBytes() { return totalBytes; }
        public long commitEntryId() { return commitEntryId; }
    }

    final class RecoveredState {
        private final PositionInfo positionInfo;
        private final long commitEntryId;
        private final long stateSize;

        public RecoveredState(PositionInfo positionInfo, long commitEntryId, long stateSize) {
            this.positionInfo = positionInfo;
            this.commitEntryId = commitEntryId;
            this.stateSize = stateSize;
        }

        public PositionInfo positionInfo() { return positionInfo; }
        public long commitEntryId() { return commitEntryId; }
        public long stateSize() { return stateSize; }
    }
}
