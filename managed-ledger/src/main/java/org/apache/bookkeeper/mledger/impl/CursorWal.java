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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.CursorStateChunk;
import org.apache.bookkeeper.mledger.proto.LongListMap;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Append-only WAL over a BookKeeper cursor ledger. Transparently splits large
 * {@link PositionInfo} across multiple entries via {@link CursorStateChunk} envelopes.
 * Not thread-safe — callers must hold the cursor lock.
 */
class CursorWal {

    /** Scan-back cap: defensive upper bound on how many entries we walk looking for a commit. */
    private static final long MAX_SCAN_BACK = 10_000L;

    /** Batch size for scan-back reads. Each batch costs one BK read RTT instead of one per entry. */
    private static final int SCAN_BACK_BATCH = 100;

    // entry-type discriminator stored in PositionInfo.properties (LongProperty named "entry-type").
    static final String ENTRY_TYPE_PROPERTY = "entry-type";
    static final long ENTRY_TYPE_DE = 1;
    static final long ENTRY_TYPE_CM = 2;

    private final int maxEntrySize;
    // BookKeeper client + password used to open historical cursor ledgers when a CM references
    // DE entries that live in a previous (rolled-over) cursor ledger. Null when only same-ledger
    // recovery is needed (legacy P1 sharded-PositionInfo path).
    private final BookKeeper bookKeeper;
    private final byte[] password;

    CursorWal(int maxEntrySize) {
        this(maxEntrySize, null, null);
    }

    CursorWal(int maxEntrySize, BookKeeper bookKeeper, byte[] password) {
        if (maxEntrySize < 1024) {
            throw new IllegalArgumentException("maxEntrySize must be at least 1024 bytes");
        }
        this.maxEntrySize = maxEntrySize;
        this.bookKeeper = bookKeeper;
        this.password = password;
    }

    // ============================ append ============================

    /**
     * Appends {@code pi} to {@code lh}, transparently splitting into chunks when its serialized
     * size exceeds {@code maxEntrySize}.
     *
     * @return a future completed with the append result once the commit entry (or the single
     *         entry) is BK-acked
     */
    CompletableFuture<AppendResult> append(LedgerHandle lh, PositionInfo pi) {
        final byte[] data = pi.toByteArray();
        if (data.length <= maxEntrySize) {
            return addEntry(lh, data).thenApply(entryId -> new AppendResult(data.length, entryId));
        }
        return appendChunked(lh, pi, data);
    }

    private CompletableFuture<AppendResult> appendChunked(LedgerHandle lh, PositionInfo pi,
                                                          byte[] data) {
        final int partCount = (data.length + maxEntrySize - 1) / maxEntrySize;
        // PositionInfo.ledgerId and entryId are proto-required; copy them onto every wrapper
        // so toByteArray() passes required-field checks. The recovered PositionInfo is parsed
        // from reassembled bytes, so these wrapper values are envelope-only.
        final long wrapperLedgerId = pi.getLedgerId();
        final long wrapperEntryId = pi.getEntryId();

        // Submit all part chunks; collect their futures. BK preserves append order within a
        // ledger, so the commit chunk (submitted next) is durable only after all parts.
        int offset = 0;
        List<CompletableFuture<Long>> partFutures = new ArrayList<>(partCount);
        for (int partIndex = 0; partIndex < partCount; partIndex++) {
            int length = Math.min(maxEntrySize, data.length - offset);
            byte[] payload = new byte[length];
            System.arraycopy(data, offset, payload, 0, length);
            offset += length;

            PositionInfo wrapper = new PositionInfo()
                    .setLedgerId(wrapperLedgerId)
                    .setEntryId(wrapperEntryId);
            wrapper.setCursorStateChunk()
                    .setPartIndex(partIndex)
                    .setPartCount(partCount)
                    .setPayload(payload);
            partFutures.add(addEntry(lh, wrapper.toByteArray()));
        }

        PositionInfo commitWrapper = new PositionInfo()
                .setLedgerId(wrapperLedgerId)
                .setEntryId(wrapperEntryId);
        commitWrapper.setCursorStateChunk()
                .setPartIndex(partCount)
                .setPartCount(partCount);
        CompletableFuture<Long> commitFuture = addEntry(lh, commitWrapper.toByteArray());

        // Chain allOf(partFutures) before commitFuture so any part-level exception propagates
        // instead of being silently swallowed. BK order guarantees that commitFuture cannot
        // succeed while a partFuture failed; the explicit chain makes the failure visible to
        // the caller instead of relying on that invariant.
        long totalBytes = data.length;
        CompletableFuture<Void> allParts = CompletableFuture.allOf(
                partFutures.toArray(new CompletableFuture[0]));
        return allParts.thenCompose(ignored -> commitFuture)
                .thenApply(entryId -> new AppendResult(totalBytes, entryId));
    }

    // ============================ recover / readLatest ============================

    /**
     * Reads the most recent committed PositionInfo from {@code lh}.
     *
     * <p>Equivalent to {@link #readLatest(LedgerHandle)}; provided as an instance method so
     * callers can keep a reference to the configured WAL.
     */
    CompletableFuture<RecoveredState> recover(LedgerHandle lh) {
        return readLatest(lh);
    }

    /**
     * Static recovery entry point. Reads the last entry, detects single vs chunked form,
     * and (for chunked form) reads backwards to assemble.
     */
    static CompletableFuture<RecoveredState> readLatest(LedgerHandle lh) {
        long lastAddConfirmed = lh.getLastAddConfirmed();
        if (lastAddConfirmed < 0) {
            return FutureUtil.failedFuture(
                    new ManagedLedgerException("Cursor ledger has no entries"));
        }
        return readEntry(lh, lastAddConfirmed).thenCompose(lastBytes -> {
            PositionInfo lastPi = parsePositionInfo(lastBytes);
            if (!lastPi.hasCursorStateChunk()) {
                return CompletableFuture.completedFuture(
                        new RecoveredState(lastPi, lastAddConfirmed, lastBytes.length));
            }
            CursorStateChunk chunk = lastPi.getCursorStateChunk();
            if (chunk.getPartIndex() == chunk.getPartCount()) {
                return assembleCommitted(lh, lastAddConfirmed, chunk);
            }
            // Partial write: broker crashed before the commit chunk was acked. Scan back for
            // the previous commit chunk and replay from there.
            return scanBackForCommit(lh, lastAddConfirmed - 1);
        });
    }

    private static CompletableFuture<RecoveredState> assembleCommitted(LedgerHandle lh,
                                                                       long commitEntryId,
                                                                       CursorStateChunk commit) {
        int partCount = commit.getPartCount();
        if (partCount <= 0) {
            return FutureUtil.failedFuture(
                    new ManagedLedgerException("Invalid partCount: " + partCount));
        }
        if (commitEntryId - partCount < -1) {
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "Cursor ledger too short for chunk group: commitEntryId=" + commitEntryId
                            + ", partCount=" + partCount));
        }
        // Part chunks live at entryIds [commitEntryId - partCount, commitEntryId - 1].
        // BK preserves append order, so the read result is already in partIndex order;
        // we verify continuity explicitly to defend against malformed input.
        long startEntryId = commitEntryId - partCount;
        long endEntryId = commitEntryId - 1;
        return readEntryRange(lh, startEntryId, endEntryId).thenApply(parts -> {
            int totalLength = 0;
            for (int i = 0; i < parts.size(); i++) {
                PositionInfo pi = parsePositionInfo(parts.get(i));
                if (!pi.hasCursorStateChunk()) {
                    throw new IllegalStateException(
                            "Expected CursorStateChunk at part " + i);
                }
                CursorStateChunk part = pi.getCursorStateChunk();
                if (part.getPartIndex() != i) {
                    throw new IllegalStateException(
                            "Part index continuity broken at position " + i
                                    + ": expected " + i + ", got " + part.getPartIndex());
                }
                if (part.getPartCount() != partCount) {
                    throw new IllegalStateException(
                            "Part count mismatch at part " + i
                                    + ": expected " + partCount + ", got " + part.getPartCount());
                }
                totalLength += part.getPayload().length;
            }

            byte[] assembled = new byte[totalLength];
            int writeOffset = 0;
            for (byte[] partBytes : parts) {
                PositionInfo pi = parsePositionInfo(partBytes);
                byte[] payload = pi.getCursorStateChunk().getPayload();
                System.arraycopy(payload, 0, assembled, writeOffset, payload.length);
                writeOffset += payload.length;
            }

            // parseFrom is the soft integrity check: mis-assembled bytes will almost
            // always fail to parse as a valid PositionInfo.
            PositionInfo recovered = parsePositionInfo(assembled);
            return new RecoveredState(recovered, commitEntryId, assembled.length);
        });
    }

    private static CompletableFuture<RecoveredState> scanBackForCommit(LedgerHandle lh,
                                                                       long fromEntryId) {
        long lac = lh.getLastAddConfirmed();
        long lowerBound = Math.max(0, lac - MAX_SCAN_BACK);
        return scanBackBatched(lh, fromEntryId, lowerBound);
    }

    /**
     * Batched reverse scan for the most recent committed state. Each round reads up to
     * {@link #SCAN_BACK_BATCH} entries in one BK call and scans them in-memory, keeping
     * per-entry RTT bounded regardless of orphan tail length.
     */
    private static CompletableFuture<RecoveredState> scanBackBatched(LedgerHandle lh,
                                                                     long entryId,
                                                                     long lowerBound) {
        if (entryId < 0 || entryId < lowerBound) {
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "No commit chunk found while scanning cursor ledger backwards"));
        }
        long batchEnd = entryId;
        long batchStart = Math.max(lowerBound, Math.max(0, entryId - SCAN_BACK_BATCH + 1));
        return readEntryRange(lh, batchStart, batchEnd).thenCompose(parts -> {
            // Iterate the batch in reverse so we return the most recent committed state.
            for (int i = parts.size() - 1; i >= 0; i--) {
                byte[] bytes = parts.get(i);
                long currentEntryId = batchStart + i;
                PositionInfo pi = parsePositionInfo(bytes);
                if (!pi.hasCursorStateChunk()) {
                    // Single-entry PositionInfo is itself a valid committed state.
                    return CompletableFuture.completedFuture(
                            new RecoveredState(pi, currentEntryId, bytes.length));
                }
                CursorStateChunk csc = pi.getCursorStateChunk();
                if (csc.getPartIndex() == csc.getPartCount()) {
                    return assembleCommitted(lh, currentEntryId, csc);
                }
                // Orphan part chunk, keep going.
            }
            // Batch exhausted without a hit. Continue with the previous batch.
            return scanBackBatched(lh, batchStart - 1, lowerBound);
        });
    }

    // ============================ DE+CM recovery (PIP-488 P2) ============================

    /**
     * Reads the last entry of the cursor ledger and recovers DE+CM state. Since every
     * flush writes DE × N + CM, the last entry is always a CM. This method reads the CM,
     * parallel-fetches all referenced DEs (cross-ledger supported), and merges them into
     * a single synthetic PositionInfo.
     */
    CompletableFuture<RecoveredState> readLatestDeCmInstance(LedgerHandle lh) {
        long lastAddConfirmed = lh.getLastAddConfirmed();
        if (lastAddConfirmed < 0) {
            return FutureUtil.failedFuture(
                    new ManagedLedgerException("Cursor ledger has no entries"));
        }
        return readEntry(lh, lastAddConfirmed).thenCompose(lastBytes -> {
            PositionInfo lastPi = parsePositionInfo(lastBytes);
            // If the last entry is a chunked CM commit chunk, assemble it first.
            if (lastPi.hasCursorStateChunk()
                    && lastPi.getCursorStateChunk().getPartIndex()
                            == lastPi.getCursorStateChunk().getPartCount()) {
                return assembleCommitted(lh, lastAddConfirmed, lastPi.getCursorStateChunk())
                        .thenCompose(assembled ->
                                recoverFromCmFastPath(lh, lastAddConfirmed, assembled.positionInfo()));
            }
            return recoverFromCmFastPath(lh, lastAddConfirmed, lastPi);
        });
    }

    /**
     * CM fast path: parallel-fetch every DE referenced by the CM index and reassemble a
     * synthetic PositionInfo (mdPos from CM + merged bitmaps from each DE). Both single-entry
     * and chunked DE entries are handled via {@link #readEntryOrChunk}. Cross-ledger DE
     * references (cursor-ledger rollover case) are supported when BookKeeper is configured.
     */
    private CompletableFuture<RecoveredState> recoverFromCmFastPath(LedgerHandle lh,
                                                                    long cmEntryId,
                                                                    PositionInfo cm) {
        int indexSize = cm.getIndividualDeletedMessageRangesCount();
        if (indexSize == 0) {
            return CompletableFuture.completedFuture(new RecoveredState(cm, cmEntryId, 0));
        }
        List<CompletableFuture<PositionInfo>> deFutures = new ArrayList<>(indexSize);
        for (int i = 0; i < indexSize; i++) {
            LongListMap ref = cm.getIndividualDeletedMessageRangeAt(i);
            long cursorLedgerId = ref.getValueAt(0);
            long entryId = ref.getValueAt(1);
            deFutures.add(readEntryOrChunk(lh, cursorLedgerId, entryId));
        }
        return CompletableFuture.allOf(deFutures.toArray(new CompletableFuture[0]))
                .thenApply(ignored -> {
                    PositionInfo merged = new PositionInfo()
                            .setLedgerId(cm.getLedgerId())
                            .setEntryId(cm.getEntryId());
                    long totalBytes = 0;
                    for (CompletableFuture<PositionInfo> f : deFutures) {
                        PositionInfo de = f.join();
                        if (de.getIndividualDeletedMessageRangesCount() > 0) {
                            LongListMap deRange = de.getIndividualDeletedMessageRangeAt(0);
                            LongListMap copy = merged.addIndividualDeletedMessageRange();
                            copy.setKey(deRange.getKey());
                            if (deRange.hasBitmap()) {
                                copy.setBitmap(deRange.getBitmap());
                            }
                        }
                        if (de.getBatchedEntryDeletionIndexInfosCount() > 0) {
                            merged.addAllBatchedEntryDeletionIndexInfos(de.getBatchedEntryDeletionIndexInfosList());
                        }
                        totalBytes += de.getIndividualDeletedMessageRangesCount();
                    }
                    return new RecoveredState(merged, cmEntryId, totalBytes);
                });
    }

    /**
     * Reads one DE from {@code (cursorLedgerId, entryId)}, transparently handling both
     * single-entry and chunked DE entries. Cross-ledger references (cursor-ledger rollover
     * case) are supported when {@link #bookKeeper} was supplied at construction.
     */
    private CompletableFuture<PositionInfo> readEntryOrChunk(LedgerHandle lh,
                                                             long cursorLedgerId,
                                                             long entryId) {
        if (cursorLedgerId == lh.getId()) {
            return readEntryOrChunkSameLedger(lh, entryId);
        }
        if (bookKeeper == null || password == null) {
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "Cross-ledger DE reference but BookKeeper not configured: cursorLedgerId="
                            + cursorLedgerId + ", lh.id=" + lh.getId()));
        }
        // Open the historical cursor ledger read-only, fetch the DE, then close.
        CompletableFuture<LedgerHandle> openFuture = new CompletableFuture<>();
        bookKeeper.asyncOpenLedgerNoRecovery(cursorLedgerId, DigestType.CRC32C, password,
                (rc, handle, ctx) -> {
                    if (rc == BKException.Code.OK) {
                        openFuture.complete(handle);
                    } else {
                        openFuture.completeExceptionally(BKException.create(rc));
                    }
                }, null);
        return openFuture.thenCompose(historicalLh ->
                readEntryOrChunkSameLedger(historicalLh, entryId).whenComplete((pi, err) -> {
                    try {
                        historicalLh.close();
                    } catch (Exception e) {
                        // Best-effort close; the read result is what we care about.
                    }
                }));
    }

    private static CompletableFuture<PositionInfo> readEntryOrChunkSameLedger(LedgerHandle lh,
                                                                              long entryId) {
        return readEntry(lh, entryId).thenCompose(bytes -> {
            PositionInfo pi = parsePositionInfo(bytes);
            if (!pi.hasCursorStateChunk()) {
                return CompletableFuture.completedFuture(pi);
            }
            CursorStateChunk chunk = pi.getCursorStateChunk();
            if (chunk.getPartIndex() == chunk.getPartCount()) {
                return assembleCommitted(lh, entryId, chunk).thenApply(RecoveredState::positionInfo);
            }
            return FutureUtil.failedFuture(new ManagedLedgerException(
                    "CM referenced a part chunk (partIndex=" + chunk.getPartIndex()
                            + "), expected commit chunk"));
        });
    }

    // ---- BK helpers ----

    private static CompletableFuture<Long> addEntry(LedgerHandle lh, byte[] data) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        lh.asyncAddEntry(data, (rc, handle, entryId, ctx) -> {
            if (rc == BKException.Code.OK) {
                future.complete(entryId);
            } else {
                future.completeExceptionally(BKException.create(rc));
            }
        }, null);
        return future;
    }

    private static CompletableFuture<byte[]> readEntry(LedgerHandle lh, long entryId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        lh.asyncReadEntries(entryId, entryId, (rc, handle, seq, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(BKException.create(rc));
                return;
            }
            if (!seq.hasMoreElements()) {
                future.completeExceptionally(
                        new ManagedLedgerException("Empty read result for entry " + entryId));
                return;
            }
            LedgerEntry entry = seq.nextElement();
            future.complete(entry.getEntry());
        }, null);
        return future;
    }

    private static CompletableFuture<List<byte[]>> readEntryRange(LedgerHandle lh,
                                                                  long startEntryId,
                                                                  long endEntryId) {
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        lh.asyncReadEntries(startEntryId, endEntryId, (rc, handle, seq, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(BKException.create(rc));
                return;
            }
            List<byte[]> buffers = new ArrayList<>();
            long expected = endEntryId - startEntryId + 1;
            while (seq.hasMoreElements()) {
                buffers.add(seq.nextElement().getEntry());
            }
            if (buffers.size() != expected) {
                future.completeExceptionally(new ManagedLedgerException(
                        "Short read: expected " + expected + ", got " + buffers.size()));
                return;
            }
            future.complete(buffers);
        }, null);
        return future;
    }

    private static PositionInfo parsePositionInfo(byte[] data) {
        PositionInfo pi = new PositionInfo();
        pi.parseFrom(data);
        return pi;
    }

    // ============================ DTOs ============================

    /** Result of an append: total bytes the logical PositionInfo occupied (pre-chunking). */
    static final class AppendResult {
        private final long totalBytes;
        private final long commitEntryId;

        AppendResult(long totalBytes, long commitEntryId) {
            this.totalBytes = totalBytes;
            this.commitEntryId = commitEntryId;
        }

        public long totalBytes() {
            return totalBytes;
        }

        public long commitEntryId() {
            return commitEntryId;
        }
    }

    /** Recovered cursor state: the parsed PositionInfo plus provenance for metrics. */
    static final class RecoveredState {
        private final PositionInfo positionInfo;
        private final long commitEntryId;
        private final long stateSize;

        RecoveredState(PositionInfo positionInfo, long commitEntryId, long stateSize) {
            this.positionInfo = positionInfo;
            this.commitEntryId = commitEntryId;
            this.stateSize = stateSize;
        }

        public PositionInfo positionInfo() {
            return positionInfo;
        }

        public long commitEntryId() {
            return commitEntryId;
        }

        public long stateSize() {
            return stateSize;
        }
    }
}
