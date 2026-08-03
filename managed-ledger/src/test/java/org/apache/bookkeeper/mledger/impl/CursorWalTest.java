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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.proto.CursorStateChunk;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link CursorWal} covering the three paths that the end-to-end
 * {@code ManagedCursorTest} cases do not exercise:
 *
 * <ol>
 *   <li>Partial-write recovery (broker crash after N part chunks but before commit chunk) —
 *       exercises {@code scanBackForCommit} / {@code scanBackBatched}.</li>
 *   <li>partIndex continuity broken — exercises the
 *       {@code "Part index continuity broken"} throw branch.</li>
 *   <li>Exact {@code data.length == maxEntrySize} boundary — single entry vs first chunked form.</li>
 * </ol>
 */
public class CursorWalTest extends MockedBookKeeperTestCase {

    private static final byte[] PASSWORD = "test".getBytes();

    private LedgerHandle newLedger() throws BKException, InterruptedException {
        return bkc.createLedger(3, 3, DigestType.CRC32C, PASSWORD);
    }

    private static PositionInfo newPiWithPayload(int desiredSizeBytes) {
        // Pad with LongListMap entries (~12 bytes each) until we reach the desired payload size.
        PositionInfo pi = new PositionInfo().setLedgerId(1).setEntryId(1);
        long key = 0;
        while (pi.toByteArray().length < desiredSizeBytes) {
            pi.addIndividualDeletedMessageRange().setKey(key++).addValue(0xFFFF_FFFFL);
        }
        return pi;
    }

    /**
     * Append a full group, then write a dangling part chunk (no commit) to simulate a broker
     * crash mid-append. {@code readLatest} must skip the orphan and return the first group.
     */
    @Test(timeOut = 20000)
    public void testPartialChunkGroupRecoverFromPreviousCommit() throws Exception {
        CursorWal wal = new CursorWal(1024);

        PositionInfo first = new PositionInfo().setLedgerId(100).setEntryId(50);
        first.addIndividualDeletedMessageRange().setKey(7L).addValue(0xABCDL);

        try (LedgerHandle lh = newLedger()) {
            // Write group A in full.
            wal.append(lh, first).get(5, TimeUnit.SECONDS);

            // Now write an orphan part chunk (partIndex = 0, no commit to follow).
            PositionInfo orphan = new PositionInfo().setLedgerId(100).setEntryId(51);
            orphan.setCursorStateChunk()
                    .setPartIndex(0)
                    .setPartCount(2)
                    .setPayload(new byte[]{1, 2, 3, 4});
            lh.addEntry(orphan.toByteArray());

            CursorWal.RecoveredState state =
                    wal.recover(lh).get(5, TimeUnit.SECONDS);

            // Recovery must have skipped the orphan and replayed group A.
            assertThat(state.positionInfo().hasCursorStateChunk()).isFalse();
            assertThat(state.positionInfo().getLedgerId()).isEqualTo(100);
            assertThat(state.positionInfo().getEntryId()).isEqualTo(50);
            assertThat(state.positionInfo().getIndividualDeletedMessageRangesCount()).isEqualTo(1);
            assertThat(state.positionInfo().getIndividualDeletedMessageRangeAt(0).getKey())
                    .isEqualTo(7L);
        }
    }

    /**
     * Build a chunk group by hand with deliberately broken partIndex continuity at position 1
     * (partIndex jumps 0 → 5 instead of 0 → 1). {@code readLatest} must fail rather than
     * silently assemble corrupted state.
     */
    @Test(timeOut = 20000)
    public void testPartIndexContinuityFailsRecovery() throws Exception {
        try (LedgerHandle lh = newLedger()) {
            // Part 0: valid, partIndex=0, partCount=2.
            PositionInfo part0 = new PositionInfo().setLedgerId(1).setEntryId(1);
            part0.setCursorStateChunk()
                    .setPartIndex(0)
                    .setPartCount(2)
                    .setPayload(new byte[]{1, 2, 3, 4});
            lh.addEntry(part0.toByteArray());

            // Part 1: deliberately broken partIndex (5 instead of 1).
            PositionInfo part1Broken = new PositionInfo().setLedgerId(1).setEntryId(1);
            part1Broken.setCursorStateChunk()
                    .setPartIndex(5)
                    .setPartCount(2)
                    .setPayload(new byte[]{5, 6, 7, 8});
            lh.addEntry(part1Broken.toByteArray());

            // Commit chunk (partIndex == partCount == 2).
            PositionInfo commit = new PositionInfo().setLedgerId(1).setEntryId(1);
            commit.setCursorStateChunk()
                    .setPartIndex(2)
                    .setPartCount(2);
            lh.addEntry(commit.toByteArray());

            assertThatThrownBy(() -> CursorWal.readLatest(lh).get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Part index continuity broken");
        }
    }

    /**
     * At exactly {@code data.length == maxEntrySize} we must take the single-entry path
     * (no {@link CursorStateChunk} envelope). At {@code maxEntrySize == data.length - 1}
     * (when ≥ 1024) we must take the chunked path.
     */
    @Test(timeOut = 20000)
    public void testExactBoundarySingleVsChunked() throws Exception {
        // Construct a PositionInfo whose serialized form is comfortably above 1024 bytes
        // (the minimum maxEntrySize), so we can test both sides of the boundary.
        PositionInfo pi = new PositionInfo().setLedgerId(42).setEntryId(7);
        for (long key = 0; key < 200; key++) {
            pi.addIndividualDeletedMessageRange().setKey(key).addValue(0xFFFF_FFFFL);
        }
        int dataLen = pi.toByteArray().length;
        assertThat(dataLen).isGreaterThan(1024);

        // Case 1: maxEntrySize == dataLen → single entry, no envelope.
        try (LedgerHandle lh1 = newLedger()) {
            CursorWal wal = new CursorWal(dataLen);
            wal.append(lh1, pi).get(5, TimeUnit.SECONDS);

            CursorWal.RecoveredState state =
                    CursorWal.readLatest(lh1).get(5, TimeUnit.SECONDS);
            assertThat(state.positionInfo().hasCursorStateChunk()).isFalse();
            assertThat(state.positionInfo().getIndividualDeletedMessageRangesCount())
                    .isEqualTo(pi.getIndividualDeletedMessageRangesCount());
        }

        // Case 2: maxEntrySize == dataLen - 1 → chunked, multiple entries.
        int chunkedMax = Math.max(1024, dataLen - 1);
        try (LedgerHandle lh2 = newLedger()) {
            CursorWal wal = new CursorWal(chunkedMax);
            wal.append(lh2, pi).get(5, TimeUnit.SECONDS);

            // The very last entry must be a commit chunk.
            long lac = lh2.getLastAddConfirmed();
            assertThat(lac).isGreaterThan(0);

            CursorWal.RecoveredState state =
                    CursorWal.readLatest(lh2).get(5, TimeUnit.SECONDS);
            // Recovered PositionInfo comes from reassembled bytes, so it is not the wrapper.
            assertThat(state.positionInfo().hasCursorStateChunk()).isFalse();
            assertThat(state.positionInfo().getIndividualDeletedMessageRangesCount())
                    .isEqualTo(pi.getIndividualDeletedMessageRangesCount());
            // Sanity: last entry on the ledger IS the commit chunk (partIndex == partCount).
            try (LedgerHandle read = bkc.openLedgerNoRecovery(
                    lh2.getId(), DigestType.CRC32C, PASSWORD)) {
                PositionInfo lastPi = new PositionInfo();
                lastPi.parseFrom(read.readEntries(lac, lac).nextElement().getEntry());
                assertThat(lastPi.hasCursorStateChunk()).isTrue();
                CursorStateChunk csc = lastPi.getCursorStateChunk();
                assertThat(csc.getPartIndex()).isEqualTo(csc.getPartCount());
            }
        }
    }
}
