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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ResetCursorCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ConcurrentFindCursorPositionException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedCursorResetAcknowledgmentTest extends MockedBookKeeperTestCase {

    @DataProvider
    public Object[][] pendingRead() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "pendingRead", timeOut = 30000)
    public void individualAckDuringDurableReset(boolean pendingRead) throws Exception {
        ManagedLedger ledger = factory.open("reset-ack-race",
                defaultConfig().setPersistIndividualAckAsLongArray(false));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        cursor.setInactive(); // Send the read to BookKeeper instead of serving it from cache.
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        cursor.markDelete(positions.get(4));
        Position resetMarkDelete = positions.get(1);
        Position ackPosition = positions.get(6);
        long lastEntryBeforeReset = cursor.getCursorLedgerLastEntry();

        CompletableFuture<Void> releaseReset = bkc.addEntryResponseGate();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        AtomicInteger ackCallbacks = new AtomicInteger();
        try {
            CompletableFuture<Void> reset = resetCursor(cursor, positions.get(2));
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(cursor.getCursorLedgerLastEntry()).isEqualTo(lastEntryBeforeReset + 1));
            assertThat(reset).isNotDone();

            CompletableFuture<Void> read = CompletableFuture.completedFuture(null);
            if (pendingRead) {
                CompletableFuture<Void> readStarted = new CompletableFuture<>();
                bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
                    readStarted.complete(null);
                    return releaseRead.thenApply(__ -> entries);
                });
                read = ManagedLedgerUtils.readEntries(cursor, 1, PositionFactory.LATEST)
                        .thenAccept(entries -> entries.forEach(Entry::release));
                readStarted.get(5, TimeUnit.SECONDS);
                assertThat(cursor.getPendingReadOpsCount()).isEqualTo(1);
            }

            CompletableFuture<ManagedLedgerException> ack = new CompletableFuture<>();
            cursor.asyncDelete(ackPosition, new DeleteCallback() {
                @Override
                public void deleteComplete(Object ctx) {
                    ackCallbacks.incrementAndGet();
                    ack.complete(null);
                }

                @Override
                public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                    ackCallbacks.incrementAndGet();
                    ack.complete(exception);
                }
            }, null);

            releaseReset.complete(null);
            reset.get(5, TimeUnit.SECONDS);
            releaseRead.complete(null);
            read.get(5, TimeUnit.SECONDS);
            ManagedLedgerException ackResult = ack.get(5, TimeUnit.SECONDS);

            // Check the record recovery would read before another write (including close) can replace it.
            PositionInfo checkpoint = readCursorCheckpoint(cursor);
            assertThat(PositionFactory.create(checkpoint.getLedgerId(), checkpoint.getEntryId()))
                    .isEqualTo(resetMarkDelete);
            assertThat(checkpoint.getIndividualDeletedMessagesCount()).isZero();
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(resetMarkDelete);
            assertThat(cursor.isMessageDeleted(ackPosition)).isFalse();
            assertThat(ackResult).isInstanceOf(ConcurrentFindCursorPositionException.class);

            cursor.delete(ackPosition);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(resetMarkDelete);
            assertThat(cursor.isMessageDeleted(ackPosition)).isTrue();
            assertThat(ackCallbacks).hasValue(1);
            PositionInfo retried = readCursorCheckpoint(cursor);
            assertThat(PositionFactory.create(retried.getLedgerId(), retried.getEntryId()))
                    .isEqualTo(resetMarkDelete);
            assertThat(retried.getIndividualDeletedMessagesCount()).isEqualTo(1);
            MessageRange ackRange = retried.getIndividualDeletedMessageAt(0);
            assertThat(PositionFactory.create(ackRange.getUpperEndpoint().getLedgerId(),
                    ackRange.getUpperEndpoint().getEntryId())).isEqualTo(ackPosition);
        } finally {
            releaseReset.complete(null);
            releaseRead.complete(null);
            bkc.setReadHandleInterceptor(null);
        }
    }

    private static CompletableFuture<Void> resetCursor(ManagedCursorImpl cursor, Position position) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        cursor.asyncResetCursor(position, false, new ResetCursorCallback() {
            @Override
            public void resetComplete(Object ctx) {
                result.complete(null);
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        });
        return result;
    }

    private static PositionInfo readCursorCheckpoint(ManagedCursorImpl cursor) throws Exception {
        CompletableFuture<PositionInfo> result = new CompletableFuture<>();
        long entryId = cursor.getCursorLedgerLastEntry();
        cursor.cursorLedger.asyncReadEntries(entryId, entryId, (rc, lh, entries, ctx) -> {
            if (rc != BKException.Code.OK) {
                result.completeExceptionally(BKException.create(rc));
                return;
            }
            try {
                PositionInfo checkpoint = new PositionInfo();
                checkpoint.parseFrom(entries.nextElement().getEntry());
                result.complete(checkpoint);
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
        }, null);
        return result.get(5, TimeUnit.SECONDS);
    }
}
