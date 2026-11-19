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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ResetCursorCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ManagedCursorResetDeleteTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 30000)
    public void testDeletingCursorSettlesQueuedResetAndCursorClose() throws Exception {
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("delete-cursor-with-queued-reset",
                defaultConfig().setThrottleMarkDelete(0));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        cursor.setInactive();
        Position first = ledger.addEntry(new byte[] {0});
        Position ackPosition = ledger.addEntry(new byte[] {1});
        Position resetPosition = ledger.addEntry(new byte[] {2});
        cursor.markDelete(first);

        CompletableFuture<Void> readStarted = new CompletableFuture<>();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        CompletableFuture<Void> readComplete = new CompletableFuture<>();
        CompletableFuture<Void> markDelete = new CompletableFuture<>();
        CompletableFuture<Void> reset = new CompletableFuture<>();
        AtomicInteger markDeleteCompletions = new AtomicInteger();
        AtomicInteger resetCompletions = new AtomicInteger();
        CloseFuture close = new CloseFuture();
        bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
            if (ledgerId == first.getLedgerId()) {
                readStarted.complete(null);
                return releaseRead.thenApply(ignored -> entries);
            }
            return CompletableFuture.completedFuture(entries);
        });
        try {
            cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entries.forEach(Entry::release);
                    readComplete.complete(null);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    readComplete.complete(null);
                }
            }, null, PositionFactory.LATEST);
            readStarted.get(5, TimeUnit.SECONDS);
            cursor.asyncMarkDelete(ackPosition, new MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    markDeleteCompletions.incrementAndGet();
                    markDelete.complete(null);
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    markDeleteCompletions.incrementAndGet();
                    markDelete.completeExceptionally(exception);
                }
            }, null);
            cursor.asyncResetCursor(resetPosition, false, new ResetCursorCallback() {
                @Override
                public void resetComplete(Object ctx) {
                    resetCompletions.incrementAndGet();
                    reset.complete(null);
                }

                @Override
                public void resetFailed(ManagedLedgerException exception, Object ctx) {
                    resetCompletions.incrementAndGet();
                    reset.completeExceptionally(exception);
                }
            });
            awaitExecutor(ledger);
            cursor.asyncClose(close, null);
            awaitExecutor(ledger);
            assertThat(ledger.getState()).isEqualTo(ManagedLedgerImpl.State.LedgerOpened);
            assertThat(reset).isNotDone();
            assertThat(close).isNotDone();
            assertThat(markDelete).isNotDone();

            ledger.deleteCursor(cursor.getName());

            assertThatThrownBy(() -> markDelete.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(CursorAlreadyClosedException.class);
            assertThatThrownBy(() -> reset.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(InvalidCursorPositionException.class);
            close.get(5, TimeUnit.SECONDS);
            assertThat(readComplete).as("deletion must settle the queued operations while the read is held")
                    .isNotDone();

            releaseRead.complete(null);
            readComplete.get(5, TimeUnit.SECONDS);
            awaitExecutor(ledger);
            assertThat(markDeleteCompletions).hasValue(1);
            assertThat(resetCompletions).hasValue(1);
        } finally {
            bkc.setReadHandleInterceptor(null);
            releaseRead.complete(null);
        }
    }

    private static void awaitExecutor(ManagedLedgerImpl ledger) throws Exception {
        CompletableFuture<Void> fence = new CompletableFuture<>();
        ledger.getExecutor().execute(() -> fence.complete(null));
        fence.get(5, TimeUnit.SECONDS);
    }
}
