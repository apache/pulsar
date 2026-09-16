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
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Mock read-only view of a ledger, the counterpart of {@link ReadOnlyLedgerHandle}.
 *
 * <p>It is a {@link LedgerHandle} sharing the entries of the {@link PulsarMockLedgerHandle} that wrote them, so that
 * what {@link PulsarMockBookKeeper#newOpenLedgerOp()} returns can be cast to {@link LedgerHandle} like the handle
 * returned by the real client. Closing it never affects the ledger.
 */
@CustomLog
class PulsarMockReadHandle extends LedgerHandle {
    private final PulsarMockBookKeeper bk;
    private final List<LedgerEntryImpl> entries;
    private final Supplier<PulsarMockReadHandleInterceptor> readHandleInterceptorSupplier;
    private final AtomicLong totalLengthCounter;

    PulsarMockReadHandle(PulsarMockBookKeeper bk, long ledgerId, LedgerMetadata metadata, DigestType digestType,
                         byte[] password, List<LedgerEntryImpl> entries,
                         Supplier<PulsarMockReadHandleInterceptor> readHandleInterceptorSupplier,
                         AtomicLong totalLengthCounter) throws GeneralSecurityException {
        super(bk.getClientCtx(), ledgerId, new Versioned<>(metadata, new LongVersion(0L)), digestType, password,
                WriteFlag.NONE);
        this.bk = bk;
        this.entries = entries;
        this.readHandleInterceptorSupplier = readHandleInterceptorSupplier;
        this.totalLengthCounter = totalLengthCounter;
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        return bk.getProgrammedFailure().thenComposeAsync((res) -> {
            log.debug().attr("first", firstEntry).attr("last", lastEntry)
                    .attr("total", entries.size()).log("readEntries");
            List<LedgerEntry> seq = new ArrayList<>();
            long entryId = firstEntry;
            while (entryId <= lastEntry && entryId < entries.size()) {
                seq.add(entries.get((int) entryId++).duplicate());
            }
            log.debug().attr("entries", seq).log("Entries read");
            LedgerEntriesImpl ledgerEntries = LedgerEntriesImpl.create(seq);
            PulsarMockReadHandleInterceptor pulsarMockReadHandleInterceptor = readHandleInterceptorSupplier.get();
            if (pulsarMockReadHandleInterceptor != null) {
                return pulsarMockReadHandleInterceptor.interceptReadAsync(ledgerId, firstEntry, lastEntry,
                        ledgerEntries);
            }
            return FutureUtils.value(ledgerEntries);
        });
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<LedgerEntries> batchReadAsync(long firstEntry, int maxCount, long maxSize) {
        return readAsync(firstEntry, batchReadLastEntry(entries, firstEntry, maxCount, maxSize));
    }

    @Override
    public CompletableFuture<LedgerEntries> batchReadUnconfirmedAsync(long firstEntry, int maxCount, long maxSize) {
        return readUnconfirmedAsync(firstEntry, batchReadLastEntry(entries, firstEntry, maxCount, maxSize));
    }

    /**
     * Resolves the last entry of a batch read the way a bookie bounds it: at most {@code maxCount} entries, at most
     * {@code maxSize} bytes (a non-positive size is unlimited), always at least the first entry, and only entries that
     * exist. Batch reads then go through the handle's own {@code readAsync} / {@code readUnconfirmedAsync}, so that
     * stubs installed on a Mockito spy keep intercepting them.
     */
    static long batchReadLastEntry(List<LedgerEntryImpl> entries, long firstEntry, int maxCount, long maxSize) {
        long lastEntryByCount = Math.min(firstEntry + maxCount - 1, entries.size() - 1);
        long accumulatedSize = 0;
        long lastEntry = firstEntry;
        for (long eid = firstEntry; eid <= lastEntryByCount; eid++) {
            long entrySize = entries.get((int) eid).getLength();
            if (maxSize > 0 && eid > firstEntry && accumulatedSize + entrySize > maxSize) {
                break;
            }
            accumulatedSize += entrySize;
            lastEntry = eid;
        }
        return lastEntry;
    }

    @Override
    public void asyncReadEntries(long firstEntry, long lastEntry, ReadCallback cb, Object ctx) {
        PulsarMockLedgerHandle.asyncReadEntries(bk, entries, this, firstEntry, lastEntry, cb, ctx);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return readLastAddConfirmedAsync();
    }

    @Override
    public long getLastAddConfirmed() {
        // Honor the programmed "empty ledger" steps exactly like the write handle does, since tests that force a
        // cursor recovery from an empty ledger see this view once opens go through the builder.
        if (bk.checkReturnEmptyLedger() || entries.isEmpty()) {
            return -1;
        } else {
            return entries.get(entries.size() - 1).getEntryId();
        }
    }

    @Override
    public long getLength() {
        return totalLengthCounter.get();
    }

    @Override
    public boolean isClosed() {
        return getLedgerMetadata().isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        CompletableFuture<LastConfirmedAndEntry> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException("Long poll not implemented"));
        return promise;
    }

    // Like ReadOnlyLedgerHandle: a read-only view rejects writes instead of reaching the write path

    @Override
    public long addEntry(byte[] data) throws InterruptedException, BKException {
        return addEntry(data, 0, data.length);
    }

    @Override
    public long addEntry(byte[] data, int offset, int length) throws InterruptedException, BKException {
        throw BKException.create(BKException.Code.IllegalOpException);
    }

    @Override
    public void asyncAddEntry(byte[] data, AddCallback cb, Object ctx) {
        asyncAddEntry(data, 0, data.length, cb, ctx);
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AddCallback cb, Object ctx) {
        cb.addComplete(BKException.Code.IllegalOpException, this, INVALID_ENTRY_ID, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf data, AddCallback cb, Object ctx) {
        cb.addComplete(BKException.Code.IllegalOpException, this, INVALID_ENTRY_ID, ctx);
    }

    @Override
    public void asyncClose(CloseCallback cb, Object ctx) {
        // Like ReadOnlyLedgerHandle: closing the view does not touch the ledger.
        cb.closeComplete(BKException.Code.OK, this, ctx);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }
}
