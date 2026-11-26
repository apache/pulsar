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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;

/**
 * Mock read-only view of a ledger, the counterpart of {@link ReadOnlyLedgerHandle}.
 *
 * <p>{@link PulsarMockBookKeeper#asyncOpenLedger} returns it instead of the {@link PulsarMockLedgerHandle} that wrote
 * the ledger, so that closing an opened handle, as the real client allows, releases only the view and never closes or
 * fences the ledger for its writer, nor consumes a programmed failure.
 */
class PulsarMockReadOnlyLedgerHandle extends LedgerHandle {
    private final PulsarMockLedgerHandle ledger;

    PulsarMockReadOnlyLedgerHandle(PulsarMockLedgerHandle ledger) throws GeneralSecurityException {
        super(ledger.bk.getClientCtx(), ledger.getId(), ledger.getVersionedLedgerMetadata(), ledger.digest,
                ledger.passwd, WriteFlag.NONE);
        this.ledger = ledger;
    }

    @Override
    public long getLastAddConfirmed() {
        return ledger.getLastAddConfirmed();
    }

    @Override
    public long getLength() {
        return ledger.getLength();
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return ledger.getLedgerMetadata();
    }

    @Override
    public Map<String, byte[]> getCustomMetadata() {
        return ledger.getCustomMetadata();
    }

    @Override
    public boolean isClosed() {
        return ledger.isClosed();
    }

    @Override
    public void asyncReadEntries(long firstEntry, long lastEntry, ReadCallback cb, Object ctx) {
        ledger.asyncReadEntries(firstEntry, lastEntry,
                (rc, lh, entries, readCtx) -> cb.readComplete(rc, this, entries, readCtx), ctx);
    }

    @Override
    public void asyncReadUnconfirmedEntries(long firstEntry, long lastEntry, ReadCallback cb, Object ctx) {
        asyncReadEntries(firstEntry, lastEntry, cb, ctx);
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        return ledger.readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return ledger.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return ledger.readLastAddConfirmedAsync();
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return ledger.tryReadLastAddConfirmedAsync();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        return ledger.readLastAddConfirmedAndEntryAsync(entryId, timeOutInMillis, parallel);
    }

    // Like ReadOnlyLedgerHandle: closing the view does not touch the ledger, and writes are rejected

    @Override
    public void asyncClose(CloseCallback cb, Object ctx) {
        cb.closeComplete(BKException.Code.OK, this, ctx);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
    }

    @Override
    public long addEntry(byte[] data) throws BKException {
        throw BKException.create(BKException.Code.IllegalOpException);
    }

    @Override
    public long addEntry(byte[] data, int offset, int length) throws BKException {
        throw BKException.create(BKException.Code.IllegalOpException);
    }

    @Override
    public void asyncAddEntry(byte[] data, AddCallback cb, Object ctx) {
        cb.addComplete(BKException.Code.IllegalOpException, this, INVALID_ENTRY_ID, ctx);
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AddCallback cb, Object ctx) {
        cb.addComplete(BKException.Code.IllegalOpException, this, INVALID_ENTRY_ID, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf data, AddCallback cb, Object ctx) {
        cb.addComplete(BKException.Code.IllegalOpException, this, INVALID_ENTRY_ID, ctx);
    }
}
