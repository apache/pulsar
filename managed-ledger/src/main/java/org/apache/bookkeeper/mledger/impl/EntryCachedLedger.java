package org.apache.bookkeeper.mledger.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;

public class EntryCachedLedger implements ReadHandle {
    final EntryCache entryCache;
    final ReadHandle innerLedger;

    public EntryCachedLedger(EntryCache entryCache, ReadHandle innerLedger) {
        this.entryCache = entryCache;
        this.innerLedger = innerLedger;
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        throw new UnsupportedOperationException("An entry cached ledger should not read unconfirmed entry");
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return innerLedger.readLastAddConfirmedAsync();
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return innerLedger.tryReadLastAddConfirmedAsync();
    }

    @Override
    public long getLastAddConfirmed() {
        return innerLedger.getLastAddConfirmed();
    }

    @Override
    public long getLength() {
        return innerLedger.getLength();
    }

    @Override
    public boolean isClosed() {
        return innerLedger.isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        return innerLedger.readLastAddConfirmedAndEntryAsync(entryId, timeOutInMillis, parallel);
    }

    @Override
    public long getId() {
        return innerLedger.getId();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return innerLedger.closeAsync();
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return innerLedger.getLedgerMetadata();
    }
}
