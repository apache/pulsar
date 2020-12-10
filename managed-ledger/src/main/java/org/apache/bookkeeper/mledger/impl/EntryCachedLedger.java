/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
