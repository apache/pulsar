/**
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Mock implementation of ReadHandle.
 */
@Slf4j
class PulsarMockReadHandle implements ReadHandle {
    private final PulsarMockBookKeeper bk;
    private final long ledgerId;
    private final LedgerMetadata metadata;
    private final List<LedgerEntryImpl> entries;

    PulsarMockReadHandle(PulsarMockBookKeeper bk, long ledgerId, LedgerMetadata metadata,
                         List<LedgerEntryImpl> entries) {
        this.bk = bk;
        this.ledgerId = ledgerId;
        this.metadata = metadata;
        this.entries = entries;
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        return bk.getProgrammedFailure().thenComposeAsync((res) -> {
                log.debug("readEntries: first={} last={} total={}", firstEntry, lastEntry, entries.size());
                List<LedgerEntry> seq = new ArrayList<>();
                long entryId = firstEntry;
                while (entryId <= lastEntry && entryId < entries.size()) {
                    seq.add(entries.get((int) entryId++).duplicate());
                }
                log.debug("Entries read: {}", seq);

                return FutureUtils.value(LedgerEntriesImpl.create(seq));
            });
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsync(firstEntry, lastEntry);
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
        if (entries.isEmpty()) {
            return -1;
        } else {
            return entries.get(entries.size() - 1).getEntryId();
        }
    }

    @Override
    public long getLength() {
        long length = 0;
        for (LedgerEntryImpl entry : entries) {
            length += entry.getLength();
        }

        return length;
    }

    @Override
    public boolean isClosed() {
        return metadata.isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        CompletableFuture<LastConfirmedAndEntry> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException("Long poll not implemented"));
        return promise;
    }

    // Handle interface
    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return metadata;
    }
}
