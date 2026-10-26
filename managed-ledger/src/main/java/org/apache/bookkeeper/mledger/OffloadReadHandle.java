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
package org.apache.bookkeeper.mledger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;

/**
 * OffloadReadHandle is a wrapper of ReadHandle to offload read operations.
 */
public final class OffloadReadHandle implements ReadHandle {
    private final ReadHandle delegate;
    private final AsyncTokenBucket tokenBucket;
    private final ScheduledExecutorService scheduledExecutor;

    private OffloadReadHandle(ReadHandle handle, AsyncTokenBucket tokenBucket,
                              ScheduledExecutorService scheduledExecutor) {
        this.delegate = Objects.requireNonNull(handle);
        this.tokenBucket = tokenBucket;
        this.scheduledExecutor = Objects.requireNonNull(scheduledExecutor);
    }

    public static CompletableFuture<ReadHandle> create(ReadHandle handle, AsyncTokenBucket tokenBucket,
                                                       ScheduledExecutorService scheduledExecutor) {
        try {
            return CompletableFuture.completedFuture(new OffloadReadHandle(handle, tokenBucket, scheduledExecutor));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        CompletableFuture<LedgerEntries> result = new CompletableFuture<>();
        readAsyncWithThrottling(firstEntry, lastEntry, result);
        return result;
    }

    private void readAsyncWithThrottling(long firstEntry, long lastEntry,
                                         CompletableFuture<LedgerEntries> result) {
        if (result.isDone()) {
            return;
        }
        try {
            if (tokenBucket != null && !tokenBucket.containsTokens()) {
                long delayNanos = tokenBucket.calculateThrottlingDuration();
                if (delayNanos > 0) {
                    scheduledExecutor.schedule(() -> readAsyncWithThrottling(firstEntry, lastEntry, result),
                            delayNanos, TimeUnit.NANOSECONDS);
                    return;
                }
            }
            delegate.readAsync(firstEntry, lastEntry).whenComplete((entries, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                    return;
                }
                try {
                    if (tokenBucket != null) {
                        long bytesRead = 0;
                        for (LedgerEntry entry : entries) {
                            bytesRead += entry.getLength();
                        }
                        tokenBucket.consumeTokens(bytesRead);
                    }
                    result.complete(entries);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return this.delegate.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return this.delegate.readLastAddConfirmedAsync();
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return this.delegate.tryReadLastAddConfirmedAsync();
    }

    @Override
    public long getLastAddConfirmed() {
        return this.delegate.getLastAddConfirmed();
    }

    @Override
    public long getLength() {
        return this.delegate.getLength();
    }

    @Override
    public boolean isClosed() {
        return this.delegate.isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(
            long entryId, long timeOutInMillis, boolean parallel) {
        return this.delegate.readLastAddConfirmedAndEntryAsync(entryId, timeOutInMillis, parallel);
    }

    @Override
    public long getId() {
        return this.delegate.getId();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return this.delegate.closeAsync();
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return this.delegate.getLedgerMetadata();
    }

}
