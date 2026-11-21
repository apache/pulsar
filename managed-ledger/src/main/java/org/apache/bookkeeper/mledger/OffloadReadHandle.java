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
import java.util.function.Supplier;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;

/**
 * OffloadReadHandle is a wrapper of ReadHandle to throttle the entry-data reads issued while
 * offloading. A read is issued only after the token bucket contains tokens, and the bytes
 * actually read are charged against the bucket once the read completes. The range reads
 * ({@code readAsync}, {@code readUnconfirmedAsync}, {@code batchReadAsync} and
 * {@code batchReadUnconfirmedAsync}) are throttled; every other operation is delegated unchanged.
 */
public final class OffloadReadHandle implements ReadHandle {
    private final ReadHandle delegate;
    private final AsyncTokenBucket tokenBucket;
    private final ScheduledExecutorService scheduledExecutor;

    private OffloadReadHandle(ReadHandle handle, AsyncTokenBucket tokenBucket,
                              ScheduledExecutorService scheduledExecutor) {
        this.delegate = Objects.requireNonNull(handle);
        this.tokenBucket = Objects.requireNonNull(tokenBucket);
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
        return readAsyncWithThrottling(() -> delegate.readAsync(firstEntry, lastEntry));
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsyncWithThrottling(() -> delegate.readUnconfirmedAsync(firstEntry, lastEntry));
    }

    @Override
    public CompletableFuture<LedgerEntries> batchReadAsync(long firstEntry, int maxEntries, long maxSize) {
        return readAsyncWithThrottling(() -> delegate.batchReadAsync(firstEntry, maxEntries, maxSize));
    }

    @Override
    public CompletableFuture<LedgerEntries> batchReadUnconfirmedAsync(long firstEntry, int maxEntries,
                                                                      long maxSize) {
        return readAsyncWithThrottling(() -> delegate.batchReadUnconfirmedAsync(firstEntry, maxEntries, maxSize));
    }

    private CompletableFuture<LedgerEntries> readAsyncWithThrottling(
            Supplier<CompletableFuture<LedgerEntries>> readOperation) {
        CompletableFuture<LedgerEntries> result = new CompletableFuture<>();
        readAsyncWithThrottling(readOperation, result);
        return result;
    }

    private void readAsyncWithThrottling(Supplier<CompletableFuture<LedgerEntries>> readOperation,
                                         CompletableFuture<LedgerEntries> result) {
        if (result.isDone()) {
            return;
        }
        try {
            if (!tokenBucket.containsTokens()) {
                long delayNanos = tokenBucket.calculateThrottlingDuration();
                if (delayNanos > 0) {
                    scheduledExecutor.schedule(() -> readAsyncWithThrottling(readOperation, result),
                            delayNanos, TimeUnit.NANOSECONDS);
                    return;
                }
            }
            readOperation.get().whenComplete((entries, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                    return;
                }
                try {
                    long bytesRead = 0;
                    for (LedgerEntry entry : entries) {
                        bytesRead += entry.getLength();
                    }
                    tokenBucket.consumeTokens(bytesRead);
                } catch (Throwable t) {
                    try {
                        entries.close();
                    } catch (Throwable closeException) {
                        t.addSuppressed(closeException);
                    }
                    result.completeExceptionally(t);
                    return;
                }
                if (!result.complete(entries)) {
                    entries.close();
                }
            });
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return delegate.readLastAddConfirmedAsync();
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return delegate.tryReadLastAddConfirmedAsync();
    }

    @Override
    public long getLastAddConfirmed() {
        return delegate.getLastAddConfirmed();
    }

    @Override
    public long getLength() {
        return delegate.getLength();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(
            long entryId, long timeOutInMillis, boolean parallel) {
        return delegate.readLastAddConfirmedAndEntryAsync(entryId, timeOutInMillis, parallel);
    }

    @Override
    public long getId() {
        return delegate.getId();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return delegate.closeAsync();
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return delegate.getLedgerMetadata();
    }

}
