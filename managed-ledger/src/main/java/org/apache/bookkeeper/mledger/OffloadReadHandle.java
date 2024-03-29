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

import com.google.common.annotations.VisibleForTesting;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.util.TimeWindow;
import org.apache.pulsar.common.util.WindowWrap;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OffloadReadHandle is a wrapper of ReadHandle to offload read operations.
 */
public final class OffloadReadHandle implements ReadHandle {
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static volatile long flowPermits = -1L;
    private static volatile TimeWindow<AtomicLong> window;

    private final ReadHandle delegate;
    private final long averageEntrySize;

    private OffloadReadHandle(ReadHandle handle, ManagedLedgerConfig config,
                              MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo) {
        initialize(config);
        this.delegate = Objects.requireNonNull(handle);
        Objects.requireNonNull(ledgerInfo);
        long averageEntrySize = ledgerInfo.getSize() / ledgerInfo.getEntries();
        if (averageEntrySize <= 0) {
            averageEntrySize = 1;
        }
        this.averageEntrySize = averageEntrySize;
    }

    private static void initialize(ManagedLedgerConfig config) {
        if (INITIALIZED.compareAndSet(false, true)) {
            flowPermits = config.getManagedLedgerOffloadFlowPermitsPerSecond();
            window = new TimeWindow<>(2, 1000);
        }
    }

    public static CompletableFuture<ReadHandle> create(ReadHandle handle, ManagedLedgerConfig config,
                                                       MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo) {
        return CompletableFuture.completedFuture(new OffloadReadHandle(handle, config, ledgerInfo));
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        long numEntries = lastEntry - firstEntry + 1;
        long numBytes = numEntries * averageEntrySize;

        long delayMillis;
        // block the offloader thread if the flow control permits is exceeded.
        while ((delayMillis = calculateDelayMillis(numBytes)) > 0) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException ex) {
                return CompletableFuture.failedFuture(ex);
            }
        }

        return delegate.readAsync(firstEntry, lastEntry);
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


    private static synchronized long calculateDelayMillis(long numBytes) {
        if (flowPermits <= 0) {
            return 0;
        }
        if (numBytes <= 0) {
            return 0;
        }

        WindowWrap<AtomicLong> wrap = window.current(__ -> new AtomicLong(0));
        if (wrap == null) {
            // it should never goes here
            return 0;
        }
        AtomicLong counter = wrap.value();

        long delayMillis = 0;
        // Cannot use `counter.addAndGet(bytes) >= flowPermits` directly.
        // If flowPermits is less than the bytes of a single entry or a read request,
        // the read request will be blocked forever.
        if (counter.get() >= flowPermits) {
            // park until next window start
            long end = wrap.start() + wrap.interval();
            delayMillis = end - System.currentTimeMillis();
        }
        counter.addAndGet(numBytes);
        return delayMillis;
    }

    @VisibleForTesting
    public void reset() {
        INITIALIZED.set(false);
        flowPermits = -1L;
        window = null;
    }
}
