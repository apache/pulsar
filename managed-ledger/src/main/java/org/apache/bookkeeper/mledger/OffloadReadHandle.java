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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.util.TimeWindow;
import org.apache.bookkeeper.mledger.util.WindowWrap;

public class OffloadReadHandle implements ReadHandle {
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static volatile long flowPermits = -1L;
    private static volatile TimeWindow<AtomicLong> window;

    private final ReadHandle delegator;
    private final OrderedScheduler scheduler;

    private OffloadReadHandle(ReadHandle handle, ManagedLedgerConfig config,
                              OrderedScheduler scheduler) {
        initialize(config);
        this.delegator = handle;
        this.scheduler = Objects.requireNonNull(scheduler);
    }

    private static void initialize(ManagedLedgerConfig config) {
        if (INITIALIZED.compareAndSet(false, true)) {
            flowPermits = config.getManagedLedgerOffloadFlowPermitsPerSecond();
            window = new TimeWindow<>(2, 1000);
        }
    }

    public static CompletableFuture<ReadHandle> create(ReadHandle handle, ManagedLedgerConfig config,
                                                       OrderedScheduler scheduler) {
        return CompletableFuture.completedFuture(new OffloadReadHandle(handle, config, scheduler));
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        final long delayMills = calculateDelayMillis();
        if (delayMills > 0) {
            CompletableFuture<LedgerEntries> f = new CompletableFuture<>();
            Runnable cmd = new ReadAsyncCommand(firstEntry, lastEntry, f);
            scheduler.schedule(cmd, delayMills, TimeUnit.MILLISECONDS);
            return f;
        }

        return this.delegator
                .readAsync(firstEntry, lastEntry)
                .whenComplete((v, t) -> {
                    if (t == null) {
                        recordReadBytes(v);
                    }
                });
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return this.delegator.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return this.delegator.readLastAddConfirmedAsync();
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return this.delegator.tryReadLastAddConfirmedAsync();
    }

    @Override
    public long getLastAddConfirmed() {
        return this.delegator.getLastAddConfirmed();
    }

    @Override
    public long getLength() {
        return this.delegator.getLength();
    }

    @Override
    public boolean isClosed() {
        return this.delegator.isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(
            long entryId, long timeOutInMillis, boolean parallel) {
        return this.delegator.readLastAddConfirmedAndEntryAsync(entryId, timeOutInMillis, parallel);
    }

    @Override
    public long getId() {
        return this.delegator.getId();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return this.delegator.closeAsync();
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return this.delegator.getLedgerMetadata();
    }


    private long calculateDelayMillis() {
        if (flowPermits <= 0) {
            return 0;
        }

        WindowWrap<AtomicLong> wrap = window.current(__ -> new AtomicLong(0));
        if (wrap == null) {
            // it should never goes here
            return 0;
        }

        if (wrap.value().get() >= flowPermits) {
            // park until next window start
            long end = wrap.start() + wrap.interval();
            return end - System.currentTimeMillis();
        }

        return 0;
    }

    private void recordReadBytes(LedgerEntries entries) {
        if (flowPermits <= 0) {
            return;
        }

        if (entries == null) {
            return;
        }

        AtomicLong num = new AtomicLong(0);
        entries.forEach(en -> num.addAndGet(en.getLength()));

        WindowWrap<AtomicLong> wrap = window.current(__ -> new AtomicLong(0));
        if (wrap == null) {
            // it should never goes here
            return;
        }

        wrap.value().addAndGet(num.get());
    }


    private final class ReadAsyncCommand implements Runnable {

        private final long firstEntry;
        private final long lastEntry;
        private final CompletableFuture<LedgerEntries> f;

        ReadAsyncCommand(long firstEntry, long lastEntry, CompletableFuture<LedgerEntries> f) {
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.f = f;
        }

        @Override
        public void run() {
            long delayMillis = calculateDelayMillis();
            if (delayMillis > 0) {
                scheduler.schedule(this, delayMillis, TimeUnit.MILLISECONDS);
                return;
            }

            delegator.readAsync(firstEntry, lastEntry)
                    .whenComplete((entries, e) -> {
                        if (e != null) {
                            f.completeExceptionally(e);
                        } else {
                            f.complete(entries);
                            recordReadBytes(entries);
                        }
                    });
        }
    }

    @VisibleForTesting
    public void reset() {
        INITIALIZED.set(false);
        flowPermits = -1L;
        window = null;
    }
}
