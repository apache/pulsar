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
package org.apache.bookkeeper.mledger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.util.TimeWindow;
import org.apache.bookkeeper.mledger.util.WindowWrap;

public class FlowControllableReadHandle implements ReadHandle {
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static volatile long permittedBytes0;
    private static volatile TimeWindow<AtomicLong> window0;

    private final ReadHandle delegator;

    private FlowControllableReadHandle(ReadHandle handle, long permittedBytes) {
        this.delegator = handle;
        if (INITIALIZED.compareAndSet(false, true)) {
            permittedBytes0 = permittedBytes;
            window0 = new TimeWindow<>(2, Long.valueOf(TimeUnit.SECONDS.toMillis(1)).intValue());
        }
    }

    public static CompletableFuture<ReadHandle> create(ReadHandle handle, long permitBytes) {
        return CompletableFuture.completedFuture(new FlowControllableReadHandle(handle, permitBytes));
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        if (!checkFlow()) {
            return this.readAsync(firstEntry, lastEntry);
        }

        return this.delegator
                .readAsync(firstEntry, lastEntry)
                .whenComplete((v, t) -> {
                    if (t == null) {
                        AtomicLong total = new AtomicLong(0);
                        v.forEach(entry -> total.addAndGet(entry.getLength()));
                        recordReadBytes(total.get());
                    }
                });
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        if (!checkFlow()) {
            return this.readUnconfirmedAsync(firstEntry, lastEntry);
        }

        return this.delegator
                .readUnconfirmedAsync(firstEntry, lastEntry)
                .whenComplete((v, t) -> {
                    if (t == null) {
                        AtomicLong total = new AtomicLong(0);
                        v.forEach(entry -> total.addAndGet(entry.getLength()));
                        recordReadBytes(total.get());
                    }
                });
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
        if (!checkFlow()) {
            return this.readLastAddConfirmedAndEntryAsync(entryId, timeOutInMillis, parallel);
        }

        return this.delegator
                .readLastAddConfirmedAndEntryAsync(entryId, timeOutInMillis, parallel)
                .whenComplete((v, t) -> {
                    if (t == null) {
                        recordReadBytes(v.getEntry().getLength());
                    }
                });
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


    private static boolean checkFlow() {
        if (permittedBytes0 <= 0) {
            return true;
        }

        WindowWrap<AtomicLong> wrap = window0.current(__ -> new AtomicLong(0));
        if (wrap == null) {
            // it should never goes here
            return true;
        }

        if (wrap.value().get() >= permittedBytes0) {
            // park until next window start
            long end = wrap.start() + wrap.interval();
            long parkMillis = end - System.currentTimeMillis();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(parkMillis));
            return false;
        }

        return true;
    }

    private static void recordReadBytes(long bytes) {
        if (permittedBytes0 <= 0) {
            return;
        }

        WindowWrap<AtomicLong> wrap = window0.current(__ -> new AtomicLong(0));
        if (wrap == null) {
            // it should never goes here
            return;
        }

        wrap.value().addAndGet(bytes);
    }
}
