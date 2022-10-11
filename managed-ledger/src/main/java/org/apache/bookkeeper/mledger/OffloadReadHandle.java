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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.TopicName;

public class OffloadReadHandle implements ReadHandle {
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static volatile Long brokerFlowPermit;
    private static volatile Map<String, Long> namespaceFlowPermit;
    private static volatile Map<String, Long> topicFlowPermit;
    private static volatile TimeWindow<AtomicLong> brokerFlowPermit0;
    private static volatile Map<String, TimeWindow<AtomicLong>> namespaceFlowPermit0;
    private static volatile Map<String, TimeWindow<AtomicLong>> topicFlowPermit0;


    private final String ledgerName;
    private final String namespace;
    private final ReadHandle delegator;
    private final OrderedScheduler scheduler;
    private final long ledgerFlowPermit;
    private final TimeWindow<AtomicLong> window;

    private OffloadReadHandle(ReadHandle handle, String ledgerName, ManagedLedgerConfig config,
                              OrderedScheduler scheduler) {
        initialize(config);
        this.delegator = handle;
        this.ledgerName = ledgerName;
        this.namespace = TopicName.get(ledgerName).getNamespace();
        this.scheduler = Objects.requireNonNull(scheduler);

        Pair<Long, TimeWindow<AtomicLong>> pair = getFlowPermitAndController(ledgerName, namespace);
        this.ledgerFlowPermit = pair.getLeft();
        this.window = pair.getRight();
    }

    private static void initialize(ManagedLedgerConfig config) {
        if (INITIALIZED.compareAndSet(false, true)) {
            brokerFlowPermit = config.getManagedLedgerOffloadBrokerFlowPermit();
            namespaceFlowPermit = config.getManagedLedgerOffloadNamespaceFlowPermit();
            topicFlowPermit = config.getManagedLedgerOffloadTopicFlowPermit();

            brokerFlowPermit0 = new TimeWindow<>(2, 1000);
            namespaceFlowPermit0 = new ConcurrentHashMap<>();
            topicFlowPermit0 = new ConcurrentHashMap<>();
        }
    }

    private static Pair<Long, TimeWindow<AtomicLong>> getFlowPermitAndController(String ledgerName, String namespace) {
        if (null != topicFlowPermit.get(ledgerName)) {
            long permit = topicFlowPermit.get(ledgerName);
            TimeWindow<AtomicLong> window =
                    topicFlowPermit0.computeIfAbsent(ledgerName, __ -> new TimeWindow<>(2, 1000));
            return Pair.of(permit, window);
        } else if (null != namespaceFlowPermit.get(namespace)) {
            long permit = namespaceFlowPermit.get(namespace);
            TimeWindow<AtomicLong> window =
                    namespaceFlowPermit0.computeIfAbsent(namespace, __ -> new TimeWindow<>(2, 1000));
            return Pair.of(permit, window);
        } else {
            return Pair.of(brokerFlowPermit, brokerFlowPermit0);
        }
    }

    public static CompletableFuture<ReadHandle> create(ReadHandle handle, String ledgerName,
                                                       ManagedLedgerConfig config, OrderedScheduler scheduler) {
        return CompletableFuture.completedFuture(new OffloadReadHandle(handle, ledgerName, config, scheduler));
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
        if (ledgerFlowPermit <= 0) {
            return 0;
        }

        WindowWrap<AtomicLong> wrap = window.current(__ -> new AtomicLong(0));
        if (wrap == null) {
            // it should never goes here
            return 0;
        }

        if (wrap.value().get() >= ledgerFlowPermit) {
            // park until next window start
            long end = wrap.start() + wrap.interval();
            return end - System.currentTimeMillis();
        }

        return 0;
    }

    private void recordReadBytes(LedgerEntries entries) {
        if (ledgerFlowPermit <= 0) {
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
}
