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
package org.apache.bookkeeper.mledger.impl;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.mledger.LedgerOffloaderMXBean;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

public class LedgerOffloaderMXBeanImpl implements LedgerOffloaderMXBean {

    public static final long[] READ_ENTRY_LATENCY_BUCKETS_USEC = {500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 1000_000};

    private final String name;

    private final ConcurrentHashMap<String, LongAdder> offloadTimeMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> offloadErrorMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Rate> readOffloadRateMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> readOffloadErrorMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, StatsBuckets> writeToStorageBucketsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Rate> writeToStorageRateMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> writeToStorageErrorMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StatsBuckets> buildJcloundIndexBucketsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> buildJcloundIndexErrorMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Rate> streamingWriteToStorageRateMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> streamingWriteToStorageErrorMap = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler;
    private long refreshInterval;

    public LedgerOffloaderMXBeanImpl(String name, long refreshInterval) {
        this.name = name;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("ledger-offloader-metrics"));
        this.scheduler.scheduleAtFixedRate(
                safeRun(() -> refreshStats()), refreshInterval, refreshInterval, TimeUnit.SECONDS);
    }

    public void refreshStats() {
        offloadTimeMap.clear();
        offloadErrorMap.clear();

        readOffloadRateMap.clear();
        readOffloadErrorMap.clear();

        writeToStorageBucketsMap.clear();
        writeToStorageRateMap.clear();
        writeToStorageErrorMap.clear();
        buildJcloundIndexBucketsMap.clear();
        buildJcloundIndexErrorMap.clear();

        streamingWriteToStorageRateMap.clear();
        streamingWriteToStorageErrorMap.clear();
    }

    //TODO metrics在namespace这个level的输出。

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Map<String, LongAdder> getOffloadTimes() {
        return offloadTimeMap;
    }

    @Override
    public Map<String, LongAdder> getOffloadErrors() {
        return offloadErrorMap;
    }

    public void recordOffloadTime(String managedLedgerName, long time, TimeUnit unit) {
        LongAdder adder = offloadTimeMap.get(managedLedgerName);
        if (adder == null) {
            adder = offloadTimeMap.compute(managedLedgerName, (k, v) -> new LongAdder());
        }
        adder.add(unit.toMillis(time));
    }

    public void recordOffloadError(String managedLedgerName) {
        LongAdder adder = offloadErrorMap.get(managedLedgerName);
        if (adder == null) {
            adder = offloadErrorMap.compute(managedLedgerName, (k, v) -> new LongAdder());
        }
        adder.increment();
    }

    @Override
    public Map<String, StatsBuckets> getWriteToStorageLatencyBuckets() {
        return writeToStorageBucketsMap;
    }

    @Override
    public Map<String, Rate> getWriteToStorageRates() {
        return writeToStorageRateMap;
    }

    @Override
    public Map<String, LongAdder> getWriteToStorageErrors() {
        return writeToStorageErrorMap;
    }

    @Override
    public Map<String, StatsBuckets> getBuildJcloundIndexLatency() {
        return buildJcloundIndexBucketsMap;
    }

    @Override
    public Map<String, LongAdder> getBuildJcloundIndexErrors() {
        return buildJcloundIndexErrorMap;
    }

    @Override
    public Map<String, Rate> getReadOffloadRates() {
        return readOffloadRateMap;
    }

    @Override
    public Map<String, LongAdder> getReadOffloadErrors() {
        return readOffloadErrorMap;
    }

    @Override
    public Map<String, Rate> getStreamingWriteToStorageRates() {
        return streamingWriteToStorageRateMap;
    }

    @Override
    public Map<String, LongAdder> getStreamingWriteToStorageErrors() {
        return streamingWriteToStorageErrorMap;
    }

    public void recordReadOffloadError(String managedLedgerName) {
        LongAdder adder = readOffloadErrorMap.get(managedLedgerName);
        if (adder == null) {
            adder = readOffloadErrorMap.compute(managedLedgerName, (k, v) -> new LongAdder());
        }
        adder.increment();
    }

    public void recordReadOffloadRate(String managedLedgerName, int size) {
        Rate rate = readOffloadRateMap.get(managedLedgerName);
        if (rate == null) {
            rate = readOffloadRateMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        rate.recordEvent(size);
    }

    public void recordWriteToStorageError(String managedLedgerName) {
        LongAdder adder = writeToStorageErrorMap.get(managedLedgerName);
        if (adder == null) {
            adder = writeToStorageErrorMap.compute(managedLedgerName, (k, v) -> new LongAdder());
        }
        adder.increment();
    }

    public void recordWriteToStorageRate(String managedLedgerName, int size) {
        Rate rate = writeToStorageRateMap.get(managedLedgerName);
        if (rate == null) {
            rate = writeToStorageRateMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        rate.recordEvent(size);
    }

    public void recordStreamingWriteToStorageRate(String managedLedgerName, int size) {
        Rate rate = streamingWriteToStorageRateMap.get(managedLedgerName);
        if (rate == null) {
            rate = streamingWriteToStorageRateMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        rate.recordEvent(size);
    }

    public void recordStreamingWriteToStorageError(String managedLedgerName) {
        LongAdder adder = streamingWriteToStorageErrorMap.get(managedLedgerName);
        if (adder == null) {
            adder = streamingWriteToStorageErrorMap.compute(managedLedgerName, (k, v) -> new LongAdder());
        }
        adder.increment();
    }

    public void recordWriteToStorageLatency(String managedLedgerName, long latency, TimeUnit unit) {
        StatsBuckets statsBuckets = writeToStorageBucketsMap.get(managedLedgerName);
        if (statsBuckets == null) {
            statsBuckets = writeToStorageBucketsMap.compute(managedLedgerName,
                    (k, v) -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        }
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordBuildJcloundIndexLatency(String managedLedgerName, long latency, TimeUnit unit) {
        StatsBuckets statsBuckets = buildJcloundIndexBucketsMap.get(managedLedgerName);
        if (statsBuckets == null) {
            statsBuckets = buildJcloundIndexBucketsMap.compute(managedLedgerName,
                    (k, v) -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        }
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordBuildJcloundIndexError(String managedLedgerName) {
        LongAdder adder = buildJcloundIndexErrorMap.get(managedLedgerName);
        if (adder == null) {
            adder = buildJcloundIndexErrorMap.compute(managedLedgerName, (k, v) -> new LongAdder());
        }
        adder.increment();
    }
}
