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


import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.LedgerOffloaderMXBean;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;

public class LedgerOffloaderMXBeanImpl implements LedgerOffloaderMXBean {

    public static final long[] READ_ENTRY_LATENCY_BUCKETS_USEC = {500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 1000_000};

    private static final long OUT_DATED_CLEAN_PERIOD_MILLISECOND = 10 * 60 * 1000;

    private final String driverName;

    private Map<String, Long> topicAccessTime;

    // offloadTimeMap record the time cost by one round offload
    private final ConcurrentHashMap<String, Rate> offloadTimeMap = new ConcurrentHashMap<>();
    // offloadErrorMap record error ocurred
    private final ConcurrentHashMap<String, Rate> offloadErrorMap = new ConcurrentHashMap<>();
    // offloadRateMap record the offload rate
    private final ConcurrentHashMap<String, Rate> offloadRateMap = new ConcurrentHashMap<>();


    // readLedgerLatencyBucketsMap record the time cost by ledger read
    private final ConcurrentHashMap<String, StatsBuckets> readLedgerLatencyBucketsMap = new ConcurrentHashMap<>();
    // writeToStorageLatencyBucketsMap record the time cost by write to storage
    private final ConcurrentHashMap<String, StatsBuckets> writeToStorageLatencyBucketsMap = new ConcurrentHashMap<>();
    // writeToStorageErrorMap record the error occurred in write storage
    private final ConcurrentHashMap<String, Rate> writeToStorageErrorMap = new ConcurrentHashMap<>();


    // streamingWriteToStorageRateMap and streamingWriteToStorageErrorMap is for streamingOffload
    private final ConcurrentHashMap<String, Rate> streamingWriteToStorageRateMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Rate> streamingWriteToStorageErrorMap = new ConcurrentHashMap<>();

    // readOffloadIndexLatencyBucketsMap and readOffloadDataLatencyBucketsMap are latency metrics about index and data
    // readOffloadDataRateMap and readOffloadErrorMap is for reading offloaded data
    private final ConcurrentHashMap<String, StatsBuckets> readOffloadIndexLatencyBucketsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StatsBuckets> readOffloadDataLatencyBucketsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Rate> readOffloadDataRateMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Rate> readOffloadErrorMap = new ConcurrentHashMap<>();

    public LedgerOffloaderMXBeanImpl(String driverName) {
        this.driverName = driverName;
        topicAccessTime = new ConcurrentHashMap<>();
    }

    private void accessTopic(String topicName) {
        topicAccessTime.compute(topicName, (k, v) -> new Long(System.currentTimeMillis()));
    }

    private void removeOutdatedTopicMetricsIfNeeded() {
        Iterator<Map.Entry<String, Long>> it = topicAccessTime.entrySet().iterator();
        while (it.hasNext()) {
            Long lastAccess = it.next().getValue();
            if (System.currentTimeMillis() > lastAccess + OUT_DATED_CLEAN_PERIOD_MILLISECOND) {
                removeTopicIfExist(it.next().getKey());
                it.remove();
            }
        }
    }

    public void refreshStats(long period, TimeUnit unit) {
        double seconds = unit.toMillis(period) / 1000.0;
        if (seconds <= 0.0) {
            // skip refreshing stats
            return;
        }
        offloadTimeMap.values().forEach(rate->rate.calculateRate(seconds));
        offloadErrorMap.values().forEach(rate->rate.calculateRate(seconds));
        offloadRateMap.values().forEach(rate-> rate.calculateRate(seconds));
        readLedgerLatencyBucketsMap.values().forEach(stat-> stat.refresh());
        writeToStorageLatencyBucketsMap.values().forEach(stat -> stat.refresh());
        writeToStorageErrorMap.values().forEach(rate->rate.calculateRate(seconds));
        streamingWriteToStorageRateMap.values().forEach(rate -> rate.calculateRate(seconds));
        streamingWriteToStorageErrorMap.values().forEach(rate->rate.calculateRate(seconds));
        readOffloadDataRateMap.values().forEach(rate->rate.calculateRate(seconds));
        readOffloadErrorMap.values().forEach(rate->rate.calculateRate(seconds));
        readOffloadIndexLatencyBucketsMap.values().forEach(stat->stat.refresh());
        readOffloadDataLatencyBucketsMap.values().forEach(stat->stat.refresh());

        removeOutdatedTopicMetricsIfNeeded();
    }

    @Override
    public String getDriverName() {
        return this.driverName;
    }

    @Override
    public Map<String, Rate> getOffloadTimes() {
        return offloadTimeMap;
    }

    @Override
    public Map<String, Rate> getOffloadErrors() {
        return offloadErrorMap;
    }

    @Override
    public Map<String, Rate> getOffloadRates() {
        return offloadRateMap;
    }

    @Override
    public Map<String, StatsBuckets> getReadLedgerLatencyBuckets() {
        return readLedgerLatencyBucketsMap;
    }

    @Override
    public Map<String, StatsBuckets> getWriteToStorageLatencyBuckets() {
        return writeToStorageLatencyBucketsMap;
    }

    @Override
    public Map<String, Rate> getWriteToStorageErrors() {
        return writeToStorageErrorMap;
    }

    @Override
    public Map<String, Rate> getStreamingWriteToStorageRates() {
        return streamingWriteToStorageRateMap;
    }

    @Override
    public Map<String, Rate> getStreamingWriteToStorageErrors() {
        return streamingWriteToStorageErrorMap;
    }

    public void removeTopicIfExist(String topicName) {
        offloadTimeMap.remove(topicName);
        offloadErrorMap.remove(topicName);
        offloadRateMap.remove(topicName);
        readLedgerLatencyBucketsMap.remove(topicName);
        writeToStorageLatencyBucketsMap.remove(topicName);
        writeToStorageErrorMap.remove(topicName);
        streamingWriteToStorageRateMap.remove(topicName);
        streamingWriteToStorageErrorMap.remove(topicName);
        readOffloadDataRateMap.remove(topicName);
        readOffloadErrorMap.remove(topicName);
        readOffloadIndexLatencyBucketsMap.remove(topicName);
        readOffloadDataLatencyBucketsMap.remove(topicName);
    }


    @Override
    public Map<String, StatsBuckets> getReadOffloadIndexLatencyBuckets() {
        return readOffloadIndexLatencyBucketsMap;
    }

    @Override
    public Map<String, StatsBuckets> getReadOffloadDataLatencyBuckets() {
        return readOffloadDataLatencyBucketsMap;
    }

    @Override
    public Map<String, Rate> getReadOffloadRates() {
        return readOffloadDataRateMap;
    }

    @Override
    public Map<String, Rate> getReadOffloadErrors() {
        return readOffloadErrorMap;
    }


    public void recordOffloadTime(String topicName, long time, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        Rate adder = offloadTimeMap.computeIfAbsent(topicName, k -> new Rate());
        adder.recordEvent(unit.toMillis(time));
        accessTopic(topicName);
    }

    public void recordOffloadError(String topicName) {
        if (topicName == null) {
            return;
        }
        Rate adder = offloadErrorMap.computeIfAbsent(topicName, k -> new Rate());
        adder.recordEvent(1);
        accessTopic(topicName);
    }

    public void recordOffloadRate(String topicName, int size) {
        if (topicName == null) {
            return;
        }
        Rate rate = offloadRateMap.computeIfAbsent(topicName, k -> new Rate());
        rate.recordEvent(size);
        accessTopic(topicName);
    }

    public void recordReadLedgerLatency(String topicName, long latency, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        StatsBuckets statsBuckets = readLedgerLatencyBucketsMap.computeIfAbsent(topicName,
                k -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        statsBuckets.addValue(unit.toMicros(latency));
        accessTopic(topicName);
    }

    public void recordWriteToStorageLatency(String topicName, long latency, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        StatsBuckets statsBuckets = writeToStorageLatencyBucketsMap.computeIfAbsent(topicName,
                k -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        statsBuckets.addValue(unit.toMicros(latency));
        accessTopic(topicName);
    }

    public void recordWriteToStorageError(String topicName) {
        if (topicName == null) {
            return;
        }
        Rate adder = writeToStorageErrorMap.computeIfAbsent(topicName, k -> new Rate());
        adder.recordEvent(1);
        accessTopic(topicName);
    }

    public void recordStreamingWriteToStorageRate(String topicName, int size) {
        if (topicName == null) {
            return;
        }
        Rate rate = streamingWriteToStorageRateMap.computeIfAbsent(topicName, k -> new Rate());
        rate.recordEvent(size);
        accessTopic(topicName);
    }

    public void recordStreamingWriteToStorageError(String topicName) {
        if (topicName == null) {
            return;
        }
        Rate adder = streamingWriteToStorageErrorMap.computeIfAbsent(topicName, k -> new Rate());
        adder.recordEvent(1);
        accessTopic(topicName);
    }


    public void recordReadOffloadError(String topicName) {
        if (topicName == null) {
            return;
        }
        Rate adder = readOffloadErrorMap.computeIfAbsent(topicName, k -> new Rate());
        adder.recordEvent(1);
        accessTopic(topicName);
    }

    public void recordReadOffloadRate(String topicName, int size) {
        if (topicName == null) {
            return;
        }
        Rate rate = readOffloadDataRateMap.computeIfAbsent(topicName, k -> new Rate());
        rate.recordEvent(size);
        accessTopic(topicName);
    }

    public void recordReadOffloadIndexLatency(String topicName, long latency, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        StatsBuckets statsBuckets = readOffloadIndexLatencyBucketsMap.computeIfAbsent(topicName,
                k -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        statsBuckets.addValue(unit.toMicros(latency));
        accessTopic(topicName);
    }

    public void recordReadOffloadDataLatency(String topicName, long latency, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        StatsBuckets statsBuckets = readOffloadDataLatencyBucketsMap.computeIfAbsent(topicName,
                k -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        statsBuckets.addValue(unit.toMicros(latency));
        accessTopic(topicName);
    }

}
