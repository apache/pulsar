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


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.LedgerOffloaderMXBean;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

public class LedgerOffloaderMXBeanImpl implements LedgerOffloaderMXBean {

    private final int DEFAULT_SIZE = 4;
    public static final long[] READ_ENTRY_LATENCY_BUCKETS_USEC = {500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 1000_000};

    private final String driverName;

    // offloadTimeMap record the time cost by one round offload
    private final ConcurrentHashMap<String, LongAdder> offloadTimeMap = new ConcurrentHashMap<>(DEFAULT_SIZE);
    // offloadErrorMap record error ocurred
    private final ConcurrentHashMap<String, LongAdder> offloadErrorMap = new ConcurrentHashMap<>(DEFAULT_SIZE);
    // offloadRateMap record the offload rate
    private final ConcurrentHashMap<String, LongAdder> offloadBytesMap = new ConcurrentHashMap<>(DEFAULT_SIZE);


    // readLedgerLatencyBucketsMap record the time cost by ledger read
    private final ConcurrentHashMap<String, StatsBuckets> readLedgerLatencyBucketsMap = new ConcurrentHashMap<>(
            DEFAULT_SIZE);
    // writeToStorageLatencyBucketsMap record the time cost by write to storage
    private final ConcurrentHashMap<String, StatsBuckets> writeToStorageLatencyBucketsMap = new ConcurrentHashMap<>(
            DEFAULT_SIZE);
    // writeToStorageErrorMap record the error occurred in write storage
    private final ConcurrentHashMap<String, LongAdder> writeToStorageErrorMap = new ConcurrentHashMap<>();


    // streamingWriteToStorageRateMap and streamingWriteToStorageErrorMap is for streamingOffload
    private final ConcurrentHashMap<String, LongAdder> streamingWriteToStorageBytesMap = new ConcurrentHashMap<>(
            DEFAULT_SIZE);
    private final ConcurrentHashMap<String, LongAdder> streamingWriteToStorageErrorMap = new ConcurrentHashMap<>(
            DEFAULT_SIZE);

    // readOffloadIndexLatencyBucketsMap and readOffloadDataLatencyBucketsMap are latency metrics about index and data
    // readOffloadDataRateMap and readOffloadErrorMap is for reading offloaded data
    private final ConcurrentHashMap<String, StatsBuckets> readOffloadIndexLatencyBucketsMap = new ConcurrentHashMap<>(
            DEFAULT_SIZE);
    private final ConcurrentHashMap<String, StatsBuckets> readOffloadDataLatencyBucketsMap = new ConcurrentHashMap<>(
            DEFAULT_SIZE);
    private final ConcurrentHashMap<String, LongAdder> readOffloadDataBytesMap = new ConcurrentHashMap<>(DEFAULT_SIZE);
    private final ConcurrentHashMap<String, LongAdder> readOffloadErrorMap = new ConcurrentHashMap<>(DEFAULT_SIZE);

    public LedgerOffloaderMXBeanImpl(String driverName) {
        this.driverName = driverName;
    }

    @Override
    public String getDriverName() {
        return this.driverName;
    }

    @Override
    public long getOffloadTime(String topic) {
        LongAdder offloadTime = this.offloadTimeMap.remove(topic);
        return null == offloadTime ? 0L : offloadTime.sum();
    }

    @Override
    public long getOffloadErrors(String topic) {
        LongAdder errors = this.offloadErrorMap.remove(topic);
        return null == errors ? 0L : errors.sum();
    }

    @Override
    public long getOffloadBytes(String topic) {
        LongAdder offloadBytes = this.offloadBytesMap.remove(topic);
        return null == offloadBytes ? 0L : offloadBytes.sum();
    }

    @Override
    public StatsBuckets getReadLedgerLatencyBuckets(String topic) {
        return this.readLedgerLatencyBucketsMap.remove(topic);
    }

    @Override
    public StatsBuckets getWriteToStorageLatencyBuckets(String topic) {
        return this.writeToStorageLatencyBucketsMap.remove(topic);
    }

    @Override
    public long getWriteToStorageErrors(String topic) {
        LongAdder errors = this.writeToStorageErrorMap.remove(topic);
        return null == errors ? 0L : errors.sum();
    }

    @Override
    public long getStreamingWriteToStorageBytes(String topic) {
        LongAdder bytes = this.streamingWriteToStorageBytesMap.remove(topic);
        return null == bytes ? 0L : bytes.sum();
    }

    @Override
    public long getStreamingWriteToStorageErrors(String topic) {
        LongAdder errors = this.streamingWriteToStorageErrorMap.remove(topic);
        return null == errors ? 0L : errors.sum();
    }


    @Override
    public StatsBuckets getReadOffloadIndexLatencyBuckets(String topic) {
        return this.readOffloadIndexLatencyBucketsMap.remove(topic);
    }

    @Override
    public StatsBuckets getReadOffloadDataLatencyBuckets(String topic) {
        return this.readOffloadDataLatencyBucketsMap.remove(topic);
    }

    @Override
    public long getReadOffloadBytes(String topic) {
        LongAdder bytes = this.readOffloadDataBytesMap.remove(topic);
        return null == bytes ? 0L : bytes.sum();
    }

    @Override
    public long getReadOffloadErrors(String topic) {
        LongAdder errors = this.readOffloadErrorMap.remove(topic);
        return null == errors ? 0L : errors.sum();
    }

    public void recordOffloadTime(String topicName, long time, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        LongAdder adder = offloadTimeMap.computeIfAbsent(topicName, k -> new LongAdder());
        adder.add(unit.toMillis(time));
    }


    public void recordOffloadError(String topicName) {
        if (topicName == null) {
            return;
        }
        LongAdder adder = offloadErrorMap.computeIfAbsent(topicName, k -> new LongAdder());
        adder.add(1L);
    }

    public void recordOffloadBytes(String topicName, int size) {
        if (topicName == null) {
            return;
        }
        LongAdder adder = offloadBytesMap.computeIfAbsent(topicName, k -> new LongAdder());
        adder.add(size);
    }

    public void recordReadLedgerLatency(String topicName, long latency, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        StatsBuckets statsBuckets = readLedgerLatencyBucketsMap.computeIfAbsent(topicName,
                k -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordWriteToStorageLatency(String topicName, long latency, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        StatsBuckets statsBuckets = writeToStorageLatencyBucketsMap.computeIfAbsent(topicName,
                k -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordWriteToStorageError(String topicName) {
        if (topicName == null) {
            return;
        }
        LongAdder adder = writeToStorageErrorMap.computeIfAbsent(topicName, k -> new LongAdder());
        adder.add(1L);
    }

    public void recordStreamingWriteToStorageBytes(String topicName, int size) {
        if (topicName == null) {
            return;
        }
        LongAdder adder = streamingWriteToStorageBytesMap.computeIfAbsent(topicName, k -> new LongAdder());
        adder.add(size);
    }

    public void recordStreamingWriteToStorageError(String topicName) {
        if (topicName == null) {
            return;
        }
        LongAdder adder = streamingWriteToStorageErrorMap.computeIfAbsent(topicName, k -> new LongAdder());
        adder.add(1L);
    }


    public void recordReadOffloadError(String topicName) {
        if (topicName == null) {
            return;
        }
        LongAdder adder = readOffloadErrorMap.computeIfAbsent(topicName, k -> new LongAdder());
        adder.add(1L);
    }

    public void recordReadOffloadBytes(String topicName, int size) {
        if (topicName == null) {
            return;
        }
        LongAdder adder = readOffloadDataBytesMap.computeIfAbsent(topicName, k -> new LongAdder());
        adder.add(size);
    }

    public void recordReadOffloadIndexLatency(String topicName, long latency, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        StatsBuckets statsBuckets = readOffloadIndexLatencyBucketsMap.computeIfAbsent(topicName,
                k -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordReadOffloadDataLatency(String topicName, long latency, TimeUnit unit) {
        if (topicName == null) {
            return;
        }
        StatsBuckets statsBuckets = readOffloadDataLatencyBucketsMap.computeIfAbsent(topicName,
                k -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        statsBuckets.addValue(unit.toMicros(latency));
    }
}
