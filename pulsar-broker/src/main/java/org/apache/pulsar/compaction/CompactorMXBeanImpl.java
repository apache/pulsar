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
package org.apache.pulsar.compaction;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;

public class CompactorMXBeanImpl implements CompactorMXBean {

    private final ConcurrentHashMap<String, CompactionRecord> compactionRecordOps = new ConcurrentHashMap<>();

    public void addCompactionRemovedEvent(String topic) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionRemovedEvent();
    }

    public void addCompactionStartOp(String topic) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionStartOp();
    }

    public void addCompactionEndOp(String topic, boolean succeed) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionEndOp(succeed);
    }

    @Override
    public long getLastCompactionRemovedEventCount(String topic) {
        return compactionRecordOps.getOrDefault(topic, new CompactionRecord()).lastCompactionRemovedEventCount;
    }

    @Override
    public long getLastCompactionSucceedTimestamp(String topic) {
        return compactionRecordOps.getOrDefault(topic, new CompactionRecord()).lastCompactionSucceedTimestamp;
    }

    @Override
    public long getLastCompactionFailedTimestamp(String topic) {
        return compactionRecordOps.getOrDefault(topic, new CompactionRecord()).lastCompactionFailedTimestamp;
    }

    @Override
    public long getLastCompactionDurationTimeInMills(String topic) {
        return compactionRecordOps.getOrDefault(topic, new CompactionRecord()).lastCompactionDurationTimeInMills;
    }

    @Override
    public void removeTopic(String topic) {
        compactionRecordOps.remove(topic);
    }

    @Override
    public long getCompactionRemovedEventCount(String topic) {
        return compactionRecordOps.getOrDefault(topic, new CompactionRecord())
                .compactionRemovedEventCount.longValue();
    }

    @Override
    public long getCompactionSucceedCount(String topic) {
        return compactionRecordOps.getOrDefault(topic, new CompactionRecord())
                .compactionSucceedCount.longValue();
    }

    @Override
    public long getCompactionFailedCount(String topic) {
        return compactionRecordOps.getOrDefault(topic, new CompactionRecord())
                .compactionFailedCount.longValue();
    }

    @Override
    public long getCompactionDurationTimeInMills(String topic) {
        return compactionRecordOps.getOrDefault(topic, new CompactionRecord())
                .compactionDurationTimeInMills.longValue();
    }

    @Override
    public long[] getCompactionLatencyBuckets(String topic) {
        CompactionRecord compactionRecord = compactionRecordOps.getOrDefault(topic, new CompactionRecord());
        compactionRecord.writeLatencyStats.refresh();
        return compactionRecord.writeLatencyStats.getBuckets();
    }

    @Override
    public double getCompactionReadThroughput(String topic) {
        CompactionRecord compactionRecord = compactionRecordOps.getOrDefault(topic, new CompactionRecord());
        compactionRecord.readRate.calculateRate();
        return compactionRecord.readRate.getValueRate();
    }

    @Override
    public double getCompactionWriteThroughput(String topic) {
        CompactionRecord compactionRecord = compactionRecordOps.getOrDefault(topic, new CompactionRecord());
        compactionRecord.writeRate.calculateRate();
        return compactionRecord.writeRate.getValueRate();
    }
    public Set<String> getTopics() {
        return compactionRecordOps.keySet();
    }

    public void reset() {
        compactionRecordOps.values().forEach(CompactionRecord::reset);
    }

    public void addCompactionReadOp(String topic, long readableBytes) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionReadOp(readableBytes);
    }

    public void addCompactionWriteOp(String topic, long writeableBytes) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionWriteOp(writeableBytes);
    }

    public void addCompactionLatencyOp(String topic, long latency, TimeUnit unit) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionLatencyOp(latency, unit);
    }

    static class CompactionRecord {

        public static final long[] WRITE_LATENCY_BUCKETS_USEC = { 500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
                200_000, 1000_000 };

        private long lastCompactionRemovedEventCount = 0L;
        private long lastCompactionSucceedTimestamp = 0L;
        private long lastCompactionFailedTimestamp = 0L;
        private long lastCompactionDurationTimeInMills = 0L;

        private LongAdder lastCompactionRemovedEventCountOp = new LongAdder();
        private long lastCompactionStartTimeOp;

        private final LongAdder compactionRemovedEventCount = new LongAdder();
        private final LongAdder compactionSucceedCount = new LongAdder();
        private final LongAdder compactionFailedCount = new LongAdder();
        private final LongAdder compactionDurationTimeInMills = new LongAdder();
        private final StatsBuckets writeLatencyStats = new StatsBuckets(WRITE_LATENCY_BUCKETS_USEC);
        private final Rate writeRate = new Rate();
        private final Rate readRate = new Rate();

        public void reset() {
            compactionRemovedEventCount.reset();
            compactionSucceedCount.reset();
            compactionFailedCount.reset();
            compactionDurationTimeInMills.reset();
            writeLatencyStats.reset();
        }

        public void addCompactionRemovedEvent() {
            lastCompactionRemovedEventCountOp.increment();
            compactionRemovedEventCount.increment();
        }

        public void addCompactionStartOp() {
            lastCompactionRemovedEventCountOp.reset();
            lastCompactionStartTimeOp = System.currentTimeMillis();
        }

        public void addCompactionEndOp(boolean succeed) {
            lastCompactionDurationTimeInMills = System.currentTimeMillis()
                    - lastCompactionStartTimeOp;
            compactionDurationTimeInMills.add(lastCompactionDurationTimeInMills);
            lastCompactionRemovedEventCount = lastCompactionRemovedEventCountOp.longValue();
            if (succeed) {
                lastCompactionSucceedTimestamp = System.currentTimeMillis();
                compactionSucceedCount.increment();
            } else {
                lastCompactionFailedTimestamp = System.currentTimeMillis();
                compactionFailedCount.increment();
            }
        }

        public void addCompactionReadOp(long readableBytes) {
            readRate.recordEvent(readableBytes);
        }

        public void addCompactionWriteOp(long writeableBytes) {
            writeRate.recordEvent(writeableBytes);
        }

        public void addCompactionLatencyOp(long latency, TimeUnit unit) {
            writeLatencyStats.addValue(unit.toMicros(latency));
        }
    }
}
