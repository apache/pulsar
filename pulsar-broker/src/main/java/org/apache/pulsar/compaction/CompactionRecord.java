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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.Getter;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;

public class CompactionRecord {

    public static final long[] WRITE_LATENCY_BUCKETS_USEC = { 500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 1000_000 };

    @Getter
    private long lastCompactionRemovedEventCount = 0L;
    @Getter
    private long lastCompactionSucceedTimestamp = 0L;
    @Getter
    private long lastCompactionFailedTimestamp = 0L;
    @Getter
    private long lastCompactionDurationTimeInMills = 0L;

    private LongAdder lastCompactionRemovedEventCountOp = new LongAdder();
    private long lastCompactionStartTimeOp;

    private final LongAdder compactionRemovedEventCount = new LongAdder();
    private final LongAdder compactionSucceedCount = new LongAdder();
    private final LongAdder compactionFailedCount = new LongAdder();
    private final LongAdder compactionDurationTimeInMills = new LongAdder();
    public final StatsBuckets writeLatencyStats = new StatsBuckets(WRITE_LATENCY_BUCKETS_USEC);
    public final Rate writeRate = new Rate();
    public final Rate readRate = new Rate();

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

    public long getCompactionRemovedEventCount() {
        return compactionRemovedEventCount.longValue();
    }

    public long getCompactionSucceedCount() {
        return compactionSucceedCount.longValue();
    }

    public long getCompactionFailedCount() {
        return compactionFailedCount.longValue();
    }

    public long getCompactionDurationTimeInMills() {
        return compactionDurationTimeInMills.longValue();
    }

    public long[] getCompactionLatencyBuckets() {
        writeLatencyStats.refresh();
        return writeLatencyStats.getBuckets();
    }

    public StatsBuckets getCompactionLatencyStats() {
        return writeLatencyStats;
    }

    public double getCompactionReadThroughput() {
        readRate.calculateRate();
        return readRate.getValueRate();
    }

    public double getCompactionWriteThroughput() {
        writeRate.calculateRate();
        return writeRate.getValueRate();
    }
}
