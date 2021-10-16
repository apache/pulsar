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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;

public class ManagedLedgerMBeanImpl implements ManagedLedgerMXBean {

    public static final long[] ENTRY_LATENCY_BUCKETS_USEC = { 500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 1000_000 };
    public static final long[] ENTRY_SIZE_BUCKETS_BYTES = { 128, 512, 1024, 2048, 4096, 16_384, 102_400, 1_232_896 };

    private final ManagedLedgerImpl managedLedger;

    private final Rate addEntryOps = new Rate();
    private final Rate addEntryWithReplicasOps = new Rate();
    private final Rate addEntryOpsFailed = new Rate();
    private final Rate readEntriesOps = new Rate();
    private final Rate readEntriesOpsFailed = new Rate();
    private final Rate markDeleteOps = new Rate();

    private final LongAdder dataLedgerOpenOp = new LongAdder();
    private final LongAdder dataLedgerCloseOp = new LongAdder();
    private final LongAdder dataLedgerCreateOp = new LongAdder();
    private final LongAdder dataLedgerDeleteOp = new LongAdder();
    private final LongAdder cursorLedgerOpenOp = new LongAdder();
    private final LongAdder cursorLedgerCloseOp = new LongAdder();
    private final LongAdder cursorLedgerCreateOp = new LongAdder();
    private final LongAdder cursorLedgerDeleteOp = new LongAdder();

    // addEntryLatencyStatsUsec measure total latency including time entry spent while waiting in queue
    private final StatsBuckets addEntryLatencyStatsUsec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
    // ledgerAddEntryLatencyStatsUsec measure latency to persist entry into ledger
    private final StatsBuckets ledgerAddEntryLatencyStatsUsec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
    private final StatsBuckets ledgerSwitchLatencyStatsUsec = new StatsBuckets(ENTRY_LATENCY_BUCKETS_USEC);
    private final StatsBuckets entryStats = new StatsBuckets(ENTRY_SIZE_BUCKETS_BYTES);

    public ManagedLedgerMBeanImpl(ManagedLedgerImpl managedLedger) {
        this.managedLedger = managedLedger;
    }

    public void refreshStats(long period, TimeUnit unit) {
        double seconds = unit.toMillis(period) / 1000.0;
        addEntryOps.calculateRate(seconds);
        addEntryWithReplicasOps.calculateRate(seconds);
        addEntryOpsFailed.calculateRate(seconds);
        readEntriesOps.calculateRate(seconds);
        readEntriesOpsFailed.calculateRate(seconds);
        markDeleteOps.calculateRate(seconds);

        addEntryLatencyStatsUsec.refresh();
        ledgerAddEntryLatencyStatsUsec.refresh();
        ledgerSwitchLatencyStatsUsec.refresh();
        entryStats.refresh();
    }

    public void addAddEntrySample(long size) {
        addEntryOps.recordEvent(size);
        entryStats.addValue(size);
        addEntryWithReplicasOps.recordEvent(size * managedLedger.getConfig().getWriteQuorumSize());
    }

    public void addMarkDeleteOp() {
        markDeleteOps.recordEvent();
    }

    public void recordAddEntryError() {
        addEntryOpsFailed.recordEvent();
    }

    public void recordReadEntriesError() {
        readEntriesOpsFailed.recordEvent();
    }

    public void addAddEntryLatencySample(long latency, TimeUnit unit) {
        addEntryLatencyStatsUsec.addValue(unit.toMicros(latency));
    }

    public void addLedgerAddEntryLatencySample(long latency, TimeUnit unit) {
        ledgerAddEntryLatencyStatsUsec.addValue(unit.toMicros(latency));
    }

    public void addLedgerSwitchLatencySample(long latency, TimeUnit unit) {
        ledgerSwitchLatencyStatsUsec.addValue(unit.toMicros(latency));
    }

    public void addReadEntriesSample(int count, long totalSize) {
        readEntriesOps.recordMultipleEvents(count, totalSize);
    }

    public void startDataLedgerOpenOp() {
        dataLedgerOpenOp.increment();
    }

    public void endDataLedgerOpenOp() {
        dataLedgerOpenOp.decrement();
    }

    public void startDataLedgerCloseOp() {
        dataLedgerCloseOp.increment();
    }

    public void endDataLedgerCloseOp() {
        dataLedgerCloseOp.decrement();
    }

    public void startDataLedgerCreateOp() {
        dataLedgerCreateOp.increment();
    }

    public void endDataLedgerCreateOp() {
        dataLedgerCreateOp.decrement();
    }

    public void startDataLedgerDeleteOp() {
        dataLedgerDeleteOp.increment();
    }

    public void endDataLedgerDeleteOp() {
        dataLedgerDeleteOp.decrement();
    }

    public void startCursorLedgerOpenOp() {
        cursorLedgerOpenOp.increment();
    }

    public void endCursorLedgerOpenOp() {
        cursorLedgerOpenOp.decrement();
    }

    public void startCursorLedgerCloseOp() {
        cursorLedgerCloseOp.increment();
    }

    public void endCursorLedgerCloseOp() {
        cursorLedgerCloseOp.decrement();
    }

    public void startCursorLedgerCreateOp() {
        cursorLedgerCreateOp.increment();
    }

    public void endCursorLedgerCreateOp() {
        cursorLedgerCreateOp.decrement();
    }

    public void startCursorLedgerDeleteOp() {
        cursorLedgerDeleteOp.increment();
    }

    public void endCursorLedgerDeleteOp() {
        cursorLedgerDeleteOp.decrement();
    }

    @Override
    public String getName() {
        return managedLedger.getName();
    }

    @Override
    public double getAddEntryMessagesRate() {
        return addEntryOps.getRate();
    }

    @Override
    public double getAddEntryBytesRate() {
        return addEntryOps.getValueRate();
    }

    @Override
    public double getAddEntryWithReplicasBytesRate() {
        return addEntryWithReplicasOps.getValueRate();
    }

    @Override
    public double getReadEntriesRate() {
        return readEntriesOps.getRate();
    }

    @Override
    public double getReadEntriesBytesRate() {
        return readEntriesOps.getValueRate();
    }

    @Override
    public long getAddEntrySucceed() {
        return addEntryOps.getCount();
    }

    @Override
    public long getAddEntryErrors() {
        return addEntryOpsFailed.getCount();
    }

    @Override
    public long getReadEntriesSucceeded() {
        return readEntriesOps.getCount();
    }

    @Override
    public long getReadEntriesErrors() {
        return readEntriesOpsFailed.getCount();
    }

    @Override
    public double getMarkDeleteRate() {
        return markDeleteOps.getRate();
    }

    @Override
    public double getEntrySizeAverage() {
        return entryStats.getAvg();
    }

    @Override
    public long[] getEntrySizeBuckets() {
        return entryStats.getBuckets();
    }

    @Override
    public double getAddEntryLatencyAverageUsec() {
        return addEntryLatencyStatsUsec.getAvg();
    }

    @Override
    public long[] getAddEntryLatencyBuckets() {
        return addEntryLatencyStatsUsec.getBuckets();
    }

    @Override
    public double getLedgerAddEntryLatencyAverageUsec() {
        return ledgerAddEntryLatencyStatsUsec.getAvg();
    }

    @Override
    public long[] getLedgerAddEntryLatencyBuckets() {
        return ledgerAddEntryLatencyStatsUsec.getBuckets();
    }

    @Override
    public long[] getLedgerSwitchLatencyBuckets() {
        return ledgerSwitchLatencyStatsUsec.getBuckets();
    }

    @Override
    public StatsBuckets getInternalAddEntryLatencyBuckets() {
        return addEntryLatencyStatsUsec;
    }

    @Override
    public StatsBuckets getInternalLedgerAddEntryLatencyBuckets() {
        return ledgerAddEntryLatencyStatsUsec;
    }

    @Override
    public StatsBuckets getInternalEntrySizeBuckets() {
        return entryStats;
    }

    @Override
    public double getLedgerSwitchLatencyAverageUsec() {
        return ledgerSwitchLatencyStatsUsec.getAvg();
    }

    @Override
    public long getStoredMessagesSize() {
        return managedLedger.getTotalSize() * managedLedger.getConfig().getWriteQuorumSize();
    }

    @Override
    public long getStoredMessagesLogicalSize() {
        return managedLedger.getTotalSize();
    }

    @Override
    public long getNumberOfMessagesInBacklog() {
        long count = 0;

        for (ManagedCursor cursor : managedLedger.getCursors()) {
            count += cursor.getNumberOfEntriesInBacklog(false);
        }

        return count;
    }

    @Override
    public PendingBookieOpsStats getPendingBookieOpsStats() {
        PendingBookieOpsStats result = new PendingBookieOpsStats();
        result.dataLedgerOpenOp = dataLedgerOpenOp.longValue();
        result.dataLedgerCloseOp = dataLedgerCloseOp.longValue();
        result.dataLedgerCreateOp = dataLedgerCreateOp.longValue();
        result.dataLedgerDeleteOp = dataLedgerDeleteOp.longValue();
        result.cursorLedgerOpenOp = cursorLedgerOpenOp.longValue();
        result.cursorLedgerCloseOp = cursorLedgerCloseOp.longValue();
        result.cursorLedgerCreateOp = cursorLedgerCreateOp.longValue();
        result.cursorLedgerDeleteOp = cursorLedgerDeleteOp.longValue();
        return result;
    }

}
