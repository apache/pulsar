/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.stats.prometheus;

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

public class AggregatedNamespaceStats {
    public int topicsCount;
    public int subscriptionsCount;
    public int producersCount;
    public int consumersCount;
    public double rateIn;
    public double rateOut;
    public double throughputIn;
    public double throughputOut;

    public long storageSize;
    public long msgBacklog;

    public StatsBuckets storageWriteLatencyBuckets = new StatsBuckets(
            ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC);
    public StatsBuckets entrySizeBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES);

    public double storageWriteRate;
    public double storageReadRate;

    public Map<String, AggregatedReplicationStats> replicationStats = new HashMap<>();

    public void reset() {
        topicsCount = 0;
        subscriptionsCount = 0;
        producersCount = 0;
        consumersCount = 0;
        rateIn = 0;
        rateOut = 0;
        throughputIn = 0;
        throughputOut = 0;

        storageSize = 0;
        msgBacklog = 0;
        storageWriteRate = 0;
        storageReadRate = 0;

        replicationStats.clear();
        storageWriteLatencyBuckets.reset();
        entrySizeBuckets.reset();
    }
}
