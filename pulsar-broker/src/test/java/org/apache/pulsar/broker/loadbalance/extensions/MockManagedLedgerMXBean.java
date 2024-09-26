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
package org.apache.pulsar.broker.loadbalance.extensions;

import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

public class MockManagedLedgerMXBean extends ManagedLedgerMBeanImpl {

    public MockManagedLedgerMXBean() {
        super(null);
    }

    @Override
    public String getName() {
        return "";
    }

    @Override
    public long getStoredMessagesSize() {
        return 0;
    }

    @Override
    public long getStoredMessagesLogicalSize() {
        return 0;
    }

    @Override
    public long getNumberOfMessagesInBacklog() {
        return 0;
    }

    @Override
    public double getAddEntryMessagesRate() {
        return 0;
    }

    @Override
    public double getAddEntryBytesRate() {
        return 0;
    }

    @Override
    public long getAddEntryBytesTotal() {
        return 0;
    }

    @Override
    public double getAddEntryWithReplicasBytesRate() {
        return 0;
    }

    @Override
    public long getAddEntryWithReplicasBytesTotal() {
        return 0;
    }

    @Override
    public double getReadEntriesRate() {
        return 0;
    }

    @Override
    public double getReadEntriesBytesRate() {
        return 0;
    }

    @Override
    public long getReadEntriesBytesTotal() {
        return 0;
    }

    @Override
    public double getMarkDeleteRate() {
        return 0;
    }

    @Override
    public long getMarkDeleteTotal() {
        return 0;
    }

    @Override
    public long getAddEntrySucceed() {
        return 0;
    }

    @Override
    public long getAddEntrySucceedTotal() {
        return 0;
    }

    @Override
    public long getAddEntryErrors() {
        return 0;
    }

    @Override
    public long getAddEntryErrorsTotal() {
        return 0;
    }

    @Override
    public long getEntriesReadTotalCount() {
        return 0;
    }

    @Override
    public long getReadEntriesSucceeded() {
        return 0;
    }

    @Override
    public long getReadEntriesSucceededTotal() {
        return 0;
    }

    @Override
    public long getReadEntriesErrors() {
        return 0;
    }

    @Override
    public long getReadEntriesErrorsTotal() {
        return 0;
    }

    @Override
    public double getReadEntriesOpsCacheMissesRate() {
        return 0;
    }

    @Override
    public long getReadEntriesOpsCacheMissesTotal() {
        return 0;
    }

    @Override
    public double getEntrySizeAverage() {
        return 0;
    }

    @Override
    public long[] getEntrySizeBuckets() {
        return new long[0];
    }

    @Override
    public double getAddEntryLatencyAverageUsec() {
        return 0;
    }

    @Override
    public long[] getAddEntryLatencyBuckets() {
        return new long[0];
    }

    @Override
    public long[] getLedgerSwitchLatencyBuckets() {
        return new long[0];
    }

    @Override
    public double getLedgerSwitchLatencyAverageUsec() {
        return 0;
    }

    @Override
    public StatsBuckets getInternalAddEntryLatencyBuckets() {
        return new StatsBuckets(500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 1000_000);
    }

    @Override
    public StatsBuckets getInternalEntrySizeBuckets() {
        return new StatsBuckets(128, 512, 1024, 2048, 4096, 16_384, 102_400, 1_048_576);
    }

    @Override
    public PendingBookieOpsStats getPendingBookieOpsStats() {
        return new PendingBookieOpsStats();
    }

    @Override
    public double getLedgerAddEntryLatencyAverageUsec() {
        return 0;
    }

    @Override
    public long[] getLedgerAddEntryLatencyBuckets() {
        return new long[0];
    }

    @Override
    public StatsBuckets getInternalLedgerAddEntryLatencyBuckets() {
        return new StatsBuckets(500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 1000_000);
    }
}
