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
package org.apache.pulsar.broker.stats.prometheus;

import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

public class ManagedLedgerStats {

    long storageSize;
    long backlogSize;
    long offloadedStorageUsed;
    long storageLogicalSize;

    StatsBuckets storageWriteLatencyBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC);
    StatsBuckets storageLedgerWriteLatencyBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC);
    StatsBuckets entrySizeBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES);

    double storageWriteRate;
    double storageReadRate;

    public void reset() {
        storageSize = 0;
        storageWriteRate = 0;
        storageReadRate = 0;
        backlogSize = 0;
        offloadedStorageUsed = 0;
        storageLogicalSize = 0;

        storageWriteLatencyBuckets.reset();
        storageLedgerWriteLatencyBuckets.reset();
        entrySizeBuckets.reset();
    }
}
