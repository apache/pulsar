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
package org.apache.pulsar.transaction.coordinator.impl;

import lombok.Data;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

/**
 * Transaction metadata store stats.
 */
@Data
public class TransactionMetadataStoreStats {

    private static final long[] TRANSACTION_EXECUTION_BUCKETS = { 10, 20, 50, 100, 500, 1_000, 5_000,
            15_000, 30_000, 60_000, 300_000, 1_500_000, 3_000_000 };

    /** The transaction coordinatorId. */
    private long coordinatorId;

    /** The active transactions. */
    private int actives;

    /** The committed transaction count of this transaction coordinator. */
    public long committedCount;

    /** The aborted transaction count of this transaction coordinator. */
    public long abortedCount;

    /** The created transaction count of this transaction coordinator. */
    public long createdCount;

    /** The append transaction op log count of this transaction coordinator. */
    public long appendLogCount;

    /** The timeout out transaction count of this transaction coordinator. */
    public long timeoutCount;

    /** The transaction execution latency. */
    public StatsBuckets executionLatencyBuckets = new StatsBuckets(TRANSACTION_EXECUTION_BUCKETS);

    public void addTransactionExecutionLatencySample(long latency) {
        executionLatencyBuckets.addValue(latency);
    }

}
