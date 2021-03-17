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

/**
 * Transaction metadata store stats.
 */
@Data
public class TransactionMetadataStoreStats {
    /** The transaction coordinatorId. */
    private long transactionCoordinatorId;

    /** The transaction max sequenceId. */
    private long transactionSequenceId;

    /** The active transactions. */
    private int activeTransactions;

    /** The low water mark of this transaction coordinator. */
    private long lowWaterMark;

    /** The commit transaction count of this transaction coordinator. */
    public long commitTransactionCount;

    /** The abort transaction count of this transaction coordinator. */
    public long abortTransactionCount;

    /** The create transaction count of this transaction coordinator. */
    public long createTransactionCount;

    /** The add produced partition count transaction coordinator. */
    public long addProducedPartitionCount;

    /** The add transaction count of this transaction coordinator. */
    public long addAckedPartitionCount;

    /** The timeout out transaction count of this transaction coordinator. */
    public long transactionTimeoutCount;
}
