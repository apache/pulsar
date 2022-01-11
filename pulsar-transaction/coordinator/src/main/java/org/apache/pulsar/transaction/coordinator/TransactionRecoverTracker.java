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
package org.apache.pulsar.transaction.coordinator;

import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;

/**
 * This tracker is for transaction metadata store recover handle the different status transaction.
 */
public interface TransactionRecoverTracker {

    /**
     * Handle recover transaction update status.
     * @param sequenceId {@link long} the sequenceId of this transaction.
     * @param txnStatus {@link long} the txn status of this operation.
     */
    void updateTransactionStatus(long sequenceId, TxnStatus txnStatus)
            throws CoordinatorException.InvalidTxnStatusException;

    /**
     * Handle recover transaction in open status.
     * @param sequenceId {@link Long} the sequenceId of this transaction.
     * @param timeout {@link long} the timeout time of this transaction.
     */
    void handleOpenStatusTransaction(long sequenceId, long timeout);

    /**
     * Handle the transaction in open status append to transaction timeout tracker.
     */
    void appendOpenTransactionToTimeoutTracker();

    /**
     * Handle the transaction in committing and aborting status.
     */
    void handleCommittingAndAbortingTransaction();
}
