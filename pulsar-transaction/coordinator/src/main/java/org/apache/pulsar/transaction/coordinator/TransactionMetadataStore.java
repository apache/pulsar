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

import com.google.common.annotations.Beta;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

/**
 * A store for storing all the transaction metadata.
 */
@Beta
public interface TransactionMetadataStore {

    /**
     * Query the {@link TxnStatus} of a given transaction <tt>txnid</tt>.
     *
     * @param txnid transaction id
     * @return a future represents the result of this operation.
     *         it returns {@link TxnStatus} of the given transaction.
     */
    default CompletableFuture<TxnStatus> getTxnStatus(TxnID txnid) {
        return getTxnMeta(txnid).thenApply(txnMeta -> txnMeta.status());
    }

    /**
     * Query the {@link TxnMeta} of a given transaction <tt>txnid</tt>.
     *
     * @param txnid transaction id
     * @return a future represents the result of this operation.
     *         it returns {@link TxnMeta} of the given transaction.
     */
    CompletableFuture<TxnMeta> getTxnMeta(TxnID txnid);

    /**
     * Create a new transaction in the transaction metadata store.
     *
     * @return a future represents the result of creating a new transaction.
     *         it returns {@link TxnID} as the identifier for identifying the
     *         transaction.
     */
    CompletableFuture<TxnID> newTransaction();

    /**
     * Add the produced partitions to transaction identified by <tt>txnid</tt>.
     *
     * @param txnid transaction id
     * @param partitions the list of partitions that a transaction produces to
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> addProducedPartitionToTxn(
        TxnID txnid, List<String> partitions);

    /**
     * Add the acked partitions to transaction identified by <tt>txnid</tt>.
     *
     * @param txnid transaction id
     * @param partitions the list of partitions that a transaction acknowledge to
     * @return a future represents the result of the operation
     */
    CompletableFuture<Void> addAckedPartitionToTxn(
        TxnID txnid, List<String> partitions);

    /**
     * Update the transaction from <tt>expectedStatus</tt> to <tt>newStatus</tt>.
     *
     * <p>If the current transaction status is not <tt>expectedStatus</tt>, the
     * update will be failed.
     *
     * @param newStatus the new txn status that the transaction should be updated to
     * @param expectedStatus the expected status that the transaction should be
     * @return a future represents the result of the operation
     */
    CompletableFuture<Void> updateTxnStatus(
        TxnID txnid, TxnStatus newStatus, TxnStatus expectedStatus);

    /**
     * Close the transaction metadata store.
     */
    CompletableFuture<Void> closeAsync();

}
