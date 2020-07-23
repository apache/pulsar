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

import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;

/**
 * A store for storing all the transaction metadata.
 */
@Beta
public interface TransactionMetadataStore {

    /**
     * Query the {@link TxnStatus} of a given transaction <tt>txnId</tt>.
     *
     * @param txnID {@link TxnID} for get transaction status
     * @return a future represents the result of this operation.
     *         it returns {@link TxnStatus} of the given transaction.
     */
    default CompletableFuture<TxnStatus> getTxnStatusAsync(TxnID txnID) {
        return getTxnMetaAsync(txnID).thenApply(TxnMeta::status);
    }

    /**
     * Query the {@link TxnMeta} of a given transaction <tt>txnid</tt>.
     *
     * @param txnID {@link TxnID} for get transaction metadata
     * @return a future represents the result of this operation.
     *         it returns {@link TxnMeta} of the given transaction.
     */
    CompletableFuture<TxnMeta> getTxnMetaAsync(TxnID txnID);

    /**
     * Create a new transaction in the transaction metadata store.
     *
     * @param timeoutInMills the timeout duration of the transaction in mills
     * @return a future represents the result of creating a new transaction.
     *         it returns {@link TxnID} as the identifier for identifying the
     *         transaction.
     */
    CompletableFuture<TxnID> newTransactionAsync(long timeoutInMills);

    /**
     * Add the produced partitions to transaction identified by <tt>txnId</tt>.
     *
     * @param txnID {@link TxnID} for add produced partition to transaction
     * @param partitions the list of partitions that a transaction produces to
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> addProducedPartitionToTxnAsync(
        TxnID txnID, List<String> partitions);

    /**
     * Add the acked subscriptions to transaction identified by <tt>txnId</tt>.
     *
     * @param txnID {@link TxnID} for add acked subscription
     * @param txnSubscriptions the list of subscriptions that a transaction ack to
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> addAckedPartitionToTxnAsync(
            TxnID txnID, List<TxnSubscription> txnSubscriptions);

    /**
     * Update the transaction from <tt>expectedStatus</tt> to <tt>newStatus</tt>.
     *
     * <p>If the current transaction status is not <tt>expectedStatus</tt>, the
     * update will be failed.
     *
     * @param txnID {@link TxnID} for update txn status
     * @param newStatus the new txn status that the transaction should be updated to
     * @param expectedStatus the expected status that the transaction should be
     * @return a future represents the result of the operation
     */
    CompletableFuture<Void> updateTxnStatusAsync(
        TxnID txnID, TxnStatus newStatus, TxnStatus expectedStatus);

    /**
     * Get the transaction coordinator id.
     * @return transaction coordinator id
     */
    TransactionCoordinatorID getTransactionCoordinatorID();

    /**
     * Close the transaction metadata store.
     *
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> closeAsync();
}
