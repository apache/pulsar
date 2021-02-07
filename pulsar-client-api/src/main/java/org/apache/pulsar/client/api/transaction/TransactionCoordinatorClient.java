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
package org.apache.pulsar.client.api.transaction;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Transaction coordinator client.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TransactionCoordinatorClient extends Closeable {

    /**
     * Default transaction ttl in mills.
     */
    long DEFAULT_TXN_TTL_MS = 60000L;

    /**
     * State of the transaction coordinator client.
     */
    enum State {
        NONE,
        STARTING,
        READY,
        CLOSING,
        CLOSED
    }

    /**
     * Start transaction meta store client.
     *
     * <p>This will create connections to transaction meta store service.
     *
     * @throws TransactionCoordinatorClientException exception occur while start
     */
    void start() throws TransactionCoordinatorClientException;

    /**
     * Start transaction meta store client asynchronous.
     *
     * <p>This will create connections to transaction meta store service.
     *
     * @return a future represents the result of start transaction meta store
     */
    CompletableFuture<Void> startAsync();

    /**
     * Close the transaction meta store client asynchronous.
     *
     * @return a future represents the result of close transaction meta store
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Create a new transaction.
     *
     * @return {@link TxnID} as the identifier for identifying the transaction.
     */
    TxnID newTransaction() throws TransactionCoordinatorClientException;

    /**
     * Create a new transaction asynchronously.
     *
     * @return a future represents the result of creating a new transaction.
     *         it returns {@link TxnID} as the identifier for identifying the
     *         transaction.
     */
    CompletableFuture<TxnID> newTransactionAsync();

    /**
     * Create a new transaction.
     *
     * @param timeout timeout for new transaction
     * @param unit time unit for new transaction
     *
     * @return {@link TxnID} as the identifier for identifying the transaction.
     */
    TxnID newTransaction(long timeout, TimeUnit unit) throws TransactionCoordinatorClientException;

    /**
     * Create a new transaction asynchronously.
     *
     * @param timeout timeout for new transaction
     * @param unit time unit for new transaction
     *
     * @return a future represents the result of creating a new transaction.
     *         it returns {@link TxnID} as the identifier for identifying the
     *         transaction.
     */
    CompletableFuture<TxnID> newTransactionAsync(long timeout, TimeUnit unit);

    /**
     * Add publish partition to txn.
     *
     * @param txnID txn id which add partitions to.
     * @param partitions partitions add to the txn.
     */
    void addPublishPartitionToTxn(TxnID txnID, List<String> partitions) throws TransactionCoordinatorClientException;

    /**
     * Add publish partition to txn asynchronously.
     *
     * @param txnID txn id which add partitions to.
     * @param partitions partitions add to the txn.
     *
     * @return a future represents the result of add publish partition to txn.
     */
    CompletableFuture<Void> addPublishPartitionToTxnAsync(TxnID txnID, List<String> partitions);

    /**
     * Add ack subscription to txn.
     *
     * @param txnID transaction id
     * @param topic topic name
     * @param subscription subscription name
     * @throws TransactionCoordinatorClientException while transaction is conflict
     */
    void addSubscriptionToTxn(TxnID txnID, String topic, String subscription)
            throws TransactionCoordinatorClientException;

    /**
     * Add ack subscription to txn asynchronously.
     *
     * @param txnID transaction id
     * @param topic topic name
     * @param subscription subscription name
     * @return the future of the result
     */
    CompletableFuture<Void> addSubscriptionToTxnAsync(TxnID txnID, String topic, String subscription);

    /**
     * Commit txn.
     * @param txnID txn id to commit.
     */
    void commit(TxnID txnID) throws TransactionCoordinatorClientException;

    /**
     * Commit txn asynchronously.
     * @param txnID txn id to commit.
     * @return a future represents the result of commit txn.
     */
    CompletableFuture<Void> commitAsync(TxnID txnID);

    /**
     * Abort txn.
     * @param txnID txn id to abort.
     */
    void abort(TxnID txnID) throws TransactionCoordinatorClientException;

    /**
     * Abort txn asynchronously.
     * @param txnID txn id to abort.
     * @return a future represents the result of abort txn.
     */
    CompletableFuture<Void> abortAsync(TxnID txnID);

    /**
     * Get current state of the transaction meta store.
     *
     * @return current state {@link State} of the transaction meta store
     */
    State getState();
}
