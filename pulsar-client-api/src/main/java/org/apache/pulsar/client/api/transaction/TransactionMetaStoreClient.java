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

import org.apache.pulsar.transaction.impl.common.TxnID;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Transaction meta store client.
 */
public interface TransactionMetaStoreClient extends Closeable {

    /**
     * Default transaction ttl in mills.
     */
    long DEFAULT_TXN_TTL_MS = 60000L;

    /**
     * Default pending ops.
     */
    int DEFAULT_MAX_PADDING_OPS = 1000;

    /**
     * Block if reach max padding ops, default is true.
     */
    boolean BLOCK_IF_REACH_MAX_PADDING_OPS = true;

    enum State {
        NONE,
        STARTING,
        READY,
        CLOSING,
        CLOSED
    }

    /**
     * Start transaction meta store client.
     * <p>This will create connections to transaction meta store service.
     *
     * @throws TransactionMetaStoreClientException exception occur while start
     */
    void start() throws TransactionMetaStoreClientException;

    /**
     * Start transaction meta store client asynchronous.
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
    TxnID newTransaction() throws TransactionMetaStoreClientException;

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
    TxnID newTransaction(long timeout, TimeUnit unit) throws TransactionMetaStoreClientException;

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
     * Get current state of the transaction meta store.
     *
     * @return current state {@link State} of the transaction meta store
     */
    State getState();
}
