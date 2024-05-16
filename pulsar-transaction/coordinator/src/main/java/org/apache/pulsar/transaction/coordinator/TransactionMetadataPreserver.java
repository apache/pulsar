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
package org.apache.pulsar.transaction.coordinator;

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;

/**
 * An interface for persist metadata of aborted txn and recover from a compacted topic.
 */
public interface TransactionMetadataPreserver {
    /**
     * Replay transaction metadata to initialize the terminatedTxnMetaMap.
     */
    void replay() throws PulsarClientException;

    /**
     * Close the transaction metadata preserver.
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Append the transaction metadata to the system topic __terminated_txn_state.
     *
     * @param txnMeta
     * @return
     */
    void append(TxnMeta txnMeta, String clientName);

    /**
     * flush the transaction metadata to the system topic __terminated_txn_state
     * before the state of txn is aborted.
     * @param clientName the client name of the transaction metadata need to be flushed.
     */
    void flush(String clientName) throws CoordinatorException.PreserverClosedException;

    /**
     * check if transaction metadata preserver is enabled.
     * @return
     */
    boolean enabled();

    /**
     * Get the aborted transaction metadata of the given transaction id.
     * Committed transaction metadata will not be persisted.
     *
     * @param txnID
     * @return
     */
    TxnMeta getTxnMeta(TxnID txnID, String clientName);

    /**
     * Expire the transaction metadata periodically.
     */
    void expireTransactionMetadata();

    /**
     * Get the interval of expiring the transaction metadata in MS.
     * @return the interval of expiring the transaction metadata in MS.
     */
    long getExpireOldTransactionMetadataIntervalMS();

    /**
     * Get the time used for recovery in MS.
     * @return
     */
    long getRecoveryTime();
}
