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
package org.apache.pulsar.broker.transaction.buffer;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.transaction.TxnID;


public interface AbortedTxnProcessor {

    /**
     * After the transaction buffer writes a transaction aborted marker to the topic,
     * the transaction buffer will put the aborted txnID and the aborted marker position to AbortedTxnProcessor.
     * @param txnID aborted transaction ID.
     * @param abortedMarkerPersistentPosition the position of the abort txn marker.
     */
    void putAbortedTxnAndPosition(TxnID txnID, PositionImpl abortedMarkerPersistentPosition);

    /**
     * Clean up invalid aborted transactions.
     */
    void trimExpiredAbortedTxns();

    /**
     * Check whether the transaction ID is an aborted transaction ID.
     * @param txnID the transaction ID that needs to be checked.
     * @return a boolean, whether the transaction ID is an aborted transaction ID.
     */
    boolean checkAbortedTransaction(TxnID txnID);

    /**
     * Recover transaction buffer by transaction buffer snapshot.
     * @return a Position (startReadCursorPosition) determiner where to start to recover in the original topic.
     */

    CompletableFuture<PositionImpl> recoverFromSnapshot();

    /**
     * Delete the transaction buffer aborted transaction snapshot.
     * @return a completableFuture.
     */
    CompletableFuture<Void> clearAbortedTxnSnapshot();

    /**
     * Take aborted transactions snapshot.
     * @return a completableFuture.
     */
    CompletableFuture<Void> takeAbortedTxnsSnapshot(PositionImpl maxReadPosition);

    /**
     * Get the lastSnapshotTimestamps.
     * @return the lastSnapshotTimestamps.
     */
    long getLastSnapshotTimestamps();

    CompletableFuture<Void> closeAsync();

}