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
package org.apache.pulsar.broker.transaction.buffer;

import io.netty.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferRecoverCallBack;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TxnIDData;


public interface AbortedTxnProcessor extends TimerTask {

    /**
     * After the transaction buffer writes a transaction aborted mark to the topic,
     * the transaction buffer will add the aborted transaction ID to AbortedTxnProcessor.
     * @param txnID aborted transaction ID.
     */
    void appendAbortedTxn(TxnIDData txnID, PositionImpl position);

    /**
     * After the transaction buffer writes a transaction aborted mark to the topic,
     * the transaction buffer will update max read position in AbortedTxnProcessor
     * @param maxReadPosition  the Max read position after the transaction is aborted.
     */
    void updateMaxReadPosition(Position maxReadPosition);


    /**
     * Pulsar has a configuration for ledger retention time.
     * If the transaction aborted mark position has been deleted, the transaction is valid and can be clear.
     * In the old implementation we clear the invalid aborted txn ID one by one.
     * In the new implementation, we adopt snapshot segments. And then we clear invalid segment by its max read position.
     */
    void trimExpiredTxnIDDataOrSnapshotSegments();

    /**
     * Check whether the transaction ID is an aborted transaction ID.
     * @param txnID the transaction ID that needs to be checked.
     * @param readPosition the read position of the transaction message, can be used to find the segment.
     * @return a boolean, whether the transaction ID is an aborted transaction ID.
     */
    boolean checkAbortedTransaction(TxnIDData  txnID, Position readPosition);

    /**
     * Recover transaction buffer by transaction buffer snapshot.
     * @return a pair consists of a Boolean if the transaction buffer needs to recover and a Position (startReadCursorPosition) determiner where to start to recover in the original topic.
     */

    CompletableFuture<PositionImpl> recoverFromSnapshot(TopicTransactionBufferRecoverCallBack callBack);

    public CompletableFuture<Void> clearSnapshot();
    public CompletableFuture<Void> takesFirstSnapshot();
    public PositionImpl getMaxReadPosition();

    public long getLastSnapshotTimestamps();

}