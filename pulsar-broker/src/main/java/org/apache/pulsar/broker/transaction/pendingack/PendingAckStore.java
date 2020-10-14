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
package org.apache.pulsar.broker.transaction.pendingack;

import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;

import java.util.concurrent.CompletableFuture;

public interface PendingAckStore {
    /**
     * Replay pending ack to recover the pending ack subscription pending ack stat.
     *
     * @param pendingAckReplyCallBack the call back for replaying the pending ack stat
     */
    void replayAsync(PendingAckReplyCallBack pendingAckReplyCallBack);

    /**
     * Close the transaction log.
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Append the pending ack operation to the ack persistent store.
     *
     * @param txnID {@link TxnID} transaction id.
     * @param position {@link PositionImpl} the pending ack postion.
     * @param ackType {@link AckType} the transaction ack type.
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> append(TxnID txnID, PositionImpl position, AckType ackType);

    /**
     * Delete the pending ack persistent log for the txnID.
     *
     * @param txnID {@link Position} the txnID
     * @param ackType {@link AckType} the transaction ack type.
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> deleteTxn(TxnID txnID, AckType ackType);
}