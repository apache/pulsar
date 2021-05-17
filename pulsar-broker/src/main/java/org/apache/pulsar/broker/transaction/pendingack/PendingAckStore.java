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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;

/**
 * To store transaction pending ack.
 */
public interface PendingAckStore {
    /**
     * Replay pending ack to recover the pending ack subscription pending ack state.
     *
     * @param pendingAckHandle the handle of pending ack
     * @param executorService the replay executor service
     */
    void replayAsync(PendingAckHandleImpl pendingAckHandle, ScheduledExecutorService executorService);

    /**
     * Close the transaction pending ack store.
     *
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Append the individual pending ack operation to the ack persistent store.
     *
     * @param txnID {@link TxnID} transaction id.
     * @param positions {@link List} the list of position and batch size.
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> appendIndividualAck(TxnID txnID, List<MutablePair<PositionImpl, Integer>> positions);

    /**
     * Append the cumulative pending ack operation to the ack persistent store.
     *
     * @param txnID {@link TxnID} transaction id.
     * @param position {@link PositionImpl} the pending ack position.
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> appendCumulativeAck(TxnID txnID, PositionImpl position);

    /**
     * Append the pending ack commit mark to the ack persistent store.
     *
     * @param txnID {@link TxnID} the transaction id for add commit mark.
     * @param ackType {@link AckType} the ack type of the commit.
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> appendCommitMark(TxnID txnID, AckType ackType);

    /**
     * Append the pending ack abort mark to the ack persistent store.
     *
     * @param txnID {@link Position} the txnID
     * @param ackType {@link AckType} the ack type of the abort.
     * @return a future represents the result of this operation
     */
    CompletableFuture<Void> appendAbortMark(TxnID txnID, AckType ackType);
}