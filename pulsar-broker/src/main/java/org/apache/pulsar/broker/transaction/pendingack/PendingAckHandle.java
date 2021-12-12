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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;

/**
 * Handle for processing pending acks for transactions.
 */
public interface PendingAckHandle {

    /**
     * Acknowledge message(s) for an ongoing transaction.
     * <p>
     * It can be of {@link AckType#Individual}. Single messages acked by ongoing transaction will be put
     * in pending_ack state and only marked as deleted after transaction is committed.
     * <p>
     * If transaction is aborted all messages acked by it will be put back to pending state.
     * <p>
     * Client will not send batch size to server, we get the batch size from consumer pending ack. When we get the Batch
     * size, we can accurate batch ack of this position.
     *
     * @param txnID                  {@link TxnID} TransactionID of an ongoing transaction trying to sck message.
     * @param positions              {@link MutablePair} the pair of positions and these batch size.
     * @param isInCacheRequest       {@link Boolean} the boolean of the request in cache whether or not.
     * @return the future of this operation.
     * @throws TransactionConflictException if the ack with transaction is conflict with pending ack.
     * @throws NotAllowedException if Use this method incorrectly eg. not use
     * PositionImpl or cumulative ack with a list of positions.
     */
    CompletableFuture<Void> individualAcknowledgeMessage(TxnID txnID, List<MutablePair<PositionImpl,
            Integer>> positions, boolean isInCacheRequest);

    /**
     * Acknowledge message(s) for an ongoing transaction.
     * <p>
     * It can be of {@link AckType#Cumulative}. Single messages acked by ongoing transaction will be put in
     * pending_ack state and only marked as deleted after transaction is committed.
     * <p>
     * For a moment, we only allow one transaction cumulative ack multiple times when the position is greater than the
     * old one.
     * <p>
     * We have a transaction with cumulative ack, if other transaction want to cumulative ack, we will
     * return {@link TransactionConflictException}.
     * <p>
     * If an ongoing transaction cumulative acked a message and then try to ack single message which is
     * greater than that one it cumulative acked, it'll succeed.
     *
     * @param txnID                  {@link TxnID} TransactionID of an ongoing transaction trying to sck message.
     * @param positions              {@link MutablePair} the pair of positions and these batch size.
     * @param isInCacheRequest       {@link Boolean} the boolean of the request in cache whether or not.
     * @return the future of this operation.
     * @throws TransactionConflictException if the ack with transaction is conflict with pending ack.
     * @throws NotAllowedException if Use this method incorrectly eg. not use
     * PositionImpl or cumulative ack with a list of positions.
     */
    CompletableFuture<Void> cumulativeAcknowledgeMessage(TxnID txnID, List<PositionImpl> positions,
                                                         boolean isInCacheRequest);

    /**
     * Commit a transaction.
     *
     * @param txnID      {@link TxnID} to identify the transaction.
     * @param properties Additional user-defined properties that can be
     *                   associated with a particular cursor position.
     * @param lowWaterMark the low water mark of this transaction
     * @param isInCacheRequest       {@link Boolean} the boolean of the request in cache whether or not.
     * @return the future of this operation.
     */
    CompletableFuture<Void> commitTxn(TxnID txnID, Map<String, Long> properties,
                                      long lowWaterMark, boolean isInCacheRequest);

    /**
     * Abort a transaction.
     *
     * @param txnId  {@link TxnID} to identify the transaction.
     * @param consumer {@link Consumer} which aborting transaction.
     * @param lowWaterMark the low water mark of this transaction
     * @param isInCacheRequest       {@link Boolean} the boolean of the request in cache whether or not.
     * @return the future of this operation.
     */
    CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer, long lowWaterMark, boolean isInCacheRequest);

    /**
     * Sync the position ack set, in order to clean up the cache of this position for pending ack handle.
     *
     * @param position {@link Position} which position need to sync and carry it batch size
     */
    void syncBatchPositionAckSetForTransaction(PositionImpl position);

    /**
     * Judge the all ack set point have acked by normal ack and transaction pending ack.
     *
     * @param position {@link Position} which position need to check
     */
    boolean checkIsCanDeleteConsumerPendingAck(PositionImpl position);

    /**
     * When the position is actually deleted, we can use this method to clear the cache for individual ack messages.
     *
     * @param position {@link Position} which position need to clear
     */
    void clearIndividualPosition(Position position);

    /**
     * Pending ack recover whether ready future.
     *
     * @return the future of result.
     */
    CompletableFuture<PendingAckHandle> pendingAckHandleFuture();

    /**
     * Get transaction in pending ack stats.
     *
     * @param txnID the txnID
     * @return the stats of this transaction in pending ack.
     */
    TransactionInPendingAckStats getTransactionInPendingAckStats(TxnID txnID);

    /**
     * Get pending ack handle stats.
     *
     * @return the stats of this pending ack handle.
     */
    TransactionPendingAckStats getStats();

    /**
     * Close the pending ack handle.
     *
     * @return the future of this operation.
     */
    CompletableFuture<Void> close();

    /**
     * Check if the PendingAckStore is init.
     * @return if the PendingAckStore is init.
     */
    boolean checkIfPendingAckStoreInit();
}