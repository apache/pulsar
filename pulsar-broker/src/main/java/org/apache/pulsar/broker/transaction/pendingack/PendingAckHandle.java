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
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface PendingAckHandle {

    /**
     * Acknowledge message(s) for an ongoing transaction.
     * <p>
     * It can be of {@link AckType#Individual} or {@link AckType#Cumulative}. Single messages acked by ongoing
     * transaction will be put in pending_ack state and only marked as deleted after transaction is committed.
     * <p>
     * For a moment, we only allow one transaction cumulative ack multiple times when the position is greater than the
     * old one.
     * <p>
     * We have a transaction with cumulative ack, if other transaction want to cumulative ack, we will
     * return {@link TransactionConflictException}.
     * <p>
     * If an ongoing transaction cumulative acked a message and then try to ack single message which is
     * greater than that one it cumulative acked, it'll succeed.
     * <p>
     * If transaction is aborted all messages acked by it will be put back to pending state.
     *
     * @param txnId                  TransactionID of an ongoing transaction trying to sck message.
     * @param positions              {@link Position}(s) it try to ack.
     * @param ackType                {@link AckType}.
     * @return the future of this operation.
     * @throws TransactionConflictException if the ack with transaction is conflict with pending ack.
     * @throws NotAllowedException if Use this method incorrectly eg. not use
     * PositionImpl or cumulative ack with a list of positions.
     */
    CompletableFuture<Void> acknowledgeMessage(TxnID txnId, List<Position> positions, AckType ackType);

    /**
     * Commit a transaction.
     *
     * @param txnId         {@link TxnID} to identify the transaction.
     * @param properties    Additional user-defined properties that can be associated with a particular cursor position.
     * @return the future of this operation.
     */
    CompletableFuture<Void> commitTxn(TxnID txnId, Map<String,Long> properties);

    /**
     * Abort a transaction.
     *
     * @param txnId  {@link TxnID} to identify the transaction.
     * @param consumer {@link Consumer} which aborting transaction.
     * @return the future of this operation.
     */
    CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer);

    /**
     * Redeliver the unacknowledged messages with consumer which wait for ack in broker, we will filter the positions
     * in pending ack state.
     *
     * @param consumer {@link Consumer} which will be redelivered.
     */
    void redeliverUnacknowledgedMessages(Consumer consumer);

    /**
     * Redeliver the unacknowledged messages with the positions for this consumer, we will filter the
     * position in pending ack state.
     *
     * @param consumer {@link Consumer} which will redeliver the position.
     * @param positions {@link List} the list of positions which will be redelivered.
     */
    void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions);

}