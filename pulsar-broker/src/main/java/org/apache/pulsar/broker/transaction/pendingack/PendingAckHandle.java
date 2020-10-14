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
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface PendingAckHandle {

    /**
     * Acknowledge message(s) for an ongoing transaction.
     * <p>
     * It can be of {@link PulsarApi.CommandAck.AckType#Individual} or {@link PulsarApi.CommandAck.AckType#Cumulative}. Single messages acked by ongoing
     * transaction will be put in pending_ack state and only marked as deleted after transaction is committed.
     * <p>
     * Only one transaction is allowed to do cumulative ack on a subscription at a given time.
     * If a transaction do multiple cumulative ack, only the one with largest position determined by
     * {@link PositionImpl#compareTo(PositionImpl)} will be kept as it cover all position smaller than it.
     * <p>
     * If an ongoing transaction cumulative acked a message and then try to ack single message which is
     * smaller than that one it cumulative acked, it'll succeed.
     * <p>
     * If transaction is aborted all messages acked by it will be put back to pending state.
     *
     * @param txnId                  TransactionID of an ongoing transaction trying to sck message.
     * @param positions              {@link Position}(s) it try to ack.
     * @param ackType                {@link PulsarApi.CommandAck.AckType}.
     *  cumulative ack or try to single ack message already acked by any ongoing transaction.
     * @throws IllegalArgumentException if try to cumulative ack but passed in multiple positions.
     */
    CompletableFuture<Void> acknowledgeMessage(TxnID txnId, List<Position> positions, PulsarApi.CommandAck.AckType ackType);

    /**
     * Commit a transaction.
     *
     * @param txnId         {@link TxnID} to identify the transaction.
     * @param properties    Additional user-defined properties that can be associated with a particular cursor position.
     * @throws IllegalArgumentException if given {@link TxnID} is not found in this subscription.
     */
    CompletableFuture<Void> commitTxn(TxnID txnId, Map<String,Long> properties);

    /**
     * Abort a transaction.
     *
     * @param txnId  {@link TxnID} to identify the transaction.
     * @param consumer {@link Consumer} which aborting transaction.
     *
     * @throws IllegalArgumentException if given {@link TxnID} is not found in this subscription.
     */
    CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer);

    void setPersistentSubscription(PersistentSubscription persistentSubscription);

    void redeliverUnacknowledgedMessages(Consumer consumer);

    void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions);

}