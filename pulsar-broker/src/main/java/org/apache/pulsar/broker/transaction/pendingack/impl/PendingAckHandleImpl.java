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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import com.google.common.collect.ComparisonChain;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.isAckSetOverlap;

@Slf4j
public class PendingAckHandleImpl implements PendingAckHandle {

    /**
     * The map is for transaction with position witch was individual acked by this transaction.
     * <p>
     *     If the position is no batch position, it will be added to the map.
     * <p>
     *     If the position is batch position and it does not exits the map, will be added to the map.
     *     If the position is batch position and it exits the map, will do operation `and` for this
     *     two positions bit set.
     */
    private Map<TxnID, HashMap<PositionImpl, PositionImpl>> individualAckOfTransaction;

    /**
     * The map is for individual ack of positions for transaction.
     * <p>
     *     When no batch position was acked by transaction, it will be checked to see if it exists in the map.
     *     If it exits in the map, prove than it has been acked by another transaction. Broker will throw the
     *     TransactionConflictException {@link TransactionConflictException}.
     *     If it does not exits int the map, the position will be added to the map.
     * <p>
     *     When batch position was acked by transaction, it will be checked to see if it exists in the map.
     *     <p>
     *         If it exits in the map, it will checked to see if it duplicates the existing bit set point.
     *         If it exits at the bit set point, broker will throw the
     *         TransactionConflictException {@link TransactionConflictException}.
     *         If it exits at the bit set point, the bit set of the acked position will do the operation `and` for the
     *         two positions bit set.
     *     <p>
     *         If it does not exits the map, the position will be added to the map.
     */
    private Map<PositionImpl, PositionImpl> individualAckPositions;

    /**
     * The map is for transaction with position witch was cumulative acked by this transaction.
     * Only one cumulative ack position was acked by one transaction at the same time.
     */
    private Pair<TxnID, PositionImpl> cumulativeAckOfTransaction;

    private final String topicName;

    private final String subName;

    private final PersistentSubscription persistentSubscription;

    public PendingAckHandleImpl(PersistentSubscription persistentSubscription) {
        this.topicName = persistentSubscription.getTopicName();
        this.subName = persistentSubscription.getName();
        this.persistentSubscription = persistentSubscription;
    }

    @Override
    public synchronized CompletableFuture<Void> acknowledgeMessage(TxnID txnId,
                                                                   List<Position> positions, AckType ackType) {
        if (txnId == null) {
            return FutureUtil.failedFuture(new NotAllowedException("TransactionID can not be null."));
        }
        if (positions == null) {
            return FutureUtil.failedFuture(new NotAllowedException("Positions can not be null."));
        }
        if (AckType.Cumulative == ackType) {
            return acknowledgeMessageCumulative(positions, txnId);
        } else {
            return acknowledgeMessageIndividual(positions, txnId);
        }
    }

    private CompletableFuture<Void> acknowledgeMessageCumulative(List<Position> positions, TxnID txnID) {

        if (positions.size() != 1) {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                    " invalid cumulative ack received with multiple message ids.";
            log.error(errorMsg);
            return FutureUtil.failedFuture(new NotAllowedException(errorMsg));
        }

        if (!(positions.get(0) instanceof PositionImpl)) {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                    " invalid cumulative ack received with position is not PositionImpl";
            log.error(errorMsg);
            return FutureUtil.failedFuture(new NotAllowedException(errorMsg));
        }

        PositionImpl position = (PositionImpl) positions.get(0);

        if (position.compareTo((PositionImpl) persistentSubscription.getCursor().getMarkDeletedPosition()) <= 0) {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                    " try to cumulative ack position: " + position + " within range of cursor's " +
                    "markDeletePosition: " + persistentSubscription.getCursor().getMarkDeletedPosition();
            log.error(errorMsg);
            return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] TxnID:[{}] Cumulative ack on {}.", topicName, subName, txnID.toString(), position);
        }

        if (this.cumulativeAckOfTransaction == null) {
            this.cumulativeAckOfTransaction = MutablePair.of(txnID, position);
        } else if (this.cumulativeAckOfTransaction.getKey().equals(txnID)
                && compareToWithAckSetForCumulativeAck(position, this.cumulativeAckOfTransaction.getValue()) > 0) {
            this.cumulativeAckOfTransaction.setValue(position);

        } else {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                    " try to cumulative batch ack position: " + position + " within range of current " +
                    "currentPosition: " + this.cumulativeAckOfTransaction.getValue();
            log.error(errorMsg);
            return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> acknowledgeMessageIndividual(List<Position> positions, TxnID txnID) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> positionsFuture = new ArrayList<>(positions.size());
        for (int i = 0; i < positions.size(); i++) {
            if (!(positions.get(i) instanceof PositionImpl)) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                        " invalid individual ack received with position is not PositionImpl";
                log.error(errorMsg);
                positionsFuture.add(FutureUtil.failedFuture(new NotAllowedException(errorMsg)));
                break;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}] Individual acks on {}", topicName, subName, txnID.toString(), positions);
            }
            if (individualAckOfTransaction == null) {
                individualAckOfTransaction = new HashMap<>();
            }

            if (individualAckPositions == null) {
                individualAckPositions = new HashMap<>();
            }
            PositionImpl position = (PositionImpl) positions.get(i);
            // If try to ack message already acked by committed transaction or normal acknowledge, throw exception.
            if (((ManagedCursorImpl) persistentSubscription.getCursor()).isMessageDeleted(position)) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                        " try to ack message:" + position + " already acked before.";
                log.error(errorMsg);
                positionsFuture.add(FutureUtil.failedFuture(new TransactionConflictException(errorMsg)));
                break;
            }

            if (position.hasAckSet()) {

                if (individualAckPositions.containsKey(position)
                        && isAckSetOverlap(individualAckPositions.get(position).getAckSet(), position.getAckSet())) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                            " try to ack batch message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    positionsFuture.add(FutureUtil.failedFuture(new TransactionConflictException(errorMsg)));
                    break;
                }

                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.computeIfAbsent(txnID, txn -> new HashMap<>());

                if (pendingAckMessageForCurrentTxn.containsKey(position)) {
                    andAckSet(pendingAckMessageForCurrentTxn.get(position), position);
                } else {
                    pendingAckMessageForCurrentTxn.put(position, position);
                }

                if (!individualAckPositions.containsKey(position)) {
                    this.individualAckPositions.put(position, position);
                } else {
                    andAckSet(this.individualAckPositions.get(position), position);
                }

                positionsFuture.add(CompletableFuture.completedFuture(null));
            } else {

                if (this.individualAckPositions.containsKey(position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                            " try to ack message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    positionsFuture.add(FutureUtil.failedFuture(new TransactionConflictException(errorMsg)));
                    break;
                }
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.computeIfAbsent(txnID, txn -> new HashMap<>());
                pendingAckMessageForCurrentTxn.put(position, position);
                this.individualAckPositions.put(position, position);
                positionsFuture.add(CompletableFuture.completedFuture(null));
            }
        }
        FutureUtil.waitForAll(positionsFuture).whenComplete((v, e) -> {
            if (e != null) {
                completableFuture.completeExceptionally(e);
            } else {
                completableFuture.complete(null);
            }
        });
        return completableFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> commitTxn(TxnID txnId, Map<String, Long> properties) {

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        // It's valid to create transaction then commit without doing any operation, which will cause
        // pendingAckMessagesMap to be null.
        if (this.cumulativeAckOfTransaction != null) {
            if (cumulativeAckOfTransaction.getKey().equals(txnId)) {
                persistentSubscription.acknowledgeMessage(Collections
                        .singletonList(this.cumulativeAckOfTransaction.getValue()), AckType.Cumulative, properties);
                this.cumulativeAckOfTransaction = null;
            }
        } else {
            if (individualAckOfTransaction != null && individualAckOfTransaction.containsKey(txnId)) {
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.remove(txnId);
                if (pendingAckMessageForCurrentTxn != null) {
                    persistentSubscription.acknowledgeMessage(new ArrayList<>(pendingAckMessageForCurrentTxn.values()),
                            AckType.Individual, properties);
                    endIndividualAckTxnCommon(pendingAckMessageForCurrentTxn);
                }
            }
        }
        commitFuture.complete(null);
        return commitFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer) {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        if (this.cumulativeAckOfTransaction != null) {
            if (this.cumulativeAckOfTransaction.getKey().equals(txnId)) {
                this.cumulativeAckOfTransaction = null;
            }
        } else if (this.individualAckOfTransaction != null){
            HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                    individualAckOfTransaction.remove(txnId);
            if (pendingAckMessageForCurrentTxn != null) {
                endIndividualAckTxnCommon(pendingAckMessageForCurrentTxn);
            }
        }
        abortFuture.complete(null);
        return abortFuture;
    }

    private void endIndividualAckTxnCommon(HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn){
        for (Entry<PositionImpl, PositionImpl> entry : pendingAckMessageForCurrentTxn.entrySet()) {
            if (entry.getValue().hasAckSet() && individualAckPositions.containsKey(entry.getValue())) {
                BitSetRecyclable thisBitSet = BitSetRecyclable.valueOf(entry.getKey().getAckSet());
                thisBitSet.flip(0, thisBitSet.size());
                BitSetRecyclable otherBitSet =
                        BitSetRecyclable.valueOf(individualAckPositions.get(entry.getValue()).getAckSet());
                otherBitSet.or(thisBitSet);
                long [] orAckSet = otherBitSet.toLongArray();
                otherBitSet.flip(0, otherBitSet.size());
                if (otherBitSet.isEmpty()) {
                    thisBitSet.recycle();
                    otherBitSet.recycle();
                    individualAckPositions.remove(entry.getValue());
                } else {
                    individualAckPositions.get(entry.getValue()).setAckSet(orAckSet);
                    thisBitSet.recycle();
                    otherBitSet.recycle();
                }
            } else {
                individualAckPositions.remove(entry.getValue());
            }
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer) {
        ConcurrentLongLongPairHashMap positionMap = consumer.getPendingAcks();
        // Only check if message is in pending_ack status when there's ongoing transaction.
        if (null != positionMap && ((individualAckPositions != null && individualAckPositions.size() != 0)
                || this.cumulativeAckOfTransaction != null)) {
            List<PositionImpl> pendingPositions = new ArrayList<>();
            PositionImpl cumulativeAckPosition =
                    (null == this.cumulativeAckOfTransaction) ? null : this.cumulativeAckOfTransaction.getValue();

            positionMap.asMap().forEach((key, value) -> {
                PositionImpl position = new PositionImpl(key.first, key.second);
                redeliverUnacknowledgedMessagesCommon(position, pendingPositions, cumulativeAckPosition);
            });

            persistentSubscription.getDispatcher().redeliverUnacknowledgedMessages(consumer, pendingPositions);
        } else {
            persistentSubscription.getDispatcher().redeliverUnacknowledgedMessages(consumer);
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        // If there's ongoing transaction.
        if ((individualAckPositions != null && individualAckPositions.size() != 0) || this.cumulativeAckOfTransaction != null) {
            // Check if message is in pending_ack status.
            List<PositionImpl> pendingPositions = new ArrayList<>();
            PositionImpl cumulativeAckPosition =
                    (null == this.cumulativeAckOfTransaction) ? null : this.cumulativeAckOfTransaction.getValue();
            positions.forEach(position ->
                    redeliverUnacknowledgedMessagesCommon(position, pendingPositions, cumulativeAckPosition));
            persistentSubscription.getDispatcher().redeliverUnacknowledgedMessages(consumer, pendingPositions);
        } else {
            persistentSubscription.getDispatcher().redeliverUnacknowledgedMessages(consumer, positions);
        }
    }

    private void redeliverUnacknowledgedMessagesCommon(PositionImpl position, List<PositionImpl> pendingPositions,
                                                       PositionImpl cumulativeAckPosition) {
        if (position.hasAckSet()) {
            if (this.individualAckPositions != null) {
                if (this.individualAckPositions.containsKey(position)) {
                    if (!isAckSetOverlap(individualAckPositions.get(position).getAckSet(), position.getAckSet())) {
                    pendingPositions.add(position);
                }
            } else {
                pendingPositions.add(position);
            }
        } else if (cumulativeAckPosition != null) {
            int compareNumber = position.compareTo((cumulativeAckPosition));
            if (compareNumber > 0 || (compareNumber == 0
                    && !isAckSetOverlap(cumulativeAckPosition.getAckSet(), position.getAckSet()))) {
                    pendingPositions.add(position);
                }
            }
        } else {
            if (this.individualAckPositions != null) {
                if (!this.individualAckPositions.containsKey(position)) {
                    pendingPositions.add(position);
                }
            } else if (cumulativeAckPosition != null ) {
                if (position.compareTo(cumulativeAckPosition) > 0) {
                    pendingPositions.add(position);
                }
            }
        }
    }

    public static int compareToWithAckSetForCumulativeAck(PositionImpl currentPosition,PositionImpl otherPosition) {
        if (currentPosition == null || otherPosition ==null) {
            return -1;
        }
        int result = ComparisonChain.start().compare(currentPosition.getLedgerId(),
                otherPosition.getLedgerId()).compare(currentPosition.getEntryId(), otherPosition.getEntryId())
                .result();
        if (result == 0) {
            if (otherPosition.getAckSet() == null && currentPosition.getAckSet() == null) {
                return result;
            }
            BitSetRecyclable otherAckSet = BitSetRecyclable.valueOf(otherPosition.getAckSet());
            BitSetRecyclable thisAckSet = BitSetRecyclable.valueOf(currentPosition.getAckSet());
            return thisAckSet.nextSetBit(0) - otherAckSet.nextSetBit(0);
        }
        return result;
    }

    public static void andAckSet(PositionImpl currentPosition, PositionImpl otherPosition) {
        if (currentPosition == null || otherPosition == null) {
            return;
        }
        BitSetRecyclable thisAckSet = BitSetRecyclable.valueOf(currentPosition.getAckSet());
        BitSetRecyclable otherAckSet = BitSetRecyclable.valueOf(otherPosition.getAckSet());
        thisAckSet.and(otherAckSet);
        currentPosition.setAckSet(thisAckSet.toLongArray());
        thisAckSet.recycle();
        otherAckSet.recycle();
    }
}