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

import static org.apache.bookkeeper.mledger.util.PositionAckSetUtil.andAckSet;
import static org.apache.bookkeeper.mledger.util.PositionAckSetUtil.compareToWithAckSet;
import static org.apache.bookkeeper.mledger.util.PositionAckSetUtil.isAckSetOverlap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;

/**
 * The default implementation of {@link PendingAckHandle}.
 */
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
    private LinkedMap<TxnID, HashMap<PositionImpl, PositionImpl>> individualAckOfTransaction;

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
    private Map<PositionImpl, MutablePair<PositionImpl, Integer>> individualAckPositions;

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
    public synchronized CompletableFuture<Void> individualAcknowledgeMessage(TxnID txnID,
                                                                List<MutablePair<PositionImpl, Integer>> positions) {
        if (txnID == null) {
            return FutureUtil.failedFuture(new NotAllowedException("TransactionID can not be null."));
        }
        if (positions == null) {
            return FutureUtil.failedFuture(new NotAllowedException("Positions can not be null."));
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        for (MutablePair<PositionImpl, Integer> positionIntegerMutablePair : positions) {
            PositionImpl position = positionIntegerMutablePair.left;

            // If try to ack message already acked by committed transaction or normal acknowledge, throw exception.
            if (((ManagedCursorImpl) persistentSubscription.getCursor())
                    .isMessageDeleted(position)) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                        + " try to ack message:" + position + " already acked before.";
                log.error(errorMsg);
                return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
            }

            if (position.hasAckSet()) {
                //in order to jude the bit set is over lap, so set the covering the batch size bit to 1,
                // should know the two bit set don't have the same point is 0
                BitSetRecyclable bitSetRecyclable = BitSetRecyclable.valueOf(position.getAckSet());
                if (positionIntegerMutablePair.right > bitSetRecyclable.size()) {
                    bitSetRecyclable.set(positionIntegerMutablePair.right);
                }
                bitSetRecyclable.set(positionIntegerMutablePair.right, bitSetRecyclable.size());
                long[] ackSetOverlap = bitSetRecyclable.toLongArray();
                bitSetRecyclable.recycle();
                if (isAckSetOverlap(ackSetOverlap,
                        ((ManagedCursorImpl) persistentSubscription.getCursor()).getBatchPositionAckSet(position))) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                            + " try to ack message:" + position + " already acked before.";
                    log.error(errorMsg);
                    return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
                }

                if (this.individualAckPositions != null && individualAckPositions.containsKey(position)
                        && isAckSetOverlap(individualAckPositions.get(position).getLeft().getAckSet(), ackSetOverlap)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                            + " try to ack batch message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
                }
            } else {
                if (this.individualAckPositions != null && this.individualAckPositions.containsKey(position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                            + " try to ack message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
                }
            }
        }
        for (int i = 0; i < positions.size(); i++) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}] Individual acks on {}", topicName, subName, txnID.toString(), positions);
            }
            if (individualAckOfTransaction == null) {
                individualAckOfTransaction = new LinkedMap<>();
            }

            if (individualAckPositions == null) {
                individualAckPositions = new ConcurrentSkipListMap<>();
            }

            PositionImpl position = positions.get(i).left;

            if (position.hasAckSet()) {

                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.computeIfAbsent(txnID, txn -> new HashMap<>());

                if (pendingAckMessageForCurrentTxn.containsKey(position)) {
                    andAckSet(pendingAckMessageForCurrentTxn.get(position), position);
                } else {
                    pendingAckMessageForCurrentTxn.put(position, position);
                }

                if (!individualAckPositions.containsKey(position)) {
                    this.individualAckPositions.put(position, positions.get(i));
                } else {
                    MutablePair<PositionImpl, Integer> positionPair = this.individualAckPositions.get(position);
                    positionPair.setRight(positions.get(i).right);
                    andAckSet(positionPair.getLeft(), position);
                }

            } else {
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.computeIfAbsent(txnID, txn -> new HashMap<>());
                pendingAckMessageForCurrentTxn.put(position, position);
                this.individualAckPositions.putIfAbsent(position, positions.get(i));
            }
        }
        completableFuture.complete(null);
        return completableFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> cumulativeAcknowledgeMessage(TxnID txnID,
                                                                             List<PositionImpl> positions) {

        if (txnID == null) {
            return FutureUtil.failedFuture(new NotAllowedException("TransactionID can not be null."));
        }
        if (positions == null) {
            return FutureUtil.failedFuture(new NotAllowedException("Positions can not be null."));
        }

        if (positions.size() != 1) {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                    + " invalid cumulative ack received with multiple message ids.";
            log.error(errorMsg);
            return FutureUtil.failedFuture(new NotAllowedException(errorMsg));
        }

        PositionImpl position = positions.get(0);

        if (position.compareTo((PositionImpl) persistentSubscription.getCursor().getMarkDeletedPosition()) <= 0) {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                    + " try to cumulative ack position: " + position + " within range of cursor's "
                    + "markDeletePosition: " + persistentSubscription.getCursor().getMarkDeletedPosition();
            log.error(errorMsg);
            return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] TxnID:[{}] Cumulative ack on {}.", topicName, subName, txnID.toString(), position);
        }

        if (this.cumulativeAckOfTransaction == null) {
            this.cumulativeAckOfTransaction = MutablePair.of(txnID, position);
        } else if (this.cumulativeAckOfTransaction.getKey().equals(txnID)
                && compareToWithAckSet(position, this.cumulativeAckOfTransaction.getValue()) > 0) {
            this.cumulativeAckOfTransaction.setValue(position);

        } else {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                    + " try to cumulative batch ack position: " + position + " within range of current "
                    + "currentPosition: " + this.cumulativeAckOfTransaction.getValue();
            log.error(errorMsg);
            return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> commitTxn(TxnID txnID, Map<String, Long> properties,
                                                          long lowWaterMark) {

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        // It's valid to create transaction then commit without doing any operation, which will cause
        // pendingAckMessagesMap to be null.
        if (this.cumulativeAckOfTransaction != null) {
            if (cumulativeAckOfTransaction.getKey().equals(txnID)) {
                persistentSubscription.acknowledgeMessage(Collections
                        .singletonList(this.cumulativeAckOfTransaction.getValue()), AckType.Cumulative, properties);
                this.cumulativeAckOfTransaction = null;
            }
        } else {
            if (individualAckOfTransaction != null && individualAckOfTransaction.containsKey(txnID)) {
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.remove(txnID);
                if (pendingAckMessageForCurrentTxn != null) {
                    persistentSubscription.acknowledgeMessage(new ArrayList<>(pendingAckMessageForCurrentTxn.values()),
                            AckType.Individual, properties);
                }
            }
        }
        handleLowWaterMark(txnID, lowWaterMark);
        commitFuture.complete(null);
        return commitFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer, long lowWaterMark) {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        if (this.cumulativeAckOfTransaction != null) {
            if (this.cumulativeAckOfTransaction.getKey().equals(txnId)) {
                this.cumulativeAckOfTransaction = null;
            }
            this.persistentSubscription.redeliverUnacknowledgedMessages(consumer);
        } else if (this.individualAckOfTransaction != null){
            HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                    individualAckOfTransaction.remove(txnId);
            if (pendingAckMessageForCurrentTxn != null) {
                for (Entry<PositionImpl, PositionImpl> entry : pendingAckMessageForCurrentTxn.entrySet()) {
                    if (entry.getValue().hasAckSet() && individualAckPositions.containsKey(entry.getValue())) {
                        BitSetRecyclable thisBitSet = BitSetRecyclable.valueOf(entry.getValue().getAckSet());
                        thisBitSet.flip(0, individualAckPositions.get(entry.getValue()).right);
                        BitSetRecyclable otherBitSet =
                                BitSetRecyclable.valueOf(individualAckPositions.get(entry.getValue()).left.getAckSet());
                        otherBitSet.or(thisBitSet);
                        individualAckPositions.get(entry.getKey()).left.setAckSet(otherBitSet.toLongArray());
                        otherBitSet.recycle();
                        thisBitSet.recycle();
                    } else {
                        individualAckPositions.remove(entry.getValue());
                    }
                }
                this.persistentSubscription.redeliverUnacknowledgedMessages(consumer,
                        new ArrayList<>(pendingAckMessageForCurrentTxn.values()));
            }
        }
        handleLowWaterMark(txnId, lowWaterMark);
        abortFuture.complete(null);
        return abortFuture;
    }

    private void handleLowWaterMark(TxnID txnID, long lowWaterMark) {
        if (individualAckOfTransaction != null && !individualAckOfTransaction.isEmpty()) {
            TxnID firstTxn = individualAckOfTransaction.firstKey();

            if (firstTxn.getMostSigBits() == txnID.getMostSigBits()
                    && firstTxn.getLeastSigBits() <= lowWaterMark) {
                individualAckOfTransaction.remove(firstTxn);
                handleLowWaterMark(txnID, lowWaterMark);
            }
        }
    }

    @Override
    public synchronized void syncBatchPositionAckSetForTransaction(PositionImpl position) {
        if (individualAckPositions == null) {
            individualAckPositions = new ConcurrentSkipListMap<>();
        }
        //sync don't carry the batch size
        //when one position is ack by transaction the batch size is for `and` operation.
        if (!individualAckPositions.containsKey(position)) {
            this.individualAckPositions.put(position, new MutablePair<>(position, 0));
        } else {
            andAckSet(this.individualAckPositions.get(position).left, position);
        }
    }

    @Override
    public boolean checkIsCanDeleteConsumerPendingAck(PositionImpl position) {
        if (!individualAckPositions.containsKey(position)) {
            return true;
        } else {
            position = individualAckPositions.get(position).left;
            if (position.hasAckSet()) {
                BitSetRecyclable bitSetRecyclable = BitSetRecyclable.valueOf(position.getAckSet());
                if (bitSetRecyclable.isEmpty()) {
                    bitSetRecyclable.recycle();
                    return true;
                } else {
                    bitSetRecyclable.recycle();
                    return false;
                }
            } else {
                return true;
            }
        }
    }

    @Override
    public void clearIndividualPosition(Position position) {
        if (individualAckPositions == null) {
            return;
        }

        if (position instanceof PositionImpl) {
            individualAckPositions.remove(position);
            for (PositionImpl individualAckPosition : individualAckPositions.keySet()) {
                // individualAckPositions is currentSkipListMap, delete the position form individualAckPositions which
                // is smaller than can delete position
                if (individualAckPosition.compareTo((PositionImpl) position) <= 0) {
                    individualAckPositions.remove(individualAckPosition);
                } else {
                    return;
                }
            }
        }
    }

}
