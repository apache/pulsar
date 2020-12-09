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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;

/**
 * The default implementation of {@link PendingAckHandle}.
 */
@Slf4j
public class PendingAckHandleImpl extends PendingAckHandleState implements PendingAckHandle {

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
    private Map<PositionImpl, MutablePair<PositionImpl, Integer>> individualAckPositions;

    /**
     * The map is for transaction with position witch was cumulative acked by this transaction.
     * Only one cumulative ack position was acked by one transaction at the same time.
     */
    private Pair<TxnID, PositionImpl> cumulativeAckOfTransaction;

    private final String topicName;

    private final String subName;

    private final PersistentSubscription persistentSubscription;

    private final CompletableFuture<PendingAckStore> pendingAckStoreFuture;

    /**
     * This is for multi thread handle transaction pending ack,
     * we should think a better method to handle the multi thread problem.
     */
    private final Semaphore semaphore;

    public PendingAckHandleImpl(PersistentSubscription persistentSubscription) {
        super(State.None);
        this.topicName = persistentSubscription.getTopicName();
        this.subName = persistentSubscription.getName();
        this.persistentSubscription = persistentSubscription;
        this.semaphore = new Semaphore(1);

        TransactionPendingAckStoreProvider pendingAckStoreProvider =
                ((PersistentTopic) this.persistentSubscription.getTopic())
                        .getBrokerService().getPulsar().getTransactionPendingAckStoreProvider();
        this.pendingAckStoreFuture =
                pendingAckStoreProvider.newPendingAckStore(persistentSubscription);

        this.pendingAckStoreFuture.thenAccept(pendingAckStore -> {
            if (pendingAckStore instanceof MLPendingAckStore) {
                changeToInitializingState();
                pendingAckStore.replayAsync(this,
                        ((PersistentTopic) persistentSubscription.getTopic()).getBrokerService()
                                .getPulsar().getTransactionReplayExecutor());
            }
        }).exceptionally(e -> {
            log.error("PendingAckHandleImpl init fail! TopicName : {}, SubName: {}", topicName, subName, e);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> individualAcknowledgeMessage(TxnID txnID,
                                                                List<MutablePair<PositionImpl, Integer>> positions) {
        if (txnID == null) {
            return FutureUtil.failedFuture(new NotAllowedException("TransactionID can not be null."));
        }
        if (positions == null) {
            return FutureUtil.failedFuture(new NotAllowedException("Positions can not be null."));
        }
        this.semaphore.acquireUninterruptibly();
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        for (MutablePair<PositionImpl, Integer> positionIntegerMutablePair : positions) {
            PositionImpl position = positionIntegerMutablePair.left;

            // If try to ack message already acked by committed transaction or normal acknowledge, throw exception.
            if (((ManagedCursorImpl) persistentSubscription.getCursor())
                    .isMessageDeleted(position)) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                        " try to ack message:" + position + " already acked before.";
                log.error(errorMsg);
                semaphore.release();
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
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                            " try to ack message:" + position + " already acked before.";
                    log.error(errorMsg);
                    semaphore.release();
                    return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
                }

                if (this.individualAckPositions != null && individualAckPositions.containsKey(position)
                        && isAckSetOverlap(individualAckPositions.get(position).getLeft().getAckSet(), ackSetOverlap)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                            " try to ack batch message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    semaphore.release();
                    return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
                }
            } else {
                if (this.individualAckPositions != null && this.individualAckPositions.containsKey(position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                            " try to ack message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    semaphore.release();
                    return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
                }
            }
        }
        this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                pendingAckStore.appendIndividualAck(txnID, positions).whenComplete((v, e) -> {
                    if (e != null) {
                        //we also modify the in memory state when append fail, because we don't know the persistent
                        // state, when wereplay it, it will produce the wrong operation. so we append fail, we should
                        // wait tc time out or client abort this transaction.
                        handleIndividualAck(txnID, positions);
                        semaphore.release();
                        completableFuture.completeExceptionally(e);
                    } else {
                        handleIndividualAck(txnID, positions);
                        semaphore.release();
                        completableFuture.complete(null);
                    }
                })).exceptionally(e -> {
            semaphore.release();
            completableFuture.completeExceptionally(e);
            return null;
        });
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> cumulativeAcknowledgeMessage(TxnID txnID,
                                                                             List<PositionImpl> positions) {

        if (txnID == null) {
            return FutureUtil.failedFuture(new NotAllowedException("TransactionID can not be null."));
        }
        if (positions == null) {
            return FutureUtil.failedFuture(new NotAllowedException("Positions can not be null."));
        }
        this.semaphore.acquireUninterruptibly();

        if (positions.size() != 1) {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                    " invalid cumulative ack received with multiple message ids.";
            log.error(errorMsg);
            semaphore.release();
            return FutureUtil.failedFuture(new NotAllowedException(errorMsg));
        }

        PositionImpl position = positions.get(0);

        if (position.compareTo((PositionImpl) persistentSubscription.getCursor().getMarkDeletedPosition()) <= 0) {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                    " try to cumulative ack position: " + position + " within range of cursor's " +
                    "markDeletePosition: " + persistentSubscription.getCursor().getMarkDeletedPosition();
            log.error(errorMsg);
            semaphore.release();
            return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] TxnID:[{}] Cumulative ack on {}.", topicName, subName, txnID.toString(), position);
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (cumulativeAckOfTransaction != null && (!this.cumulativeAckOfTransaction.getKey().equals(txnID)
                || compareToWithAckSet(position, this.cumulativeAckOfTransaction.getValue()) <= 0)) {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                    " try to cumulative batch ack position: " + position + " within range of current " +
                    "currentPosition: " + this.cumulativeAckOfTransaction.getValue();
            log.error(errorMsg);
            semaphore.release();
            return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
        }
        this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                pendingAckStore.appendCumulativeAck(txnID, position).whenComplete((v, e) -> {
                    if (e != null) {
                        //we also modify the in memory state when append fail, because we don't know the persistent
                        // state, when wereplay it, it will produce the wrong operation. so we append fail, we should
                        // wait tc time out or client abort this transaction.
                        handleCumulativeAck(txnID, position);
                        semaphore.release();
                        completableFuture.completeExceptionally(e);
                    } else {
                        handleCumulativeAck(txnID, position);
                        semaphore.release();
                        completableFuture.complete(null);
                    }
        })).exceptionally(e -> {
            semaphore.release();
            completableFuture.completeExceptionally(e);
            return null;
        });
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, Map<String, Long> properties) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(new ServiceUnitNotReadyException("PendingAckHandle not replay complete!"));
        }

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();

        // It's valid to create transaction then commit without doing any operation, which will cause
        // pendingAckMessagesMap to be null.
        this.semaphore.acquireUninterruptibly();
        if (this.cumulativeAckOfTransaction != null) {
            if (cumulativeAckOfTransaction.getKey().equals(txnID)) {
                pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.appendCommitMark(txnID, AckType.Cumulative).whenComplete((v, e) -> {
                            if (e != null) {
                                semaphore.release();
                                commitFuture.completeExceptionally(e);
                            } else {
                                persistentSubscription.acknowledgeMessage(
                                        Collections.singletonList(this.cumulativeAckOfTransaction.getValue()),
                                        AckType.Cumulative, properties);
                                this.cumulativeAckOfTransaction = null;
                                semaphore.release();
                                commitFuture.complete(null);
                            }
                        })
                ).exceptionally(e -> {
                    semaphore.release();
                    commitFuture.completeExceptionally(e);
                    return null;
                });
            } else {
                semaphore.release();
                commitFuture.complete(null);
            }
        } else {
            if (individualAckOfTransaction != null && individualAckOfTransaction.containsKey(txnID)) {
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.get(txnID);
                pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.appendCommitMark(txnID, AckType.Individual).whenComplete((v, e) -> {
                            if (e != null) {
                                semaphore.release();
                                commitFuture.completeExceptionally(e);
                            } else {
                                individualAckCommitCommon(txnID, pendingAckMessageForCurrentTxn, properties);
                                semaphore.release();
                                commitFuture.complete(null);
                            }
                        })
                ).exceptionally(e -> {
                    semaphore.release();
                    commitFuture.completeExceptionally(e);
                    return null;
                });
            } else {
                semaphore.release();
                commitFuture.complete(null);
            }
        }
        return commitFuture;
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer) {
        if (!checkIfReady()) {
            return FutureUtil.failedFuture(new ServiceUnitNotReadyException("PendingAckHandle not replay complete!"));
        }
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        this.semaphore.acquireUninterruptibly();
        if (this.cumulativeAckOfTransaction != null) {
            pendingAckStoreFuture.thenAccept(pendingAckStore ->
                    pendingAckStore.appendAbortMark(txnId, AckType.Individual).whenComplete((v, e) -> {
                        if (e != null) {
                            semaphore.release();
                            abortFuture.completeExceptionally(e);
                        } else {
                            if (this.cumulativeAckOfTransaction.getKey().equals(txnId)) {
                                this.cumulativeAckOfTransaction = null;
                            }
                            this.persistentSubscription.redeliverUnacknowledgedMessages(consumer);
                            semaphore.release();
                            abortFuture.complete(null);
                        }
                    })
            ).exceptionally(e -> {
                semaphore.release();
                abortFuture.completeExceptionally(e);
                return null;
            });
        } else if (this.individualAckOfTransaction != null){
            HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                    individualAckOfTransaction.get(txnId);
            if (pendingAckMessageForCurrentTxn != null) {
                pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.appendAbortMark(txnId, AckType.Individual).whenComplete((v, e) -> {
                            if (e != null) {
                                semaphore.release();
                                abortFuture.completeExceptionally(e);
                            } else {
                                individualAckAbortCommon(txnId, pendingAckMessageForCurrentTxn);
                                this.persistentSubscription.redeliverUnacknowledgedMessages(consumer,
                                        new ArrayList<>(pendingAckMessageForCurrentTxn.values()));
                                semaphore.release();
                                abortFuture.complete(null);
                            }
                        })
                ).exceptionally(e -> {
                    semaphore.release();
                    abortFuture.completeExceptionally(e);
                    return null;
                });
            } else {
                semaphore.release();
                abortFuture.complete(null);
            }
        } else {
            semaphore.release();
            abortFuture.complete(null);
        }
        return abortFuture;
    }

    @Override
    public void syncBatchPositionAckSetForTransaction(PositionImpl position) {
        this.semaphore.acquireUninterruptibly();
        try {
            if (individualAckPositions == null) {
                individualAckPositions = new HashMap<>();
            }
            // sync don't carry the batch size
            // when one position is ack by transaction the batch size is for `and` operation.
            if (!individualAckPositions.containsKey(position)) {
                this.individualAckPositions.put(position, new MutablePair<>(position, 0));
            } else {
                andAckSet(this.individualAckPositions.get(position).left, position);
            }
        } finally {
            semaphore.release();
        }
    }

    @Override
    public boolean checkIsCanDeleteConsumerPendingAck(PositionImpl position) {
        this.semaphore.acquireUninterruptibly();
        try {
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
        } finally {
            semaphore.release();
        }
    }

    protected void handleAbort(TxnID txnID, AckType ackType) {
        if (ackType == AckType.Cumulative) {
            this.cumulativeAckOfTransaction = null;
        } else {
            if (this.individualAckOfTransaction != null) {
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.get(txnID);
                individualAckAbortCommon(txnID, pendingAckMessageForCurrentTxn);
            }
        }
    }

    private void individualAckAbortCommon(TxnID txnID,
                                          HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn) {
        for (Map.Entry<PositionImpl, PositionImpl> entry :
                pendingAckMessageForCurrentTxn.entrySet()) {
            if (entry.getValue().hasAckSet() &&
                    individualAckPositions.containsKey(entry.getValue())) {
                BitSetRecyclable thisBitSet =
                        BitSetRecyclable.valueOf(entry.getValue().getAckSet());
                thisBitSet.flip(0, individualAckPositions.get(entry.getValue()).right);
                BitSetRecyclable otherBitSet =
                        BitSetRecyclable.valueOf(individualAckPositions
                                .get(entry.getValue()).left.getAckSet());
                otherBitSet.or(thisBitSet);
                individualAckPositions.get(entry.getKey())
                        .left.setAckSet(otherBitSet.toLongArray());
                otherBitSet.recycle();
                thisBitSet.recycle();
            } else {
                individualAckPositions.remove(entry.getValue());
            }
        }
        individualAckOfTransaction.remove(txnID);
    }

    protected void handleCommit(TxnID txnID, AckType ackType, Map<String, Long> properties) {
        if (ackType == AckType.Cumulative) {
            persistentSubscription.acknowledgeMessage(
                    Collections.singletonList(this.cumulativeAckOfTransaction.getValue()),
                    AckType.Cumulative, properties);
            this.cumulativeAckOfTransaction = null;
        } else {
            if (this.individualAckOfTransaction != null) {
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.get(txnID);
                individualAckCommitCommon(txnID, pendingAckMessageForCurrentTxn, null);
            }
        }
    }

    private void individualAckCommitCommon(TxnID txnID,
                                           HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn,
                                           Map<String,Long> properties) {
        if (pendingAckMessageForCurrentTxn != null) {
            persistentSubscription.acknowledgeMessage(
                    new ArrayList<>(pendingAckMessageForCurrentTxn.values()),
                    AckType.Individual, properties);
            individualAckOfTransaction.remove(txnID);
        }
    }

    protected void handleIndividualAck(TxnID txnID, List<MutablePair<PositionImpl, Integer>> positions) {
        for (int i = 0; i < positions.size(); i++) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}] Individual acks on {}", topicName,
                        subName, txnID.toString(), positions);
            }
            if (individualAckOfTransaction == null) {
                individualAckOfTransaction = new HashMap<>();
            }

            if (individualAckPositions == null) {
                individualAckPositions = new HashMap<>();
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
                    MutablePair<PositionImpl, Integer> positionPair =
                            this.individualAckPositions.get(position);
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
    }

    protected void handleCumulativeAck(TxnID txnID, PositionImpl position) {
        if (this.cumulativeAckOfTransaction == null) {
            this.cumulativeAckOfTransaction = MutablePair.of(txnID, position);
        } else if (this.cumulativeAckOfTransaction.getKey().equals(txnID)
                && compareToWithAckSet(position, this.cumulativeAckOfTransaction.getValue()) > 0) {
            this.cumulativeAckOfTransaction.setValue(position);
        }
    }

    public String getTopicName() {
        return topicName;
    }

    public String getSubName() {
        return subName;
    }

    @Override
    public void clearIndividualPosition(Position position) {
        if (individualAckPositions == null) {
            return;
        }

        if (position instanceof PositionImpl) {
            individualAckPositions.remove(position);
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        return this.pendingAckStoreFuture.thenAccept(PendingAckStore::closeAsync);
    }
}