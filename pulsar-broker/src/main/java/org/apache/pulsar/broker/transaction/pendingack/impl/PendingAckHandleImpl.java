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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.collections4.map.LinkedMap;
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
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
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

    private CompletableFuture<PendingAckStore> pendingAckStoreFuture;

    private final CompletableFuture<PendingAckHandle> pendingAckHandleCompletableFuture = new CompletableFuture<>();

    private final TransactionPendingAckStoreProvider pendingAckStoreProvider;

    private final BlockingQueue<Runnable> acceptQueue = new LinkedBlockingDeque<>();

    public PendingAckHandleImpl(PersistentSubscription persistentSubscription) {
        super(State.None);
        this.topicName = persistentSubscription.getTopicName();
        this.subName = persistentSubscription.getName();
        this.persistentSubscription = persistentSubscription;

        this.pendingAckStoreProvider = ((PersistentTopic) this.persistentSubscription.getTopic())
                        .getBrokerService().getPulsar().getTransactionPendingAckStoreProvider();
        pendingAckStoreProvider.checkInitializedBefore(persistentSubscription).thenAccept(init -> {
            if (init) {
                initPendingAckStore();
            } else {
                completeHandleFuture();
            }
        });
    }

    private void initPendingAckStore() {
        if (changeToInitializingState()) {
            synchronized (PendingAckHandleImpl.this) {
                if (!checkIfClose()) {
                    this.pendingAckStoreFuture =
                            pendingAckStoreProvider.newPendingAckStore(persistentSubscription);
                    this.pendingAckStoreFuture.thenAccept(pendingAckStore -> {
                        pendingAckStore.replayAsync(this,
                                ((PersistentTopic) persistentSubscription.getTopic()).getBrokerService()
                                        .getPulsar().getTransactionReplayExecutor());
                    }).exceptionally(e -> {
                        acceptQueue.clear();
                        changeToErrorState();
                        log.error("PendingAckHandleImpl init fail! TopicName : {}, SubName: {}", topicName, subName, e);
                        return null;
                    });
                }
            }
        }
    }

    private void addIndividualAcknowledgeMessageRequest(TxnID txnID,
                                                        List<MutablePair<PositionImpl, Integer>> positions,
                                                        CompletableFuture<Void> completableFuture) {
        acceptQueue.add(() -> individualAcknowledgeMessage(txnID, positions, true).thenAccept(v ->
                completableFuture.complete(null)).exceptionally(e -> {
            completableFuture.completeExceptionally(e);
            return null;
        }));
    }

    @Override
    public CompletableFuture<Void> individualAcknowledgeMessage(TxnID txnID,
                                                                List<MutablePair<PositionImpl, Integer>> positions,
                                                                boolean isInCacheRequest) {

        if (!checkIfReady()) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            synchronized (PendingAckHandleImpl.this) {
                switch (state) {
                    case Initializing:
                        addIndividualAcknowledgeMessageRequest(txnID, positions, completableFuture);
                        return completableFuture;
                    case None:
                        addIndividualAcknowledgeMessageRequest(txnID, positions, completableFuture);
                        initPendingAckStore();
                        return completableFuture;
                    case Error:
                        completableFuture.completeExceptionally(
                                new ServiceUnitNotReadyException("PendingAckHandle not replay error!"));
                        return completableFuture;
                    case Close:
                        completableFuture.completeExceptionally(
                                new ServiceUnitNotReadyException("PendingAckHandle have been closed!"));
                        return completableFuture;
                    default:
                        break;
                }
            }
        }

        if (txnID == null) {
            return FutureUtil.failedFuture(new NotAllowedException("TransactionID can not be null."));
        }
        if (positions == null) {
            return FutureUtil.failedFuture(new NotAllowedException("Positions can not be null."));
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                pendingAckStore.appendIndividualAck(txnID, positions).thenAccept(v -> {
                    synchronized (org.apache.pulsar.broker.transaction.pendingack.impl.PendingAckHandleImpl.this) {
                        for (MutablePair<PositionImpl, Integer> positionIntegerMutablePair : positions) {

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] individualAcknowledgeMessage position: [{}], "
                                                + "txnId: [{}], subName: [{}]", topicName,
                                        positionIntegerMutablePair.left, txnID, subName);
                            }
                            PositionImpl position = positionIntegerMutablePair.left;

                            // If try to ack message already acked by committed transaction or
                            // normal acknowledge,throw exception.
                            if (((ManagedCursorImpl) persistentSubscription.getCursor())
                                    .isMessageDeleted(position)) {
                                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                                        + " try to ack message:" + position + " already acked before.";
                                log.error(errorMsg);
                                completableFuture
                                        .completeExceptionally(new TransactionConflictException(errorMsg));
                                return;
                            }

                            if (position.hasAckSet()) {
                                //in order to jude the bit set is over lap, so set the covering
                                // the batch size bit to 1,should know the two
                                // bit set don't have the same point is 0
                                BitSetRecyclable bitSetRecyclable =
                                        BitSetRecyclable.valueOf(position.getAckSet());
                                if (positionIntegerMutablePair.right > bitSetRecyclable.size()) {
                                    bitSetRecyclable.set(positionIntegerMutablePair.right);
                                }
                                bitSetRecyclable.set(positionIntegerMutablePair.right, bitSetRecyclable.size());
                                long[] ackSetOverlap = bitSetRecyclable.toLongArray();
                                bitSetRecyclable.recycle();
                                if (isAckSetOverlap(ackSetOverlap,
                                        ((ManagedCursorImpl) persistentSubscription.getCursor())
                                                .getBatchPositionAckSet(position))) {
                                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:"
                                            + txnID + " try to ack message:"
                                            + position + " already acked before.";
                                    log.error(errorMsg);
                                    completableFuture
                                            .completeExceptionally(new TransactionConflictException(errorMsg));
                                    return;
                                }

                                if (individualAckPositions != null
                                        && individualAckPositions.containsKey(position)
                                        && isAckSetOverlap(individualAckPositions
                                        .get(position).getLeft().getAckSet(), ackSetOverlap)) {
                                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:"
                                            + txnID + " try to ack batch message:"
                                            + position + " in pending ack status.";
                                    log.error(errorMsg);
                                    completableFuture
                                            .completeExceptionally(new TransactionConflictException(errorMsg));
                                    return;
                                }
                            } else {
                                if (individualAckPositions != null
                                        && individualAckPositions.containsKey(position)) {
                                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:"
                                            + txnID + " try to ack message:"
                                            + position + " in pending ack status.";
                                    log.error(errorMsg);
                                    completableFuture
                                            .completeExceptionally(new TransactionConflictException(errorMsg));
                                    return;
                                }
                            }
                        }

                        handleIndividualAck(txnID, positions);
                        completableFuture.complete(null);
                    }
                }).exceptionally(e -> {
                    synchronized (PendingAckHandleImpl.this) {
                        // we also modify the in memory state when append fail,
                        // because we don't know the persistent state, when were replay it,
                        // it will produce the wrong operation. so we append fail,
                        // we should wait tc time out or client abort this transaction.
                        handleIndividualAck(txnID, positions);
                        completableFuture.completeExceptionally(e.getCause());
                    }
                    return null;
                })).exceptionally(e -> {
            completableFuture.completeExceptionally(e);
            return null;
        });
        return completableFuture;
    }

    private void addCumulativeAcknowledgeMessageRequest(TxnID txnID,
                                                        List<PositionImpl> positions,
                                                        CompletableFuture<Void> completableFuture) {
        acceptQueue.add(() -> cumulativeAcknowledgeMessage(txnID, positions, true).thenAccept(v ->
                completableFuture.complete(null)).exceptionally(e -> {
            completableFuture.completeExceptionally(e);
            return null;
        }));
    }

    @Override
    public CompletableFuture<Void> cumulativeAcknowledgeMessage(TxnID txnID,
                                                                List<PositionImpl> positions,
                                                                boolean isInCacheRequest) {
        if (!checkIfReady()) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            synchronized (PendingAckHandleImpl.this) {
                switch (state) {
                    case Initializing:
                        addCumulativeAcknowledgeMessageRequest(txnID, positions, completableFuture);
                        return completableFuture;
                    case None:
                        addCumulativeAcknowledgeMessageRequest(txnID, positions, completableFuture);
                        initPendingAckStore();
                        return completableFuture;
                    case Error:
                        completableFuture.completeExceptionally(
                                new ServiceUnitNotReadyException("PendingAckHandle not replay error!"));
                        return completableFuture;
                    case Close:
                        completableFuture.completeExceptionally(
                                new ServiceUnitNotReadyException("PendingAckHandle have been closed!"));
                        return completableFuture;
                    default:
                        break;

                }
            }
        }

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

        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                pendingAckStore.appendCumulativeAck(txnID, position).thenAccept(v -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] cumulativeAcknowledgeMessage position: [{}], "
                                + "txnID:[{}], subName: [{}].", topicName, txnID.toString(), position, subName);
                    }

                    if (position.compareTo((PositionImpl) persistentSubscription.getCursor()
                            .getMarkDeletedPosition()) <= 0) {
                        String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                                + " try to cumulative ack position: " + position + " within range of cursor's "
                                + "markDeletePosition: "
                                + persistentSubscription.getCursor().getMarkDeletedPosition();
                        log.error(errorMsg);
                        completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                        return;
                    }

                    if (cumulativeAckOfTransaction != null && (!cumulativeAckOfTransaction.getKey().equals(txnID)
                            || compareToWithAckSet(position, cumulativeAckOfTransaction.getValue()) <= 0)) {
                        String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                                + " try to cumulative batch ack position: " + position + " within range of current "
                                + "currentPosition: " + cumulativeAckOfTransaction.getValue();
                        log.error(errorMsg);
                        completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                        return;
                    }

                    handleCumulativeAck(txnID, position);
                    completableFuture.complete(null);
                }).exceptionally(e -> {
                    //we also modify the in memory state when append fail, because we don't know the persistent
                    // state, when wereplay it, it will produce the wrong operation. so we append fail, we should
                    // wait tc time out or client abort this transaction.
                    handleCumulativeAck(txnID, position);
                    completableFuture.completeExceptionally(e.getCause());
                    return null;
                })
        ).exceptionally(e -> {
            completableFuture.completeExceptionally(e);
            return null;
        });
        return completableFuture;
    }

    private void addCommitTxnRequest(TxnID txnId, Map<String, Long> properties, long lowWaterMark,
                                    CompletableFuture<Void> completableFuture) {
        acceptQueue.add(() -> commitTxn(txnId, properties, lowWaterMark, true).thenAccept(v ->
                completableFuture.complete(null)).exceptionally(e -> {
            completableFuture.completeExceptionally(e);
            return null;
        }));
    }

    @Override
    public synchronized CompletableFuture<Void> commitTxn(TxnID txnID, Map<String, Long> properties,
                                                          long lowWaterMark, boolean isInCacheRequest) {
        if (!checkIfReady()) {
            synchronized (PendingAckHandleImpl.this) {
                if (state == State.Initializing) {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    addCommitTxnRequest(txnID, properties, lowWaterMark, completableFuture);
                    return completableFuture;
                } else if (state == State.None) {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    addCommitTxnRequest(txnID, properties, lowWaterMark, completableFuture);
                    initPendingAckStore();
                    return completableFuture;
                } else if (checkIfReady()) {

                } else {
                    if (state == State.Error) {
                        return FutureUtil.failedFuture(
                                new ServiceUnitNotReadyException("PendingAckHandle not replay error!"));
                    } else {
                        return FutureUtil.failedFuture(
                                new ServiceUnitNotReadyException("PendingAckHandle have been closed!"));
                    }

                }
            }
        }

        if (!acceptQueue.isEmpty() && !isInCacheRequest) {
            synchronized (PendingAckHandleImpl.this) {
                if (!acceptQueue.isEmpty()) {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    addCommitTxnRequest(txnID, properties, lowWaterMark, completableFuture);
                    return completableFuture;
                }
            }
        }

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();

        // It's valid to create transaction then commit without doing any operation, which will cause
        // pendingAckMessagesMap to be null.
        if (this.cumulativeAckOfTransaction != null) {
            if (cumulativeAckOfTransaction.getKey().equals(txnID)) {
                pendingAckStoreFuture.thenAccept(pendingAckStore -> pendingAckStore
                        .appendCommitMark(txnID, AckType.Cumulative).thenAccept(v -> {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Transaction pending ack store commit txnId : [{}] "
                                        + "success! subName: [{}]", topicName, txnID, subName);
                            }
                            persistentSubscription.acknowledgeMessage(
                                    Collections.singletonList(cumulativeAckOfTransaction.getValue()),
                                    AckType.Cumulative, properties);
                            cumulativeAckOfTransaction = null;
                            commitFuture.complete(null);
                        }).exceptionally(e -> {
                            log.error("[{}] Transaction pending ack store commit txnId : [{}] fail!",
                                    topicName, txnID, e);
                            commitFuture.completeExceptionally(e);
                            return null;
                        })).exceptionally(e -> {
                    commitFuture.completeExceptionally(e);
                    return null;
                });
            } else {
                commitFuture.complete(null);
            }
        } else {
            pendingAckStoreFuture.thenAccept(pendingAckStore ->
                    pendingAckStore.appendCommitMark(txnID, AckType.Individual).thenAccept(v -> {
                        synchronized (PendingAckHandleImpl.this) {
                            if (individualAckOfTransaction != null && individualAckOfTransaction.containsKey(txnID)) {
                                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                                        individualAckOfTransaction.get(txnID);
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Transaction pending ack store commit txnId : "
                                            + "[{}] success! subName: [{}]", topicName, txnID, subName);
                                }
                                individualAckCommitCommon(txnID, pendingAckMessageForCurrentTxn, properties);
                                commitFuture.complete(null);
                                handleLowWaterMark(txnID, lowWaterMark);
                            } else {
                                commitFuture.complete(null);
                            }
                        }
                    }).exceptionally(e -> {
                        log.error("[{}] Transaction pending ack store commit txnId : [{}] fail!",
                                topicName, txnID, e);
                        commitFuture.completeExceptionally(e.getCause());
                        return null;
                    })).exceptionally(e -> {
                commitFuture.completeExceptionally(e);
                return null;
            });
        }
        return commitFuture;
    }

    private void addAbortTxnRequest(TxnID txnId, Consumer consumer, long lowWaterMark,
                                    CompletableFuture<Void> completableFuture) {
        acceptQueue.add(() -> abortTxn(txnId, consumer, lowWaterMark, true).thenAccept(v ->
                completableFuture.complete(null)).exceptionally(e -> {
            completableFuture.completeExceptionally(e);
            return null;
        }));
    }

    @Override
    public synchronized CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer,
                                                         long lowWaterMark, boolean isInCacheRequest) {
        if (!checkIfReady()) {
            synchronized (PendingAckHandleImpl.this) {
                if (state == State.Initializing) {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    addAbortTxnRequest(txnId, consumer, lowWaterMark, completableFuture);
                    return completableFuture;
                } else if (state == State.None) {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    addAbortTxnRequest(txnId, consumer, lowWaterMark, completableFuture);
                    initPendingAckStore();
                    return completableFuture;
                } else if (checkIfReady()) {

                } else {
                    if (state == State.Error) {
                        return FutureUtil.failedFuture(
                                new ServiceUnitNotReadyException("PendingAckHandle not replay error!"));
                    } else {
                        return FutureUtil.failedFuture(
                                new ServiceUnitNotReadyException("PendingAckHandle have been closed!"));
                    }
                }
            }
        }


        if (!acceptQueue.isEmpty() && !isInCacheRequest) {
            synchronized (PendingAckHandleImpl.this) {
                if (!acceptQueue.isEmpty()) {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    addAbortTxnRequest(txnId, consumer, lowWaterMark, completableFuture);
                    return completableFuture;
                }
            }
        }

        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        if (this.cumulativeAckOfTransaction != null) {
            pendingAckStoreFuture.thenAccept(pendingAckStore ->
                    pendingAckStore.appendAbortMark(txnId, AckType.Cumulative).thenAccept(v -> {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Transaction pending ack store abort txnId : [{}] success! subName: [{}]",
                                    topicName, txnId, subName);
                        }
                        if (cumulativeAckOfTransaction.getKey().equals(txnId)) {
                            cumulativeAckOfTransaction = null;
                        }
                        persistentSubscription.redeliverUnacknowledgedMessages(consumer);
                        abortFuture.complete(null);
                    }).exceptionally(e -> {
                        log.error("[{}] Transaction pending ack store abort txnId : [{}] fail!",
                                topicName, txnId, e);
                        abortFuture.completeExceptionally(e);
                        return null;
                    })
            ).exceptionally(e -> {
                abortFuture.completeExceptionally(e);
                return null;
            });
        } else if (this.individualAckOfTransaction != null) {
            pendingAckStoreFuture.thenAccept(pendingAckStore ->
                    pendingAckStore.appendAbortMark(txnId, AckType.Individual).thenAccept(v -> {
                        synchronized (PendingAckHandleImpl.this) {
                            HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                                    individualAckOfTransaction.get(txnId);
                            if (pendingAckMessageForCurrentTxn != null) {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Transaction pending ack store abort txnId : [{}] success! "
                                            + "subName: [{}]", topicName, txnId, subName);
                                }
                                individualAckAbortCommon(txnId, pendingAckMessageForCurrentTxn);
                                persistentSubscription.redeliverUnacknowledgedMessages(consumer,
                                        new ArrayList<>(pendingAckMessageForCurrentTxn.values()));
                                abortFuture.complete(null);
                                handleLowWaterMark(txnId, lowWaterMark);
                            } else {
                                abortFuture.complete(null);
                            }
                        }
                    }).exceptionally(e -> {
                        log.error("[{}] Transaction pending ack store abort txnId : [{}] fail!",
                                topicName, txnId, e);
                        abortFuture.completeExceptionally(e);
                        return null;
                    })
            ).exceptionally(e -> {
                log.error("[{}] abortTxn", txnId, e);
                abortFuture.completeExceptionally(e);
                return null;
            });
        } else {
            abortFuture.complete(null);
        }
        return abortFuture;
    }

    private void handleLowWaterMark(TxnID txnID, long lowWaterMark) {
        if (individualAckOfTransaction != null && !individualAckOfTransaction.isEmpty()) {
            TxnID firstTxn = individualAckOfTransaction.firstKey();

            if (firstTxn.getMostSigBits() == txnID.getMostSigBits()
                    && firstTxn.getLeastSigBits() <= lowWaterMark) {
                this.pendingAckStoreFuture.whenComplete((pendingAckStore, throwable) -> {
                    if (throwable == null) {
                        pendingAckStore.appendAbortMark(txnID, AckType.Individual).thenAccept(v -> {
                            synchronized (PendingAckHandleImpl.this) {
                                log.warn("[{}] Transaction pending ack handle low water mark success! txnId : [{}], "
                                        + "lowWaterMark : [{}]", topicName, txnID, lowWaterMark);
                                individualAckOfTransaction.remove(firstTxn);
                                handleLowWaterMark(txnID, lowWaterMark);
                            }
                        }).exceptionally(e -> {
                            log.warn("[{}] Transaction pending ack handle low water mark fail! txnId : [{}], "
                                    + "lowWaterMark : [{}]", topicName, txnID, lowWaterMark);
                            return null;
                        });
                    }
                });
            }
        }
    }

    @Override
    public synchronized void syncBatchPositionAckSetForTransaction(PositionImpl position) {
        if (individualAckPositions == null) {
            individualAckPositions = new ConcurrentSkipListMap<>();
        }
        // sync don't carry the batch size
        // when one position is ack by transaction the batch size is for `and` operation.
        if (!individualAckPositions.containsKey(position)) {
            this.individualAckPositions.put(position, new MutablePair<>(position, 0));
        } else {
            andAckSet(this.individualAckPositions.get(position).left, position);
        }
    }

    @Override
    public synchronized boolean checkIsCanDeleteConsumerPendingAck(PositionImpl position) {
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

    protected void handleAbort(TxnID txnID, AckType ackType) {
        if (ackType == AckType.Cumulative) {
            this.cumulativeAckOfTransaction = null;
        } else {
            if (this.individualAckOfTransaction != null) {
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.get(txnID);
                if (pendingAckMessageForCurrentTxn != null) {
                    individualAckAbortCommon(txnID, pendingAckMessageForCurrentTxn);
                }
            }
        }
    }

    private void individualAckAbortCommon(TxnID txnID, HashMap<PositionImpl, PositionImpl> currentTxn) {
        for (Map.Entry<PositionImpl, PositionImpl> entry :
                currentTxn.entrySet()) {
            if (entry.getValue().hasAckSet()
                    && individualAckPositions.containsKey(entry.getValue())) {
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
            if (this.cumulativeAckOfTransaction != null) {
                persistentSubscription.acknowledgeMessage(
                        Collections.singletonList(this.cumulativeAckOfTransaction.getValue()),
                        AckType.Cumulative, properties);
            }
            this.cumulativeAckOfTransaction = null;
        } else {
            if (this.individualAckOfTransaction != null) {
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        individualAckOfTransaction.get(txnID);
                if (pendingAckMessageForCurrentTxn != null) {
                    individualAckCommitCommon(txnID, pendingAckMessageForCurrentTxn, null);
                }
            }
        }
    }

    private void individualAckCommitCommon(TxnID txnID,
                                           HashMap<PositionImpl, PositionImpl> currentTxn,
                                           Map<String, Long> properties) {
        if (currentTxn != null) {
            persistentSubscription.acknowledgeMessage(new ArrayList<>(currentTxn.values()),
                    AckType.Individual, properties);
            individualAckOfTransaction.remove(txnID);
        }
    }

    private void handleIndividualAck(TxnID txnID, List<MutablePair<PositionImpl, Integer>> positions) {
        for (int i = 0; i < positions.size(); i++) {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}] Individual acks on {}", topicName,
                        subName, txnID.toString(), positions);
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

    private void handleCumulativeAck(TxnID txnID, PositionImpl position) {
        if (this.cumulativeAckOfTransaction == null) {
            this.cumulativeAckOfTransaction = MutablePair.of(txnID, position);
        } else if (this.cumulativeAckOfTransaction.getKey().equals(txnID)
                && compareToWithAckSet(position, this.cumulativeAckOfTransaction.getValue()) > 0) {
            this.cumulativeAckOfTransaction.setValue(position);
        }
    }

    protected void handleCumulativeAckRecover(TxnID txnID, PositionImpl position) {
        if ((position.compareTo((PositionImpl) persistentSubscription.getCursor()
                .getMarkDeletedPosition()) > 0) && (cumulativeAckOfTransaction == null
                || (cumulativeAckOfTransaction.getKey().equals(txnID)
                && compareToWithAckSet(position, cumulativeAckOfTransaction.getValue()) > 0))) {
            handleCumulativeAck(txnID, position);
        }
    }

    protected void handleIndividualAckRecover(TxnID txnID, List<MutablePair<PositionImpl, Integer>> positions) {
        for (MutablePair<PositionImpl, Integer> positionIntegerMutablePair : positions) {
            PositionImpl position = positionIntegerMutablePair.left;

            // If try to ack message already acked by committed transaction or
            // normal acknowledge,throw exception.
            if (((ManagedCursorImpl) persistentSubscription.getCursor())
                    .isMessageDeleted(position)) {
                return;
            }

            if (position.hasAckSet()) {
                //in order to jude the bit set is over lap, so set the covering
                // the batch size bit to 1,should know the two
                // bit set don't have the same point is 0
                BitSetRecyclable bitSetRecyclable =
                        BitSetRecyclable.valueOf(position.getAckSet());
                if (positionIntegerMutablePair.right > bitSetRecyclable.size()) {
                    bitSetRecyclable.set(positionIntegerMutablePair.right);
                }
                bitSetRecyclable.set(positionIntegerMutablePair.right, bitSetRecyclable.size());
                long[] ackSetOverlap = bitSetRecyclable.toLongArray();
                bitSetRecyclable.recycle();
                if (isAckSetOverlap(ackSetOverlap,
                        ((ManagedCursorImpl) persistentSubscription.getCursor())
                                .getBatchPositionAckSet(position))) {
                    return;
                }

                if (individualAckPositions != null
                        && individualAckPositions.containsKey(position)
                        && isAckSetOverlap(individualAckPositions
                        .get(position).getLeft().getAckSet(), ackSetOverlap)) {
                    return;
                }
            } else {
                if (individualAckPositions != null
                        && individualAckPositions.containsKey(position)) {
                    return;
                }
            }
        }
        handleIndividualAck(txnID, positions);
    }

    public String getTopicName() {
        return topicName;
    }

    public String getSubName() {
        return subName;
    }

    @Override
    public synchronized void clearIndividualPosition(Position position) {
        if (individualAckPositions == null) {
            return;
        }

        if (position instanceof PositionImpl) {
            individualAckPositions.remove(position);
        }

        individualAckPositions.forEach((persistentPosition, positionIntegerMutablePair) -> {
            if (persistentPosition.compareTo((PositionImpl) persistentSubscription
                    .getCursor().getMarkDeletedPosition()) < 0) {
                individualAckPositions.remove(persistentPosition);
            }
        });
    }

    @Override
    public CompletableFuture<PendingAckHandle> pendingAckHandleFuture() {
        return pendingAckHandleCompletableFuture;
    }

    @Override
    public TransactionPendingAckStats getStats() {
        TransactionPendingAckStats transactionPendingAckStats = new TransactionPendingAckStats();
        transactionPendingAckStats.state = this.getState().name();
        return transactionPendingAckStats;
    }

    public synchronized void completeHandleFuture() {
        if (!this.pendingAckHandleCompletableFuture.isDone()) {
            this.pendingAckHandleCompletableFuture.complete(PendingAckHandleImpl.this);
        }
    }

    @Override
    public TransactionInPendingAckStats getTransactionInPendingAckStats(TxnID txnID) {
        TransactionInPendingAckStats transactionInPendingAckStats = new TransactionInPendingAckStats();
        if (cumulativeAckOfTransaction != null && cumulativeAckOfTransaction.getLeft().equals(txnID)) {
            PositionImpl position = cumulativeAckOfTransaction.getRight();
            StringBuilder stringBuilder = new StringBuilder()
                    .append(position.getLedgerId())
                    .append(':')
                    .append(position.getEntryId());
            if (cumulativeAckOfTransaction.getRight().hasAckSet()) {
                BitSetRecyclable bitSetRecyclable =
                        BitSetRecyclable.valueOf(cumulativeAckOfTransaction.getRight().getAckSet());
                if (!bitSetRecyclable.isEmpty()) {
                    stringBuilder.append(":").append(bitSetRecyclable.nextSetBit(0) - 1);
                }
            }
            transactionInPendingAckStats.cumulativeAckPosition = stringBuilder.toString();
        }
        return transactionInPendingAckStats;
    }

    @Override
    public CompletableFuture<Void> close() {
        changeToCloseState();
        synchronized (PendingAckHandleImpl.this) {
            if (this.pendingAckStoreFuture != null) {
                return this.pendingAckStoreFuture.thenAccept(PendingAckStore::closeAsync);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    public CompletableFuture<ManagedLedger> getStoreManageLedger() {
        if (this.pendingAckStoreFuture.isDone()) {
            return this.pendingAckStoreFuture.thenCompose(pendingAckStore -> {
                if (pendingAckStore instanceof MLPendingAckStore) {
                    return ((MLPendingAckStore) pendingAckStore).getManagedLedger();
                } else {
                    return FutureUtil.failedFuture(
                            new NotAllowedException("Pending ack handle don't use managedLedger!"));
                }
            });
        } else {
            return FutureUtil.failedFuture(new ServiceUnitNotReadyException("Pending ack have not init success!"));
        }
    }

    protected void handleCacheRequest() {
        while (true) {
            Runnable runnable = acceptQueue.poll();

            if (runnable != null) {
                runnable.run();
            } else {
                break;
            }
        }
    }
}