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
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import lombok.Getter;
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

    /**
     * The map is used to store the lowWaterMarks which key is TC ID and value is lowWaterMark of the TC.
     */
    private final ConcurrentHashMap<Long, Long> lowWaterMarks = new ConcurrentHashMap<>();

    private final Semaphore handleLowWaterMark = new Semaphore(1);

    @Getter
    private final ExecutorService internalPinnedExecutor;


    public PendingAckHandleImpl(PersistentSubscription persistentSubscription) {
        super(State.None);
        this.topicName = persistentSubscription.getTopicName();
        this.subName = persistentSubscription.getName();
        this.persistentSubscription = persistentSubscription;
        internalPinnedExecutor = persistentSubscription
                .getTopic()
                .getBrokerService()
                .getPulsar()
                .getTransactionExecutorProvider()
                .getExecutor(this);

        this.pendingAckStoreProvider = this.persistentSubscription.getTopic()
                        .getBrokerService().getPulsar().getTransactionPendingAckStoreProvider();

        pendingAckStoreProvider.checkInitializedBefore(persistentSubscription)
                .thenAcceptAsync(init -> {
                    if (init) {
                        initPendingAckStore();
                    } else {
                        completeHandleFuture();
                    }
                }, internalPinnedExecutor)
                .exceptionally(e -> {
                    Throwable t = FutureUtil.unwrapCompletionException(e);
                    changeToErrorState();
                    exceptionHandleFuture(t);
                    this.pendingAckStoreFuture.completeExceptionally(t);
                    return null;
                });
    }

    private void initPendingAckStore() {
        if (changeToInitializingState()) {
            if (!checkIfClose()) {
                this.pendingAckStoreFuture =
                        pendingAckStoreProvider.newPendingAckStore(persistentSubscription);
                this.pendingAckStoreFuture.thenAccept(pendingAckStore -> {
                    pendingAckStore.replayAsync(this, internalPinnedExecutor);
                }).exceptionally(e -> {
                    handleCacheRequest();
                    changeToErrorState();
                    log.error("PendingAckHandleImpl init fail! TopicName : {}, SubName: {}", topicName, subName, e);
                    exceptionHandleFuture(e.getCause());
                    return null;
                });
            }
        }
    }

    private void addIndividualAcknowledgeMessageRequest(TxnID txnID,
                                                        List<MutablePair<PositionImpl, Integer>> positions,
                                                        CompletableFuture<Void> completableFuture) {
        acceptQueue.add(() -> internalIndividualAcknowledgeMessage(txnID, positions, completableFuture));
    }

    public void internalIndividualAcknowledgeMessage(TxnID txnID, List<MutablePair<PositionImpl, Integer>> positions,
                                                     CompletableFuture<Void> completableFuture) {
        if (txnID == null) {
            completableFuture.completeExceptionally(new NotAllowedException("txnID can not be null."));
            return;

        }
        if (positions == null) {
            completableFuture.completeExceptionally(new NotAllowedException("Positions can not be null."));
            return;
        }

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
    }

    @Override
    public CompletableFuture<Void> individualAcknowledgeMessage(TxnID txnID,
                                                                List<MutablePair<PositionImpl, Integer>> positions) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        internalPinnedExecutor.execute(() -> {
            if (!checkIfReady()) {
                switch (state) {
                    case Initializing:
                        addIndividualAcknowledgeMessageRequest(txnID, positions, completableFuture);
                        return;
                    case None:
                        addIndividualAcknowledgeMessageRequest(txnID, positions, completableFuture);
                        initPendingAckStore();
                        return;
                    case Error:
                        completableFuture.completeExceptionally(
                                new ServiceUnitNotReadyException("PendingAckHandle not replay error!"));
                        return;
                    case Close:
                        completableFuture.completeExceptionally(
                                new ServiceUnitNotReadyException("PendingAckHandle have been closed!"));
                        return;
                    default:
                        break;
                }
            }
            internalIndividualAcknowledgeMessage(txnID, positions, completableFuture);
        });
        return completableFuture;
    }

    private void addCumulativeAcknowledgeMessageRequest(TxnID txnID,
                                                        List<PositionImpl> positions,
                                                        CompletableFuture<Void> completableFuture) {
        acceptQueue.add(() -> internalCumulativeAcknowledgeMessage(txnID, positions, completableFuture));
    }

    public void internalCumulativeAcknowledgeMessage(TxnID txnID,
                                                     List<PositionImpl> positions,
                                                     CompletableFuture<Void> completableFuture) {
        if (txnID == null) {
            completableFuture.completeExceptionally(new NotAllowedException("TransactionID can not be null."));
            return;
        }
        if (positions == null) {
            completableFuture.completeExceptionally(new NotAllowedException("Positions can not be null."));
            return;
        }

        if (positions.size() != 1) {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID
                    + " invalid cumulative ack received with multiple message ids.";
            log.error(errorMsg);
            completableFuture.completeExceptionally(new NotAllowedException(errorMsg));
            return;
        }

        PositionImpl position = positions.get(0);

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
    }

    @Override
    public CompletableFuture<Void> cumulativeAcknowledgeMessage(TxnID txnID, List<PositionImpl> positions) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        internalPinnedExecutor.execute(() -> {
            if (!checkIfReady()) {
                switch (state) {
                    case Initializing:
                        addCumulativeAcknowledgeMessageRequest(txnID, positions, completableFuture);
                        return;
                    case None:
                        addCumulativeAcknowledgeMessageRequest(txnID, positions, completableFuture);
                        initPendingAckStore();
                        return;
                    case Error:
                        completableFuture.completeExceptionally(
                                new ServiceUnitNotReadyException("PendingAckHandle not replay error!"));
                        return;
                    case Close:
                        completableFuture.completeExceptionally(
                                new ServiceUnitNotReadyException("PendingAckHandle have been closed!"));
                        return;
                    default:
                        break;
                }
            }
            internalCumulativeAcknowledgeMessage(txnID, positions, completableFuture);
        });

        return completableFuture;
    }

    private void addCommitTxnRequest(TxnID txnId, Map<String, Long> properties, long lowWaterMark,
                                    CompletableFuture<Void> completableFuture) {
        acceptQueue.add(() -> internalCommitTxn(txnId, properties, lowWaterMark, completableFuture));
    }

    private void internalCommitTxn(TxnID txnID, Map<String, Long> properties, long lowWaterMark,
                                   CompletableFuture<Void> commitFuture) {
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
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, Map<String, Long> properties, long lowWaterMark) {
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        internalPinnedExecutor.execute(() -> {
            if (!checkIfReady()) {
                switch (state) {
                    case Initializing:
                        addCommitTxnRequest(txnID, properties, lowWaterMark, commitFuture);
                        return;
                    case None:
                        addCommitTxnRequest(txnID, properties, lowWaterMark, commitFuture);
                        initPendingAckStore();
                        return;
                    case Error:
                        if (state == State.Error) {
                            commitFuture.completeExceptionally(
                                    new ServiceUnitNotReadyException("PendingAckHandle not replay error!"));
                        } else {
                            commitFuture.completeExceptionally(
                                    new ServiceUnitNotReadyException("PendingAckHandle have been closed!"));
                        }
                        return;
                }
            }
            internalCommitTxn(txnID, properties, lowWaterMark, commitFuture);
        });
        return commitFuture;
    }

    private void addAbortTxnRequest(TxnID txnId, Consumer consumer, long lowWaterMark,
                                    CompletableFuture<Void> completableFuture) {
        acceptQueue.add(() -> internalAbortTxn(txnId, consumer, lowWaterMark, completableFuture));
    }

    public CompletableFuture<Void> internalAbortTxn(TxnID txnId, Consumer consumer,
                                 long lowWaterMark, CompletableFuture<Void> abortFuture) {
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
                        //TODO: pendingAck handle next pr will fix
                        persistentSubscription.redeliverUnacknowledgedMessages(consumer, DEFAULT_CONSUMER_EPOCH);
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

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer, long lowWaterMark) {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        internalPinnedExecutor.execute(() -> {
            if (!checkIfReady()) {
                switch (state) {
                    case Initializing:
                        addAbortTxnRequest(txnId, consumer, lowWaterMark, abortFuture);
                        return;
                    case None:
                        addAbortTxnRequest(txnId, consumer, lowWaterMark, abortFuture);
                        initPendingAckStore();
                        return;
                    default:
                        if (state == State.Error) {
                            abortFuture.completeExceptionally(
                                    new ServiceUnitNotReadyException("PendingAckHandle not replay error!"));
                        } else {
                            abortFuture.completeExceptionally(
                                    new ServiceUnitNotReadyException("PendingAckHandle have been closed!"));
                        }
                        return;
                }
            }
            internalAbortTxn(txnId, consumer, lowWaterMark, abortFuture);
        });
        return abortFuture;
    }

    private void handleLowWaterMark(TxnID txnID, long lowWaterMark) {
        lowWaterMarks.compute(txnID.getMostSigBits(), (tcId, oldLowWaterMark) -> {
            if (oldLowWaterMark == null || oldLowWaterMark < lowWaterMark) {
                return lowWaterMark;
            } else {
                return oldLowWaterMark;
            }
        });

        if (handleLowWaterMark.tryAcquire()) {
            if (individualAckOfTransaction != null && !individualAckOfTransaction.isEmpty()) {
                TxnID firstTxn = individualAckOfTransaction.firstKey();
                long tCId = firstTxn.getMostSigBits();
                Long lowWaterMarkOfFirstTxnId = lowWaterMarks.get(tCId);
                if (lowWaterMarkOfFirstTxnId != null && firstTxn.getLeastSigBits() <= lowWaterMarkOfFirstTxnId) {
                    abortTxn(firstTxn, null, lowWaterMarkOfFirstTxnId).thenRun(() -> {
                        log.warn("[{}] Transaction pending ack handle low water mark success! txnId : [{}], "
                                + "lowWaterMark : [{}]", topicName, firstTxn, lowWaterMarkOfFirstTxnId);
                        handleLowWaterMark.release();
                    }).exceptionally(ex -> {
                        log.warn("[{}] Transaction pending ack handle low water mark fail! txnId : [{}], "
                                + "lowWaterMark : [{}]", topicName, firstTxn, lowWaterMarkOfFirstTxnId);
                        handleLowWaterMark.release();
                        return null;
                    });
                    return;
                }
            }
            handleLowWaterMark.release();
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
                int batchSize = individualAckPositions.get(entry.getValue()).right;
                thisBitSet.flip(0, batchSize);
                BitSetRecyclable otherBitSet =
                        BitSetRecyclable.valueOf(individualAckPositions
                                .get(entry.getValue()).left.getAckSet());
                otherBitSet.or(thisBitSet);
                if (otherBitSet.cardinality() == batchSize) {
                    individualAckPositions.remove(entry.getValue());
                } else {
                    individualAckPositions.get(entry.getKey())
                            .left.setAckSet(otherBitSet.toLongArray());
                }
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
                    /**
                     *  if the position does not exist in individualAckPositions {@link individualAckPositions},
                     *  should new the same position and put the new position into
                     *  the individualAckPositions {@link individualAckPositions}
                     *  because when another ack the same batch message will change the ackSet with the new transaction
                     *  when the tc commits the first txn will ack all of the ackSet which has in pending ack status
                     *  individualAckPositions{@link individualAckPositions} can't include the same position
                     *  object on individualAckOfTransaction {@link individualAckOfTransaction}
                     */
                    MutablePair<PositionImpl, Integer> positionPair = positions.get(i);
                    positionPair.left = PositionImpl.get(positionPair.getLeft().getLedgerId(),
                            positionPair.getLeft().getEntryId(),
                            Arrays.copyOf(positionPair.left.getAckSet(), positionPair.left.getAckSet().length));
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

    public void completeHandleFuture() {
        this.pendingAckHandleCompletableFuture.complete(PendingAckHandleImpl.this);
    }

    public void exceptionHandleFuture(Throwable t) {
        this.pendingAckHandleCompletableFuture.completeExceptionally(t);
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
    public CompletableFuture<Void> closeAsync() {
        changeToCloseState();
        synchronized (PendingAckHandleImpl.this) {
            if (this.pendingAckStoreFuture != null) {
                CompletableFuture<Void> closeFuture = new CompletableFuture<>();
                this.pendingAckStoreFuture.whenComplete((pendingAckStore, e) -> {
                    if (e != null) {
                        // init pending ack store fail, close don't need to
                        // retry and throw exception, complete directly
                        closeFuture.complete(null);
                    } else {
                        pendingAckStore.closeAsync().whenComplete((q, ex) -> {
                            if (ex != null) {
                                Throwable t = FutureUtil.unwrapCompletionException(ex);
                                closeFuture.completeExceptionally(t);
                            } else {
                                closeFuture.complete(null);
                            }
                        });
                    }
                });

                return closeFuture;
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    public CompletableFuture<ManagedLedger> getStoreManageLedger() {
        if (this.pendingAckStoreFuture != null && this.pendingAckStoreFuture.isDone()) {
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

    @Override
    public boolean checkIfPendingAckStoreInit() {
        return this.pendingAckStoreFuture != null && this.pendingAckStoreFuture.isDone();
    }

    @Override
    public PositionImpl getPositionInPendingAck(PositionImpl position) {
        if (individualAckPositions != null) {
            MutablePair<PositionImpl, Integer> positionPair = this.individualAckPositions.get(position);
            if (positionPair != null) {
                return positionPair.getLeft();
            }
        }
        return null;
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
