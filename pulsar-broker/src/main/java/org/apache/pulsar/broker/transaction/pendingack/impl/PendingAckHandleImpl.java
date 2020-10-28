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


import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;


import com.google.common.collect.ComparisonChain;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;

import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.util.FutureUtil;

import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;


import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;


/**
 * Implement of pending ack handle.
 */
@Slf4j
public class PendingAckHandleImpl extends PendingAckHandleState implements PendingAckHandle {

    private Map<TxnID, HashMap<PositionImpl, PositionImpl>> pendingIndividualAckMessagesMap;

    private Map<PositionImpl, PositionImpl> pendingAckMessages;

    private Map<PositionImpl, HashSet<TxnID>> pendingAckBatchMessageMap;

    private Pair<TxnID, PositionImpl> cumulativeAckPair;

    private final String topicName;

    private final String subName;

    private final PersistentSubscription persistentSubscription;

    private final CompletableFuture<PendingAckStore> pendingAckStoreFuture;

    private final PersistentSubscription persistentSubscription;

    private final CompletableFuture<Void> readyCompletableFuture = new CompletableFuture<>();

    public PendingAckHandleImpl(PersistentSubscription persistentSubscription) {
        super(State.None);
        this.topicName = persistentSubscription.getTopicName();
        this.subName = persistentSubscription.getName();
        PersistentTopic topic = (PersistentTopic) persistentSubscription.getTopic();
        TransactionPendingAckStoreProvider pendingAckStoreProvider = topic.getBrokerService()
                .getPulsar().getTransactionPendingAckStoreProvider();
        if (pendingAckStoreProvider != null) {
            if (pendingAckStoreProvider instanceof MLPendingAckStoreProvider) {
                this.pendingAckStoreFuture = pendingAckStoreProvider.newPendingAckStore(topic, subName);
            } else {
                this.pendingAckStoreFuture = CompletableFuture.completedFuture(new InMemoryPendingAckStore());
            }
        } else {
            this.pendingAckStoreFuture = CompletableFuture.completedFuture(new InMemoryPendingAckStore());
        }
        this.pendingAckStoreFuture.thenAccept(pendingAckStore -> {
            if (pendingAckStore instanceof MLPendingAckStore) {
                pendingAckStore.replayAsync(new MLPendingAckReplyCallBack((MLPendingAckStore) pendingAckStore, this));
                changeToInitializingState();
            } else {
                //no-op
            }
        }).exceptionally(e -> {
            log.error("PendingAckHandleImpl haven't init fail TopicName : {}, SubName: {}", topicName, subName, e);
            return null;
        });
    }

    @Override
    public synchronized CompletableFuture<Void> acknowledgeMessage(TxnID txnId,
                                                                   List<Position> positions, AckType ackType) {
        if (!checkIfReady()) {
            log.error("PendingAckHandleImpl haven't init success before ack " +
                    "with transaction! TopicName : {}, SubName: {}", topicName, subName);
            return FutureUtil.failedFuture
                    (new ServiceUnitNotReadyException("PendingAckHandleImpl haven't init " +
                            "success! " + "TopicName : " + topicName + ", SubName: " + subName));
        }
        checkArgument(txnId != null, "TransactionID can not be null.");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        checkArgument(positions.get(0) instanceof PositionImpl);
        PositionImpl position = (PositionImpl) positions.get(0);
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
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
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
        if (this.cumulativeAckPair == null || (this.cumulativeAckPair.getKey().equals(txnID)
                && compareToWithAckSetForCumulativeAck(position, this.cumulativeAckPair.getValue()) > 0)) {
            this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                    pendingAckStore.append(txnID, position, AckType.Cumulative).whenComplete((v, e) -> {
                        if (e != null) {
                            completableFuture.completeExceptionally(e);
                        } else {
                            this.cumulativeAckPair = MutablePair.of(txnID, position);
                            completableFuture.complete(null);
                        }
                    })).exceptionally(e -> {
                        completableFuture.completeExceptionally(e);
                        return null;
            });
        }else {
            String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                    " try to cumulative batch ack position: " + position + " within range of current " +
                    "currentPosition: " + this.cumulativeAckPair.getValue();
            log.error(errorMsg);
            return FutureUtil.failedFuture(new TransactionConflictException(errorMsg));
        }
        return completableFuture;
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
                continue;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}] Individual acks on {}", topicName, subName, txnID.toString(), positions);
            }
            if (pendingIndividualAckMessagesMap == null) {
                pendingIndividualAckMessagesMap = new HashMap<>();
            }

            if (pendingAckMessages == null) {
                pendingAckMessages = new HashMap<>();
            }
            PositionImpl position = (PositionImpl) positions.get(i);
            // If try to ack message already acked by committed transaction or normal acknowledge, throw exception.
            if (((ManagedCursorImpl) persistentSubscription.getCursor()).isMessageDeleted(position)) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                        " try to ack message:" + position + " already acked before.";
                log.error(errorMsg);
                positionsFuture.add(FutureUtil.failedFuture(new TransactionConflictException(errorMsg)));
                continue;
            }

            if (position.hasAckSet()) {

                if (pendingAckMessages.containsKey(position)
                        && isAckSetRepeated(pendingAckMessages.get(position), position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                            " try to ack batch message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    positionsFuture.add(FutureUtil.failedFuture(new TransactionConflictException(errorMsg)));
                    continue;
                }
                this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.append(txnID, position, AckType.Cumulative).whenComplete((v, e) -> {
                            if (e != null) {
                                positionsFuture.add(c);
                                completableFuture.completeExceptionally(e);
                            } else {
                                this.cumulativeAckPair = MutablePair.of(txnID, position);
                                completableFuture.complete(null);
                            }
                        })).exceptionally(e -> {
                    completableFuture.completeExceptionally(e);
                    return null;
                });
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        pendingIndividualAckMessagesMap.computeIfAbsent(txnID, txn -> new HashMap<>());

                if (pendingAckBatchMessageMap == null) {
                    this.pendingAckBatchMessageMap = new HashMap<>();
                }

                HashSet<TxnID> txnSet = this.pendingAckBatchMessageMap
                        .computeIfAbsent(position, txn -> new HashSet<>());

                if (pendingAckMessageForCurrentTxn.containsKey(position)) {
                    andAckSet(pendingAckMessageForCurrentTxn.get(position), position);
                } else {
                    pendingAckMessageForCurrentTxn.put(position, position);
                }

                if (!pendingAckMessages.containsKey(position)) {
                    this.pendingAckMessages.put(position, position);
                } else {
                    andAckSet(this.pendingAckMessages.get(position), position);
                }
                txnSet.add(txnID);
                positionsFuture.add(CompletableFuture.completedFuture(null));
            } else {
                if (this.pendingAckMessages.containsKey(position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnID +
                            " try to ack message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    positionsFuture.add(FutureUtil.failedFuture(new TransactionConflictException(errorMsg)));
                    continue;
                }
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        pendingIndividualAckMessagesMap.computeIfAbsent(txnID, txn -> new HashMap<>());
                pendingAckMessageForCurrentTxn.put(position, position);
                this.pendingAckMessages.put(position, position);
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
        if (!checkIfReady()) {
            log.error("PendingAckHandleImpl haven't init success before commit " +
                    "with transaction! TopicName : {}, SubName: {}", topicName, subName);
            return FutureUtil.failedFuture
                    (new ServiceUnitNotReadyException("PendingAckHandleImpl haven't init " +
                            "success! " + "TopicName : " + topicName + ", SubName: " + subName));
        }

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        // It's valid to create transaction then commit without doing any operation, which will cause
        // pendingAckMessagesMap to be null.
        if (this.cumulativeAckPair != null) {
            if (cumulativeAckPair.getKey().equals(txnId)) {
                persistentSubscription.acknowledgeMessage(Collections
                        .singletonList(this.cumulativeAckPair.getValue()), AckType.Cumulative, properties);

                this.cumulativeAckPair = null;
            }
        } else {
            if (pendingIndividualAckMessagesMap != null && pendingIndividualAckMessagesMap.containsKey(txnId)) {
                HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                        pendingIndividualAckMessagesMap.remove(txnId);
                if (pendingAckMessageForCurrentTxn != null) {
                    for (Entry<PositionImpl, PositionImpl> entry : pendingAckMessageForCurrentTxn.entrySet()) {
                        if (pendingAckBatchMessageMap != null
                                && pendingAckBatchMessageMap.containsKey(entry.getValue())) {
                            HashSet<TxnID> txnIDConcurrentOpenHashSet = pendingAckBatchMessageMap.get(entry.getValue());
                            txnIDConcurrentOpenHashSet.remove(txnId);
                            if (txnIDConcurrentOpenHashSet.isEmpty()) {
                                pendingAckBatchMessageMap.remove(entry.getValue());
                                pendingAckMessages.remove(entry.getValue());
                            }
                        } else {
                            pendingAckMessages.remove(entry.getValue());
                        }
                    }
                    persistentSubscription.acknowledgeMessage(new ArrayList<>(pendingAckMessageForCurrentTxn.values()),
                            AckType.Individual, properties);
                }
            }
        }
        return commitFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer) {
        if (!checkIfReady()) {
            log.error("PendingAckHandleImpl haven't init success before abort " +
                    "with transaction! TopicName : {}, SubName: {}", topicName, subName);
            return FutureUtil.failedFuture
                    (new ServiceUnitNotReadyException("PendingAckHandleImpl haven't init " +
                            "success! " + "TopicName : " + topicName + ", SubName: " + subName));
        }
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        if (this.cumulativeAckPair != null) {
            if (this.cumulativeAckPair.getKey().equals(txnId)) {
                this.cumulativeAckPair = null;
                redeliverUnacknowledgedMessages(consumer);
            }
        } else if (this.pendingIndividualAckMessagesMap != null){
            HashMap<PositionImpl, PositionImpl> pendingAckMessageForCurrentTxn =
                    pendingIndividualAckMessagesMap.remove(txnId);
            if (pendingAckMessageForCurrentTxn != null) {
                for (Entry<PositionImpl, PositionImpl> entry : pendingAckMessageForCurrentTxn.entrySet()) {
                    if (pendingAckBatchMessageMap != null && pendingAckBatchMessageMap.containsKey(entry.getValue())) {
                        HashSet<TxnID> txnIDConcurrentOpenHashSet =
                                pendingAckBatchMessageMap.get(entry.getValue());
                        txnIDConcurrentOpenHashSet.remove(txnId);
                        if (txnIDConcurrentOpenHashSet.isEmpty()) {
                            pendingAckBatchMessageMap.remove(entry.getValue());
                            pendingAckMessages.remove(entry.getValue());
                        }
                    } else {
                        this.pendingAckMessages.remove(entry.getValue());
                    }
                }
                redeliverUnacknowledgedMessages(consumer, new ArrayList<>(pendingAckMessageForCurrentTxn.values()));
            }
        }
        return abortFuture;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
        if (consumer == null) {
            return;
        }
        ConcurrentLongLongPairHashMap positionMap = consumer.getPendingAcks();
        // Only check if message is in pending_ack status when there's ongoing transaction.
        if (null != positionMap && ((pendingAckMessages != null && pendingAckMessages.size() != 0)
                || this.cumulativeAckPair != null)) {
            List<PositionImpl> pendingPositions = new ArrayList<>();
            PositionImpl cumulativeAckPosition =
                    (null == this.cumulativeAckPair) ? null : this.cumulativeAckPair.getValue();

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
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {
        if (consumer == null) {
            return;
        }
        // If there's ongoing transaction.
        if ((pendingAckMessages != null && pendingAckMessages.size() != 0) || this.cumulativeAckPair != null) {
            // Check if message is in pending_ack status.
            List<PositionImpl> pendingPositions = new ArrayList<>();
            PositionImpl cumulativeAckPosition =
                    (null == this.cumulativeAckPair) ? null : this.cumulativeAckPair.getValue();

            positions.forEach(position ->
                    redeliverUnacknowledgedMessagesCommon(position, pendingPositions, cumulativeAckPosition));
            persistentSubscription.getDispatcher().redeliverUnacknowledgedMessages(consumer, pendingPositions);
        } else {
            persistentSubscription.getDispatcher().redeliverUnacknowledgedMessages(consumer, positions);
        }
    }

    @Override
    public void handleMetadataEntry(TxnID txnId, Position position, AckType ackType) {
        if (AckType.Cumulative == ackType) {
            PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, txnId);
            POSITION_UPDATER.set(this, position);
        } else {
            if (pendingIndividualAckMessagesMap == null) {
                pendingIndividualAckMessagesMap = new ConcurrentOpenHashMap<>();
            }

            if (pendingAckMessages == null) {
                pendingAckMessages = new ConcurrentOpenHashMap<>();
            }
            ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                    pendingIndividualAckMessagesMap.computeIfAbsent(txnId, txn -> new ConcurrentOpenHashMap<>());

            if (((PositionImpl) position).hasAckSet()) {
                PositionImpl currentPosition = (PositionImpl) position;

                ConcurrentOpenHashSet<TxnID> txnSet = getBatchTxnSetByPosition(position);

                if (pendingAckMessageForCurrentTxn.containsKey(currentPosition)) {
                    ((PositionImpl) pendingAckMessageForCurrentTxn
                            .get(currentPosition)).andAckSet(currentPosition);
                } else {
                    pendingAckMessageForCurrentTxn.put(currentPosition, currentPosition);
                }

                if (!pendingAckMessages.containsKey(currentPosition)) {
                    this.pendingAckMessages.put(currentPosition, currentPosition);
                } else {
                    ((PositionImpl) this.pendingAckMessages.get(currentPosition)).andAckSet(currentPosition);
                }
                txnSet.add(txnId);
            } else {
                pendingAckMessageForCurrentTxn.put(position, position);
                this.pendingAckMessages.putIfAbsent(position, position);
            }
        }
    }

    public String getTopicName() {
        return topicName;
    }

    public String getSubName() {
        return subName;
    }

    @Override
    public boolean checkIfReady() {
        return super.checkIfReady();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        changeToCloseState();
        return this.pendingAckStoreFuture.thenCompose(PendingAckStore::closeAsync);
    }

    @Override
    public CompletableFuture<Void> getReadyCompletableFuture() {
        pendingAckStoreFuture.whenComplete((store, e) -> {
            if (e != null) {
                readyCompletableFuture.completeExceptionally(e);
            }
        });
        return readyCompletableFuture;
    }

    protected void readyFutureComplete() {
        readyCompletableFuture.complete(null);
    }

    private void redeliverUnacknowledgedMessagesCommon(PositionImpl position, List<PositionImpl> pendingPositions,
                                                       PositionImpl cumulativeAckPosition) {
        if (position.hasAckSet()) {
            if (this.pendingAckMessages != null) {
                if (this.pendingAckMessages.containsKey(position)) {
                    if (!isAckSetRepeated(this.pendingAckMessages.get(position), position)) {
                        pendingPositions.add(position);
                    }
                } else {
                    pendingPositions.add(position);
                }
            } else if (cumulativeAckPosition != null) {
                int compareNumber = position.compareTo((cumulativeAckPosition));
                if (compareNumber > 0 || (compareNumber == 0 && !isAckSetRepeated(position, cumulativeAckPosition))) {
                    pendingPositions.add(position);
                }
            }
        } else {
            if (this.pendingAckMessages != null) {
                if (!this.pendingAckMessages.containsKey(position)) {
                    pendingPositions.add(position);
                }
            } else if (cumulativeAckPosition != null ) {
                if (position.compareTo(cumulativeAckPosition) > 0) {
                    pendingPositions.add(position);
                }
            }
        }
    }

    private HashSet<TxnID> getBatchTxnSetByPosition(PositionImpl position) {
        if (pendingAckBatchMessageMap == null) {
            this.pendingAckBatchMessageMap = new HashMap<>();
        }
        return this.pendingAckBatchMessageMap
                .computeIfAbsent(position, txn -> new HashSet<>());
    }
    public int compareToWithAckSetForCumulativeAck(PositionImpl currentPosition,PositionImpl otherPosition) {
        int result = ComparisonChain.start().compare(currentPosition.getLedgerId(),
                otherPosition.getLedgerId()).compare(currentPosition.getEntryId(), otherPosition.getEntryId())
                .result();
        if (result == 0) {
            if (otherPosition.getAckSet() == null && currentPosition.getAckSet() == null) {
                return result;
            }
            BitSetRecyclable otherAckSet = BitSetRecyclable.valueOf(otherPosition.getAckSet());
            BitSetRecyclable thisAckSet = BitSetRecyclable.valueOf(currentPosition.getAckSet());
            if (otherAckSet.equals(thisAckSet)) {
                return result;
            }
            otherAckSet.and(thisAckSet);
            boolean flag = otherAckSet.equals(thisAckSet);
            thisAckSet.recycle();
            otherAckSet.recycle();
            return flag ? 1 : 0;
        }
        return result;
    }

    public boolean isAckSetRepeated(PositionImpl currentPosition, PositionImpl otherPosition) {
        if (currentPosition.getAckSet() == null || otherPosition.getAckSet() == null) {
            return false;
        }

        BitSetRecyclable thisAckSet = BitSetRecyclable.valueOf(currentPosition.getAckSet());
        BitSetRecyclable otherAckSet = BitSetRecyclable.valueOf(otherPosition.getAckSet());
        if (otherAckSet.size() < thisAckSet.size()) {
            otherAckSet.set(otherAckSet.size(), thisAckSet.size());
        }
        thisAckSet.flip(0, thisAckSet.size());
        otherAckSet.flip(0, otherAckSet.size());
        thisAckSet.and(otherAckSet);
        boolean isAckSetRepeated = !thisAckSet.isEmpty();
        thisAckSet.recycle();
        otherAckSet.recycle();
        return isAckSetRepeated;
    }

    public void andAckSet(PositionImpl currentPosition, PositionImpl otherPosition) {
        BitSetRecyclable thisAckSet = BitSetRecyclable.valueOf(currentPosition.getAckSet());
        BitSetRecyclable otherAckSet = BitSetRecyclable.valueOf(otherPosition.getAckSet());
        if (otherAckSet.size() < thisAckSet.size()) {
            otherAckSet.set(otherAckSet.size(), thisAckSet.size());
        }
        thisAckSet.and(otherAckSet);
        currentPosition.setAckSet(thisAckSet.toLongArray());
        thisAckSet.recycle();
        otherAckSet.recycle();
    }
}