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

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;

/**
 * Implement of pending ack handle.
 */
@Slf4j
public class PendingAckHandleImpl extends PendingAckHandleState implements PendingAckHandle {

    // Map to keep track of message ack by each txn.
    private ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashMap<Position, Position>> pendingIndividualAckMessagesMap;

    // Messages acked by ongoing transaction, pending transaction commit to materialize the acks. For faster look up.
    // Using hashset as a message should only be acked once by one transaction.
    private ConcurrentOpenHashMap<Position, Position> pendingAckMessages;

    private ConcurrentOpenHashMap<Position, ConcurrentOpenHashSet<TxnID>> pendingAckBatchMessageMap;

    // Message cumulative acked by ongoing transaction, pending transaction commit to materialize the ack.
    // Only one transaction can cumulative ack.
    // This parameter only keep the the largest Position it cumulative ack,as any Position smaller will also be covered.
    private volatile Position pendingCumulativeAckMessage;

    private static final AtomicReferenceFieldUpdater<PendingAckHandleImpl, Position> POSITION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PendingAckHandleImpl.class, Position.class,
                    "pendingCumulativeAckMessage");

    // ID of transaction currently using cumulative ack.
    private volatile TxnID pendingCumulativeAckTxnId;

    private static final AtomicReferenceFieldUpdater<PendingAckHandleImpl, TxnID> PENDING_CUMULATIVE_ACK_TXNID_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PendingAckHandleImpl.class, TxnID.class,
                    "pendingCumulativeAckTxnId");

    private PersistentSubscription persistentSubscription;

    private final String topicName;

    private final String subName;

    private final CompletableFuture<PendingAckStore> pendingAckStoreFuture;

    private final CompletableFuture<Void> readyCompletableFuture = new CompletableFuture<>();

    public PendingAckHandleImpl(String topicName, String subName,
                                CompletableFuture<PendingAckStore> pendingAckStoreFuture) {
        super(State.None);
        this.topicName = topicName;
        this.subName = subName;
        this.pendingAckStoreFuture = pendingAckStoreFuture;
        this.pendingAckStoreFuture.thenAccept(pendingAckStore -> {
            if (pendingAckStore instanceof MLPendingAckStore) {
                pendingAckStore.replayAsync(new MLPendingAckReplyCallBack((MLPendingAckStore) pendingAckStore, this));
                changeToInitializingState();
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
        if (AckType.Cumulative == ackType) {

            if (positions.size() != 1) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " invalid cumulative ack received with multiple message ids.";
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                return completableFuture;
            }

            checkArgument(positions.get(0) instanceof PositionImpl);
            if (position.compareTo((PositionImpl) persistentSubscription.getCursor().getMarkDeletedPosition()) <= 0) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " try to cumulative ack position: " + position + " within range of cursor's " +
                        "markDeletePosition: " + persistentSubscription.getCursor().getMarkDeletedPosition();
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                return completableFuture;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}] Cumulative ack on {}.", topicName, subName, txnId.toString(), position);
            }
            if (this.pendingCumulativeAckTxnId == null || (this.pendingCumulativeAckTxnId.equals(txnId)
                    && position.compareToWithAckSet((PositionImpl) pendingCumulativeAckMessage) > 0)) {
                // Only set pendingCumulativeAckTxnId if no transaction is doing cumulative ack.
                this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.append(txnId, position, ackType).whenComplete((v, e) -> {
                            if (e != null) {
                                completableFuture.completeExceptionally(e);
                            } else {
                                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, txnId);
                                POSITION_UPDATER.set(this, position);
                                completableFuture.complete(null);
                            }
                })).exceptionally(e -> {
                   completableFuture.completeExceptionally(e);
                   return null;
                });
            } else {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " try to cumulative batch ack position: " + position + " is not " +
                        "currentPosition: " + this.pendingCumulativeAckMessage;
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}] Individual acks on {}", topicName, subName, txnId.toString(), positions);
            }
            if (pendingIndividualAckMessagesMap == null) {
                pendingIndividualAckMessagesMap = new ConcurrentOpenHashMap<>();
            }

            if (pendingAckMessages == null) {
                pendingAckMessages = new ConcurrentOpenHashMap<>();
            }
            // If try to ack message already acked by committed transaction or normal acknowledge, throw exception.
            if (((ManagedCursorImpl) persistentSubscription.getCursor()).isMessageDeleted(position)) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " try to ack message:" + position + " already acked before.";
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
            }

            // If try to ack message already acked by some ongoing transaction(can be itself), throw exception.
            // Acking single message within range of cumulative ack(if exist) is considered valid operation.
            if (!position.isBatchPosition() && pendingAckMessages.containsKey(position)) {

                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " try to ack message:" + position + " in pending ack status.";
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
            }

            if (position.isBatchPosition()) {

                if (pendingAckMessages.containsKey(position)
                        && ((PositionImpl) pendingAckMessages.get(position))
                        .isAckSetRepeated(position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                            " try to ack batch message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                }
                this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.append(txnId, position, ackType).whenComplete((v, e) -> {
                            if (e != null) {
                                completableFuture.completeExceptionally(e);
                            } else {
                                ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                                        pendingIndividualAckMessagesMap
                                                .computeIfAbsent(txnId, txn -> new ConcurrentOpenHashMap<>());

                                ConcurrentOpenHashSet<TxnID> txnSet = getBatchTxnSetByPosition(position);

                                if (pendingAckMessageForCurrentTxn.containsKey(position)) {
                                    ((PositionImpl) pendingAckMessageForCurrentTxn
                                            .get(position)).andAckSet(position);
                                } else {
                                    pendingAckMessageForCurrentTxn.put(position, position);
                                }

                                if (!pendingAckMessages.containsKey(position)) {
                                    this.pendingAckMessages.put(position, position);
                                } else {
                                    ((PositionImpl) this.pendingAckMessages.get(position)).andAckSet(position);
                                }
                                txnSet.add(txnId);
                                completableFuture.complete(null);
                            }
                })).exceptionally(e -> {
                    completableFuture.completeExceptionally(e);
                    return null;
                });
            } else {
                if (this.pendingAckMessages.containsKey(position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                            " try to ack message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                }
                this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.append(txnId, position, ackType).whenComplete((v, e) -> {
                            if (e != null) {
                                completableFuture.completeExceptionally(e);
                            } else {
                                ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                                        pendingIndividualAckMessagesMap
                                                .computeIfAbsent(txnId, txn -> new ConcurrentOpenHashMap<>());
                                pendingAckMessageForCurrentTxn.put(position, position);
                                this.pendingAckMessages.put(position, position);
                                completableFuture.complete(null);
                            }
                })).exceptionally(e -> {
                    completableFuture.completeExceptionally(e);
                    return null;
                });
            }
        }
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
        if (pendingCumulativeAckTxnId != null) {
            if (pendingCumulativeAckTxnId.equals(txnId)) {
                persistentSubscription.acknowledgeMessage(Collections
                        .singletonList(POSITION_UPDATER.get(this)), AckType.Cumulative, properties);
                this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.deleteTxn(txnId, AckType.Cumulative).whenComplete((v, e) -> {
                            if (e != null) {
                                commitFuture.completeExceptionally(e);
                            } else {
                                // Reset txdID and position for cumulative ack.
                                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, null);
                                POSITION_UPDATER.set(this, null);
                                commitFuture.complete(null);
                            }
                })).exceptionally(e -> {
                    commitFuture.completeExceptionally(e);
                    return null;
                });
            } else {
                commitFuture.complete(null);
            }
        } else {
            if (pendingIndividualAckMessagesMap != null && pendingIndividualAckMessagesMap.containsKey(txnId)) {
                ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                        pendingIndividualAckMessagesMap.get(txnId);
                if (pendingAckMessageForCurrentTxn != null) {
                    List<Position> positions = pendingAckMessageForCurrentTxn.values();
                    persistentSubscription.acknowledgeMessage(positions, AckType.Individual, properties);
                    this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                            pendingAckStore.deleteTxn(txnId, AckType.Individual).whenComplete((v, e) -> {
                                if (e != null) {
                                    commitFuture.completeExceptionally(e);
                                } else {
                                    for (Position position : positions) {
                                        if (pendingAckBatchMessageMap != null
                                                && pendingAckBatchMessageMap.containsKey(position)) {
                                            ConcurrentOpenHashSet<TxnID> txnIDConcurrentOpenHashSet =
                                                    pendingAckBatchMessageMap.get(position);
                                            txnIDConcurrentOpenHashSet.remove(txnId);
                                            if (txnIDConcurrentOpenHashSet.isEmpty()) {
                                                pendingAckBatchMessageMap.remove(position);
                                                pendingAckMessages.remove(position);
                                            }
                                        } else {
                                            pendingAckMessages.remove(position);
                                        }
                                    }
                                    pendingIndividualAckMessagesMap.remove(txnId);
                                    commitFuture.complete(null);
                                }
                    })).exceptionally(e -> {
                        commitFuture.completeExceptionally(e);
                        return null;
                    });
                } else {
                    commitFuture.complete(null);
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
        if (pendingCumulativeAckTxnId != null && pendingCumulativeAckMessage != null) {
            if (PENDING_CUMULATIVE_ACK_TXNID_UPDATER.get(this).equals(txnId)) {
                this.pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.deleteTxn(txnId, AckType.Cumulative).whenComplete((v, e) -> {
                            if (e != null) {
                                abortFuture.completeExceptionally(e);
                            } else {
                                POSITION_UPDATER.set(this, null);
                                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, null);
                                redeliverUnacknowledgedMessages(consumer);
                                abortFuture.complete(null);
                            }
                })).exceptionally(e -> {
                    abortFuture.completeExceptionally(e);
                    return null;
                });
            } else {
                abortFuture.complete(null);
            }
        } else if (this.pendingIndividualAckMessagesMap != null){
            ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                    pendingIndividualAckMessagesMap.get(txnId);
            if (pendingAckMessageForCurrentTxn != null) {
                pendingAckStoreFuture.thenAccept(pendingAckStore ->
                        pendingAckStore.deleteTxn(txnId, AckType.Individual).whenComplete((v, e) -> {
                            if (e != null) {
                                abortFuture.completeExceptionally(null);
                            } else {
                                List<Position> positions = pendingAckMessageForCurrentTxn.values();
                                for (Position position : positions) {
                                    if (pendingAckBatchMessageMap != null
                                            && pendingAckBatchMessageMap.containsKey(position)) {
                                        ConcurrentOpenHashSet<TxnID> txnIDConcurrentOpenHashSet =
                                                pendingAckBatchMessageMap.get(position);
                                        txnIDConcurrentOpenHashSet.remove(txnId);
                                        if (txnIDConcurrentOpenHashSet.isEmpty()) {
                                            pendingAckBatchMessageMap.remove(position);
                                            pendingAckMessages.remove(position);
                                        }
                                    } else {
                                        this.pendingAckMessages.remove(position);
                                    }
                                }
                                pendingIndividualAckMessagesMap.remove(txnId);
                                redeliverUnacknowledgedMessages(consumer,
                                        (List<PositionImpl>) (List<?>) pendingAckMessageForCurrentTxn.values());
                                abortFuture.complete(null);
                            }
                })).exceptionally(e -> {
                    abortFuture.completeExceptionally(e);
                    return null;
                });
            } else {
                abortFuture.complete(null);
            }
        }
        return abortFuture;
    }

    @Override
    public void setPersistentSubscription(PersistentSubscription persistentSubscription) {
        this.persistentSubscription = persistentSubscription;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
        if (consumer == null) {
            return;
        }
        ConcurrentLongLongPairHashMap positionMap = consumer.getPendingAcks();
        // Only check if message is in pending_ack status when there's ongoing transaction.
        if (null != positionMap && ((pendingAckMessages != null && pendingAckMessages.size() != 0)
                || pendingCumulativeAckMessage != null)) {
            List<PositionImpl> pendingPositions = new ArrayList<>();
            PositionImpl cumulativeAckPosition = (null == this.pendingCumulativeAckMessage) ? null :
                    (PositionImpl) this.pendingCumulativeAckMessage;

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
        if ((pendingAckMessages != null && pendingAckMessages.size() != 0) || pendingCumulativeAckMessage != null) {
            // Check if message is in pending_ack status.
            List<PositionImpl> pendingPositions = new ArrayList<>();
            PositionImpl cumulativeAckPosition = (null == this.pendingCumulativeAckMessage) ? null :
                    (PositionImpl) this.pendingCumulativeAckMessage;

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

            if (((PositionImpl) position).isBatchPosition()) {
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

    @Override
    public String getTopicName() {
        return topicName;
    }

    @Override
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
        if (position.isBatchPosition()) {
            if (this.pendingAckMessages != null) {
                if (this.pendingAckMessages.containsKey(position)) {
                    if (!((PositionImpl) this.pendingAckMessages.get(position)).isAckSetRepeated(position)) {
                        pendingPositions.add(position);
                    }
                } else {
                    pendingPositions.add(position);
                }
            } else if (cumulativeAckPosition != null) {
                int compareNumber = position.compareTo((cumulativeAckPosition));
                if (compareNumber > 0 || (compareNumber == 0
                        && !position.isAckSetRepeated(cumulativeAckPosition))) {
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

    private ConcurrentOpenHashSet<TxnID> getBatchTxnSetByPosition(Position position) {
        if (pendingAckBatchMessageMap == null) {
            this.pendingAckBatchMessageMap = new ConcurrentOpenHashMap<>();
        }
        return this.pendingAckBatchMessageMap
                .computeIfAbsent(position, txn -> new ConcurrentOpenHashSet<>());
    }
}