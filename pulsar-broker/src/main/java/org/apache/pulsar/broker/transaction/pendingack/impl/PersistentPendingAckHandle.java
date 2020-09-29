package org.apache.pulsar.broker.transaction.pendingack.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.transaction.common.exception.TransactionAbortConflictException;
import org.apache.pulsar.transaction.common.exception.TransactionAckConflictException;
import org.apache.pulsar.transaction.common.exception.TransactionCommitConflictException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class PersistentPendingAckHandle implements PendingAckHandle {

    // Map to keep track of message ack by each txn.
    private ConcurrentOpenHashMap<TxnID, ConcurrentOpenHashMap<Position, Position>> pendingAckMessagesMap;

    // Messages acked by ongoing transaction, pending transaction commit to materialize the acks. For faster look up.
    // Using hashset as a message should only be acked once by one transaction.
    private ConcurrentOpenHashMap<Position, Position> pendingAckMessages;

    private ConcurrentOpenHashMap<Position, ConcurrentOpenHashSet<TxnID>> pendingAckBatchMessageMap;

    // Message cumulative acked by ongoing transaction, pending transaction commit to materialize the ack.
    // Only one transaction can cumulative ack.
    // This parameter only keep the the largest Position it cumulative ack,as any Position smaller will also be covered.
    private volatile Position pendingCumulativeAckMessage;

    private static final AtomicReferenceFieldUpdater<PersistentPendingAckHandle, Position> POSITION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PersistentPendingAckHandle.class, Position.class,
                    "pendingCumulativeAckMessage");

    // ID of transaction currently using cumulative ack.
    private volatile TxnID pendingCumulativeAckTxnId;

    private static final AtomicReferenceFieldUpdater<PersistentPendingAckHandle, TxnID> PENDING_CUMULATIVE_ACK_TXNID_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PersistentPendingAckHandle.class, TxnID.class,
                    "pendingCumulativeAckTxnId");

    public PersistentSubscription persistentSubscription;

    private final String topicName;

    private final String subName;

    private final ManagedCursor cursor;

    public PersistentPendingAckHandle(ManagedCursor cursor, String topicName, String subName) {
        this.cursor = cursor;
        this.topicName = topicName;
        this.subName = subName;
    }
    @Override
    public synchronized CompletableFuture<Void> acknowledgeMessage(TxnID txnId,
                                                                   List<Position> positions, AckType ackType) {
        checkArgument(txnId != null, "TransactionID can not be null.");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (AckType.Cumulative == ackType) {
            // Check if another transaction is already using cumulative ack on this subscription.
            if (this.pendingCumulativeAckTxnId != null && !this.pendingCumulativeAckTxnId.equals(txnId)) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " try to cumulative ack message while transaction:" + this.pendingCumulativeAckTxnId +
                        " already cumulative acked messages.";
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionAckConflictException(errorMsg));
                return completableFuture;
            }

            if (positions.size() != 1) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " invalid cumulative ack received with multiple message ids.";
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionAckConflictException(errorMsg));
                return completableFuture;
            }

            checkArgument(positions.get(0) instanceof PositionImpl);
            PositionImpl position = (PositionImpl) positions.get(0);
            if (position.compareTo((PositionImpl) persistentSubscription.getCursor().getMarkDeletedPosition()) <= 0) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " try to cumulative ack position: " + position + " within range of cursor's " +
                        "markDeletePosition: " + persistentSubscription.getCursor().getMarkDeletedPosition();
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionAckConflictException(errorMsg));
                return completableFuture;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}] Cumulative ack on {}.", topicName, subName, txnId.toString(), position);
            }
            if (this.pendingCumulativeAckTxnId == null) {
                // Only set pendingCumulativeAckTxnId if no transaction is doing cumulative ack.
                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, txnId);
                POSITION_UPDATER.set(this, position);
            } else if (position.compareToWithAckSet((PositionImpl) this.pendingCumulativeAckMessage) > 0) {
                // If new cumulative ack position is greater than current one, update it.
                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, txnId);
                POSITION_UPDATER.set(this, position);
            } else {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " try to cumulative batch ack position: " + position + " within range of current  " +
                        "currentPosition: " + this.pendingCumulativeAckMessage;
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionAckConflictException(errorMsg));
                return completableFuture;
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] TxnID:[{}] Individual acks on {}", topicName, subName, txnId.toString(), positions);
            }
            if (pendingAckMessagesMap == null) {
                pendingAckMessagesMap = new ConcurrentOpenHashMap<>();
            }

            if (pendingAckMessages == null) {
                pendingAckMessages = new ConcurrentOpenHashMap<>();
            }


            for (Position position : positions) {
                // If try to ack message already acked by committed transaction or normal acknowledge, throw exception.
                if (((ManagedCursorImpl) persistentSubscription.getCursor()).isMessageDeleted(position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                            " try to ack message:" + position + " already acked before.";
                    log.error(errorMsg);
                    completableFuture.completeExceptionally(new TransactionAckConflictException(errorMsg));
                    return completableFuture;
                }

                // If try to ack message already acked by some ongoing transaction(can be itself), throw exception.
                // Acking single message within range of cumulative ack(if exist) is considered valid operation.
                if (!((PositionImpl) position).isBatchPosition() && pendingAckMessages.containsKey(position)) {

                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                            " try to ack message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    completableFuture.completeExceptionally(new TransactionAckConflictException(errorMsg));
                    return completableFuture;
                }

                ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                        pendingAckMessagesMap.computeIfAbsent(txnId, txn -> new ConcurrentOpenHashMap<>());

                if (((PositionImpl) position).isBatchPosition()) {
                    PositionImpl currentPosition = (PositionImpl) position;
                    if (pendingAckBatchMessageMap == null) {
                        this.pendingAckBatchMessageMap = new ConcurrentOpenHashMap<>();
                    }
                    ConcurrentOpenHashSet<TxnID> txnSet = this.pendingAckBatchMessageMap
                            .computeIfAbsent(position, txn -> new ConcurrentOpenHashSet<>());
                    if (pendingAckMessages.containsKey(currentPosition)
                            && ((PositionImpl) pendingAckMessages.get(currentPosition))
                            .isAckSetRepeated(currentPosition)) {
                        String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                                " try to ack batch message:" + position + " in pending ack status.";
                        log.error(errorMsg);
                        completableFuture.completeExceptionally(new TransactionAckConflictException(errorMsg));
                        return completableFuture;
                    }

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
                    if (this.pendingAckMessages.containsKey(position)) {
                        String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                                " try to ack message:" + position + " in pending ack status.";
                        log.error(errorMsg);
                        completableFuture.completeExceptionally(new TransactionAckConflictException(errorMsg));
                        return completableFuture;
                    }
                    pendingAckMessageForCurrentTxn.put(position, position);
                    this.pendingAckMessages.putIfAbsent(position, position);
                }
            }
        }
        completableFuture.complete(null);
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnId, Map<String, Long> properties) {

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        // It's valid to create transaction then commit without doing any operation, which will cause
        // pendingAckMessagesMap to be null.
        if (pendingCumulativeAckTxnId != null && pendingCumulativeAckMessage != null) {
            if (pendingCumulativeAckTxnId.equals(txnId)) {
                persistentSubscription.acknowledgeMessage(Collections
                        .singletonList(POSITION_UPDATER.get(this)), AckType.Cumulative, properties);
                // Reset txdID and position for cumulative ack.
                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, null);
                POSITION_UPDATER.set(this, null);
                commitFuture.complete(null);
            } else {
                commitFuture
                        .completeExceptionally(
                                new TransactionCommitConflictException("Commit txn : " + txnId +" is not current txn : "
                                        + PENDING_CUMULATIVE_ACK_TXNID_UPDATER.get(this) + "."));
            }
        } else {
            if (pendingAckMessagesMap != null && pendingAckMessagesMap.containsKey(txnId)) {
                List<Position> positions = pendingAckMessagesMap.get(txnId).values();
                for (int i = 0; i < positions.size(); i++) {
                    if (pendingAckBatchMessageMap != null && pendingAckBatchMessageMap.containsKey(positions.get(i))) {
                        ConcurrentOpenHashSet<TxnID> txnIDConcurrentOpenHashSet =
                                pendingAckBatchMessageMap.get(positions.get(i));
                        txnIDConcurrentOpenHashSet.remove(txnId);
                        if (txnIDConcurrentOpenHashSet.isEmpty()) {
                            pendingAckBatchMessageMap.remove(positions.get(i));
                            pendingAckMessages.remove(positions.get(i));
                        }
                    } else {
                        pendingAckMessages.remove(positions.get(i));
                    }
                }
                pendingAckMessagesMap.remove(txnId);
                persistentSubscription.acknowledgeMessage(positions, AckType.Individual, properties);
                commitFuture.complete(null);
            } else {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction with id:" + txnId + " not found.";
                log.error(errorMsg);
                commitFuture.completeExceptionally(new TransactionCommitConflictException(
                        "This txn : " + txnId + "is not in pendingAckMessagesMap."));
            }
        }
        return commitFuture;
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer) {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        if (pendingCumulativeAckTxnId != null && pendingCumulativeAckMessage != null) {
            if (PENDING_CUMULATIVE_ACK_TXNID_UPDATER.get(this).equals(txnId)) {
                POSITION_UPDATER.set(this, null);
                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, null);
                abortFuture.complete(null);
            } else {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction with id:" + txnId + " not current id : " +
                        PENDING_CUMULATIVE_ACK_TXNID_UPDATER.get(this) + ".";
                log.error(errorMsg);
                abortFuture.completeExceptionally(new TransactionAbortConflictException(errorMsg));
            }
        } else {
            if (pendingAckMessagesMap == null || !this.pendingAckMessagesMap.containsKey(txnId)) {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction with id:" + txnId + " not found.";
                log.error(errorMsg);
                abortFuture.completeExceptionally(new TransactionAbortConflictException
                        ("This txn : " + txnId + "is not in pendingAckMessagesMap."));
            } else {
                ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn = this.pendingAckMessagesMap.get(txnId);
                checkNotNull(pendingAckMessageForCurrentTxn);
                List<Position> positions = pendingAckMessageForCurrentTxn.values();
                for (int i = 0; i < positions.size(); i++) {
                    if (pendingAckBatchMessageMap != null && pendingAckBatchMessageMap.containsKey(positions.get(i))) {
                        ConcurrentOpenHashSet<TxnID> txnIDConcurrentOpenHashSet =
                                pendingAckBatchMessageMap.get(positions.get(i));
                        txnIDConcurrentOpenHashSet.remove(txnId);
                        if (txnIDConcurrentOpenHashSet.isEmpty()) {
                            pendingAckBatchMessageMap.remove(positions.get(i));
                            pendingAckMessages.remove(positions.get(i));
                        }
                    } else {
                        this.pendingAckMessages.remove(positions.get(i));
                    }
                }
                pendingAckMessagesMap.remove(txnId);
                persistentSubscription.redeliverUnacknowledgedMessages(consumer,
                        (List<PositionImpl>) (List<?>) pendingAckMessageForCurrentTxn.values());
                abortFuture.complete(null);
            }
        }
        return abortFuture;
    }

    private void trimByMarkDeletePosition(List<PositionImpl> positions) {
        positions.removeIf(position -> cursor.getMarkDeletedPosition() != null
                && position.compareTo((PositionImpl) cursor.getMarkDeletedPosition()) <= 0);
    }

    @Override
    public void setPersistentSubscription(PersistentSubscription persistentSubscription) {
        this.persistentSubscription = persistentSubscription;
    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, Dispatcher dispatcher) {
        ConcurrentLongLongPairHashMap positionMap = consumer.getPendingAcks();
        // Only check if message is in pending_ack status when there's ongoing transaction.
        if (null != positionMap && ((pendingAckMessages != null && pendingAckMessages.size() != 0)
                || pendingCumulativeAckMessage != null)) {
            List<PositionImpl> pendingPositions = new ArrayList<>();
            PositionImpl cumulativeAckPosition = (null == this.pendingCumulativeAckMessage) ? null :
                    (PositionImpl) this.pendingCumulativeAckMessage;

            positionMap.asMap().forEach((key, value) -> {
                PositionImpl position = new PositionImpl(key.first, key.second);
                if (position.isBatchPosition()) {
                    if ((this.pendingAckMessages.containsKey(position) &&
                            !((PositionImpl) this.pendingAckMessages.get(position)).isAckSetRepeated(position))
                            || (cumulativeAckPosition != null && !cumulativeAckPosition.isAckSetRepeated(position))) {
                        pendingPositions.add(position);
                    }
                } else {
                    if (!this.pendingAckMessages.containsKey(position) ||
                            (cumulativeAckPosition != null && cumulativeAckPosition.compareTo(position) <= 0)) {
                        pendingPositions.add(position);
                    }
                }
            });

            dispatcher.redeliverUnacknowledgedMessages(consumer, pendingPositions);
        } else {
            dispatcher.redeliverUnacknowledgedMessages(consumer);
        }
    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions, Dispatcher dispatcher) {
        // If there's ongoing transaction.
        if ((pendingAckMessages != null && pendingAckMessages.size() != 0) || pendingCumulativeAckMessage != null) {
            // Check if message is in pending_ack status.
            List<PositionImpl> pendingPositions = new ArrayList<>();
            PositionImpl cumulativeAckPosition = (null == this.pendingCumulativeAckMessage) ? null :
                    (PositionImpl) this.pendingCumulativeAckMessage;

            positions.forEach(position -> {
                if (position.isBatchPosition()) {
                    if ((this.pendingAckMessages.containsKey(position) &&
                            !((PositionImpl) this.pendingAckMessages.get(position)).isAckSetRepeated(position))
                            || (cumulativeAckPosition != null && !cumulativeAckPosition.isAckSetRepeated(position))) {
                        pendingPositions.add(position);
                    }
                } else {
                    if (!this.pendingAckMessages.containsKey(position) ||
                            (cumulativeAckPosition != null && cumulativeAckPosition.compareTo(position) <= 0)) {
                        pendingPositions.add(position);
                    }
                }
            });
            trimByMarkDeletePosition(pendingPositions);
            dispatcher.redeliverUnacknowledgedMessages(consumer, pendingPositions);
        } else {
            trimByMarkDeletePosition(positions);
            dispatcher.redeliverUnacknowledgedMessages(consumer, positions);
        }
    }
}
