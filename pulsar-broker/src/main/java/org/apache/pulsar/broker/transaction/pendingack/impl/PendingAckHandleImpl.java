
package org.apache.pulsar.broker.transaction.pendingack.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class PendingAckHandleImpl implements PendingAckHandle {

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

    public PendingAckHandleImpl(String topicName, String subName) {
        this.topicName = topicName;
        this.subName = subName;
    }

    @Override
    public synchronized CompletableFuture<Void> acknowledgeMessage(TxnID txnId,
                                                                   List<Position> positions, AckType ackType) {
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
            if (this.pendingCumulativeAckTxnId == null) {
                // Only set pendingCumulativeAckTxnId if no transaction is doing cumulative ack.
                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, txnId);
                POSITION_UPDATER.set(this, position);
            } else if (this.pendingCumulativeAckTxnId.equals(txnId)
                    && position.compareToWithAckSet((PositionImpl) pendingCumulativeAckMessage) > 0) {
                // Only set pendingCumulativeAckTxnId if no transaction is doing cumulative ack.
                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, txnId);
                POSITION_UPDATER.set(this, position);
            } else {
                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " try to cumulative batch ack position: " + position + " within range of current " +
                        "currentPosition: " + this.pendingCumulativeAckMessage;
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                return completableFuture;
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
                return completableFuture;
            }

            // If try to ack message already acked by some ongoing transaction(can be itself), throw exception.
            // Acking single message within range of cumulative ack(if exist) is considered valid operation.
            if (!position.isBatchPosition() && pendingAckMessages.containsKey(position)) {

                String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                        " try to ack message:" + position + " in pending ack status.";
                log.error(errorMsg);
                completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                return completableFuture;
            }

            if (position.isBatchPosition()) {

                if (pendingAckMessages.containsKey(position)
                        && ((PositionImpl) pendingAckMessages.get(position))
                        .isAckSetRepeated(position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                            " try to ack batch message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                    return completableFuture;
                }

                ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                        pendingIndividualAckMessagesMap.computeIfAbsent(txnId, txn -> new ConcurrentOpenHashMap<>());

                if (pendingAckBatchMessageMap == null) {
                    this.pendingAckBatchMessageMap = new ConcurrentOpenHashMap<>();
                }

                ConcurrentOpenHashSet<TxnID> txnSet = this.pendingAckBatchMessageMap
                        .computeIfAbsent(position, txn -> new ConcurrentOpenHashSet<>());

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
            } else {
                if (this.pendingAckMessages.containsKey(position)) {
                    String errorMsg = "[" + topicName + "][" + subName + "] Transaction:" + txnId +
                            " try to ack message:" + position + " in pending ack status.";
                    log.error(errorMsg);
                    completableFuture.completeExceptionally(new TransactionConflictException(errorMsg));
                    return completableFuture;
                }
                ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                        pendingIndividualAckMessagesMap.computeIfAbsent(txnId, txn -> new ConcurrentOpenHashMap<>());
                pendingAckMessageForCurrentTxn.put(position, position);
                this.pendingAckMessages.put(position, position);
            }
        }
        completableFuture.complete(null);
        return completableFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> commitTxn(TxnID txnId, Map<String, Long> properties) {

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        // It's valid to create transaction then commit without doing any operation, which will cause
        // pendingAckMessagesMap to be null.
        if (pendingCumulativeAckTxnId != null) {
            if (pendingCumulativeAckTxnId.equals(txnId)) {
                persistentSubscription.acknowledgeMessage(Collections
                        .singletonList(POSITION_UPDATER.get(this)), AckType.Cumulative, properties);
                // Reset txdID and position for cumulative ack.
                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, null);
                POSITION_UPDATER.set(this, null);
            }
        } else {
            if (pendingIndividualAckMessagesMap != null && pendingIndividualAckMessagesMap.containsKey(txnId)) {
                ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                        pendingIndividualAckMessagesMap.remove(txnId);
                if (pendingAckMessageForCurrentTxn != null) {
                    List<Position> positions = pendingAckMessageForCurrentTxn.values();
                    for (Position position : positions) {
                        if (pendingAckBatchMessageMap != null && pendingAckBatchMessageMap.containsKey(position)) {
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
                    persistentSubscription.acknowledgeMessage(positions, AckType.Individual, properties);
                }
            }
        }
        commitFuture.complete(null);
        return commitFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer) {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();
        if (pendingCumulativeAckTxnId != null && pendingCumulativeAckMessage != null) {
            if (PENDING_CUMULATIVE_ACK_TXNID_UPDATER.get(this).equals(txnId)) {
                POSITION_UPDATER.set(this, null);
                PENDING_CUMULATIVE_ACK_TXNID_UPDATER.set(this, null);
                redeliverUnacknowledgedMessages(consumer);
            }
        } else if (this.pendingIndividualAckMessagesMap != null){
            ConcurrentOpenHashMap<Position, Position> pendingAckMessageForCurrentTxn =
                    pendingIndividualAckMessagesMap.remove(txnId);
            if (pendingAckMessageForCurrentTxn != null) {
                List<Position> positions = pendingAckMessageForCurrentTxn.values();
                for (Position position : positions) {
                    if (pendingAckBatchMessageMap != null && pendingAckBatchMessageMap.containsKey(position)) {
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
                redeliverUnacknowledgedMessages(consumer,
                        (List<PositionImpl>) (List<?>) pendingAckMessageForCurrentTxn.values());
            }
        }
        abortFuture.complete(null);
        return abortFuture;
    }

    @Override
    public void setPersistentSubscription(PersistentSubscription persistentSubscription) {
        this.persistentSubscription = persistentSubscription;
    }

    @Override
    public synchronized void redeliverUnacknowledgedMessages(Consumer consumer) {
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
}