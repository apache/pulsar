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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeyLongValue;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Consumer is a consumer currently connected and associated with a Subscription.
 */
public class Consumer {
    private final Subscription subscription;
    private final SubType subType;
    private final TransportCnx cnx;
    private final String appId;
    private final String topicName;
    private final int partitionIdx;
    private final InitialPosition subscriptionInitialPosition;

    private final long consumerId;
    private final int priorityLevel;
    private final boolean readCompacted;
    private final String consumerName;
    private final Rate msgOut;
    private final Rate msgRedeliver;
    private final LongAdder msgOutCounter;
    private final LongAdder bytesOutCounter;

    private long lastConsumedTimestamp;
    private long lastAckedTimestamp;
    private Rate chunkedMessageRate;

    // Represents how many messages we can safely send to the consumer without
    // overflowing its receiving queue. The consumer will use Flow commands to
    // increase its availability
    private static final AtomicIntegerFieldUpdater<Consumer> MESSAGE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Consumer.class, "messagePermits");
    private volatile int messagePermits = 0;
    // It starts keep tracking of messagePermits once consumer gets blocked, as consumer needs two separate counts:
    // messagePermits (1) before and (2) after being blocked: to dispatch only blockedPermit number of messages at the
    // time of redelivery
    private static final AtomicIntegerFieldUpdater<Consumer> PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Consumer.class, "permitsReceivedWhileConsumerBlocked");
    private volatile int permitsReceivedWhileConsumerBlocked = 0;

    private final ConcurrentLongLongPairHashMap pendingAcks;

    private final ConsumerStatsImpl stats;

    private volatile int maxUnackedMessages;
    private static final AtomicIntegerFieldUpdater<Consumer> UNACKED_MESSAGES_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Consumer.class, "unackedMessages");
    private volatile int unackedMessages = 0;
    private volatile boolean blockedConsumerOnUnackedMsgs = false;

    private final Map<String, String> metadata;

    private final KeySharedMeta keySharedMeta;

    /**
     * It starts keep tracking the average messages per entry.
     * The initial value is 1000, when new value comes, it will update with
     * avgMessagesPerEntry = avgMessagePerEntry * avgPercent + (1 - avgPercent) * new Value.
     */
    private static final AtomicIntegerFieldUpdater<Consumer> AVG_MESSAGES_PER_ENTRY =
            AtomicIntegerFieldUpdater.newUpdater(Consumer.class, "avgMessagesPerEntry");
    private volatile int avgMessagesPerEntry = 1000;

    private static final double avgPercent = 0.9;
    private boolean preciseDispatcherFlowControl;
    private PositionImpl readPositionWhenJoining;
    private final String clientAddress; // IP address only, no port number included
    private final MessageId startMessageId;

    public Consumer(Subscription subscription, SubType subType, String topicName, long consumerId,
                    int priorityLevel, String consumerName,
                    int maxUnackedMessages, TransportCnx cnx, String appId,
                    Map<String, String> metadata, boolean readCompacted, InitialPosition subscriptionInitialPosition,
                    KeySharedMeta keySharedMeta, MessageId startMessageId) {

        this.subscription = subscription;
        this.subType = subType;
        this.topicName = topicName;
        this.partitionIdx = TopicName.getPartitionIndex(topicName);
        this.consumerId = consumerId;
        this.priorityLevel = priorityLevel;
        this.readCompacted = readCompacted;
        this.consumerName = consumerName;
        this.maxUnackedMessages = maxUnackedMessages;
        this.subscriptionInitialPosition = subscriptionInitialPosition;
        this.keySharedMeta = keySharedMeta;
        this.cnx = cnx;
        this.msgOut = new Rate();
        this.chunkedMessageRate = new Rate();
        this.msgRedeliver = new Rate();
        this.bytesOutCounter = new LongAdder();
        this.msgOutCounter = new LongAdder();
        this.appId = appId;

        // Ensure we start from compacted view
        this.startMessageId = (readCompacted && startMessageId == null) ? MessageId.earliest : startMessageId;

        this.preciseDispatcherFlowControl = cnx.isPreciseDispatcherFlowControl();
        PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER.set(this, 0);
        MESSAGE_PERMITS_UPDATER.set(this, 0);
        UNACKED_MESSAGES_UPDATER.set(this, 0);
        AVG_MESSAGES_PER_ENTRY.set(this, 1000);

        this.metadata = metadata != null ? metadata : Collections.emptyMap();

        stats = new ConsumerStatsImpl();
        if (cnx.hasHAProxyMessage()) {
            stats.setAddress(cnx.getHAProxyMessage().sourceAddress() + ":" + cnx.getHAProxyMessage().sourcePort());
        } else {
            stats.setAddress(cnx.clientAddress().toString());
        }
        stats.consumerName = consumerName;
        stats.setConnectedSince(DateFormatter.now());
        stats.setClientVersion(cnx.getClientVersion());
        stats.metadata = this.metadata;

        if (Subscription.isIndividualAckMode(subType)) {
            this.pendingAcks = new ConcurrentLongLongPairHashMap(256, 1);
        } else {
            // We don't need to keep track of pending acks if the subscription is not shared
            this.pendingAcks = null;
        }

        this.clientAddress = cnx.clientSourceAddress();
    }

    public SubType subType() {
        return subType;
    }

    public long consumerId() {
        return consumerId;
    }

    public String consumerName() {
        return consumerName;
    }

    void notifyActiveConsumerChange(Consumer activeConsumer) {
        if (log.isDebugEnabled()) {
            log.debug("notify consumer {} - that [{}] for subscription {} has new active consumer : {}",
                consumerId, topicName, subscription.getName(), activeConsumer);
        }
        cnx.getCommandSender().sendActiveConsumerChange(consumerId, this == activeConsumer);
    }

    public boolean readCompacted() {
        return readCompacted;
    }

    /**
     * Dispatch a list of entries to the consumer. <br/>
     * <b>It is also responsible to release entries data and recycle entries object.</b>
     *
     * @return a SendMessageInfo object that contains the detail of what was sent to consumer
     */
    public Future<Void> sendMessages(final List<Entry> entries, EntryBatchSizes batchSizes,
                                     EntryBatchIndexesAcks batchIndexesAcks,
                                     int totalMessages, long totalBytes, long totalChunkedMessages,
                                     RedeliveryTracker redeliveryTracker) {
        this.lastConsumedTimestamp = System.currentTimeMillis();

        if (entries.isEmpty() || totalMessages == 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] List of messages is empty, triggering write future immediately for consumerId {}",
                        topicName, subscription, consumerId);
            }
            batchSizes.recyle();
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
            final Promise<Void> writePromise = cnx.newPromise();
            writePromise.setSuccess(null);
            return writePromise;
        }

        // Note
        // Must ensure that the message is written to the pendingAcks before sent is first , because this consumer
        // is possible to disconnect at this time.
        if (pendingAcks != null) {
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                if (entry != null) {
                    int batchSize = batchSizes.getBatchSize(i);
                    int stickyKeyHash = getStickyKeyHash(entry);
                    pendingAcks.put(entry.getLedgerId(), entry.getEntryId(), batchSize, stickyKeyHash);
                    if (log.isDebugEnabled()){
                        log.debug("[{}-{}] Added {}:{} ledger entry with batchSize of {} to pendingAcks in"
                                        + " broker.service.Consumer for consumerId: {}",
                             topicName, subscription, entry.getLedgerId(), entry.getEntryId(), batchSize, consumerId);
                    }
                }
            }
        }

        // calculate avg message per entry
        int tmpAvgMessagesPerEntry = AVG_MESSAGES_PER_ENTRY.get(this);
        tmpAvgMessagesPerEntry = (int) Math.round(tmpAvgMessagesPerEntry * avgPercent
                + (1 - avgPercent) * totalMessages / entries.size());
        AVG_MESSAGES_PER_ENTRY.set(this, tmpAvgMessagesPerEntry);

        // reduce permit and increment unackedMsg count with total number of messages in batch-msgs
        int ackedCount = batchIndexesAcks == null ? 0 : batchIndexesAcks.getTotalAckedIndexCount();
        MESSAGE_PERMITS_UPDATER.addAndGet(this, ackedCount - totalMessages);
        if (log.isDebugEnabled()){
            log.debug("[{}-{}] Added {} minus {} messages to MESSAGE_PERMITS_UPDATER in broker.service.Consumer"
                            + " for consumerId: {}; avgMessagesPerEntry is {}",
                   topicName, subscription, ackedCount, totalMessages, consumerId, tmpAvgMessagesPerEntry);
        }
        incrementUnackedMessages(totalMessages);
        msgOut.recordMultipleEvents(totalMessages, totalBytes);
        msgOutCounter.add(totalMessages);
        bytesOutCounter.add(totalBytes);
        chunkedMessageRate.recordMultipleEvents(totalChunkedMessages, 0);


        return cnx.getCommandSender().sendMessagesToConsumer(consumerId, topicName, subscription, partitionIdx,
                entries, batchSizes, batchIndexesAcks, redeliveryTracker);
    }

    private void incrementUnackedMessages(int ackedMessages) {
        if (Subscription.isIndividualAckMode(subType)
                && addAndGetUnAckedMsgs(this, ackedMessages) >= maxUnackedMessages
                && maxUnackedMessages > 0) {
            blockedConsumerOnUnackedMsgs = true;
        }
    }

    public boolean isWritable() {
        return cnx.isWritable();
    }

    /**
     * Close the consumer if: a. the connection is dropped b. connection is open (graceful close) and there are no
     * pending message acks
     */
    public void close() throws BrokerServiceException {
        close(false);
    }

    public void close(boolean isResetCursor) throws BrokerServiceException {
        subscription.removeConsumer(this, isResetCursor);
        cnx.removedConsumer(this);
    }

    public void disconnect() {
        disconnect(false);
    }

    public void disconnect(boolean isResetCursor) {
        log.info("Disconnecting consumer: {}", this);
        cnx.closeConsumer(this);
        try {
            close(isResetCursor);
        } catch (BrokerServiceException e) {
            log.warn("Consumer {} was already closed: {}", this, e.getMessage(), e);
        }
    }

    public void doUnsubscribe(final long requestId) {
        subscription.doUnsubscribe(this).thenAccept(v -> {
            log.info("Unsubscribed successfully from {}", subscription);
            cnx.removedConsumer(this);
            cnx.getCommandSender().sendSuccess(requestId);
        }).exceptionally(exception -> {
            log.warn("Unsubscribe failed for {}", subscription, exception);
            cnx.getCommandSender().sendError(requestId, BrokerServiceException.getClientErrorCode(exception),
                    exception.getCause().getMessage());
            return null;
        });
    }

    public CompletableFuture<Void> messageAcked(CommandAck ack) {
        this.lastAckedTimestamp = System.currentTimeMillis();
        Map<String, Long> properties = Collections.emptyMap();
        if (ack.getPropertiesCount() > 0) {
            properties = ack.getPropertiesList().stream()
                .collect(Collectors.toMap(KeyLongValue::getKey, KeyLongValue::getValue));
        }

        if (ack.getAckType() == AckType.Cumulative) {
            if (ack.getMessageIdsCount() != 1) {
                log.warn("[{}] [{}] Received multi-message ack", subscription, consumerId);
            }

            if (Subscription.isIndividualAckMode(subType)) {
                log.warn("[{}] [{}] Received cumulative ack on shared subscription, ignoring",
                        subscription, consumerId);
            }
            PositionImpl position = PositionImpl.earliest;
            if (ack.getMessageIdsCount() == 1) {
                MessageIdData msgId = ack.getMessageIdAt(0);
                if (msgId.getAckSetsCount() > 0) {
                    long[] ackSets = new long[msgId.getAckSetsCount()];
                    for (int j = 0; j < msgId.getAckSetsCount(); j++) {
                        ackSets[j] = msgId.getAckSetAt(j);
                    }
                    position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId(), ackSets);
                } else {
                    position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId());
                }
            }
            if (ack.hasTxnidMostBits() && ack.hasTxnidLeastBits()) {
                List<PositionImpl> positionsAcked = Collections.singletonList(position);
                return transactionCumulativeAcknowledge(ack.getTxnidMostBits(),
                        ack.getTxnidLeastBits(), positionsAcked);
            } else {
                List<Position> positionsAcked = Collections.singletonList(position);
                subscription.acknowledgeMessage(positionsAcked, AckType.Cumulative, properties);
                return CompletableFuture.completedFuture(null);
            }
        } else {
            if (ack.hasTxnidLeastBits() && ack.hasTxnidMostBits()) {
                return individualAckWithTransaction(ack);
            } else {
                return individualAckNormal(ack, properties);
            }
        }
    }

    //this method is for individual ack not carry the transaction
    private CompletableFuture<Void> individualAckNormal(CommandAck ack, Map<String, Long> properties) {
        List<Position> positionsAcked = new ArrayList<>();

        for (int i = 0; i < ack.getMessageIdsCount(); i++) {
            MessageIdData msgId = ack.getMessageIdAt(i);
            PositionImpl position;
            if (msgId.getAckSetsCount() > 0) {
                long[] ackSets = new long[msgId.getAckSetsCount()];
                for (int j = 0; j < msgId.getAckSetsCount(); j++) {
                    ackSets[j] = msgId.getAckSetAt(j);
                }
                position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId(), ackSets);
                if (isTransactionEnabled()) {
                    //sync the batch position bit set point, in order to delete the position in pending acks
                    if (Subscription.isIndividualAckMode(subType)) {
                        ((PersistentSubscription) subscription)
                                .syncBatchPositionBitSetForPendingAck(position);
                    }
                }
            } else {
                position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId());
            }
            positionsAcked.add(position);

            checkCanRemovePendingAcksAndHandle(position, msgId);

            checkAckValidationError(ack, position);
        }
        subscription.acknowledgeMessage(positionsAcked, AckType.Individual, properties);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.complete(null);
        if (isTransactionEnabled() && Subscription.isIndividualAckMode(subType)) {
            completableFuture.whenComplete((v, e) -> positionsAcked.forEach(position -> {
                //check if the position can remove from the consumer pending acks.
                // the bit set is empty in pending ack handle.
                if (((PositionImpl) position).getAckSet() != null) {
                    if (((PersistentSubscription) subscription)
                            .checkIsCanDeleteConsumerPendingAck((PositionImpl) position)) {
                        removePendingAcks((PositionImpl) position);
                    }
                }
            }));
        }
        return completableFuture;
    }


    //this method is for individual ack carry the transaction
    private CompletableFuture<Void> individualAckWithTransaction(CommandAck ack) {
        // Individual ack
        List<MutablePair<PositionImpl, Integer>> positionsAcked = new ArrayList<>();

        if (!isTransactionEnabled()) {
            return FutureUtil.failedFuture(
                    new BrokerServiceException.NotAllowedException("Server don't support transaction ack!"));
        }

        for (int i = 0; i < ack.getMessageIdsCount(); i++) {
            MessageIdData msgId = ack.getMessageIdAt(i);
            PositionImpl position;
            if (msgId.getAckSetsCount() > 0) {
                long[] acksSets = new long[msgId.getAckSetsCount()];
                for (int j = 0; j < msgId.getAckSetsCount(); j++) {
                    acksSets[j] = msgId.getAckSetAt(j);
                }
                position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId(), acksSets);
            } else {
                position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId());
            }

            if (msgId.hasBatchIndex()) {
                positionsAcked.add(new MutablePair<>(position, msgId.getBatchSize()));
            } else {
                positionsAcked.add(new MutablePair<>(position, 0));
            }

            checkCanRemovePendingAcksAndHandle(position, msgId);

            checkAckValidationError(ack, position);
        }

        CompletableFuture<Void> completableFuture = transactionIndividualAcknowledge(ack.getTxnidMostBits(),
                ack.getTxnidLeastBits(), positionsAcked);
        if (Subscription.isIndividualAckMode(subType)) {
            completableFuture.whenComplete((v, e) ->
                    positionsAcked.forEach(positionLongMutablePair -> {
                        if (positionLongMutablePair.getLeft().getAckSet() != null) {
                            if (((PersistentSubscription) subscription)
                                    .checkIsCanDeleteConsumerPendingAck(positionLongMutablePair.left)) {
                                removePendingAcks(positionLongMutablePair.left);
                            }
                        }
                    }));
        }
        return completableFuture;
    }

    private void checkAckValidationError(CommandAck ack, PositionImpl position) {
        if (ack.hasValidationError()) {
            log.error("[{}] [{}] Received ack for corrupted message at {} - Reason: {}", subscription,
                    consumerId, position, ack.getValidationError());
        }
    }

    private void checkCanRemovePendingAcksAndHandle(PositionImpl position, MessageIdData msgId) {
        if (Subscription.isIndividualAckMode(subType) && msgId.getAckSetsCount() == 0) {
            removePendingAcks(position);
        }
    }

    private boolean isTransactionEnabled() {
        return subscription instanceof PersistentSubscription
                && ((PersistentTopic) subscription.getTopic())
                .getBrokerService().getPulsar().getConfig().isTransactionCoordinatorEnabled();
    }

    private CompletableFuture<Void> transactionIndividualAcknowledge(
            long txnidMostBits,
            long txnidLeastBits,
            List<MutablePair<PositionImpl, Integer>> positionList) {
        if (subscription instanceof PersistentSubscription) {
            TxnID txnID = new TxnID(txnidMostBits, txnidLeastBits);
            return ((PersistentSubscription) subscription).transactionIndividualAcknowledge(txnID, positionList);
        } else {
            String error = "Transaction acknowledge only support the `PersistentSubscription`.";
            log.error(error);
            return FutureUtil.failedFuture(new TransactionConflictException(error));
        }
    }

    private CompletableFuture<Void> transactionCumulativeAcknowledge(long txnidMostBits, long txnidLeastBits,
                                                                     List<PositionImpl> positionList) {
        if (!isTransactionEnabled()) {
            return FutureUtil.failedFuture(
                    new BrokerServiceException.NotAllowedException("Server don't support transaction ack!"));
        }
        if (subscription instanceof PersistentSubscription) {
            TxnID txnID = new TxnID(txnidMostBits, txnidLeastBits);
            return ((PersistentSubscription) subscription).transactionCumulativeAcknowledge(txnID, positionList);
        } else {
            String error = "Transaction acknowledge only support the `PersistentSubscription`.";
            log.error(error);
            return FutureUtil.failedFuture(new TransactionConflictException(error));
        }
    }

    public void flowPermits(int additionalNumberOfMessages) {
        checkArgument(additionalNumberOfMessages > 0);

        // block shared consumer when unacked-messages reaches limit
        if (shouldBlockConsumerOnUnackMsgs() && unackedMessages >= maxUnackedMessages) {
            blockedConsumerOnUnackedMsgs = true;
        }
        int oldPermits;
        if (!blockedConsumerOnUnackedMsgs) {
            oldPermits = MESSAGE_PERMITS_UPDATER.getAndAdd(this, additionalNumberOfMessages);
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] Added {} message permits in broker.service.Consumer before updating dispatcher "
                        + "for consumer {}", topicName, subscription, additionalNumberOfMessages, consumerId);
            }
            subscription.consumerFlow(this, additionalNumberOfMessages);
        } else {
            oldPermits = PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER.getAndAdd(this, additionalNumberOfMessages);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Added more flow control message permits {} (old was: {}), blocked = {} ", topicName,
                    subscription, additionalNumberOfMessages, oldPermits, blockedConsumerOnUnackedMsgs);
        }

    }

    /**
     * Triggers dispatcher to dispatch {@code blockedPermits} number of messages and adds same number of permits to
     * {@code messagePermits} as it maintains count of actual dispatched message-permits.
     *
     * @param consumer:
     *            Consumer whose blockedPermits needs to be dispatched
     */
    void flowConsumerBlockedPermits(Consumer consumer) {
        int additionalNumberOfPermits = PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER.getAndSet(consumer, 0);
        // add newly flow permits to actual consumer.messagePermits
        MESSAGE_PERMITS_UPDATER.getAndAdd(consumer, additionalNumberOfPermits);
        if (log.isDebugEnabled()){
            log.debug("[{}-{}] Added {} blocked permits to broker.service.Consumer for consumer {}", topicName,
                    subscription, additionalNumberOfPermits, consumerId);
        }
        // dispatch pending permits to flow more messages: it will add more permits to dispatcher and consumer
        subscription.consumerFlow(consumer, additionalNumberOfPermits);
    }

    public int getAvailablePermits() {
        return MESSAGE_PERMITS_UPDATER.get(this);
    }

    public int getAvgMessagesPerEntry() {
        return AVG_MESSAGES_PER_ENTRY.get(this);
    }

    public boolean isBlocked() {
        return blockedConsumerOnUnackedMsgs;
    }

    public void reachedEndOfTopic() {
        cnx.getCommandSender().sendReachedEndOfTopic(consumerId);
    }

    /**
     * Checks if consumer-blocking on unAckedMessages is allowed for below conditions:<br/>
     * a. consumer must have Shared-subscription<br/>
     * b. {@link this#maxUnackedMessages} value > 0
     *
     * @return
     */
    private boolean shouldBlockConsumerOnUnackMsgs() {
        return Subscription.isIndividualAckMode(subType) && maxUnackedMessages > 0;
    }

    public void updateRates() {
        msgOut.calculateRate();
        chunkedMessageRate.calculateRate();
        msgRedeliver.calculateRate();
        stats.msgRateOut = msgOut.getRate();
        stats.msgThroughputOut = msgOut.getValueRate();
        stats.msgRateRedeliver = msgRedeliver.getRate();
        stats.chunkedMessageRate = chunkedMessageRate.getRate();
    }

    public void updateStats(ConsumerStatsImpl consumerStats) {
        msgOutCounter.add(consumerStats.msgOutCounter);
        bytesOutCounter.add(consumerStats.bytesOutCounter);
        msgOut.recordMultipleEvents(consumerStats.msgOutCounter, consumerStats.bytesOutCounter);
        lastAckedTimestamp = consumerStats.lastAckedTimestamp;
        lastConsumedTimestamp = consumerStats.lastConsumedTimestamp;
        MESSAGE_PERMITS_UPDATER.set(this, consumerStats.availablePermits);
        if (log.isDebugEnabled()){
            log.debug("[{}-{}] Setting broker.service.Consumer's messagePermits to {} for consumer {}", topicName,
                    subscription, consumerStats.availablePermits, consumerId);
        }
        unackedMessages = consumerStats.unackedMessages;
        blockedConsumerOnUnackedMsgs = consumerStats.blockedConsumerOnUnackedMsgs;
        AVG_MESSAGES_PER_ENTRY.set(this, consumerStats.avgMessagesPerEntry);
    }

    public ConsumerStatsImpl getStats() {
        stats.msgOutCounter = msgOutCounter.longValue();
        stats.bytesOutCounter = bytesOutCounter.longValue();
        stats.lastAckedTimestamp = lastAckedTimestamp;
        stats.lastConsumedTimestamp = lastConsumedTimestamp;
        stats.availablePermits = getAvailablePermits();
        stats.unackedMessages = unackedMessages;
        stats.blockedConsumerOnUnackedMsgs = blockedConsumerOnUnackedMsgs;
        stats.avgMessagesPerEntry = getAvgMessagesPerEntry();
        if (readPositionWhenJoining != null) {
            stats.readPositionWhenJoining = readPositionWhenJoining.toString();
        }
        return stats;
    }

    public int getUnackedMessages() {
        return unackedMessages;
    }

    public KeySharedMeta getKeySharedMeta() {
        return keySharedMeta;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("subscription", subscription).add("consumerId", consumerId)
                .add("consumerName", consumerName).add("address", this.cnx.clientAddress()).toString();
    }

    public void checkPermissions() {
        TopicName topicName = TopicName.get(subscription.getTopicName());
        if (cnx.getBrokerService().getAuthorizationService() != null) {
            try {
                if (cnx.getBrokerService().getAuthorizationService().canConsume(topicName, appId,
                        cnx.getAuthenticationData(), subscription.getName())) {
                    return;
                }
            } catch (Exception e) {
                log.warn("[{}] Get unexpected error while autorizing [{}]  {}", appId, subscription.getTopicName(),
                        e.getMessage(), e);
            }
            log.info("[{}] is not allowed to consume from topic [{}] anymore", appId, subscription.getTopicName());
            disconnect();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Consumer) {
            Consumer other = (Consumer) obj;
            return Objects.equals(cnx.clientAddress(), other.cnx.clientAddress()) && consumerId == other.consumerId;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return consumerName.hashCode() + 31 * cnx.hashCode();
    }

    /**
     * first try to remove ack-position from the current_consumer's pendingAcks.
     * if ack-message doesn't present into current_consumer's pendingAcks
     *  a. try to remove from other connected subscribed consumers (It happens when client
     * tries to acknowledge message through different consumer under the same subscription)
     *
     *
     * @param position
     */
    private void removePendingAcks(PositionImpl position) {
        Consumer ackOwnedConsumer = null;
        if (pendingAcks.get(position.getLedgerId(), position.getEntryId()) == null) {
            for (Consumer consumer : subscription.getConsumers()) {
                if (!consumer.equals(this) && consumer.getPendingAcks().containsKey(position.getLedgerId(),
                        position.getEntryId())) {
                    ackOwnedConsumer = consumer;
                    break;
                }
            }
        } else {
            ackOwnedConsumer = this;
        }

        // remove pending message from appropriate consumer and unblock unAckMsg-flow if requires
        LongPair ackedPosition = ackOwnedConsumer != null
                ? ackOwnedConsumer.getPendingAcks().get(position.getLedgerId(), position.getEntryId())
                : null;
        if (ackedPosition != null) {
            int totalAckedMsgs = (int) ackedPosition.first;
            if (!ackOwnedConsumer.getPendingAcks().remove(position.getLedgerId(), position.getEntryId())) {
                // Message was already removed by the other consumer
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] consumer {} received ack {}", topicName, subscription, consumerId, position);
            }
            // unblock consumer-throttling when limit check is disabled or receives half of maxUnackedMessages =>
            // consumer can start again consuming messages
            int unAckedMsgs = addAndGetUnAckedMsgs(ackOwnedConsumer, -totalAckedMsgs);
            if ((((unAckedMsgs <= maxUnackedMessages / 2) && ackOwnedConsumer.blockedConsumerOnUnackedMsgs)
                    && ackOwnedConsumer.shouldBlockConsumerOnUnackMsgs())
                    || !shouldBlockConsumerOnUnackMsgs()) {
                ackOwnedConsumer.blockedConsumerOnUnackedMsgs = false;
                flowConsumerBlockedPermits(ackOwnedConsumer);
            }
        }
    }

    public ConcurrentLongLongPairHashMap getPendingAcks() {
        return pendingAcks;
    }

    public int getPriorityLevel() {
        return priorityLevel;
    }

    public void redeliverUnacknowledgedMessages() {
        // cleanup unackedMessage bucket and redeliver those unack-msgs again
        clearUnAckedMsgs();
        blockedConsumerOnUnackedMsgs = false;
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] consumer {} received redelivery", topicName, subscription, consumerId);
        }

        if (pendingAcks != null) {
            List<PositionImpl> pendingPositions = new ArrayList<>((int) pendingAcks.size());
            MutableInt totalRedeliveryMessages = new MutableInt(0);
            pendingAcks.forEach((ledgerId, entryId, batchSize, stickyKeyHash) -> {
                totalRedeliveryMessages.add((int) batchSize);
                pendingPositions.add(new PositionImpl(ledgerId, entryId));
            });

            for (PositionImpl p : pendingPositions) {
                pendingAcks.remove(p.getLedgerId(), p.getEntryId());
            }

            msgRedeliver.recordMultipleEvents(totalRedeliveryMessages.intValue(), totalRedeliveryMessages.intValue());
            subscription.redeliverUnacknowledgedMessages(this, pendingPositions);
        } else {
            subscription.redeliverUnacknowledgedMessages(this);
        }

        flowConsumerBlockedPermits(this);
    }

    public void redeliverUnacknowledgedMessages(List<MessageIdData> messageIds) {
        int totalRedeliveryMessages = 0;
        List<PositionImpl> pendingPositions = Lists.newArrayList();
        for (MessageIdData msg : messageIds) {
            PositionImpl position = PositionImpl.get(msg.getLedgerId(), msg.getEntryId());
            LongPair longPair = pendingAcks.get(position.getLedgerId(), position.getEntryId());
            if (longPair != null) {
                long batchSize = longPair.first;
                pendingAcks.remove(position.getLedgerId(), position.getEntryId());
                totalRedeliveryMessages += batchSize;
                pendingPositions.add(position);
            }
        }

        addAndGetUnAckedMsgs(this, -totalRedeliveryMessages);
        blockedConsumerOnUnackedMsgs = false;

        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] consumer {} received {} msg-redelivery {}", topicName, subscription, consumerId,
                    totalRedeliveryMessages, pendingPositions.size());
        }

        subscription.redeliverUnacknowledgedMessages(this, pendingPositions);
        msgRedeliver.recordMultipleEvents(totalRedeliveryMessages, totalRedeliveryMessages);

        int numberOfBlockedPermits = PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER.getAndSet(this, 0);

        // if permitsReceivedWhileConsumerBlocked has been accumulated then pass it to Dispatcher to flow messages
        if (numberOfBlockedPermits > 0) {
            MESSAGE_PERMITS_UPDATER.getAndAdd(this, numberOfBlockedPermits);
            if (log.isDebugEnabled()) {
               log.debug("[{}-{}] Added {} blockedPermits to broker.service.Consumer's messagePermits for consumer {}",
                       topicName, subscription, numberOfBlockedPermits, consumerId);
            }
            subscription.consumerFlow(this, numberOfBlockedPermits);
        }
    }

    public Subscription getSubscription() {
        return subscription;
    }

    private int addAndGetUnAckedMsgs(Consumer consumer, int ackedMessages) {
        subscription.addUnAckedMessages(ackedMessages);
        return UNACKED_MESSAGES_UPDATER.addAndGet(consumer, ackedMessages);
    }

    private void clearUnAckedMsgs() {
        int unaAckedMsgs = UNACKED_MESSAGES_UPDATER.getAndSet(this, 0);
        subscription.addUnAckedMessages(-unaAckedMsgs);
    }

    public boolean isPreciseDispatcherFlowControl() {
        return preciseDispatcherFlowControl;
    }

    public void setReadPositionWhenJoining(PositionImpl readPositionWhenJoining) {
        this.readPositionWhenJoining = readPositionWhenJoining;
    }

    public int getMaxUnackedMessages() {
        return maxUnackedMessages;
    }

    public void setMaxUnackedMessages(int maxUnackedMessages) {
        this.maxUnackedMessages = maxUnackedMessages;
    }

    public TransportCnx cnx() {
        return cnx;
    }

    public String getClientAddress() {
        return clientAddress;
    }

    public MessageId getStartMessageId() {
        return startMessageId;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    private int getStickyKeyHash(Entry entry) {
        byte[] stickyKey = Commands.peekStickyKey(entry.getDataBuffer(), topicName, subscription.getName());
        return StickyKeyConsumerSelector.makeStickyKeyHash(stickyKey);
    }

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);
}
