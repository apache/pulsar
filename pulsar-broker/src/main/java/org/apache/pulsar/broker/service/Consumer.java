/*
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
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.AtomicDouble;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated.ResourceType;
import org.apache.pulsar.common.api.proto.KeyLongValue;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData.ClusterUrl;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap.LongPair;
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

    private final long consumerId;
    private final int priorityLevel;
    private final boolean readCompacted;
    private final String consumerName;
    private final Rate msgOut;
    private final Rate msgRedeliver;
    private final LongAdder msgOutCounter;
    private final LongAdder bytesOutCounter;
    private final Rate messageAckRate;

    private volatile long lastConsumedTimestamp;
    private volatile long lastAckedTimestamp;
    private volatile long lastConsumedFlowTimestamp;
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

    private final boolean isDurable;

    private final boolean isPersistentTopic;

    private static final AtomicIntegerFieldUpdater<Consumer> UNACKED_MESSAGES_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Consumer.class, "unackedMessages");
    private volatile int unackedMessages = 0;
    private volatile boolean blockedConsumerOnUnackedMsgs = false;

    private final Map<String, String> metadata;

    private final KeySharedMeta keySharedMeta;

    /**
     * It starts keep tracking the average messages per entry.
     * The initial value is 0, when new value comes, it will update with
     * avgMessagesPerEntry = avgMessagePerEntry * avgPercent + (1 - avgPercent) * new Value.
     */
    private final AtomicDouble avgMessagesPerEntry = new AtomicDouble(0);
    private static final long [] EMPTY_ACK_SET = new long[0];

    private static final double avgPercent = 0.9;
    private boolean preciseDispatcherFlowControl;
    private PositionImpl readPositionWhenJoining;
    private final String clientAddress; // IP address only, no port number included
    private final MessageId startMessageId;
    private final boolean isAcknowledgmentAtBatchIndexLevelEnabled;

    @Getter
    @Setter
    private volatile long consumerEpoch;

    private long negativeUnackedMsgsTimestamp;

    @Getter
    private final SchemaType schemaType;

    public Consumer(Subscription subscription, SubType subType, String topicName, long consumerId,
                    int priorityLevel, String consumerName,
                    boolean isDurable, TransportCnx cnx, String appId,
                    Map<String, String> metadata, boolean readCompacted,
                    KeySharedMeta keySharedMeta, MessageId startMessageId, long consumerEpoch) {
        this(subscription, subType, topicName, consumerId, priorityLevel, consumerName, isDurable, cnx, appId,
                metadata, readCompacted, keySharedMeta, startMessageId, consumerEpoch, null);
    }

    public Consumer(Subscription subscription, SubType subType, String topicName, long consumerId,
                    int priorityLevel, String consumerName,
                    boolean isDurable, TransportCnx cnx, String appId,
                    Map<String, String> metadata, boolean readCompacted,
                    KeySharedMeta keySharedMeta, MessageId startMessageId,
                    long consumerEpoch, SchemaType schemaType) {
        this.subscription = subscription;
        this.subType = subType;
        this.topicName = topicName;
        this.partitionIdx = TopicName.getPartitionIndex(topicName);
        this.consumerId = consumerId;
        this.priorityLevel = priorityLevel;
        this.readCompacted = readCompacted;
        this.consumerName = consumerName;
        this.isDurable = isDurable;
        this.isPersistentTopic = subscription.getTopic() instanceof PersistentTopic;
        this.keySharedMeta = keySharedMeta;
        this.cnx = cnx;
        this.msgOut = new Rate();
        this.chunkedMessageRate = new Rate();
        this.msgRedeliver = new Rate();
        this.bytesOutCounter = new LongAdder();
        this.msgOutCounter = new LongAdder();
        this.messageAckRate = new Rate();
        this.appId = appId;

        // Ensure we start from compacted view
        this.startMessageId = (readCompacted && startMessageId == null) ? MessageId.earliest : startMessageId;

        this.preciseDispatcherFlowControl = cnx.isPreciseDispatcherFlowControl();
        PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER.set(this, 0);
        MESSAGE_PERMITS_UPDATER.set(this, 0);
        UNACKED_MESSAGES_UPDATER.set(this, 0);

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
            this.pendingAcks = ConcurrentLongLongPairHashMap.newBuilder()
                    .autoShrink(subscription.getTopic().getBrokerService()
                            .getPulsar().getConfiguration().isAutoShrinkForConsumerPendingAcksMap())
                    .expectedItems(256)
                    .concurrencyLevel(1)
                    .build();
        } else {
            // We don't need to keep track of pending acks if the subscription is not shared
            this.pendingAcks = null;
        }

        this.clientAddress = cnx.clientSourceAddress();
        this.consumerEpoch = consumerEpoch;
        this.isAcknowledgmentAtBatchIndexLevelEnabled = subscription.getTopic().getBrokerService()
                .getPulsar().getConfiguration().isAcknowledgmentAtBatchIndexLevelEnabled();

        this.schemaType = schemaType;
    }

    @VisibleForTesting
    Consumer(String consumerName, int availablePermits) {
        this.subscription = null;
        this.subType = null;
        this.cnx = null;
        this.appId = null;
        this.topicName = null;
        this.partitionIdx = 0;
        this.consumerId = 0L;
        this.priorityLevel = 0;
        this.readCompacted = false;
        this.consumerName = consumerName;
        this.msgOut = null;
        this.msgRedeliver = null;
        this.msgOutCounter = null;
        this.bytesOutCounter = null;
        this.messageAckRate = null;
        this.pendingAcks = null;
        this.stats = null;
        this.isDurable = false;
        this.isPersistentTopic = false;
        this.metadata = null;
        this.keySharedMeta = null;
        this.clientAddress = null;
        this.startMessageId = null;
        this.isAcknowledgmentAtBatchIndexLevelEnabled = false;
        this.schemaType = null;
        MESSAGE_PERMITS_UPDATER.set(this, availablePermits);
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

    public Future<Void> sendMessages(final List<? extends Entry> entries, EntryBatchSizes batchSizes,
                                     EntryBatchIndexesAcks batchIndexesAcks,
                                     int totalMessages, long totalBytes, long totalChunkedMessages,
                                     RedeliveryTracker redeliveryTracker) {
        return sendMessages(entries, batchSizes, batchIndexesAcks, totalMessages, totalBytes,
                totalChunkedMessages, redeliveryTracker, DEFAULT_CONSUMER_EPOCH);
    }

    /**
     * Dispatch a list of entries to the consumer. <br/>
     * <b>It is also responsible to release entries data and recycle entries object.</b>
     *
     * @return a SendMessageInfo object that contains the detail of what was sent to consumer
     */
    public Future<Void> sendMessages(final List<? extends Entry> entries, EntryBatchSizes batchSizes,
                                     EntryBatchIndexesAcks batchIndexesAcks,
                                     int totalMessages, long totalBytes, long totalChunkedMessages,
                                     RedeliveryTracker redeliveryTracker, long epoch) {
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
        int unackedMessages = totalMessages;
        int totalEntries = 0;

        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            if (entry != null) {
                totalEntries++;
                // Note
                // Must ensure that the message is written to the pendingAcks before sent is first,
                // because this consumer is possible to disconnect at this time.
                if (pendingAcks != null) {
                    int batchSize = batchSizes.getBatchSize(i);
                    int stickyKeyHash = getStickyKeyHash(entry);
                    long[] ackSet = batchIndexesAcks == null ? null : batchIndexesAcks.getAckSet(i);
                    if (ackSet != null) {
                        unackedMessages -= (batchSize - BitSet.valueOf(ackSet).cardinality());
                    }
                    pendingAcks.put(entry.getLedgerId(), entry.getEntryId(), batchSize, stickyKeyHash);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}-{}] Added {}:{} ledger entry with batchSize of {} to pendingAcks in"
                                        + " broker.service.Consumer for consumerId: {}",
                                topicName, subscription, entry.getLedgerId(), entry.getEntryId(), batchSize,
                                consumerId);
                    }
                }
            }
        }

        // calculate avg message per entry
        if (avgMessagesPerEntry.get() < 1) { //valid avgMessagesPerEntry should always >= 1
            // set init value.
            avgMessagesPerEntry.set(1.0 * totalMessages / totalEntries);
        } else {
            avgMessagesPerEntry.set(avgMessagesPerEntry.get() * avgPercent
                    + (1 - avgPercent) * totalMessages / totalEntries);
        }

        // reduce permit and increment unackedMsg count with total number of messages in batch-msgs
        int ackedCount = batchIndexesAcks == null ? 0 : batchIndexesAcks.getTotalAckedIndexCount();
        MESSAGE_PERMITS_UPDATER.addAndGet(this, ackedCount - totalMessages);
        if (log.isDebugEnabled()){
            log.debug("[{}-{}] Added {} minus {} messages to MESSAGE_PERMITS_UPDATER in broker.service.Consumer"
                            + " for consumerId: {}; avgMessagesPerEntry is {}",
                   topicName, subscription, ackedCount, totalMessages, consumerId, avgMessagesPerEntry.get());
        }
        incrementUnackedMessages(unackedMessages);
        Future<Void> writeAndFlushPromise =
                cnx.getCommandSender().sendMessagesToConsumer(consumerId, topicName, subscription, partitionIdx,
                        entries, batchSizes, batchIndexesAcks, redeliveryTracker, epoch);
        writeAndFlushPromise.addListener(status -> {
            // only increment counters after the messages have been successfully written to the TCP/IP connection
            if (status.isSuccess()) {
                msgOut.recordMultipleEvents(totalMessages, totalBytes);
                msgOutCounter.add(totalMessages);
                bytesOutCounter.add(totalBytes);
                chunkedMessageRate.recordMultipleEvents(totalChunkedMessages, 0);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}-{}] Sent messages to client fail by IO exception[{}], close the connection"
                                    + " immediately. Consumer: {}",  topicName, subscription,
                            status.cause() == null ? "" : status.cause().getMessage(), this.toString());
                }
            }
        });
        return writeAndFlushPromise;
    }

    private void incrementUnackedMessages(int unackedMessages) {
        if (Subscription.isIndividualAckMode(subType)
                && addAndGetUnAckedMsgs(this, unackedMessages) >= getMaxUnackedMessages()
                && getMaxUnackedMessages() > 0) {
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

    public CompletableFuture<Void> doUnsubscribe(final long requestId) {
       return subscription.doUnsubscribe(this).thenAccept(v -> {
            log.info("Unsubscribed successfully from {}", subscription);
            cnx.removedConsumer(this);
            cnx.getCommandSender().sendSuccessResponse(requestId);
        }).exceptionally(exception -> {
            log.warn("Unsubscribe failed for {}", subscription, exception);
            cnx.getCommandSender().sendErrorResponse(requestId, BrokerServiceException.getClientErrorCode(exception),
                    exception.getCause().getMessage());
            return null;
        });
    }

    public CompletableFuture<Void> messageAcked(CommandAck ack) {
        CompletableFuture<Long> future;

        this.lastAckedTimestamp = System.currentTimeMillis();
        Map<String, Long> properties = Collections.emptyMap();
        if (ack.getPropertiesCount() > 0) {
            properties = ack.getPropertiesList().stream()
                .collect(Collectors.toMap(KeyLongValue::getKey, KeyLongValue::getValue));
        }

        if (ack.getAckType() == AckType.Cumulative) {
            if (ack.getMessageIdsCount() != 1) {
                log.warn("[{}] [{}] Received multi-message ack", subscription, consumerId);
                return CompletableFuture.completedFuture(null);
            }

            if (Subscription.isIndividualAckMode(subType)) {
                log.warn("[{}] [{}] Received cumulative ack on shared subscription, ignoring",
                        subscription, consumerId);
                return CompletableFuture.completedFuture(null);
            }

            PositionImpl position;
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

            if (ack.hasTxnidMostBits() && ack.hasTxnidLeastBits()) {
                List<PositionImpl> positionsAcked = Collections.singletonList(position);
                future = transactionCumulativeAcknowledge(ack.getTxnidMostBits(),
                        ack.getTxnidLeastBits(), positionsAcked)
                        .thenApply(unused -> 1L);
            } else {
                List<Position> positionsAcked = Collections.singletonList(position);
                subscription.acknowledgeMessage(positionsAcked, AckType.Cumulative, properties);
                future = CompletableFuture.completedFuture(1L);
            }
        } else {
            if (ack.hasTxnidLeastBits() && ack.hasTxnidMostBits()) {
                future = individualAckWithTransaction(ack);
            } else {
                future = individualAckNormal(ack, properties);
            }
        }

        return future
                .thenApply(v -> {
                    this.messageAckRate.recordEvent(v);
                    return null;
                });
    }

    //this method is for individual ack not carry the transaction
    private CompletableFuture<Long> individualAckNormal(CommandAck ack, Map<String, Long> properties) {
        List<Pair<Consumer, Position>> positionsAcked = new ArrayList<>();
        long totalAckCount = 0;
        for (int i = 0; i < ack.getMessageIdsCount(); i++) {
            MessageIdData msgId = ack.getMessageIdAt(i);
            PositionImpl position;
            Pair<Consumer, Long> ackOwnerConsumerAndBatchSize =
                    getAckOwnerConsumerAndBatchSize(msgId.getLedgerId(), msgId.getEntryId());
            Consumer ackOwnerConsumer = ackOwnerConsumerAndBatchSize.getLeft();
            long ackedCount;
            long batchSize = ackOwnerConsumerAndBatchSize.getRight();
            if (msgId.getAckSetsCount() > 0) {
                long[] ackSets = new long[msgId.getAckSetsCount()];
                for (int j = 0; j < msgId.getAckSetsCount(); j++) {
                    ackSets[j] = msgId.getAckSetAt(j);
                }
                position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId(), ackSets);
                ackedCount = getAckedCountForBatchIndexLevelEnabled(position, batchSize, ackSets, ackOwnerConsumer);
                if (isTransactionEnabled()) {
                    //sync the batch position bit set point, in order to delete the position in pending acks
                    if (Subscription.isIndividualAckMode(subType)) {
                        ((PersistentSubscription) subscription)
                                .syncBatchPositionBitSetForPendingAck(position);
                    }
                }
                addAndGetUnAckedMsgs(ackOwnerConsumer, -(int) ackedCount);
            } else {
                position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId());
                ackedCount = getAckedCountForMsgIdNoAckSets(batchSize, position, ackOwnerConsumer);
                if (checkCanRemovePendingAcksAndHandle(ackOwnerConsumer, position, msgId)) {
                    addAndGetUnAckedMsgs(ackOwnerConsumer, -(int) ackedCount);
                    updateBlockedConsumerOnUnackedMsgs(ackOwnerConsumer);
                }
            }

            positionsAcked.add(Pair.of(ackOwnerConsumer, position));

            checkAckValidationError(ack, position);

            totalAckCount += ackedCount;
        }
        subscription.acknowledgeMessage(positionsAcked.stream()
                .map(Pair::getRight)
                .collect(Collectors.toList()), AckType.Individual, properties);
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.complete(totalAckCount);
        if (isTransactionEnabled() && Subscription.isIndividualAckMode(subType)) {
            completableFuture.whenComplete((v, e) -> positionsAcked.forEach(positionPair -> {
                Consumer ackOwnerConsumer = positionPair.getLeft();
                Position position = positionPair.getRight();
                //check if the position can remove from the consumer pending acks.
                // the bit set is empty in pending ack handle.
                if (((PositionImpl) position).getAckSet() != null) {
                    if (((PersistentSubscription) subscription)
                            .checkIsCanDeleteConsumerPendingAck((PositionImpl) position)) {
                        removePendingAcks(ackOwnerConsumer, (PositionImpl) position);
                    }
                }
            }));
        }
        return completableFuture;
    }


    //this method is for individual ack carry the transaction
    private CompletableFuture<Long> individualAckWithTransaction(CommandAck ack) {
        // Individual ack
        List<Pair<Consumer, MutablePair<PositionImpl, Integer>>> positionsAcked = new ArrayList<>();
        if (!isTransactionEnabled()) {
            return FutureUtil.failedFuture(
                    new BrokerServiceException.NotAllowedException("Server don't support transaction ack!"));
        }

        LongAdder totalAckCount = new LongAdder();
        for (int i = 0; i < ack.getMessageIdsCount(); i++) {
            MessageIdData msgId = ack.getMessageIdAt(i);
            PositionImpl position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId());
            Consumer ackOwnerConsumer = getAckOwnerConsumerAndBatchSize(msgId.getLedgerId(),
                    msgId.getEntryId()).getLeft();
            // acked count at least one
            long ackedCount;
            long batchSize;
            if (msgId.hasBatchSize()) {
                batchSize = msgId.getBatchSize();
                // ack batch messages set ackeCount = batchSize
                ackedCount = msgId.getBatchSize();
                positionsAcked.add(Pair.of(ackOwnerConsumer, new MutablePair<>(position, msgId.getBatchSize())));
            } else {
                // ack no batch message set ackedCount = 1
                batchSize = 0;
                ackedCount = 1;
                positionsAcked.add(Pair.of(ackOwnerConsumer, new MutablePair<>(position, (int) batchSize)));
            }

            if (msgId.getAckSetsCount() > 0) {
                long[] ackSets = new long[msgId.getAckSetsCount()];
                for (int j = 0; j < msgId.getAckSetsCount(); j++) {
                    ackSets[j] = msgId.getAckSetAt(j);
                }
                position.setAckSet(ackSets);
                ackedCount = getAckedCountForTransactionAck(batchSize, ackSets);
            }

            addAndGetUnAckedMsgs(ackOwnerConsumer, -(int) ackedCount);

            checkCanRemovePendingAcksAndHandle(ackOwnerConsumer, position, msgId);

            checkAckValidationError(ack, position);

            totalAckCount.add(ackedCount);
        }

        CompletableFuture<Void> completableFuture = transactionIndividualAcknowledge(ack.getTxnidMostBits(),
                ack.getTxnidLeastBits(), positionsAcked.stream().map(Pair::getRight).collect(Collectors.toList()));
        if (Subscription.isIndividualAckMode(subType)) {
            completableFuture.whenComplete((v, e) ->
                    positionsAcked.forEach(positionPair -> {
                        Consumer ackOwnerConsumer = positionPair.getLeft();
                        MutablePair<PositionImpl, Integer> positionLongMutablePair = positionPair.getRight();
                        if (positionLongMutablePair.getLeft().getAckSet() != null) {
                            if (((PersistentSubscription) subscription)
                                    .checkIsCanDeleteConsumerPendingAck(positionLongMutablePair.left)) {
                                removePendingAcks(ackOwnerConsumer, positionLongMutablePair.left);
                            }
                        }
                    }));
        }
        return completableFuture.thenApply(__ -> totalAckCount.sum());
    }

    private long getAckedCountForMsgIdNoAckSets(long batchSize, PositionImpl position, Consumer consumer) {
        if (isAcknowledgmentAtBatchIndexLevelEnabled && Subscription.isIndividualAckMode(subType)) {
            long[] cursorAckSet = getCursorAckSet(position);
            if (cursorAckSet != null) {
                return getAckedCountForBatchIndexLevelEnabled(position, batchSize, EMPTY_ACK_SET, consumer);
            }
        }
        return batchSize;
    }

    private long getAckedCountForBatchIndexLevelEnabled(PositionImpl position, long batchSize, long[] ackSets,
                                                        Consumer consumer) {
        long ackedCount = 0;
        if (isAcknowledgmentAtBatchIndexLevelEnabled && Subscription.isIndividualAckMode(subType)
            && consumer.getPendingAcks().get(position.getLedgerId(), position.getEntryId()) != null) {
            long[] cursorAckSet = getCursorAckSet(position);
            if (cursorAckSet != null) {
                BitSetRecyclable cursorBitSet = BitSetRecyclable.create().resetWords(cursorAckSet);
                int lastCardinality = cursorBitSet.cardinality();
                BitSetRecyclable givenBitSet = BitSetRecyclable.create().resetWords(ackSets);
                cursorBitSet.and(givenBitSet);
                givenBitSet.recycle();
                int currentCardinality = cursorBitSet.cardinality();
                ackedCount = lastCardinality - currentCardinality;
                cursorBitSet.recycle();
            } else {
                ackedCount = batchSize - BitSet.valueOf(ackSets).cardinality();
            }
        }
        return ackedCount;
    }

    private long getAckedCountForTransactionAck(long batchSize, long[] ackSets) {
        BitSetRecyclable bitset = BitSetRecyclable.create().resetWords(ackSets);
        long ackedCount = batchSize - bitset.cardinality();
        bitset.recycle();
        return ackedCount;
    }

    private long getUnAckedCountForBatchIndexLevelEnabled(PositionImpl position, long batchSize) {
        long unAckedCount = batchSize;
        if (isAcknowledgmentAtBatchIndexLevelEnabled) {
            long[] cursorAckSet = getCursorAckSet(position);
            if (cursorAckSet != null) {
                BitSetRecyclable cursorBitSet = BitSetRecyclable.create().resetWords(cursorAckSet);
                unAckedCount = cursorBitSet.cardinality();
                cursorBitSet.recycle();
            }
        }
        return unAckedCount;
    }

    private void checkAckValidationError(CommandAck ack, PositionImpl position) {
        if (ack.hasValidationError()) {
            log.warn("[{}] [{}] Received ack for corrupted message at {} - Reason: {}", subscription,
                    consumerId, position, ack.getValidationError());
        }
    }

    private boolean checkCanRemovePendingAcksAndHandle(Consumer ackOwnedConsumer,
                                                       PositionImpl position, MessageIdData msgId) {
        if (Subscription.isIndividualAckMode(subType) && msgId.getAckSetsCount() == 0) {
            return removePendingAcks(ackOwnedConsumer, position);
        }
        return false;
    }

    /**
     * Retrieves the acknowledgment owner consumer and batch size for the specified ledgerId and entryId.
     *
     * @param ledgerId The ID of the ledger.
     * @param entryId The ID of the entry.
     * @return Pair<Consumer, BatchSize>
     */
    private Pair<Consumer, Long> getAckOwnerConsumerAndBatchSize(long ledgerId, long entryId) {
        if (Subscription.isIndividualAckMode(subType)) {
            LongPair longPair = getPendingAcks().get(ledgerId, entryId);
            if (longPair != null) {
                return Pair.of(this, longPair.first);
            } else {
                // If there are more consumers, this step will consume more CPU, and it should be optimized later.
                for (Consumer consumer : subscription.getConsumers()) {
                    if (consumer != this) {
                        longPair = consumer.getPendingAcks().get(ledgerId, entryId);
                        if (longPair != null) {
                            return Pair.of(consumer, longPair.first);
                        }
                    }
                }
            }
        }
        return Pair.of(this, 1L);
    }

    private long[] getCursorAckSet(PositionImpl position) {
        if (!(subscription instanceof PersistentSubscription)) {
            return null;
        }
        return (((PersistentSubscription) subscription).getCursor()).getDeletedBatchIndexesAsLongArray(position);
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
        this.lastConsumedFlowTimestamp = System.currentTimeMillis();

        // block shared consumer when unacked-messages reaches limit
        if (shouldBlockConsumerOnUnackMsgs() && unackedMessages >= getMaxUnackedMessages()) {
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

    /**
     * return 0 if there is no entry dispatched yet.
     */
    public int getAvgMessagesPerEntry() {
        return (int) Math.round(avgMessagesPerEntry.get());
    }

    public boolean isBlocked() {
        return blockedConsumerOnUnackedMsgs;
    }

    public void reachedEndOfTopic() {
        cnx.getCommandSender().sendReachedEndOfTopic(consumerId);
    }

    public void topicMigrated(Optional<ClusterUrl> clusterUrl) {
        if (clusterUrl.isPresent()) {
            ClusterUrl url = clusterUrl.get();
            cnx.getCommandSender().sendTopicMigrated(ResourceType.Consumer, consumerId, url.getBrokerServiceUrl(),
                    url.getBrokerServiceUrlTls());
            // disconnect consumer after sending migrated cluster url
            disconnect();
        }
    }

    /**
     * Checks if consumer-blocking on unAckedMessages is allowed for below conditions:<br/>
     * a. consumer must have Shared-subscription<br/>
     * b. {@link this#getMaxUnackedMessages()} value > 0
     *
     * @return
     */
    private boolean shouldBlockConsumerOnUnackMsgs() {
        return Subscription.isIndividualAckMode(subType) && getMaxUnackedMessages() > 0;
    }

    public void updateRates() {
        msgOut.calculateRate();
        chunkedMessageRate.calculateRate();
        msgRedeliver.calculateRate();
        messageAckRate.calculateRate();

        stats.msgRateOut = msgOut.getRate();
        stats.msgThroughputOut = msgOut.getValueRate();
        stats.msgRateRedeliver = msgRedeliver.getRate();
        stats.messageAckRate = messageAckRate.getValueRate();
        stats.chunkedMessageRate = chunkedMessageRate.getRate();
    }

    public void updateStats(ConsumerStatsImpl consumerStats) {
        msgOutCounter.add(consumerStats.msgOutCounter);
        bytesOutCounter.add(consumerStats.bytesOutCounter);
        msgOut.recordMultipleEvents(consumerStats.msgOutCounter, consumerStats.bytesOutCounter);
        lastAckedTimestamp = consumerStats.lastAckedTimestamp;
        lastConsumedTimestamp = consumerStats.lastConsumedTimestamp;
        lastConsumedFlowTimestamp = consumerStats.lastConsumedFlowTimestamp;
        MESSAGE_PERMITS_UPDATER.set(this, consumerStats.availablePermits);
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] Setting broker.service.Consumer's messagePermits to {} for consumer {}", topicName,
                    subscription, consumerStats.availablePermits, consumerId);
        }
        unackedMessages = consumerStats.unackedMessages;
        blockedConsumerOnUnackedMsgs = consumerStats.blockedConsumerOnUnackedMsgs;
        avgMessagesPerEntry.set(consumerStats.avgMessagesPerEntry);
    }

    public ConsumerStatsImpl getStats() {
        stats.msgOutCounter = msgOutCounter.longValue();
        stats.bytesOutCounter = bytesOutCounter.longValue();
        stats.lastAckedTimestamp = lastAckedTimestamp;
        stats.lastConsumedTimestamp = lastConsumedTimestamp;
        stats.lastConsumedFlowTimestamp = lastConsumedFlowTimestamp;
        stats.availablePermits = getAvailablePermits();
        stats.unackedMessages = unackedMessages;
        stats.blockedConsumerOnUnackedMsgs = blockedConsumerOnUnackedMsgs;
        stats.avgMessagesPerEntry = getAvgMessagesPerEntry();
        stats.consumerName = consumerName;
        if (readPositionWhenJoining != null) {
            stats.readPositionWhenJoining = readPositionWhenJoining.toString();
        }
        return stats;
    }

    public long getMsgOutCounter() {
        return msgOutCounter.longValue();
    }

    public long getBytesOutCounter() {
        return bytesOutCounter.longValue();
    }

    public int getUnackedMessages() {
        return unackedMessages;
    }

    public KeySharedMeta getKeySharedMeta() {
        return keySharedMeta;
    }

    @Override
    public String toString() {
        if (subscription != null && cnx != null) {
            return MoreObjects.toStringHelper(this).add("subscription", subscription).add("consumerId", consumerId)
                    .add("consumerName", consumerName).add("address", this.cnx.toString()).toString();
        } else {
            return MoreObjects.toStringHelper(this).add("consumerId", consumerId)
                    .add("consumerName", consumerName).toString();
        }
    }

    public CompletableFuture<Void> checkPermissionsAsync() {
        TopicName topicName = TopicName.get(subscription.getTopicName());
        if (cnx.getBrokerService().getAuthorizationService() != null) {
            AuthenticationDataSubscription authData =
                    new AuthenticationDataSubscription(cnx.getAuthenticationData(), subscription.getName());
            return cnx.getBrokerService().getAuthorizationService()
                    .allowTopicOperationAsync(topicName, TopicOperation.CONSUME, appId, authData)
                    .handle((ok, e) -> {
                        if (e != null) {
                            log.warn("[{}] Get unexpected error while authorizing [{}]  {}", appId,
                                    subscription.getTopicName(), e.getMessage(), e);
                        }

                        if (ok == null || !ok) {
                            log.info("[{}] is not allowed to consume from topic [{}] anymore", appId,
                                    subscription.getTopicName());
                            disconnect();
                        }
                        return null;
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)  {
            return true;
        }
        if (obj instanceof Consumer) {
            Consumer other = (Consumer) obj;
            return consumerId == other.consumerId && Objects.equals(cnx.clientAddress(), other.cnx.clientAddress());
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
    private boolean removePendingAcks(Consumer ackOwnedConsumer, PositionImpl position) {
        if (!ackOwnedConsumer.getPendingAcks().remove(position.getLedgerId(), position.getEntryId())) {
            // Message was already removed by the other consumer
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}-{}] consumer {} received ack {}", topicName, subscription, consumerId, position);
        }
        updateBlockedConsumerOnUnackedMsgs(ackOwnedConsumer);
        return true;
    }

    public void updateBlockedConsumerOnUnackedMsgs(Consumer ackOwnedConsumer) {
        // unblock consumer-throttling when limit check is disabled or receives half of maxUnackedMessages =>
        // consumer can start again consuming messages
        int unAckedMsgs = UNACKED_MESSAGES_UPDATER.get(ackOwnedConsumer);
        if ((((unAckedMsgs <= getMaxUnackedMessages() / 2) && ackOwnedConsumer.blockedConsumerOnUnackedMsgs)
                && ackOwnedConsumer.shouldBlockConsumerOnUnackMsgs())
                || !shouldBlockConsumerOnUnackMsgs()) {
            ackOwnedConsumer.blockedConsumerOnUnackedMsgs = false;
            flowConsumerBlockedPermits(ackOwnedConsumer);
        }
    }

    public ConcurrentLongLongPairHashMap getPendingAcks() {
        return pendingAcks;
    }

    public int getPriorityLevel() {
        return priorityLevel;
    }

    public void redeliverUnacknowledgedMessages(long consumerEpoch) {
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
                int unAckedCount = (int) getUnAckedCountForBatchIndexLevelEnabled(PositionImpl.get(ledgerId, entryId),
                        batchSize);
                totalRedeliveryMessages.add(unAckedCount);
                pendingPositions.add(new PositionImpl(ledgerId, entryId));
            });

            for (PositionImpl p : pendingPositions) {
                pendingAcks.remove(p.getLedgerId(), p.getEntryId());
            }

            msgRedeliver.recordMultipleEvents(totalRedeliveryMessages.intValue(), totalRedeliveryMessages.intValue());
            subscription.redeliverUnacknowledgedMessages(this, pendingPositions);
        } else {
            subscription.redeliverUnacknowledgedMessages(this, consumerEpoch);
        }

        flowConsumerBlockedPermits(this);
    }

    public void redeliverUnacknowledgedMessages(List<MessageIdData> messageIds) {
        int totalRedeliveryMessages = 0;
        List<PositionImpl> pendingPositions = new ArrayList<>();
        for (MessageIdData msg : messageIds) {
            PositionImpl position = PositionImpl.get(msg.getLedgerId(), msg.getEntryId());
            LongPair longPair = pendingAcks.get(position.getLedgerId(), position.getEntryId());
            if (longPair != null) {
                int unAckedCount = (int) getUnAckedCountForBatchIndexLevelEnabled(position, longPair.first);
                pendingAcks.remove(position.getLedgerId(), position.getEntryId());
                totalRedeliveryMessages += unAckedCount;
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
        int unackedMsgs = 0;
        if (isPersistentTopic && Subscription.isIndividualAckMode(subType)) {
            subscription.addUnAckedMessages(ackedMessages);
            unackedMsgs = UNACKED_MESSAGES_UPDATER.addAndGet(consumer, ackedMessages);
        }
        if (unackedMsgs < 0 && System.currentTimeMillis() - negativeUnackedMsgsTimestamp >= 10_000) {
            negativeUnackedMsgsTimestamp = System.currentTimeMillis();
            log.warn("unackedMsgs is : {}, ackedMessages : {}, consumer : {}", unackedMsgs, ackedMessages, consumer);
        }
        return unackedMsgs;
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
        //Unacked messages check is disabled for non-durable subscriptions.
        if (isDurable && subscription != null) {
            return subscription.getTopic().getHierarchyTopicPolicies().getMaxUnackedMessagesOnConsumer().get();
        } else {
            return 0;
        }
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
        final byte[] stickyKey;
        if (entry instanceof EntryAndMetadata) {
            stickyKey = ((EntryAndMetadata) entry).getStickyKey();
        } else {
            stickyKey = Commands.peekStickyKey(entry.getDataBuffer(), topicName, subscription.getName());
        }
        return StickyKeyConsumerSelector.makeStickyKeyHash(stickyKey);
    }

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);
}
