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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionEntryImpl;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Consumer is a consumer currently connected and associated with a Subscription
 */
public class Consumer {
    private final Subscription subscription;
    private final SubType subType;
    private final ServerCnx cnx;
    private final String appId;
    private AuthenticationDataSource authenticationData;
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
    private Rate chuckedMessageRate;

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

    private final ConsumerStats stats;

    private final int maxUnackedMessages;
    private static final AtomicIntegerFieldUpdater<Consumer> UNACKED_MESSAGES_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Consumer.class, "unackedMessages");
    private volatile int unackedMessages = 0;
    private volatile boolean blockedConsumerOnUnackedMsgs = false;

    private final Map<String, String> metadata;

    private final PulsarApi.KeySharedMeta keySharedMeta;

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

    public Consumer(Subscription subscription, SubType subType, String topicName, long consumerId,
                    int priorityLevel, String consumerName,
                    int maxUnackedMessages, ServerCnx cnx, String appId,
                    Map<String, String> metadata, boolean readCompacted, InitialPosition subscriptionInitialPosition,
                    PulsarApi.KeySharedMeta keySharedMeta) throws BrokerServiceException {

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
        this.chuckedMessageRate = new Rate();
        this.msgRedeliver = new Rate();
        this.bytesOutCounter = new LongAdder();
        this.msgOutCounter = new LongAdder();
        this.appId = appId;
        this.authenticationData = cnx.getAuthenticationData();
        this.preciseDispatcherFlowControl = cnx.isPreciseDispatcherFlowControl();

        PERMITS_RECEIVED_WHILE_CONSUMER_BLOCKED_UPDATER.set(this, 0);
        MESSAGE_PERMITS_UPDATER.set(this, 0);
        UNACKED_MESSAGES_UPDATER.set(this, 0);
        AVG_MESSAGES_PER_ENTRY.set(this, 1000);

        this.metadata = metadata != null ? metadata : Collections.emptyMap();

        stats = new ConsumerStats();
        stats.setAddress(cnx.clientAddress().toString());
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
        if (!Commands.peerSupportsActiveConsumerListener(cnx.getRemoteEndpointProtocolVersion())) {
            // if the client is older than `v12`, we don't need to send consumer group changes.
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("notify consumer {} - that [{}] for subscription {} has new active consumer : {}",
                consumerId, topicName, subscription.getName(), activeConsumer);
        }
        cnx.ctx().writeAndFlush(
            Commands.newActiveConsumerChange(consumerId, this == activeConsumer),
            cnx.ctx().voidPromise());
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

    public ChannelPromise sendMessages(final List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
               int totalMessages, long totalBytes, long totalChunkedMessages, RedeliveryTracker redeliveryTracker) {
        this.lastConsumedTimestamp = System.currentTimeMillis();
        final ChannelHandlerContext ctx = cnx.ctx();
        final ChannelPromise writePromise = ctx.newPromise();

        if (entries.isEmpty() || totalMessages == 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] List of messages is empty, triggering write future immediately for consumerId {}",
                        topicName, subscription, consumerId);
            }
            writePromise.setSuccess();
            batchSizes.recyle();
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
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
                    pendingAcks.put(entry.getLedgerId(), entry.getEntryId(), batchSize, 0);
                }
            }
        }

        // calculate avg message per entry
        int tmpAvgMessagesPerEntry = AVG_MESSAGES_PER_ENTRY.get(this);
        tmpAvgMessagesPerEntry = (int) Math.round(tmpAvgMessagesPerEntry * avgPercent +
                    (1 - avgPercent) * totalMessages / entries.size());
        AVG_MESSAGES_PER_ENTRY.set(this, tmpAvgMessagesPerEntry);

        // reduce permit and increment unackedMsg count with total number of messages in batch-msgs
        int ackedCount = batchIndexesAcks == null ? 0 : batchIndexesAcks.getTotalAckedIndexCount();
        MESSAGE_PERMITS_UPDATER.addAndGet(this, ackedCount - totalMessages);
        incrementUnackedMessages(totalMessages);
        msgOut.recordMultipleEvents(totalMessages, totalBytes);
        msgOutCounter.add(totalMessages);
        bytesOutCounter.add(totalBytes);
        chuckedMessageRate.recordMultipleEvents(totalChunkedMessages, 0);

        ctx.channel().eventLoop().execute(() -> {
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                if (entry == null) {
                    // Entry was filtered out
                    continue;
                }

                int batchSize = batchSizes.getBatchSize(i);

                if (batchSize > 1 && !cnx.isBatchMessageCompatibleVersion()) {
                    log.warn("[{}-{}] Consumer doesn't support batch messages -  consumerId {}, msg id {}-{}",
                            topicName, subscription,
                            consumerId, entry.getLedgerId(), entry.getEntryId());
                    ctx.close();
                    entry.release();
                    continue;
                }

                MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
                if (entry instanceof TransactionEntryImpl) {
                    messageIdBuilder.setBatchIndex(((TransactionEntryImpl) entry).getStartBatchIndex());
                }
                MessageIdData messageId = messageIdBuilder
                    .setLedgerId(entry.getLedgerId())
                    .setEntryId(entry.getEntryId())
                    .setPartition(partitionIdx)
                    .build();

                ByteBuf metadataAndPayload = entry.getDataBuffer();
                // increment ref-count of data and release at the end of process: so, we can get chance to call entry.release
                metadataAndPayload.retain();
                // skip checksum by incrementing reader-index if consumer-client doesn't support checksum verification
                if (cnx.getRemoteEndpointProtocolVersion() < ProtocolVersion.v11.getNumber()) {
                    Commands.skipChecksumIfPresent(metadataAndPayload);
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}-{}] Sending message to consumerId {}, msg id {}-{}", topicName, subscription,
                            consumerId, entry.getLedgerId(), entry.getEntryId());
                }

                int redeliveryCount = 0;
                PositionImpl position = PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId());
                if (redeliveryTracker.contains(position)) {
                    redeliveryCount = redeliveryTracker.incrementAndGetRedeliveryCount(position);
                }
                ctx.write(Commands.newMessage(consumerId, messageId, redeliveryCount, metadataAndPayload,
                    batchIndexesAcks == null ? null : batchIndexesAcks.getAckSet(i)), ctx.voidPromise());
                messageId.recycle();
                messageIdBuilder.recycle();
                entry.release();
            }

            // Use an empty write here so that we can just tie the flush with the write promise for last entry
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER, writePromise);
            batchSizes.recyle();
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
        });

        return writePromise;
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

    public void sendError(ByteBuf error) {
        cnx.ctx().writeAndFlush(error).addListener(ChannelFutureListener.CLOSE);
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

    void doUnsubscribe(final long requestId) {
        final ChannelHandlerContext ctx = cnx.ctx();

        subscription.doUnsubscribe(this).thenAccept(v -> {
            log.info("Unsubscribed successfully from {}", subscription);
            cnx.removedConsumer(this);
            ctx.writeAndFlush(Commands.newSuccess(requestId));
        }).exceptionally(exception -> {
            log.warn("Unsubscribe failed for {}", subscription, exception);
            ctx.writeAndFlush(
                    Commands.newError(requestId, BrokerServiceException.getClientErrorCode(exception),
                            exception.getCause().getMessage()));
            return null;
        });
    }

    void messageAcked(CommandAck ack) {
        this.lastAckedTimestamp = System.currentTimeMillis();
        Map<String,Long> properties = Collections.emptyMap();
        if (ack.getPropertiesCount() > 0) {
            properties = ack.getPropertiesList().stream()
                .collect(Collectors.toMap((e) -> e.getKey(),
                                          (e) -> e.getValue()));
        }

        if (ack.getAckType() == AckType.Cumulative) {
            if (ack.getMessageIdCount() != 1) {
                log.warn("[{}] [{}] Received multi-message ack", subscription, consumerId);
                return;
            }

            if (Subscription.isIndividualAckMode(subType)) {
                log.warn("[{}] [{}] Received cumulative ack on shared subscription, ignoring", subscription, consumerId);
                return;
            }
            PositionImpl position = PositionImpl.earliest;
            if (ack.getMessageIdCount() == 1) {
                MessageIdData msgId = ack.getMessageId(0);
                if (msgId.getAckSetCount() > 0) {
                    position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId(), SafeCollectionUtils.longListToArray(msgId.getAckSetList()));
                } else {
                    position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId());
                }
            }
            List<Position> positionsAcked = Collections.singletonList(position);
            if (ack.hasTxnidMostBits() && ack.hasTxnidLeastBits()) {
                transactionAcknowledge(ack.getTxnidMostBits(), ack.getTxnidLeastBits(), positionsAcked, AckType.Cumulative);
            } else {
                subscription.acknowledgeMessage(positionsAcked, AckType.Cumulative, properties);
            }
        } else {
            // Individual ack
            List<Position> positionsAcked = new ArrayList<>();
            for (int i = 0; i < ack.getMessageIdCount(); i++) {
                MessageIdData msgId = ack.getMessageId(i);
                PositionImpl position;
                if (msgId.getAckSetCount() > 0) {
                    position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId(), SafeCollectionUtils.longListToArray(msgId.getAckSetList()));
                } else {
                    position = PositionImpl.get(msgId.getLedgerId(), msgId.getEntryId());
                }
                positionsAcked.add(position);

                if (Subscription.isIndividualAckMode(subType) && msgId.getAckSetCount() == 0) {
                    removePendingAcks(position);
                }

                if (ack.hasValidationError()) {
                    log.error("[{}] [{}] Received ack for corrupted message at {} - Reason: {}", subscription,
                            consumerId, position, ack.getValidationError());
                }
            }
            if (ack.hasTxnidMostBits() && ack.hasTxnidLeastBits()) {
                transactionAcknowledge(ack.getTxnidMostBits(), ack.getTxnidLeastBits(), positionsAcked, AckType.Individual);
            } else {
                subscription.acknowledgeMessage(positionsAcked, AckType.Individual, properties);
            }
        }
    }

    private void transactionAcknowledge(long txnidMostBits, long txnidLeastBits,
                                        List<Position> positionList, AckType ackType) {
        if (subscription instanceof PersistentSubscription) {
            TxnID txnID = new TxnID(txnidMostBits, txnidLeastBits);
            try {
                ((PersistentSubscription) subscription).acknowledgeMessage(txnID, positionList, ackType);
            } catch (TransactionConflictException e) {
                log.error("Transaction acknowledge failed for txn " + txnID, e);
            }
        } else {
            log.error("Transaction acknowledge only support the `PersistentSubscription`.");
        }
    }

    void flowPermits(int additionalNumberOfMessages) {
        checkArgument(additionalNumberOfMessages > 0);

        // block shared consumer when unacked-messages reaches limit
        if (shouldBlockConsumerOnUnackMsgs() && unackedMessages >= maxUnackedMessages) {
            blockedConsumerOnUnackedMsgs = true;
        }
        int oldPermits;
        if (!blockedConsumerOnUnackedMsgs) {
            oldPermits = MESSAGE_PERMITS_UPDATER.getAndAdd(this, additionalNumberOfMessages);
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
        // Only send notification if the client understand the command
        if (cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v9_VALUE) {
            log.info("[{}] Notifying consumer that end of topic has been reached", this);
            cnx.ctx().writeAndFlush(Commands.newReachedEndOfTopic(consumerId));
        }
    }

    /**
     * Checks if consumer-blocking on unAckedMessages is allowed for below conditions:<br/>
     * a. consumer must have Shared-subscription<br/>
     * b. {@link maxUnackedMessages} value > 0
     *
     * @return
     */
    private boolean shouldBlockConsumerOnUnackMsgs() {
        return Subscription.isIndividualAckMode(subType) && maxUnackedMessages > 0;
    }

    public void updateRates() {
        msgOut.calculateRate();
        chuckedMessageRate.calculateRate();
        msgRedeliver.calculateRate();
        stats.msgRateOut = msgOut.getRate();
        stats.msgThroughputOut = msgOut.getValueRate();
        stats.msgRateRedeliver = msgRedeliver.getRate();
        stats.chuckedMessageRate = chuckedMessageRate.getRate();
    }

    public ConsumerStats getStats() {
        stats.msgOutCounter = msgOutCounter.longValue();
        stats.bytesOutCounter = bytesOutCounter.longValue();
        stats.lastAckedTimestamp = lastAckedTimestamp;
        stats.lastConsumedTimestamp = lastConsumedTimestamp;
        stats.availablePermits = getAvailablePermits();
        stats.unackedMessages = unackedMessages;
        stats.blockedConsumerOnUnackedMsgs = blockedConsumerOnUnackedMsgs;
        stats.avgMessagesPerEntry = getAvgMessagesPerEntry();
        return stats;
    }

    public int getUnackedMessages() {
        return unackedMessages;
    }

    public PulsarApi.KeySharedMeta getKeySharedMeta() {
        return keySharedMeta;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("subscription", subscription).add("consumerId", consumerId)
                .add("consumerName", consumerName).add("address", this.cnx.clientAddress()).toString();
    }

    public ChannelHandlerContext ctx() {
        return cnx.ctx();
    }

    public void checkPermissions() {
        TopicName topicName = TopicName.get(subscription.getTopicName());
        if (cnx.getBrokerService().getAuthorizationService() != null) {
            try {
                if (cnx.getBrokerService().getAuthorizationService().canConsume(topicName, appId, authenticationData,
                        subscription.getName())) {
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
                if (!consumer.equals(this) && consumer.getPendingAcks().containsKey(position.getLedgerId(), position.getEntryId())) {
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
            // unblock consumer-throttling when receives half of maxUnackedMessages => consumer can start again
            // consuming messages
            if (((addAndGetUnAckedMsgs(ackOwnedConsumer, -totalAckedMsgs) <= (maxUnackedMessages / 2))
                    && ackOwnedConsumer.blockedConsumerOnUnackedMsgs)
                    && ackOwnedConsumer.shouldBlockConsumerOnUnackMsgs()) {
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
            pendingAcks.forEach((ledgerId, entryId, batchSize, none) -> {
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
            LongPair batchSize = pendingAcks.get(position.getLedgerId(), position.getEntryId());
            if (batchSize != null) {
                pendingAcks.remove(position.getLedgerId(), position.getEntryId());
                totalRedeliveryMessages += batchSize.first;
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

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);
}
