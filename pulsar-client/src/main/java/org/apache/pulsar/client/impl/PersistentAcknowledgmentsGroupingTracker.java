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
package org.apache.pulsar.client.impl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.netty.util.Recycler;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentBitSetRecyclable;

/**
 * Group the acknowledgements for a certain time and then sends them out in a single protobuf command.
 */
@Slf4j
public class PersistentAcknowledgmentsGroupingTracker implements AcknowledgmentsGroupingTracker {

    /**
     * When reaching the max group size, an ack command is sent out immediately
     */
    private static final int MAX_ACK_GROUP_SIZE = 1000;

    private final ConsumerImpl<?> consumer;

    private final long acknowledgementGroupTimeMicros;

    private volatile TimedCompletableFuture<Void> currentIndividualAckFuture;
    private volatile TimedCompletableFuture<Void> currentCumulativeAckFuture;

    private volatile LastCumulativeAck lastCumulativeAck =
            LastCumulativeAck.create((MessageIdImpl) MessageIdImpl.earliest, null);

    private volatile boolean cumulativeAckFlushRequired = false;

    // When we flush the command, we should ensure current ack request will send correct
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private static final AtomicReferenceFieldUpdater<PersistentAcknowledgmentsGroupingTracker, LastCumulativeAck> LAST_CUMULATIVE_ACK_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(PersistentAcknowledgmentsGroupingTracker.class, LastCumulativeAck.class, "lastCumulativeAck");

    /**
     * This is a set of all the individual acks that the application has issued and that were not already sent to
     * broker.
     */
    private final ConcurrentSkipListSet<MessageIdImpl> pendingIndividualAcks;
    private final ConcurrentHashMap<MessageIdImpl, ConcurrentBitSetRecyclable> pendingIndividualBatchIndexAcks;

    private final ScheduledFuture<?> scheduledTask;
    private final boolean batchIndexAckEnabled;
    private final boolean ackReceiptEnabled;

    public PersistentAcknowledgmentsGroupingTracker(ConsumerImpl<?> consumer, ConsumerConfigurationData<?> conf,
                                                    EventLoopGroup eventLoopGroup) {
        this.consumer = consumer;
        this.pendingIndividualAcks = new ConcurrentSkipListSet<>();
        this.pendingIndividualBatchIndexAcks = new ConcurrentHashMap<>();
        this.acknowledgementGroupTimeMicros = conf.getAcknowledgementsGroupTimeMicros();
        this.batchIndexAckEnabled = conf.isBatchIndexAckEnabled();
        this.ackReceiptEnabled = conf.isAckReceiptEnabled();
        this.currentIndividualAckFuture = new TimedCompletableFuture<>();
        this.currentCumulativeAckFuture = new TimedCompletableFuture<>();

        if (acknowledgementGroupTimeMicros > 0) {
            scheduledTask = eventLoopGroup.next().scheduleWithFixedDelay(this::flush, acknowledgementGroupTimeMicros,
                    acknowledgementGroupTimeMicros, TimeUnit.MICROSECONDS);
        } else {
            scheduledTask = null;
        }
    }

    /**
     * Since the ack are delayed, we need to do some best-effort duplicate check to discard messages that are being
     * resent after a disconnection and for which the user has already sent an acknowledgement.
     */
    @Override
    public boolean isDuplicate(@NonNull MessageId messageId) {
        final MessageId messageIdOfLastAck = lastCumulativeAck.messageId;
        if (messageIdOfLastAck == null) {
            return false;
        }
        if (messageId.compareTo(messageIdOfLastAck) <= 0) {
            // Already included in a cumulative ack
            return true;
        } else {
            return pendingIndividualAcks.contains(messageId);
        }
    }

    @Override
    public CompletableFuture<Void> addListAcknowledgment(List<MessageId> messageIds,
                                                         AckType ackType, Map<String, Long> properties) {
        if (AckType.Cumulative.equals(ackType)) {
            if (isAckReceiptEnabled(consumer.getClientCnx())) {
                Set<CompletableFuture<Void>> completableFutureSet = new HashSet<>();
                messageIds.forEach(messageId ->
                        completableFutureSet.add(addAcknowledgment((MessageIdImpl) messageId, ackType, properties)));
                return FutureUtil.waitForAll(new ArrayList<>(completableFutureSet));
            } else {
                messageIds.forEach(messageId -> addAcknowledgment((MessageIdImpl) messageId, ackType, properties));
                return CompletableFuture.completedFuture(null);
            }
        } else {
            if (isAckReceiptEnabled(consumer.getClientCnx())) {
                // when flush the ack, we should bind the this ack in the currentFuture, during this time we can't
                // change currentFuture. but we can lock by the read lock, because the currentFuture is not change
                // any ack operation is allowed.
                this.lock.readLock().lock();
                try {
                    if (messageIds.size() != 0) {
                        addListAcknowledgment(messageIds);
                        return this.currentIndividualAckFuture;
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                } finally {
                    this.lock.readLock().unlock();
                    if (acknowledgementGroupTimeMicros == 0 || pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                        flush();
                    }
                }
            } else {
                addListAcknowledgment(messageIds);
                if (acknowledgementGroupTimeMicros == 0 || pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                    flush();
                }
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    private void addListAcknowledgment(List<MessageId> messageIds) {
        for (MessageId messageId : messageIds) {
            consumer.onAcknowledge(messageId, null);
            if (messageId instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
                if (!batchMessageId.ackIndividual()) {
                    doIndividualBatchAckAsync((BatchMessageIdImpl) messageId);
                } else {
                    messageId = modifyBatchMessageIdAndStatesInConsumer(batchMessageId);
                    doIndividualAckAsync((MessageIdImpl) messageId);
                }
            } else {
                modifyMessageIdStatesInConsumer((MessageIdImpl) messageId);
                doIndividualAckAsync((MessageIdImpl) messageId);
            }
        }
    }

    @Override
    public CompletableFuture<Void> addAcknowledgment(MessageIdImpl msgId, AckType ackType,
                                                     Map<String, Long> properties) {
        if (msgId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) msgId;
            if (ackType == AckType.Individual) {
                consumer.onAcknowledge(msgId, null);
                // ack this ack carry bitSet index and judge bit set are all ack
                if (batchMessageId.ackIndividual()) {
                    MessageIdImpl messageId = modifyBatchMessageIdAndStatesInConsumer(batchMessageId);
                    return doIndividualAck(messageId, properties);
                } else if (batchIndexAckEnabled){
                    return doIndividualBatchAck(batchMessageId, properties);
                } else {
                    // if we prevent batchIndexAck, we can't send the ack command to broker when the batch message are
                    // all ack complete
                    return CompletableFuture.completedFuture(null);
                }
            } else {
                consumer.onAcknowledgeCumulative(msgId, null);
                if (batchMessageId.ackCumulative()) {
                    return doCumulativeAck(msgId, properties, null);
                } else {
                    if (batchIndexAckEnabled) {
                        return doCumulativeBatchIndexAck(batchMessageId, properties);
                    } else {
                        // ack the pre messageId, because we prevent the batchIndexAck, we can ensure pre messageId can
                        // ack
                        if (AckType.Cumulative == ackType
                                && !batchMessageId.getAcker().isPrevBatchCumulativelyAcked()) {
                            doCumulativeAck(batchMessageId.prevBatchMessageId(), properties, null);
                            batchMessageId.getAcker().setPrevBatchCumulativelyAcked(true);
                        }
                        return CompletableFuture.completedFuture(null);
                    }
                }
            }
        } else {
            if (ackType == AckType.Individual) {
                consumer.onAcknowledge(msgId, null);
                modifyMessageIdStatesInConsumer(msgId);
                return doIndividualAck(msgId, properties);
            } else {
                consumer.onAcknowledgeCumulative(msgId, null);
                return doCumulativeAck(msgId, properties, null);
            }
        }
    }

    private MessageIdImpl modifyBatchMessageIdAndStatesInConsumer(BatchMessageIdImpl batchMessageId) {
        MessageIdImpl messageId = new MessageIdImpl(batchMessageId.getLedgerId(),
                batchMessageId.getEntryId(), batchMessageId.getPartitionIndex());
        consumer.getStats().incrementNumAcksSent(batchMessageId.getBatchSize());
        clearMessageIdFromUnAckTrackerAndDeadLetter(messageId);
        return messageId;
    }

    private void modifyMessageIdStatesInConsumer(MessageIdImpl messageId) {
        consumer.getStats().incrementNumAcksSent(1);
        clearMessageIdFromUnAckTrackerAndDeadLetter(messageId);
    }

    private void clearMessageIdFromUnAckTrackerAndDeadLetter(MessageIdImpl messageId) {
        consumer.getUnAckedMessageTracker().remove(messageId);
        if (consumer.getPossibleSendToDeadLetterTopicMessages() != null) {
            consumer.getPossibleSendToDeadLetterTopicMessages().remove(messageId);
        }
    }

    private CompletableFuture<Void> doIndividualAck(MessageIdImpl messageId, Map<String, Long> properties) {
        if (acknowledgementGroupTimeMicros == 0 || (properties != null && !properties.isEmpty())) {
            // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
            // uncommon condition since it's only used for the compaction subscription.
            return doImmediateAck(messageId, AckType.Individual, properties, null);
        } else {
            if (isAckReceiptEnabled(consumer.getClientCnx())) {
                // when flush the ack, we should bind the this ack in the currentFuture, during this time we can't
                // change currentFuture. but we can lock by the read lock, because the currentFuture is not change
                // any ack operation is allowed.
                this.lock.readLock().lock();
                try {
                    doIndividualAckAsync(messageId);
                    return this.currentIndividualAckFuture;
                } finally {
                    this.lock.readLock().unlock();
                    if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                        flush();
                    }
                }
            } else {
                doIndividualAckAsync(messageId);
                if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                    flush();
                }
                return CompletableFuture.completedFuture(null);
            }
        }
    }


    private void doIndividualAckAsync(MessageIdImpl messageId) {
        pendingIndividualAcks.add(messageId);
        pendingIndividualBatchIndexAcks.remove(messageId);
    }

    private CompletableFuture<Void> doIndividualBatchAck(BatchMessageIdImpl batchMessageId,
                                                         Map<String, Long> properties) {
        if (acknowledgementGroupTimeMicros == 0 || (properties != null && !properties.isEmpty())) {
            return doImmediateBatchIndexAck(batchMessageId, batchMessageId.getBatchIndex(),
                    batchMessageId.getBatchSize(), AckType.Individual, properties);
        } else {
            return doIndividualBatchAck(batchMessageId);
        }
    }

    private CompletableFuture<Void> doIndividualBatchAck(BatchMessageIdImpl batchMessageId) {
        if (isAckReceiptEnabled(consumer.getClientCnx())) {
            // when flush the ack, we should bind the this ack in the currentFuture, during this time we can't
            // change currentFuture. but we can lock by the read lock, because the currentFuture is not change
            // any ack operation is allowed.
            this.lock.readLock().lock();
            try {
                doIndividualBatchAckAsync(batchMessageId);
                return this.currentIndividualAckFuture;
            } finally {
                this.lock.readLock().unlock();
            }
        } else {
            doIndividualBatchAckAsync(batchMessageId);
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> doCumulativeAck(MessageIdImpl messageId, Map<String, Long> properties,
                                                    BitSetRecyclable bitSet) {
        consumer.getStats().incrementNumAcksSent(consumer.getUnAckedMessageTracker().removeMessagesTill(messageId));
        if (acknowledgementGroupTimeMicros == 0 || (properties != null && !properties.isEmpty())) {
            // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
            // uncommon condition since it's only used for the compaction subscription.
            return doImmediateAck(messageId, AckType.Cumulative, properties, bitSet);
        } else {
            if (isAckReceiptEnabled(consumer.getClientCnx())) {
                // when flush the ack, we should bind the this ack in the currentFuture, during this time we can't
                // change currentFuture. but we can lock by the read lock, because the currentFuture is not change
                // any ack operation is allowed.
                this.lock.readLock().lock();
                try {
                    doCumulativeAckAsync(messageId, bitSet);
                    return this.currentCumulativeAckFuture;
                } finally {
                    this.lock.readLock().unlock();
                    if (pendingIndividualBatchIndexAcks.size() >= MAX_ACK_GROUP_SIZE) {
                        flush();
                    }
                }
            } else {
                doCumulativeAckAsync(messageId, bitSet);
                if (pendingIndividualBatchIndexAcks.size() >= MAX_ACK_GROUP_SIZE) {
                    flush();
                }
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    private void doIndividualBatchAckAsync(BatchMessageIdImpl batchMessageId) {
        ConcurrentBitSetRecyclable bitSet = pendingIndividualBatchIndexAcks.computeIfAbsent(
                new MessageIdImpl(batchMessageId.getLedgerId(), batchMessageId.getEntryId(),
                        batchMessageId.getPartitionIndex()), (v) -> {
                    ConcurrentBitSetRecyclable value;
                    if (batchMessageId.getAcker() != null &&
                            !(batchMessageId.getAcker() instanceof BatchMessageAckerDisabled)) {
                        value = ConcurrentBitSetRecyclable.create(batchMessageId.getAcker().getBitSet());
                    } else {
                        value = ConcurrentBitSetRecyclable.create();
                        value.set(0, batchMessageId.getBatchIndex());
                    }
                    return value;
                });
        bitSet.clear(batchMessageId.getBatchIndex());
    }

    private void doCumulativeAckAsync(MessageIdImpl msgId, BitSetRecyclable bitSet) {
        // Handle concurrent updates from different threads
        LastCumulativeAck currentCumulativeAck = LastCumulativeAck.create(msgId, bitSet);
        while (true) {
            LastCumulativeAck lastCumulativeAck = this.lastCumulativeAck;
            if (msgId.compareTo(lastCumulativeAck.messageId) > 0) {
                if (LAST_CUMULATIVE_ACK_UPDATER.compareAndSet(this, this.lastCumulativeAck, currentCumulativeAck)) {
                    if (lastCumulativeAck.bitSetRecyclable != null) {
                        try {
                            lastCumulativeAck.bitSetRecyclable.recycle();
                        } catch (Exception ignore) {
                            // no-op
                        }
                        lastCumulativeAck.bitSetRecyclable = null;
                    }
                    lastCumulativeAck.recycle();
                    // Successfully updated the last cumulative ack. Next flush iteration will send this to broker.
                    cumulativeAckFlushRequired = true;
                    return;
                }
            } else {
                currentCumulativeAck.recycle();
                // message id acknowledging an before the current last cumulative ack
                return;
            }
        }
    }

    private CompletableFuture<Void> doCumulativeBatchIndexAck(BatchMessageIdImpl batchMessageId,
                                                              Map<String, Long> properties) {
        if (acknowledgementGroupTimeMicros == 0 || (properties != null && !properties.isEmpty())) {
            return doImmediateBatchIndexAck(batchMessageId, batchMessageId.getBatchIndex(),
                    batchMessageId.getBatchSize(), AckType.Cumulative, properties);
        } else {
            BitSetRecyclable bitSet = BitSetRecyclable.create();
            bitSet.set(0, batchMessageId.getBatchSize());
            bitSet.clear(0, batchMessageId.getBatchIndex() + 1);
            return doCumulativeAck(batchMessageId, null, bitSet);
        }
    }

    private CompletableFuture<Void> doImmediateAck(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties,
                                                   BitSetRecyclable bitSet) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .ConnectException("Consumer connect fail! consumer state:" + consumer.getState()));
        }
        return newImmediateAckAndFlush(consumer.consumerId, msgId, bitSet, ackType, properties, cnx);
    }

    private CompletableFuture<Void> doImmediateBatchIndexAck(BatchMessageIdImpl msgId, int batchIndex, int batchSize, AckType ackType,
                                             Map<String, Long> properties) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .ConnectException("Consumer connect fail! consumer state:" + consumer.getState()));
        }
        BitSetRecyclable bitSet;
        if (msgId.getAcker() != null && !(msgId.getAcker() instanceof BatchMessageAckerDisabled)) {
            bitSet = BitSetRecyclable.valueOf(msgId.getAcker().getBitSet().toLongArray());
        } else {
            bitSet = BitSetRecyclable.create();
            bitSet.set(0, batchSize);
        }
        if (ackType == AckType.Cumulative) {
            bitSet.clear(0, batchIndex + 1);
        } else {
            bitSet.clear(batchIndex);
        }

        CompletableFuture<Void> completableFuture = newMessageAckCommandAndWrite(cnx, consumer.consumerId,
                msgId.ledgerId, msgId.entryId, bitSet, ackType, null, properties, true, null, null);
        bitSet.recycle();
        return completableFuture;
    }

    /**
     * Flush all the pending acks and send them to the broker
     */
    @Override
    public void flush() {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cannot flush pending acks since we're not connected to broker", consumer);
            }
            return;
        }

        if (isAckReceiptEnabled(consumer.getClientCnx())) {
            this.lock.writeLock().lock();
            try {
                flushAsync(cnx);
            } finally {
                this.lock.writeLock().unlock();
            }
        } else {
            flushAsync(cnx);
        }
    }

    private void flushAsync(ClientCnx cnx) {
        boolean shouldFlush = false;
        if (cumulativeAckFlushRequired) {
            newMessageAckCommandAndWrite(cnx, consumer.consumerId, lastCumulativeAck.messageId.ledgerId,
                    lastCumulativeAck.messageId.getEntryId(), lastCumulativeAck.bitSetRecyclable,
                    AckType.Cumulative, null, Collections.emptyMap(), false,
                    this.currentCumulativeAckFuture, null);
            this.consumer.unAckedChunkedMessageIdSequenceMap.remove(lastCumulativeAck.messageId);
            shouldFlush = true;
            cumulativeAckFlushRequired = false;
        }

        // Flush all individual acks
        List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck = new ArrayList<>(pendingIndividualAcks.size() + pendingIndividualBatchIndexAcks.size());
        if (!pendingIndividualAcks.isEmpty()) {
            if (Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion())) {
                // We can send 1 single protobuf command with all individual acks
                while (true) {
                    MessageIdImpl msgId = pendingIndividualAcks.pollFirst();
                    if (msgId == null) {
                        break;
                    }

                    // if messageId is checked then all the chunked related to that msg also processed so, ack all of
                    // them
                    MessageIdImpl[] chunkMsgIds = this.consumer.unAckedChunkedMessageIdSequenceMap.get(msgId);
                    if (chunkMsgIds != null && chunkMsgIds.length > 1) {
                        for (MessageIdImpl cMsgId : chunkMsgIds) {
                            if (cMsgId != null) {
                                entriesToAck.add(Triple.of(cMsgId.getLedgerId(), cMsgId.getEntryId(), null));
                            }
                        }
                        // messages will be acked so, remove checked message sequence
                        this.consumer.unAckedChunkedMessageIdSequenceMap.remove(msgId);
                    } else {
                        entriesToAck.add(Triple.of(msgId.getLedgerId(), msgId.getEntryId(), null));
                    }
                }
            } else {
                // When talking to older brokers, send the acknowledgements individually
                while (true) {
                    MessageIdImpl msgId = pendingIndividualAcks.pollFirst();
                    if (msgId == null) {
                        break;
                    }
                    newMessageAckCommandAndWrite(cnx, consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(), null,
                            AckType.Individual, null, Collections.emptyMap(), false, null, null);
                    shouldFlush = true;
                }
            }
        }

        if (!pendingIndividualBatchIndexAcks.isEmpty()) {
            Iterator<Map.Entry<MessageIdImpl, ConcurrentBitSetRecyclable>> iterator = pendingIndividualBatchIndexAcks.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<MessageIdImpl, ConcurrentBitSetRecyclable> entry = iterator.next();
                entriesToAck.add(Triple.of(entry.getKey().ledgerId, entry.getKey().entryId, entry.getValue()));
                iterator.remove();
            }
        }

        if (entriesToAck.size() > 0) {

            newMessageAckCommandAndWrite(cnx, consumer.consumerId, 0L, 0L,
                    null, AckType.Individual, null, null, true, currentIndividualAckFuture, entriesToAck);
            shouldFlush = true;
        }

        if (shouldFlush) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Flushing pending acks to broker: last-cumulative-ack: {} -- individual-acks: {} -- individual-batch-index-acks: {}",
                        consumer, lastCumulativeAck, pendingIndividualAcks, pendingIndividualBatchIndexAcks);
            }
            cnx.ctx().flush();
        }

    }

    @Override
    public void flushAndClean() {
        flush();
        lastCumulativeAck = LastCumulativeAck.create((MessageIdImpl) MessageIdImpl.earliest, null);
        pendingIndividualAcks.clear();
    }

    @Override
    public void close() {
        flush();
        if (scheduledTask != null && !scheduledTask.isCancelled()) {
            scheduledTask.cancel(true);
        }
    }

    private CompletableFuture<Void> newImmediateAckAndFlush(long consumerId, MessageIdImpl msgId,
                                                            BitSetRecyclable bitSet, AckType ackType,
                                                            Map<String, Long> map, ClientCnx cnx) {
        MessageIdImpl[] chunkMsgIds = this.consumer.unAckedChunkedMessageIdSequenceMap.remove(msgId);
        final CompletableFuture<Void> completableFuture;
        // cumulative ack chunk by the last messageId
        if (chunkMsgIds != null &&  ackType != AckType.Cumulative) {
            if (Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion())) {
                List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck = new ArrayList<>(chunkMsgIds.length);
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    if (cMsgId != null && chunkMsgIds.length > 1) {
                        entriesToAck.add(Triple.of(cMsgId.getLedgerId(), cMsgId.getEntryId(), null));
                    }
                }
                completableFuture = newMessageAckCommandAndWrite(cnx, consumer.consumerId, 0L, 0L,
                        null, ackType, null, null, true, null, entriesToAck);
            } else {
                // if don't support multi message ack, it also support ack receipt, so we should not think about the
                // ack receipt in this logic
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    newMessageAckCommandAndWrite(cnx, consumerId, cMsgId.getLedgerId(), cMsgId.getEntryId(),
                            bitSet, ackType, null, map, true, null, null);
                }
                completableFuture = CompletableFuture.completedFuture(null);
            }
        } else {
            completableFuture = newMessageAckCommandAndWrite(cnx, consumerId, msgId.ledgerId, msgId.getEntryId(),
                    bitSet, ackType, null, map, true, null, null);
        }
        return completableFuture;
    }

    private CompletableFuture<Void> newMessageAckCommandAndWrite(ClientCnx cnx, long consumerId, long ledgerId,
                                                                 long entryId, BitSetRecyclable ackSet, AckType ackType,
                                                                 CommandAck.ValidationError validationError,
                                                                 Map<String, Long> properties, boolean flush,
                                                                 TimedCompletableFuture<Void> timedCompletableFuture,
                                                                 List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck) {
        if (isAckReceiptEnabled(consumer.getClientCnx())) {
            final long requestId = consumer.getClient().newRequestId();
            final ByteBuf cmd;
            if (entriesToAck == null) {
                cmd = Commands.newAck(consumerId, ledgerId, entryId, ackSet,
                        ackType, null, properties, requestId);
            } else {
                cmd = Commands.newMultiMessageAck(consumerId, entriesToAck, requestId);
            }
            if (timedCompletableFuture == null) {
                return cnx.newAckForReceipt(cmd, requestId);
            } else {
                if (ackType == AckType.Individual) {
                    this.currentIndividualAckFuture = new TimedCompletableFuture<>();
                } else {
                    this.currentCumulativeAckFuture = new TimedCompletableFuture<>();
                }
                cnx.newAckForReceiptWithFuture(cmd, requestId, timedCompletableFuture);
                return timedCompletableFuture;
            }
        } else {
            // client cnx don't support ack receipt, if we don't complete the future, the client will block.
            if (ackReceiptEnabled) {
                synchronized (PersistentAcknowledgmentsGroupingTracker.this) {
                    if (!this.currentCumulativeAckFuture.isDone()) {
                        this.currentCumulativeAckFuture.complete(null);
                    }

                    if (!this.currentIndividualAckFuture.isDone()) {
                        this.currentIndividualAckFuture.complete(null);
                    }
                }
            }
            final ByteBuf cmd;
            if (entriesToAck == null) {
                cmd = Commands.newAck(consumerId, ledgerId, entryId, ackSet,
                        ackType, null, properties, -1);
            } else {
                cmd = Commands.newMultiMessageAck(consumerId, entriesToAck, -1);
            }
            if (flush) {
                cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
            } else {
                cnx.ctx().write(cmd, cnx.ctx().voidPromise());
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private boolean isAckReceiptEnabled(ClientCnx cnx) {
        return ackReceiptEnabled && cnx != null
                && Commands.peerSupportsAckReceipt(cnx.getRemoteEndpointProtocolVersion());
    }

    private static class LastCumulativeAck {
        private MessageIdImpl messageId;
        private BitSetRecyclable bitSetRecyclable;

        static LastCumulativeAck create(MessageIdImpl messageId, BitSetRecyclable bitSetRecyclable) {
            LastCumulativeAck op = RECYCLER.get();
            op.messageId = messageId;
            op.bitSetRecyclable = bitSetRecyclable;
            return op;
        }

        private LastCumulativeAck(Recycler.Handle<LastCumulativeAck> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        void recycle() {
            if (bitSetRecyclable != null) {
                this.bitSetRecyclable.recycle();
            }
            this.messageId = null;
            recyclerHandle.recycle(this);
        }

        private final Recycler.Handle<LastCumulativeAck> recyclerHandle;
        private static final Recycler<LastCumulativeAck> RECYCLER = new Recycler<LastCumulativeAck>() {
            @Override
            protected LastCumulativeAck newObject(Handle<LastCumulativeAck> handle) {
                return new LastCumulativeAck(handle);
            }
        };
    }
}
