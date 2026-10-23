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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.TimedCompletableFuture;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentBitSet;
import org.jspecify.annotations.Nullable;

/**
 * Group the acknowledgements for a certain time and then sends them out in a single protobuf command.
 */
@CustomLog
public class PersistentAcknowledgmentsGroupingTracker implements AcknowledgmentsGroupingTracker {

    /**
     * When reaching the max group size, an ack command is sent out immediately.
     */
    private final int maxAckGroupSize;

    private final ConsumerImpl<?> consumer;

    private final long acknowledgementGroupTimeMicros;

    private volatile CompletableFuture<Void> currentIndividualAckFuture;
    private volatile CompletableFuture<Void> currentCumulativeAckFuture;

    private final LastCumulativeAck lastCumulativeAck = new LastCumulativeAck();

    // When we flush the command, we should ensure current ack request will send correct
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Serializes flush snapshots, cleanup and failed-write restoration. ACK additions and duplicate checks use the
    // concurrent collections without this monitor. Never write commands or complete futures while holding it.
    private final Object pendingAcksLock = new Object();
    // Guarded by pendingAcksLock: cleanup invalidates restoration of ACKs taken by earlier flushes.
    private long ackGeneration;

    /**
     * This is a set of all the individual acks that the application has issued and that were not already sent to
     * broker.
     */
    private final ConcurrentSkipListSet<MessageIdAdv> pendingIndividualAcks;
    @VisibleForTesting
    final ConcurrentSkipListMap<MessageIdAdv, ConcurrentBitSet> pendingIndividualBatchIndexAcks;

    // the periodic flush, shared with the other consumers' trackers of the event loop; null without grouping
    private final AcknowledgmentsGroupingFlushes.Registration periodicFlush;
    // Set after an ack is added to the pending acks and cleared by a flush before it takes them. An ack added while a
    // flush takes the acks sets it again, and the next flush sends what this one didn't
    private volatile boolean acksPending;
    private final boolean batchIndexAckEnabled;
    private final boolean ackReceiptEnabled;

    public PersistentAcknowledgmentsGroupingTracker(ConsumerImpl<?> consumer, ConsumerConfigurationData<?> conf,
                                                    EventLoopGroup eventLoopGroup) {
        this.consumer = consumer;
        this.pendingIndividualAcks = new ConcurrentSkipListSet<>();
        this.pendingIndividualBatchIndexAcks = new ConcurrentSkipListMap<>();
        this.acknowledgementGroupTimeMicros = conf.getAcknowledgementsGroupTimeMicros();
        this.maxAckGroupSize = conf.getMaxAcknowledgmentGroupSize();
        this.batchIndexAckEnabled = conf.isBatchIndexAckEnabled();
        this.ackReceiptEnabled = conf.isAckReceiptEnabled();
        this.currentIndividualAckFuture = new TimedCompletableFuture<>();
        this.currentCumulativeAckFuture = new TimedCompletableFuture<>();

        if (acknowledgementGroupTimeMicros > 0) {
            periodicFlush = AcknowledgmentsGroupingFlushes.register(eventLoopGroup.next(),
                    acknowledgementGroupTimeMicros, catchingAndLoggingThrowables(this::periodicFlush));
        } else {
            periodicFlush = null;
        }
    }

    /**
     * Since the ack are delayed, we need to do some best-effort duplicate check to discard messages that are being
     * resent after a disconnection and for which the user has already sent an acknowledgement.
     */
    @Override
    public boolean isDuplicate(MessageId messageId) {
        if (!(messageId instanceof MessageIdAdv)) {
            throw new IllegalArgumentException("isDuplicated cannot accept "
                    + messageId.getClass().getName() + ": " + messageId);
        }
        final MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
        if (lastCumulativeAck.compareTo(messageIdAdv) >= 0) {
            // Already included in a cumulative ack
            return true;
        } else {
            // If "batchIndexAckEnabled" is false, the batched messages acknowledgment will be traced by
            // pendingIndividualAcks. So no matter what type the message ID is, check with "pendingIndividualAcks"
            // first.
            MessageIdAdv key = MessageIdAdvUtils.discardBatch(messageIdAdv);
            if (pendingIndividualAcks.contains(key)) {
                return true;
            }
            if (messageIdAdv.getBatchIndex() >= 0) {
                ConcurrentBitSet bitSet = pendingIndividualBatchIndexAcks.get(key);
                return bitSet != null && !bitSet.get(messageIdAdv.getBatchIndex());
            }
            return false;
        }
    }

    @Override
    public CompletableFuture<Void> addListAcknowledgment(List<MessageId> messageIds,
                                                         AckType ackType, Map<String, Long> properties) {
        if (AckType.Cumulative.equals(ackType)) {
            if (consumer.isAckReceiptEnabled()) {
                Set<CompletableFuture<Void>> completableFutureSet = new HashSet<>();
                messageIds.forEach(messageId ->
                        completableFutureSet.add(addAcknowledgment(messageId, ackType, properties)));
                return FutureUtil.waitForAll(new ArrayList<>(completableFutureSet));
            } else {
                messageIds.forEach(messageId -> addAcknowledgment(messageId, ackType, properties));
                return CompletableFuture.completedFuture(null);
            }
        } else {
            Optional<Lock> readLock = acquireReadLock();
            try {
                if (messageIds.size() != 0) {
                    addListAcknowledgment(messageIds);
                    return readLock.map(__ -> currentIndividualAckFuture)
                            .orElse(CompletableFuture.completedFuture(null));
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            } finally {
                readLock.ifPresent(Lock::unlock);
                if (acknowledgementGroupTimeMicros == 0 || isMaxAckGroupSizeReached()) {
                    flush();
                }
            }
        }
    }

    private void addListAcknowledgment(List<MessageId> messageIds) {
        for (MessageId messageId : messageIds) {
            MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
            if (MessageIdAdvUtils.isBatch(messageIdAdv)) {
                addIndividualAcknowledgment(MessageIdAdvUtils.discardBatch(messageIdAdv),
                        messageIdAdv,
                        null, true);
            } else {
                addIndividualAcknowledgment(messageIdAdv,
                        null,
                        null, true);
            }
        }
    }

    @Override
    public CompletableFuture<Void> addAcknowledgment(MessageId msgId, AckType ackType,
                                                     Map<String, Long> properties) {
        MessageIdAdv msgIdAdv = (MessageIdAdv) msgId;
        if (MessageIdAdvUtils.isBatch(msgIdAdv)) {
            return addAcknowledgment(MessageIdAdvUtils.discardBatch(msgId), ackType, properties, msgIdAdv);
        } else {
            return addAcknowledgment(msgIdAdv, ackType, properties, null);
        }
    }

    private CompletableFuture<Void> addIndividualAcknowledgment(
            MessageIdAdv msgId,
            @Nullable MessageIdAdv batchMessageId,
            Map<String, Long> properties,
            boolean groupedByCaller) {
        if (batchMessageId != null) {
            consumer.onAcknowledge(batchMessageId, null);
        } else {
            consumer.onAcknowledge(msgId, null);
        }
        if (batchMessageId == null || MessageIdAdvUtils.acknowledge(batchMessageId, true)) {
            consumer.getStats().incrementNumAcksSent((batchMessageId != null) ? batchMessageId.getBatchSize() : 1);
            consumer.getUnAckedMessageTracker().remove(msgId);
            if (consumer.getPossibleSendToDeadLetterTopicMessages() != null) {
                consumer.getPossibleSendToDeadLetterTopicMessages().remove(msgId);
            }
            // List acknowledgments already hold the grouping lock and flush after processing the list.
            return groupedByCaller ? doIndividualAckAsync(msgId) : doIndividualAck(msgId, properties);
        } else if (batchIndexAckEnabled) {
            return groupedByCaller ? doIndividualBatchAckAsync(batchMessageId)
                    : doIndividualBatchAck(batchMessageId, properties);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> addAcknowledgment(MessageIdAdv msgId,
                                                      AckType ackType,
                                                      Map<String, Long> properties,
                                                      @Nullable MessageIdAdv batchMessageId) {
        switch (ackType) {
            case Individual:
                return addIndividualAcknowledgment(msgId,
                        batchMessageId,
                        properties, false);
            case Cumulative:
                if (batchMessageId != null) {
                    consumer.onAcknowledgeCumulative(batchMessageId, null);
                } else {
                    consumer.onAcknowledgeCumulative(msgId, null);
                }
                if (batchMessageId == null || MessageIdAdvUtils.acknowledge(batchMessageId, false)) {
                    return doCumulativeAck(msgId, properties, null);
                } else if (batchIndexAckEnabled) {
                    return doCumulativeBatchIndexAck(batchMessageId, properties);
                } else {
                    doCumulativeAck(MessageIdAdvUtils.prevMessageId(batchMessageId), properties, null);
                    return CompletableFuture.completedFuture(null);
                }
            default:
                throw new IllegalStateException("Unknown AckType: " + ackType);
        }
    }

    private boolean isImmediateAck(Map<String, Long> properties) {
        // We cannot group acks when there are properties attached to them. Fortunately that's an uncommon condition
        // since it's only used for the compaction subscription. Without a delay, the acks are only grouped while the
        // consumer is not subscribed, until the flush that follows the subscription.
        return (properties != null && !properties.isEmpty())
                || (acknowledgementGroupTimeMicros == 0 && getSubscribedCnx() != null);
    }

    // The connection of the subscribed consumer: the broker drops the acks sent before the subscription completes
    private @Nullable ClientCnx getSubscribedCnx() {
        ClientCnx cnx = consumer.getClientCnx();
        return cnx != null && consumer.getState() != HandlerState.State.Connecting ? cnx : null;
    }

    private CompletableFuture<Void> doIndividualAck(MessageIdAdv messageId, Map<String, Long> properties) {
        if (isImmediateAck(properties)) {
            return doImmediateAck(messageId, AckType.Individual, properties, null);
        } else {
            Optional<Lock> readLock = acquireReadLock();
            try {
                doIndividualAckAsync(messageId);
                return readLock.map(__ -> currentIndividualAckFuture).orElse(CompletableFuture.completedFuture(null));
            } finally {
                readLock.ifPresent(Lock::unlock);
                if (acknowledgementGroupTimeMicros == 0 || isMaxAckGroupSizeReached()) {
                    flush();
                }
            }
        }
    }


    private boolean isMaxAckGroupSizeReached() {
        // Both collections are drained by the same flush; count pending entries across the entire group.
        return (long) pendingIndividualAcks.size() + pendingIndividualBatchIndexAcks.size() >= maxAckGroupSize;
    }

    private CompletableFuture<Void> doIndividualAckAsync(MessageIdAdv messageId) {
        pendingIndividualAcks.add(messageId);
        pendingIndividualBatchIndexAcks.remove(messageId);
        markAcksPending();
        return CompletableFuture.completedFuture(null);
    }

    @VisibleForTesting
    int getPendingIndividualAcksSize() {
        return pendingIndividualAcks.size();
    }

    private CompletableFuture<Void> doIndividualBatchAck(MessageIdAdv batchMessageId,
                                                         Map<String, Long> properties) {
        if (isImmediateAck(properties)) {
            return doImmediateBatchIndexAck(batchMessageId, batchMessageId.getBatchIndex(),
                    batchMessageId.getBatchSize(), AckType.Individual, properties);
        } else {
            return doIndividualBatchAck(batchMessageId);
        }
    }

    private CompletableFuture<Void> doIndividualBatchAck(MessageIdAdv batchMessageId) {
        Optional<Lock> readLock = acquireReadLock();
        try {
            doIndividualBatchAckAsync(batchMessageId);
            return readLock.map(__ -> currentIndividualAckFuture).orElse(CompletableFuture.completedFuture(null));
        } finally {
            readLock.ifPresent(Lock::unlock);
            if (acknowledgementGroupTimeMicros == 0 || isMaxAckGroupSizeReached()) {
                flush();
            }
        }
    }

    private CompletableFuture<Void> doCumulativeAck(MessageIdAdv messageId, Map<String, Long> properties,
                                                    BitSetRecyclable bitSet) {
        consumer.getStats().incrementNumAcksSent(consumer.getUnAckedMessageTracker().removeMessagesTill(messageId));
        if (isImmediateAck(properties)) {
            return doImmediateAck(messageId, AckType.Cumulative, properties, bitSet);
        } else {
            Optional<Lock> readLock = acquireReadLock();
            try {
                doCumulativeAckAsync(messageId, bitSet);
                return readLock.map(__ -> {
                    if (consumer.isAckReceiptEnabled() && lastCumulativeAck.compareTo(messageId) == 0) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    return currentCumulativeAckFuture;
                }).orElse(CompletableFuture.completedFuture(null));
            } finally {
                readLock.ifPresent(Lock::unlock);
                if (acknowledgementGroupTimeMicros == 0) {
                    flush();
                }
            }
        }
    }

    @VisibleForTesting
    CompletableFuture<Void> doIndividualBatchAckAsync(MessageIdAdv msgId) {
        ConcurrentBitSet bitSet = pendingIndividualBatchIndexAcks.computeIfAbsent(
                MessageIdAdvUtils.discardBatch(msgId), __ -> {
                    final BitSet ackSet = msgId.getAckSet();
                    final ConcurrentBitSet value;
                    if (ackSet != null) {
                        synchronized (ackSet) {
                            if (!ackSet.isEmpty()) {
                                value =  new ConcurrentBitSet();
                                value.or(ackSet);
                            } else {
                                value = new ConcurrentBitSet();
                                value.set(0, msgId.getBatchSize());
                            }
                        }
                    } else {
                        value = new ConcurrentBitSet();
                        value.set(0, msgId.getBatchSize());
                    }
                    return value;
                });
        bitSet.clear(msgId.getBatchIndex());
        markAcksPending();
        return CompletableFuture.completedFuture(null);
    }

    private void doCumulativeAckAsync(MessageIdAdv msgId, BitSetRecyclable bitSet) {
        // Handle concurrent updates from different threads
        lastCumulativeAck.update(msgId, bitSet);
        markAcksPending();
    }

    private CompletableFuture<Void> doCumulativeBatchIndexAck(MessageIdAdv batchMessageId,
                                                              Map<String, Long> properties) {
        if (isImmediateAck(properties)) {
            return doImmediateBatchIndexAck(batchMessageId, batchMessageId.getBatchIndex(),
                    batchMessageId.getBatchSize(), AckType.Cumulative, properties);
        } else {
            BitSetRecyclable bitSet = BitSetRecyclable.create();
            bitSet.set(0, batchMessageId.getBatchSize());
            bitSet.clear(0, batchMessageId.getBatchIndex() + 1);
            return doCumulativeAck(batchMessageId, null, bitSet);
        }
    }

    private CompletableFuture<Void> doImmediateAck(MessageIdAdv msgId, AckType ackType, Map<String, Long> properties,
                                                   BitSetRecyclable bitSet) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .ConnectException("Consumer connect fail! consumer state:" + consumer.getState()));
        }
        return newImmediateAckAndFlush(consumer.consumerId, msgId, bitSet, ackType, properties, cnx);
    }

    private CompletableFuture<Void> doImmediateBatchIndexAck(MessageIdAdv msgId, int batchIndex, int batchSize,
                                                             AckType ackType, Map<String, Long> properties) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .ConnectException("Consumer connect fail! consumer state:" + consumer.getState()));
        }
        BitSetRecyclable bitSet;
        BitSet ackSetFromMsgId = msgId.getAckSet();
        if (ackSetFromMsgId != null) {
            synchronized (ackSetFromMsgId) {
                bitSet = BitSetRecyclable.valueOf(ackSetFromMsgId.toLongArray());
            }
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
                msgId.getLedgerId(), msgId.getEntryId(), bitSet, ackType, properties, true, null, null, null, null);
        bitSet.recycle();
        return completableFuture;
    }

    // Marks the added acks for the next flush, and requests the periodic flush
    private void markAcksPending() {
        acksPending = true;
        if (periodicFlush != null) {
            periodicFlush.requestFlush();
        }
    }

    // The periodic flush, requested by the acks added since the last flush
    private void periodicFlush() {
        if (acksPending) {
            try {
                flush();
            } finally {
                if (acksPending) {
                    // not sent, without a connection or when the flush failed: retried by the next periodic flush.
                    // An ack added while the flush ran requested it already
                    periodicFlush.requestFlush();
                }
            }
        }
    }

    /**
     * Flush all the pending acks and send them to the broker.
     */
    @Override
    public void flush() {
        ClientCnx cnx = getSubscribedCnx();

        if (cnx == null) {
                log.debug().attr("consumer", consumer)
                        .log("Cannot flush pending acks since we're not subscribed on the broker");
            return;
        }

        Optional<Lock> writeLock = acquireWriteLock();
        try {
            flushAsync(cnx);
        } finally {
            writeLock.ifPresent(Lock::unlock);
        }
    }

    private void flushAsync(ClientCnx cnx) {
        final LastCumulativeAck lastCumulativeAckToFlush;
        final List<MessageIdAdv> entriesToAck;
        final List<MessageIdAdv> individualAcksToFlush;
        final Map<MessageIdAdv, MessageIdImpl[]> chunkedMessageIdsToRestore;
        final List<Map.Entry<MessageIdAdv, ConcurrentBitSet>> batchIndexAcksToFlush;
        final long generation;
        final boolean supportsMultiMessageAcknowledgment =
                Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion());
        synchronized (pendingAcksLock) {
            // Capture the generation and take its ACKs together: cleanup must not let an older flush take newer ACKs.
            generation = ackGeneration;
            // cleared before the acks are taken: an ack added from here on sets it again for the next flush
            acksPending = false;
            lastCumulativeAckToFlush = lastCumulativeAck.flush();
            if (lastCumulativeAckToFlush == null && pendingIndividualAcks.isEmpty()
                    && pendingIndividualBatchIndexAcks.isEmpty()) {
                // An ack added after these checks waits for the next flush, as one added after the acks are taken does.
                return;
            }
            if (lastCumulativeAckToFlush != null) {
                this.consumer.unAckedChunkedMessageIdSequenceMap.remove(lastCumulativeAckToFlush.getMessageId());
            }

            entriesToAck = new ArrayList<>(pendingIndividualAcks.size());
            individualAcksToFlush = new ArrayList<>(pendingIndividualAcks.size());
            chunkedMessageIdsToRestore = new HashMap<>();
            while (true) {
                MessageIdAdv msgId = pendingIndividualAcks.pollFirst();
                if (msgId == null) {
                    break;
                }
                individualAcksToFlush.add(msgId);
                if (supportsMultiMessageAcknowledgment) {
                    // Acknowledging the assembled message also acknowledges its chunks.
                    MessageIdImpl[] chunkMsgIds = this.consumer.unAckedChunkedMessageIdSequenceMap.get(msgId);
                    if (chunkMsgIds != null && chunkMsgIds.length > 1) {
                        chunkedMessageIdsToRestore.put(msgId, chunkMsgIds);
                        for (MessageIdImpl cMsgId : chunkMsgIds) {
                            if (cMsgId != null) {
                                entriesToAck.add(cMsgId);
                            }
                        }
                        this.consumer.unAckedChunkedMessageIdSequenceMap.remove(msgId);
                    } else {
                        entriesToAck.add(msgId);
                    }
                }
            }

            batchIndexAcksToFlush = new ArrayList<>();
            while (true) {
                Map.Entry<MessageIdAdv, ConcurrentBitSet> entry = pendingIndividualBatchIndexAcks.pollFirstEntry();
                if (entry == null) {
                    break;
                }
                batchIndexAcksToFlush.add(entry);
            }
        }

        boolean shouldFlush = false;
        if (lastCumulativeAckToFlush != null) {
            shouldFlush = true;
            final MessageIdAdv messageId = lastCumulativeAckToFlush.getMessageId();
            newMessageAckCommandAndWrite(cnx, consumer.consumerId, messageId.getLedgerId(), messageId.getEntryId(),
                    lastCumulativeAckToFlush.getBitSetRecyclable(), AckType.Cumulative,
                    Collections.emptyMap(), false,
                    (TimedCompletableFuture<Void>) this.currentCumulativeAckFuture, null, null, null);
        }

        if (!supportsMultiMessageAcknowledgment) {
            // When talking to older brokers, send the acknowledgements individually.
            for (MessageIdAdv msgId : individualAcksToFlush) {
                newMessageAckCommandAndWrite(cnx, consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(),
                        null, AckType.Individual, Collections.emptyMap(), false,
                        null, null, null, () -> restoreIndividualAck(generation, msgId, null));
                shouldFlush = true;
            }
        }

        if (!entriesToAck.isEmpty() || !batchIndexAcksToFlush.isEmpty()) {
            newMessageAckCommandAndWrite(cnx, consumer.consumerId, 0L, 0L,
                    null, AckType.Individual, null, true,
                    (TimedCompletableFuture<Void>) currentIndividualAckFuture, entriesToAck, batchIndexAcksToFlush,
                    () -> restoreIndividualAndBatchIndexAcks(generation, individualAcksToFlush,
                            chunkedMessageIdsToRestore, batchIndexAcksToFlush));
            shouldFlush = true;
        }

        if (shouldFlush) {
                log.debug().attr("consumer", consumer)
                        .attr("lastCumulativeAck", lastCumulativeAck)
                        .attr("individualAcks", pendingIndividualAcks)
                        .attr("individualBatchIndexAcks", batchIndexAcksToFlush)
                        .log("Flushing pending acks to broker");
            cnx.ctx().flush();
        }

    }

    @Override
    public void flushAndClean() {
        flush();
        synchronized (pendingAcksLock) {
            ackGeneration++;
            acksPending = false;
            lastCumulativeAck.reset();
            pendingIndividualAcks.clear();
            pendingIndividualBatchIndexAcks.clear();
        }
    }

    @Override
    public void close() {
        try {
            flush();
        } finally {
            if (periodicFlush != null) {
                periodicFlush.close();
            }
        }
    }

    private CompletableFuture<Void> newImmediateAckAndFlush(long consumerId, MessageIdAdv msgId,
                                                            BitSetRecyclable bitSet, AckType ackType,
                                                            Map<String, Long> map, ClientCnx cnx) {
        MessageIdImpl[] chunkMsgIds = this.consumer.unAckedChunkedMessageIdSequenceMap.remove(msgId);
        final CompletableFuture<Void> completableFuture;
        // cumulative ack chunk by the last messageId
        if (chunkMsgIds != null &&  ackType != AckType.Cumulative) {
            if (Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion())) {
                List<MessageIdAdv> entriesToAck = new ArrayList<>(chunkMsgIds.length);
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    if (cMsgId != null && chunkMsgIds.length > 1) {
                        entriesToAck.add(cMsgId);
                    }
                }
                completableFuture = newMessageAckCommandAndWrite(cnx, consumer.consumerId, 0L, 0L,
                        null, ackType, null, true, null, entriesToAck, Collections.emptyList(), null);
            } else {
                // if don't support multi message ack, it also support ack receipt, so we should not think about the
                // ack receipt in this logic
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    newMessageAckCommandAndWrite(cnx, consumerId, cMsgId.getLedgerId(), cMsgId.getEntryId(),
                            bitSet, ackType, map, true, null, null, null, null);
                }
                completableFuture = CompletableFuture.completedFuture(null);
            }
        } else {
            completableFuture = newMessageAckCommandAndWrite(cnx, consumerId, msgId.getLedgerId(), msgId.getEntryId(),
                    bitSet, ackType, map, true, null, null, null, null);
        }
        return completableFuture;
    }

    private CompletableFuture<Void> newMessageAckCommandAndWrite(
            ClientCnx cnx, long consumerId, long ledgerId,
            long entryId, BitSetRecyclable ackSet, AckType ackType,
            Map<String, Long> properties, boolean flush,
            TimedCompletableFuture<Void> timedCompletableFuture,
            List<MessageIdAdv> entriesToAck,
            List<Map.Entry<MessageIdAdv, ConcurrentBitSet>> batchIndexAcksToFlush,
            Runnable writeFailureCallback) {
        if (consumer.isAckReceiptEnabled()) {
            final long requestId = consumer.getClient().newRequestId();
            final ByteBuf cmd;
            if (entriesToAck == null) {
                cmd = Commands.newAck(consumerId, ledgerId, entryId, ackSet,
                        ackType, null, properties, requestId);
            } else {
                cmd = Commands.newMultiMessageAck(consumerId, entriesToAck, batchIndexAcksToFlush, requestId);
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
                cmd = Commands.newMultiMessageAck(consumerId, entriesToAck, batchIndexAcksToFlush, -1);
            }
            if (flush) {
                if (writeFailureCallback == null) {
                    cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                } else {
                    cnx.ctx().writeAndFlush(cmd).addListener(writeFuture -> {
                        if (!writeFuture.isSuccess()) {
                            writeFailureCallback.run();
                        }
                    });
                }
            } else {
                if (writeFailureCallback == null) {
                    cnx.ctx().write(cmd, cnx.ctx().voidPromise());
                } else {
                    cnx.ctx().write(cmd).addListener(writeFuture -> {
                        if (!writeFuture.isSuccess()) {
                            writeFailureCallback.run();
                        }
                    });
                }
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private void restoreIndividualAck(long generation, MessageIdAdv messageId,
                                      @Nullable MessageIdImpl[] chunkMsgIds) {
        synchronized (pendingAcksLock) {
            if (generation != ackGeneration) {
                return;
            }
            pendingIndividualAcks.add(messageId);
            restoreChunkedMessageIds(messageId, chunkMsgIds);
        }
        markAcksPending();
    }

    private void restoreIndividualAndBatchIndexAcks(long generation, List<MessageIdAdv> messageIds,
                                                    Map<MessageIdAdv, MessageIdImpl[]> chunkMsgIds,
                                                    List<Map.Entry<MessageIdAdv, ConcurrentBitSet>> batchIndexAcks) {
        synchronized (pendingAcksLock) {
            if (generation != ackGeneration) {
                return;
            }
            pendingIndividualAcks.addAll(messageIds);
            chunkMsgIds.forEach(this::restoreChunkedMessageIds);
            batchIndexAcks.forEach(entry -> pendingIndividualBatchIndexAcks.merge(entry.getKey(), entry.getValue(),
                    (currentValue, valueToRestore) -> {
                        currentValue.and(valueToRestore);
                        return currentValue;
                    }));
        }
        markAcksPending();
    }

    private void restoreChunkedMessageIds(MessageIdAdv messageId, @Nullable MessageIdImpl[] chunkMsgIds) {
        if (chunkMsgIds != null) {
            consumer.unAckedChunkedMessageIdSequenceMap.putIfAbsent(messageId, chunkMsgIds);
        }
    }

    public Optional<Lock> acquireReadLock() {
        Optional<Lock> optionalLock = Optional.ofNullable(consumer.isAckReceiptEnabled() ? lock.readLock() : null);
        optionalLock.ifPresent(Lock::lock);
        return optionalLock;
    }

    public Optional<Lock> acquireWriteLock() {
        Optional<Lock> optionalLock = Optional.ofNullable(consumer.isAckReceiptEnabled() ? lock.writeLock() : null);
        optionalLock.ifPresent(Lock::lock);
        return optionalLock;
    }
}

@Getter
class LastCumulativeAck {

    // It's used as a returned value by `flush()` to avoid creating a new instance each time `flush()` is called
    public static final FastThreadLocal<LastCumulativeAck> LOCAL_LAST_CUMULATIVE_ACK =
            new FastThreadLocal<LastCumulativeAck>() {

                @Override
                protected LastCumulativeAck initialValue() {
                    return new LastCumulativeAck();
                }
            };
    public static final MessageIdAdv DEFAULT_MESSAGE_ID = (MessageIdAdv) MessageId.earliest;

    private volatile MessageIdAdv messageId = DEFAULT_MESSAGE_ID;
    private BitSetRecyclable bitSetRecyclable = null;
    private boolean flushRequired = false;

    public synchronized void update(final MessageIdAdv messageId, final BitSetRecyclable bitSetRecyclable) {
        if (compareTo(messageId) < 0) {
            if (this.bitSetRecyclable != null && this.bitSetRecyclable != bitSetRecyclable) {
                this.bitSetRecyclable.recycle();
            }
            set(messageId, bitSetRecyclable);
            flushRequired = true;
        }
    }

    public synchronized LastCumulativeAck flush() {
        if (flushRequired) {
            final LastCumulativeAck localLastCumulativeAck = LOCAL_LAST_CUMULATIVE_ACK.get();
            if (bitSetRecyclable != null) {
                localLastCumulativeAck.set(messageId, BitSetRecyclable.valueOf(bitSetRecyclable.toLongArray()));
            } else {
                localLastCumulativeAck.set(this.messageId, null);
            }
            flushRequired = false;
            return localLastCumulativeAck;
        } else {
            // Return null to indicate nothing to be flushed
            return null;
        }
    }

    public synchronized void reset() {
        if (bitSetRecyclable != null) {
            bitSetRecyclable.recycle();
        }
        messageId = DEFAULT_MESSAGE_ID;
        bitSetRecyclable = null;
        flushRequired = false;
    }

    public synchronized int compareTo(MessageIdAdv messageId) {
        int result = Long.compare(this.messageId.getLedgerId(), messageId.getLedgerId());
        if (result != 0) {
            return result;
        }
        result = Long.compare(this.messageId.getEntryId(), messageId.getEntryId());
        if (result != 0) {
            return result;
        }
        return Integer.compare(
                (this.messageId.getBatchIndex() >= 0) ? this.messageId.getBatchIndex() : Integer.MAX_VALUE,
                (messageId.getBatchIndex() >= 0) ? messageId.getBatchIndex() : Integer.MAX_VALUE
        );
    }

    private synchronized void set(final MessageIdAdv messageId, final BitSetRecyclable bitSetRecyclable) {
        this.messageId = messageId;
        this.bitSetRecyclable = bitSetRecyclable;
    }

    @Override
    public String toString() {
        String s = messageId.toString();
        if (bitSetRecyclable != null) {
            s += " (bit set: " + bitSetRecyclable + ")";
        }
        return s;
    }
}
