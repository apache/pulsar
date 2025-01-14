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
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.TimedCompletableFuture;
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

    /**
     * This is a set of all the individual acks that the application has issued and that were not already sent to
     * broker.
     */
    private final ConcurrentSkipListSet<MessageIdAdv> pendingIndividualAcks;
    private final ConcurrentHashMap<MessageIdAdv, ConcurrentBitSetRecyclable> pendingIndividualBatchIndexAcks;

    private final ScheduledFuture<?> scheduledTask;
    private final boolean batchIndexAckEnabled;
    private final boolean ackReceiptEnabled;

    public PersistentAcknowledgmentsGroupingTracker(ConsumerImpl<?> consumer, ConsumerConfigurationData<?> conf,
                                                    EventLoopGroup eventLoopGroup) {
        this.consumer = consumer;
        this.pendingIndividualAcks = new ConcurrentSkipListSet<>();
        this.pendingIndividualBatchIndexAcks = new ConcurrentHashMap<>();
        this.acknowledgementGroupTimeMicros = conf.getAcknowledgementsGroupTimeMicros();
        this.maxAckGroupSize = conf.getMaxAcknowledgmentGroupSize();
        this.batchIndexAckEnabled = conf.isBatchIndexAckEnabled();
        this.ackReceiptEnabled = conf.isAckReceiptEnabled();
        this.currentIndividualAckFuture = new TimedCompletableFuture<>();
        this.currentCumulativeAckFuture = new TimedCompletableFuture<>();

        if (acknowledgementGroupTimeMicros > 0) {
            scheduledTask = eventLoopGroup.next().scheduleWithFixedDelay(catchingAndLoggingThrowables(this::flush),
                    acknowledgementGroupTimeMicros,
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
                ConcurrentBitSetRecyclable bitSet = pendingIndividualBatchIndexAcks.get(key);
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
                if (acknowledgementGroupTimeMicros == 0 || pendingIndividualAcks.size() >= maxAckGroupSize) {
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
                        this::doIndividualAckAsync,
                        this::doIndividualBatchAckAsync);
            } else {
                addIndividualAcknowledgment(messageIdAdv,
                        null,
                        this::doIndividualAckAsync,
                        this::doIndividualBatchAckAsync);
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
            Function<MessageIdAdv, CompletableFuture<Void>> individualAckFunction,
            Function<MessageIdAdv, CompletableFuture<Void>> batchAckFunction) {
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
            return individualAckFunction.apply(msgId);
        } else if (batchIndexAckEnabled) {
            return batchAckFunction.apply(batchMessageId);
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
                        __ -> doIndividualAck(__, properties),
                        __ -> doIndividualBatchAck(__, properties));
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

    private CompletableFuture<Void> doIndividualAck(MessageIdAdv messageId, Map<String, Long> properties) {
        if (acknowledgementGroupTimeMicros == 0 || (properties != null && !properties.isEmpty())) {
            // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
            // uncommon condition since it's only used for the compaction subscription.
            return doImmediateAck(messageId, AckType.Individual, properties, null);
        } else {
            Optional<Lock> readLock = acquireReadLock();
            try {
                doIndividualAckAsync(messageId);
                return readLock.map(__ -> currentIndividualAckFuture).orElse(CompletableFuture.completedFuture(null));
            } finally {
                readLock.ifPresent(Lock::unlock);
                if (pendingIndividualAcks.size() >= maxAckGroupSize) {
                    flush();
                }
            }
        }
    }


    private CompletableFuture<Void> doIndividualAckAsync(MessageIdAdv messageId) {
        pendingIndividualAcks.add(messageId);
        pendingIndividualBatchIndexAcks.remove(messageId);
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> doIndividualBatchAck(MessageIdAdv batchMessageId,
                                                         Map<String, Long> properties) {
        if (acknowledgementGroupTimeMicros == 0 || (properties != null && !properties.isEmpty())) {
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
            if (pendingIndividualBatchIndexAcks.size() >= maxAckGroupSize) {
                flush();
            }
        }
    }

    private CompletableFuture<Void> doCumulativeAck(MessageIdAdv messageId, Map<String, Long> properties,
                                                    BitSetRecyclable bitSet) {
        consumer.getStats().incrementNumAcksSent(consumer.getUnAckedMessageTracker().removeMessagesTill(messageId));
        if (acknowledgementGroupTimeMicros == 0 || (properties != null && !properties.isEmpty())) {
            // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
            // uncommon condition since it's only used for the compaction subscription.
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
            }
        }
    }

    private CompletableFuture<Void> doIndividualBatchAckAsync(MessageIdAdv msgId) {
        ConcurrentBitSetRecyclable bitSet = pendingIndividualBatchIndexAcks.computeIfAbsent(
                MessageIdAdvUtils.discardBatch(msgId), __ -> {
                    final BitSet ackSet = msgId.getAckSet();
                    final ConcurrentBitSetRecyclable value;
                    if (ackSet != null) {
                        synchronized (ackSet) {
                            if (!ackSet.isEmpty()) {
                                value = ConcurrentBitSetRecyclable.create(ackSet);
                            } else {
                                value = ConcurrentBitSetRecyclable.create();
                                value.set(0, msgId.getBatchSize());
                            }
                        }
                    } else {
                        value = ConcurrentBitSetRecyclable.create();
                        value.set(0, msgId.getBatchSize());
                    }
                    return value;
                });
        bitSet.clear(msgId.getBatchIndex());
        return CompletableFuture.completedFuture(null);
    }

    private void doCumulativeAckAsync(MessageIdAdv msgId, BitSetRecyclable bitSet) {
        // Handle concurrent updates from different threads
        lastCumulativeAck.update(msgId, bitSet);
    }

    private CompletableFuture<Void> doCumulativeBatchIndexAck(MessageIdAdv batchMessageId,
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
                msgId.getLedgerId(), msgId.getEntryId(), bitSet, ackType, properties, true, null, null);
        bitSet.recycle();
        return completableFuture;
    }

    /**
     * Flush all the pending acks and send them to the broker.
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

        Optional<Lock> writeLock = acquireWriteLock();
        try {
            flushAsync(cnx);
        } finally {
            writeLock.ifPresent(Lock::unlock);
        }
    }

    private void flushAsync(ClientCnx cnx) {
        final LastCumulativeAck lastCumulativeAckToFlush = lastCumulativeAck.flush();
        boolean shouldFlush = false;
        if (lastCumulativeAckToFlush != null) {
            shouldFlush = true;
            final MessageIdAdv messageId = lastCumulativeAckToFlush.getMessageId();
            newMessageAckCommandAndWrite(cnx, consumer.consumerId, messageId.getLedgerId(), messageId.getEntryId(),
                    lastCumulativeAckToFlush.getBitSetRecyclable(), AckType.Cumulative,
                    Collections.emptyMap(), false,
                    (TimedCompletableFuture<Void>) this.currentCumulativeAckFuture, null);
            this.consumer.unAckedChunkedMessageIdSequenceMap.remove(messageId);
        }

        // Flush all individual acks
        List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck =
                new ArrayList<>(pendingIndividualAcks.size() + pendingIndividualBatchIndexAcks.size());
        if (!pendingIndividualAcks.isEmpty()) {
            if (Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion())) {
                // We can send 1 single protobuf command with all individual acks
                while (true) {
                    MessageIdAdv msgId = pendingIndividualAcks.pollFirst();
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
                    MessageIdAdv msgId = pendingIndividualAcks.pollFirst();
                    if (msgId == null) {
                        break;
                    }
                    newMessageAckCommandAndWrite(cnx, consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(),
                            null, AckType.Individual, Collections.emptyMap(), false,
                            null, null);
                    shouldFlush = true;
                }
            }
        }

        if (!pendingIndividualBatchIndexAcks.isEmpty()) {
            Iterator<Map.Entry<MessageIdAdv, ConcurrentBitSetRecyclable>> iterator =
                    pendingIndividualBatchIndexAcks.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<MessageIdAdv, ConcurrentBitSetRecyclable> entry = iterator.next();
                entriesToAck.add(Triple.of(
                        entry.getKey().getLedgerId(), entry.getKey().getEntryId(), entry.getValue()));
                iterator.remove();
            }
        }

        if (entriesToAck.size() > 0) {

            newMessageAckCommandAndWrite(cnx, consumer.consumerId, 0L, 0L,
                    null, AckType.Individual, null, true,
                    (TimedCompletableFuture<Void>) currentIndividualAckFuture, entriesToAck);
            shouldFlush = true;
        }

        if (shouldFlush) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Flushing pending acks to broker: last-cumulative-ack: {} -- individual-acks: {}"
                                + " -- individual-batch-index-acks: {}",
                        consumer, lastCumulativeAck, pendingIndividualAcks, entriesToAck);
            }
            cnx.ctx().flush();
        }

    }

    @Override
    public void flushAndClean() {
        flush();
        lastCumulativeAck.reset();
        pendingIndividualAcks.clear();
    }

    @Override
    public void close() {
        flush();
        if (scheduledTask != null && !scheduledTask.isCancelled()) {
            scheduledTask.cancel(true);
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
                List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck = new ArrayList<>(chunkMsgIds.length);
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    if (cMsgId != null && chunkMsgIds.length > 1) {
                        entriesToAck.add(Triple.of(cMsgId.getLedgerId(), cMsgId.getEntryId(), null));
                    }
                }
                completableFuture = newMessageAckCommandAndWrite(cnx, consumer.consumerId, 0L, 0L,
                        null, ackType, null, true, null, entriesToAck);
            } else {
                // if don't support multi message ack, it also support ack receipt, so we should not think about the
                // ack receipt in this logic
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    newMessageAckCommandAndWrite(cnx, consumerId, cMsgId.getLedgerId(), cMsgId.getEntryId(),
                            bitSet, ackType, map, true, null, null);
                }
                completableFuture = CompletableFuture.completedFuture(null);
            }
        } else {
            completableFuture = newMessageAckCommandAndWrite(cnx, consumerId, msgId.getLedgerId(), msgId.getEntryId(),
                    bitSet, ackType, map, true, null, null);
        }
        return completableFuture;
    }

    private CompletableFuture<Void> newMessageAckCommandAndWrite(
            ClientCnx cnx, long consumerId, long ledgerId,
            long entryId, BitSetRecyclable ackSet, AckType ackType,
            Map<String, Long> properties, boolean flush,
            TimedCompletableFuture<Void> timedCompletableFuture,
            List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck) {
        if (consumer.isAckReceiptEnabled()) {
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
