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

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.FastThreadLocal;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.Getter;
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
     * When reaching the max group size, an ack command is sent out immediately.
     */
    private static final int MAX_ACK_GROUP_SIZE = 1000;

    private final ConsumerImpl<?> consumer;

    private final long acknowledgementGroupTimeMicros;

    private volatile TimedCompletableFuture<Void> currentIndividualAckFuture;
    private volatile TimedCompletableFuture<Void> currentCumulativeAckFuture;

    private final LastCumulativeAck lastCumulativeAck = new LastCumulativeAck();

    // When we flush the command, we should ensure current ack request will send correct
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

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
        if (!(messageId instanceof MessageIdImpl)) {
            throw new IllegalArgumentException("isDuplicated cannot accept "
                    + messageId.getClass().getName() + ": " + messageId);
        }
        if (lastCumulativeAck.compareTo(messageId) >= 0) {
            // Already included in a cumulative ack
            return true;
        } else {
            final MessageIdImpl key = (messageId instanceof BatchMessageIdImpl)
                    ? ((BatchMessageIdImpl) messageId).toMessageIdImpl()
                    : (MessageIdImpl) messageId;
            // If "batchIndexAckEnabled" is false, the batched messages acknowledgment will be traced by
            // pendingIndividualAcks. So no matter what type the message ID is, check with "pendingIndividualAcks"
            // first.
            if (pendingIndividualAcks.contains(key)) {
                return true;
            }
            if (messageId instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
                ConcurrentBitSetRecyclable bitSet = pendingIndividualBatchIndexAcks.get(key);
                return bitSet != null && !bitSet.get(batchMessageId.getBatchIndex());
            }
            return false;
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
            if (messageId instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
                addIndividualAcknowledgment(batchMessageId.toMessageIdImpl(),
                        batchMessageId,
                        this::doIndividualAckAsync,
                        this::doIndividualBatchAckAsync);
            } else if (messageId instanceof MessageIdImpl) {
                addIndividualAcknowledgment((MessageIdImpl) messageId,
                        null,
                        this::doIndividualAckAsync,
                        this::doIndividualBatchAckAsync);
            } else {
                throw new IllegalStateException("Unsupported message id type in addListAcknowledgement: "
                        + messageId.getClass().getCanonicalName());
            }
        }
    }

    @Override
    public CompletableFuture<Void> addAcknowledgment(MessageIdImpl msgId, AckType ackType,
                                                     Map<String, Long> properties) {
        if (msgId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) msgId;
            return addAcknowledgment(batchMessageId.toMessageIdImpl(), ackType, properties, batchMessageId);
        } else {
            return addAcknowledgment(msgId, ackType, properties, null);
        }
    }

    private CompletableFuture<Void> addIndividualAcknowledgment(
            MessageIdImpl msgId,
            @Nullable BatchMessageIdImpl batchMessageId,
            Function<MessageIdImpl, CompletableFuture<Void>> individualAckFunction,
            Function<BatchMessageIdImpl, CompletableFuture<Void>> batchAckFunction) {
        if (batchMessageId != null) {
            consumer.onAcknowledge(batchMessageId, null);
        } else {
            consumer.onAcknowledge(msgId, null);
        }
        if (batchMessageId == null || batchMessageId.ackIndividual()) {
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

    private CompletableFuture<Void> addAcknowledgment(MessageIdImpl msgId,
                                                      AckType ackType,
                                                      Map<String, Long> properties,
                                                      @Nullable BatchMessageIdImpl batchMessageId) {
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
                if (batchMessageId == null || batchMessageId.ackCumulative()) {
                    return doCumulativeAck(msgId, properties, null);
                } else if (batchIndexAckEnabled) {
                    return doCumulativeBatchIndexAck(batchMessageId, properties);
                } else {
                    if (!batchMessageId.getAcker().isPrevBatchCumulativelyAcked()) {
                        doCumulativeAck(batchMessageId.prevBatchMessageId(), properties, null);
                        batchMessageId.getAcker().setPrevBatchCumulativelyAcked(true);
                    }
                    return CompletableFuture.completedFuture(null);
                }
            default:
                throw new IllegalStateException("Unknown AckType: " + ackType);
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


    private CompletableFuture<Void> doIndividualAckAsync(MessageIdImpl messageId) {
        pendingIndividualAcks.add(messageId);
        pendingIndividualBatchIndexAcks.remove(messageId);
        return CompletableFuture.completedFuture(null);
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

    private CompletableFuture<Void> doIndividualBatchAckAsync(BatchMessageIdImpl batchMessageId) {
        ConcurrentBitSetRecyclable bitSet = pendingIndividualBatchIndexAcks.computeIfAbsent(
                batchMessageId.toMessageIdImpl(), __ -> {
                    ConcurrentBitSetRecyclable value;
                    if (batchMessageId.getAcker() != null
                            && !(batchMessageId.getAcker() instanceof BatchMessageAckerDisabled)) {
                        value = ConcurrentBitSetRecyclable.create(batchMessageId.getAcker().getBitSet());
                    } else {
                        value = ConcurrentBitSetRecyclable.create();
                        value.set(0, batchMessageId.getOriginalBatchSize());
                    }
                    return value;
                });
        bitSet.clear(batchMessageId.getBatchIndex());
        return CompletableFuture.completedFuture(null);
    }

    private void doCumulativeAckAsync(MessageIdImpl msgId, BitSetRecyclable bitSet) {
        // Handle concurrent updates from different threads
        lastCumulativeAck.update(msgId, bitSet);
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

    private CompletableFuture<Void> doImmediateBatchIndexAck(BatchMessageIdImpl msgId, int batchIndex, int batchSize,
                                                             AckType ackType, Map<String, Long> properties) {
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
        final LastCumulativeAck lastCumulativeAckToFlush = lastCumulativeAck.flush();
        boolean shouldFlush = false;
        if (lastCumulativeAckToFlush != null) {
            shouldFlush = true;
            final MessageIdImpl messageId = lastCumulativeAckToFlush.getMessageId();
            newMessageAckCommandAndWrite(cnx, consumer.consumerId, messageId.getLedgerId(), messageId.getEntryId(),
                    lastCumulativeAckToFlush.getBitSetRecyclable(), AckType.Cumulative, null,
                    Collections.emptyMap(), false, this.currentCumulativeAckFuture, null);
            this.consumer.unAckedChunkedMessageIdSequenceMap.remove(messageId);
        }

        // Flush all individual acks
        List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck =
                new ArrayList<>(pendingIndividualAcks.size() + pendingIndividualBatchIndexAcks.size());
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
                    newMessageAckCommandAndWrite(cnx, consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(),
                            null, AckType.Individual, null, Collections.emptyMap(), false,
                            null, null);
                    shouldFlush = true;
                }
            }
        }

        if (!pendingIndividualBatchIndexAcks.isEmpty()) {
            Iterator<Map.Entry<MessageIdImpl, ConcurrentBitSetRecyclable>> iterator =
                    pendingIndividualBatchIndexAcks.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<MessageIdImpl, ConcurrentBitSetRecyclable> entry = iterator.next();
                entriesToAck.add(Triple.of(entry.getKey().ledgerId, entry.getKey().entryId, entry.getValue()));
                iterator.remove();
            }
        }

        if (entriesToAck.size() > 0) {

            newMessageAckCommandAndWrite(cnx, consumer.consumerId, 0L, 0L,
                    null, AckType.Individual, null, null, true,
                    currentIndividualAckFuture, entriesToAck);
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

    private CompletableFuture<Void> newMessageAckCommandAndWrite(
            ClientCnx cnx, long consumerId, long ledgerId,
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
    public static final MessageIdImpl DEFAULT_MESSAGE_ID = (MessageIdImpl) MessageIdImpl.earliest;

    private volatile MessageIdImpl messageId = DEFAULT_MESSAGE_ID;
    private BitSetRecyclable bitSetRecyclable = null;
    private boolean flushRequired = false;

    public synchronized void update(final MessageIdImpl messageId, final BitSetRecyclable bitSetRecyclable) {
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

    public synchronized int compareTo(MessageId messageId) {
        if (this.messageId instanceof BatchMessageIdImpl && (!(messageId instanceof BatchMessageIdImpl))) {
            final BatchMessageIdImpl lhs = (BatchMessageIdImpl) this.messageId;
            final MessageIdImpl rhs = (MessageIdImpl) messageId;
            return MessageIdImpl.messageIdCompare(
                    lhs.getLedgerId(), lhs.getEntryId(), lhs.getPartitionIndex(), lhs.getBatchIndex(),
                    rhs.getLedgerId(), rhs.getEntryId(), rhs.getPartitionIndex(), Integer.MAX_VALUE);
        } else if (messageId instanceof BatchMessageIdImpl && (!(this.messageId instanceof BatchMessageIdImpl))){
            final MessageIdImpl lhs = this.messageId;
            final BatchMessageIdImpl rhs = (BatchMessageIdImpl) messageId;
            return MessageIdImpl.messageIdCompare(
                    lhs.getLedgerId(), lhs.getEntryId(), lhs.getPartitionIndex(), Integer.MAX_VALUE,
                    rhs.getLedgerId(), rhs.getEntryId(), rhs.getPartitionIndex(), rhs.getBatchIndex());
        } else {
            return this.messageId.compareTo(messageId);
        }
    }

    private synchronized void set(final MessageIdImpl messageId, final BitSetRecyclable bitSetRecyclable) {
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
