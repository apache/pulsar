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
import io.netty.util.Recycler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentBitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Group the acknowledgements for a certain time and then sends them out in a single protobuf command.
 * <>
 *     If send command successful then it will return response. But It does not mean that the message has been
 *     sent to you again.
 *     If you want to know why, you can learn about pulsar ack markDelete mechanism.
 * </>
 */
@Slf4j
public class PersistentAcknowledgmentsWithResponseGroupingTracker implements AcknowledgmentsGroupingTracker {

    /**
     * When reaching the max group size, an ack command is sent out immediately
     */
    private static final int MAX_ACK_GROUP_SIZE = 1000;

    private final ConsumerImpl<?> consumer;

    private final long acknowledgementGroupTimeMicros;

    private volatile LastCumulativeAck lastCumulativeAck;
    /**
     * Latest cumulative ack sent to broker
     */
    private volatile boolean cumulativeAckFlushRequired = false;

    private static final AtomicReferenceFieldUpdater<PersistentAcknowledgmentsWithResponseGroupingTracker, LastCumulativeAck> LAST_CUMULATIVE_ACK = AtomicReferenceFieldUpdater
            .newUpdater(PersistentAcknowledgmentsWithResponseGroupingTracker.class, LastCumulativeAck.class, "lastCumulativeAck");

    // When we flush the command, we should ensure current ack request will send correct
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile ConcurrentOpenHashSet<CompletableFuture<Void>> currentCumulativeAckRequests;

    private final boolean batchIndexAckEnabled;

    private volatile ConcurrentSkipListMap<MessageIdImpl, CompletableFuture<Void>> pendingIndividualAcks;
    private volatile ConcurrentHashMap<MessageIdImpl, MutablePair<List<CompletableFuture<Void>>, ConcurrentBitSetRecyclable>> pendingIndividualBatchIndexAcks;

    private final ScheduledFuture<?> scheduledTask;

    private final EventLoopGroup eventExecutors;

    public PersistentAcknowledgmentsWithResponseGroupingTracker(ConsumerImpl<?> consumer,
                                                                ConsumerConfigurationData<?> conf,
                                                                EventLoopGroup eventLoopGroup) {
        this.consumer = consumer;
        this.acknowledgementGroupTimeMicros = conf.getAcknowledgementsGroupTimeMicros();
        this.pendingIndividualAcks = new ConcurrentSkipListMap<>();
        this.pendingIndividualBatchIndexAcks = new ConcurrentHashMap<>();
        this.currentCumulativeAckRequests = new ConcurrentOpenHashSet<>();
        this.lastCumulativeAck = LastCumulativeAck.create((MessageIdImpl) MessageIdImpl.earliest, null);
        this.eventExecutors = eventLoopGroup.next();
        this.batchIndexAckEnabled = conf.isBatchIndexAckEnabled();
        if (acknowledgementGroupTimeMicros > 0) {
            this.scheduledTask = eventLoopGroup.next().scheduleWithFixedDelay(this::flush, acknowledgementGroupTimeMicros,
                    acknowledgementGroupTimeMicros, TimeUnit.MICROSECONDS);
        } else {
            this.scheduledTask = null;
        }
    }

    /**
     * Since the ack are delayed, we need to do some best-effort duplicate check to discard messages that are being
     * resent after a disconnection and for which the user has already sent an acknowledgement.
     */
    @Override
    public boolean isDuplicate(MessageId messageId) {
        if (messageId != null) {
            if (messageId.compareTo(this.lastCumulativeAck.messageId) <= 0) {
                // Already included in a cumulative ack
                return true;
            } else if (this.pendingIndividualAcks != null){
                return pendingIndividualAcks.containsKey(messageId);
            }
        }
        return false;
    }

    @Override
    public CompletableFuture<Void> addListAcknowledgment(List<MessageIdImpl> messageIds,
                                                         AckType ackType, Map<String, Long> properties) {
        List<CompletableFuture<Void>> completableFutureList = new ArrayList<>(messageIds.size());
        if (ackType == AckType.Cumulative) {
            messageIds.forEach(messageId -> completableFutureList.add(doCumulativeAck(messageId, null)));
        } else {
            try {
                this.lock.readLock().lock();
                messageIds.forEach(messageId -> {
                    CompletableFuture<Void> completableFuture = addAcknowledgmentSingleMessageId(messageId);
                    completableFutureList.add(completableFuture);
                });
            } finally {
                this.lock.readLock().unlock();
            }
        }
        if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
            eventExecutors.execute(this::flush);
        }

        if (acknowledgementGroupTimeMicros == 0) {
            eventExecutors.execute(this::flush);
        }
        return FutureUtil.waitForAll(completableFutureList);
    }

    @Override
    public CompletableFuture<Void> addAcknowledgment(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties,
                                  TransactionImpl txn) {
        if (acknowledgementGroupTimeMicros == 0 || !properties.isEmpty()) {
            return doImmediateAck(msgId, ackType, properties);
        } else if (ackType == AckType.Cumulative) {
            return doCumulativeAck(msgId, null);
        } else {
            CompletableFuture<Void> completableFuture;
            try {
                this.lock.readLock().lock();
                completableFuture = addAcknowledgmentSingleMessageId(msgId);
            } finally {
                this.lock.readLock().unlock();
            }
            if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                eventExecutors.execute(this::flush);
            }
            return completableFuture;
        }
    }

    private CompletableFuture<Void> addAcknowledgmentSingleMessageId(MessageIdImpl msgId) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        if (msgId instanceof BatchMessageIdImpl) {
            msgId = new MessageIdImpl(msgId.getLedgerId(),
                    msgId.getEntryId(), msgId.getPartitionIndex());

        }

        msgId = new MessageIdImpl(msgId.getLedgerId(),
                msgId.getEntryId(), msgId.getPartitionIndex());
        MutablePair<List<CompletableFuture<Void>>, ConcurrentBitSetRecyclable> mutablePair =
                pendingIndividualBatchIndexAcks.remove(msgId);
        if (mutablePair != null) {
            completableFuture.whenComplete((v, e) -> {
                if (e != null) {
                    mutablePair.getLeft().forEach(future -> future.complete(null));
                } else {
                    mutablePair.getLeft().forEach(future -> future.complete(null));
                }
            });
        }
        return completableFuture;
    }



    @Override
    public CompletableFuture<Void> addBatchIndexAcknowledgment(BatchMessageIdImpl msgId, int batchIndex, int batchSize,
                                                               AckType ackType, Map<String, Long> properties,
                                                               TransactionImpl txn) {
        if (acknowledgementGroupTimeMicros == 0 || !properties.isEmpty()) {
            return doImmediateBatchIndexAck(msgId, batchIndex, batchSize, ackType, properties);
        } else if (ackType == AckType.Cumulative) {
            BitSetRecyclable bitSet = BitSetRecyclable.create();
            bitSet.set(0, batchSize);
            bitSet.clear(0, batchIndex + 1);
            return doCumulativeAck(msgId, bitSet);
        } else if (ackType == AckType.Individual) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            try {
                this.lock.readLock().lock();
                MutablePair<List<CompletableFuture<Void>>, ConcurrentBitSetRecyclable> batchPair =
                        pendingIndividualBatchIndexAcks.computeIfAbsent(
                                new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId(), msgId.getPartitionIndex()),
                                (v) -> {
                                    MutablePair<List<CompletableFuture<Void>>,
                                            ConcurrentBitSetRecyclable> mutablePair = new MutablePair<>();
                                    List<CompletableFuture<Void>> list =
                                            Collections.synchronizedList(new ArrayList<>());
                                    if (msgId.getAcker() != null &&
                                            !(msgId.getAcker() instanceof BatchMessageAckerDisabled)) {
                                        mutablePair.setRight(ConcurrentBitSetRecyclable
                                                .create(msgId.getAcker().getBitSet()));
                                    } else {
                                        mutablePair.setRight(ConcurrentBitSetRecyclable
                                                .create(ConcurrentBitSetRecyclable.create()));
                                        ConcurrentBitSetRecyclable value = ConcurrentBitSetRecyclable.create();
                                        value.set(0, batchSize);
                                        mutablePair.setRight(ConcurrentBitSetRecyclable
                                                .create(value));
                                    }
                                    mutablePair.setLeft(list);
                                    return mutablePair;
                        });
                batchPair.getLeft().add(completableFuture);
                batchPair.getRight().clear(batchIndex);
            } finally {
                this.lock.readLock().unlock();
            }

            if (pendingIndividualBatchIndexAcks.size() >= MAX_ACK_GROUP_SIZE) {
                eventExecutors.execute(this::flush);
            }
            return completableFuture;
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> doCumulativeAck(MessageIdImpl msgId, BitSetRecyclable bitSet) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        LastCumulativeAck cumulativeAck = LastCumulativeAck.create(msgId, bitSet);
        try {
            this.lock.readLock().lock();
            this.currentCumulativeAckRequests.add(completableFuture);
            while (true) {
                LastCumulativeAck lastCumulativeAck = this.lastCumulativeAck;
                if (msgId.compareTo(lastCumulativeAck.messageId) > 0) {
                    if (LAST_CUMULATIVE_ACK.compareAndSet(this, lastCumulativeAck, cumulativeAck)) {
                        lastCumulativeAck.recycle();
                        cumulativeAckFlushRequired = true;
                        break;
                    }
                } else {
                    cumulativeAck.recycle();
                    break;
                }
            }
        } finally {
            this.lock.readLock().unlock();
        }
        return completableFuture;
    }

    private CompletableFuture<Void> doImmediateAck(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .ConnectException("Ack messageId : [" + msgId + "] fail! Consumer is not connected!"));
        }
        MessageIdImpl[] chunkMsgIds = this.consumer.unAckedChunkedMessageIdSequenceMap.get(msgId);
        if (ackType == AckType.Cumulative) {
            return doImmediateCumulativeAck(chunkMsgIds, msgId, properties, cnx);
        } else {
            return doImmediateIndividualAck(chunkMsgIds, msgId, properties, cnx);
        }

    }

    private CompletableFuture<Void> doImmediateCumulativeAck(MessageIdImpl[] chunkMsgIds, MessageIdImpl msgId,
                                                             Map<String, Long> properties, ClientCnx cnx) {
        if (chunkMsgIds != null) {
            List<CompletableFuture<Void>> completableFutureList = new ArrayList<>(chunkMsgIds.length);
            for (MessageIdImpl cMsgId : chunkMsgIds) {
                completableFutureList.add(singleMessageAckCommand(cMsgId, properties, cnx, AckType.Cumulative));
            }
            this.consumer.unAckedChunkedMessageIdSequenceMap.remove(msgId);
            return FutureUtil.waitForAll(completableFutureList);
        } else {
            return singleMessageAckCommand(msgId, properties, cnx, AckType.Cumulative);
        }
    }

    private CompletableFuture<Void> doImmediateIndividualAck(MessageIdImpl[] chunkMsgIds, MessageIdImpl msgId,
                                                             Map<String, Long> properties, ClientCnx cnx) {
        if (chunkMsgIds != null) {
            long requestId = consumer.getClient().newRequestId();
            List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck = new ArrayList<>(chunkMsgIds.length);
            for (MessageIdImpl cMsgId : chunkMsgIds) {
                if (cMsgId != null && chunkMsgIds.length > 1) {
                    entriesToAck.add(Triple.of(cMsgId.getLedgerId(), cMsgId.getEntryId(), null));
                }
            }
            ByteBuf cmd = Commands.newMultiMessageAck(consumer.consumerId, entriesToAck, requestId);
            CompletableFuture<Void> completableFuture = cnx.newAckForResponse(cmd, requestId, true);
            this.consumer.unAckedChunkedMessageIdSequenceMap.remove(msgId);
            return completableFuture;
        } else {
            return singleMessageAckCommand(msgId, properties, cnx, AckType.Individual);
        }
    }

    private CompletableFuture<Void> singleMessageAckCommand(MessageIdImpl msgId, Map<String, Long> properties,
                                                                 ClientCnx cnx, AckType ackType) {
        long requestId = consumer.getClient().newRequestId();
        ByteBuf cmd = Commands.newAck(consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(), null,
                ackType, null, properties, -1, -1, requestId);
        return cnx.newAckForResponse(cmd, requestId, true);
    }

    private CompletableFuture<Void> doImmediateBatchIndexAck(BatchMessageIdImpl msgId, int batchIndex, int batchSize,
                                                             AckType ackType, Map<String, Long> properties) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .ConnectException("Ack messageId : [" + msgId + "] fail! Consumer is not connected!"));
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
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        long requestId = consumer.getClient().newRequestId();
        final ByteBuf cmd = Commands.newAck(consumer.consumerId, msgId.ledgerId, msgId.entryId, bitSet, ackType,
                null, properties, -1, -1, requestId);
        bitSet.recycle();
        cnx.newAckForResponse(cmd,requestId, true);
        return completableFuture;
    }

    /**
     * Flush all the pending acks and send them to the broker
     */
    @Override
    public void flush() {
        ClientCnx cnx = consumer.getClientCnx();
        final ConcurrentOpenHashSet<CompletableFuture<Void>> currentCumulativeAckRequests;
        final ConcurrentSkipListMap<MessageIdImpl, CompletableFuture<Void>> pendingIndividualAcks;
        final ConcurrentHashMap<MessageIdImpl, MutablePair<List<CompletableFuture<Void>>,
                ConcurrentBitSetRecyclable>> pendingIndividualBatchIndexAcks;
        final LastCumulativeAck lastCumulativeAck;
        try {
            this.lock.writeLock().lock();
            if (!this.currentCumulativeAckRequests.isEmpty()) {
                currentCumulativeAckRequests = this.currentCumulativeAckRequests;
                this.currentCumulativeAckRequests = new ConcurrentOpenHashSet<>();
            } else {
                currentCumulativeAckRequests = null;
            }
            lastCumulativeAck = this.lastCumulativeAck;

            if (!this.pendingIndividualAcks.isEmpty()) {
                pendingIndividualAcks = this.pendingIndividualAcks;
                this.pendingIndividualAcks = new ConcurrentSkipListMap<>();
            } else {
                pendingIndividualAcks = null;
            }

            if (!this.pendingIndividualBatchIndexAcks.isEmpty() && batchIndexAckEnabled) {
                pendingIndividualBatchIndexAcks = this.pendingIndividualBatchIndexAcks;
                this.pendingIndividualBatchIndexAcks = new ConcurrentHashMap<>();
            } else {
                pendingIndividualBatchIndexAcks = null;
            }
        } finally {
            this.lock.writeLock().unlock();
        }

        if (cnx == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cannot flush pending acks since we're not connected to broker", consumer);
            }
            return;
        }

        boolean shouldFlush = false;
        if (cumulativeAckFlushRequired) {
            final ByteBuf cmd;
            long requestId = consumer.getClient().newRequestId();
            cmd = Commands.newAck(consumer.consumerId, lastCumulativeAck.messageId.getLedgerId(),
                    lastCumulativeAck.messageId.getEntryId(), lastCumulativeAck.bitSetRecyclable,
                    AckType.Cumulative, null, Collections.emptyMap(),
                    -1, -1, requestId);

            CompletableFuture<Void> completableFuture = cnx.newAckForResponse(cmd, requestId, false);
            completableFuture.whenComplete((v ,e) -> {
                if (currentCumulativeAckRequests != null) {
                    if (e != null) {
                        currentCumulativeAckRequests.forEach(callback -> callback.completeExceptionally(e));
                    } else {
                        currentCumulativeAckRequests.forEach(callback -> callback.complete(null));
                    }
                }
            });
            shouldFlush = true;
            cumulativeAckFlushRequired = false;
        }

        // Flush all individual acks
        final List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck;
        int pendingIndividualAcksSize = pendingIndividualAcks == null ? 0 : pendingIndividualAcks.size();
        int pendingIndividualBatchIndexAcksSize =
                pendingIndividualBatchIndexAcks == null ? 0 : pendingIndividualBatchIndexAcks.size();
            if (batchIndexAckEnabled) {
                entriesToAck = new ArrayList<>(
                        pendingIndividualAcksSize + pendingIndividualBatchIndexAcksSize);
            } else {
                entriesToAck = new ArrayList<>(
                        pendingIndividualAcksSize);
            }
        if (pendingIndividualAcks != null && !pendingIndividualAcks.isEmpty()) {
            pendingIndividualAcks.forEach((k ,v) -> {
                MessageIdImpl[] chunkMsgIds = this.consumer.unAckedChunkedMessageIdSequenceMap.get(k);
                if (chunkMsgIds != null && chunkMsgIds.length > 1) {
                    for (MessageIdImpl cMsgId : chunkMsgIds) {
                        if (cMsgId != null) {
                            entriesToAck.add(Triple.of(cMsgId.getLedgerId(), cMsgId.getEntryId(), null));
                        }
                    }
                    this.consumer.unAckedChunkedMessageIdSequenceMap.remove(k);
                } else {
                    entriesToAck.add(Triple.of(k.getLedgerId(), k.getEntryId(), null));
                }
            });
        }

        if (pendingIndividualBatchIndexAcks != null && !pendingIndividualBatchIndexAcks.isEmpty()
                && batchIndexAckEnabled) {
            pendingIndividualBatchIndexAcks.forEach((k, v) ->
                    entriesToAck.add(Triple.of(k.ledgerId, k.entryId, v.getRight())));
        }

        if (entriesToAck.size() > 0) {
            long requestId = consumer.getClient().newRequestId();
            CompletableFuture<Void> completableFuture = cnx.newAckForResponse(Commands
                    .newMultiMessageAck(consumer.consumerId, entriesToAck, requestId), requestId, false);
            completableFuture
                    .whenComplete((q, e) -> {
                        if (e != null) {
                            if (pendingIndividualAcks != null) {
                                pendingIndividualAcks.forEach((k, v) -> v.completeExceptionally(e));
                            }
                            if (pendingIndividualBatchIndexAcks != null && batchIndexAckEnabled) {
                                pendingIndividualBatchIndexAcks.forEach((k, v) ->
                                        v.getLeft().forEach(future -> {
                                            future.completeExceptionally(e);
                                        }));
                            }
                        } else {
                            if (pendingIndividualAcks != null) {
                                pendingIndividualAcks.forEach((k, v) -> v.complete(null));
                            }
                            if (pendingIndividualBatchIndexAcks != null && batchIndexAckEnabled) {
                                pendingIndividualBatchIndexAcks.forEach((k, v) ->
                                        v.getLeft().forEach(future -> {
                                            future.complete(null);
                                        }));
                            }
                        }
                    });
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
