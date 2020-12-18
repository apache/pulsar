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
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.AckResponseTimeoutException;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentBitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
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
public class PersistentAcknowledgmentsWithResponseGroupingTracker implements AcknowledgmentsGroupingTracker, TimerTask {

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

    private final Timer timer;

    private final long intervalTime;

    private volatile boolean isCloseTimer = false;

    // When we flush the command, we should ensure current ack request will send correct
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // when flush cumulative and individual ack it need different requestId, so we should distinguish the different ack
    // type
    private final ConcurrentOpenHashSet<OpForAckCallBack> currentIndividualAckRequest;
    private final ConcurrentOpenHashSet<OpForAckCallBack> currentCumulativeAckRequest;
    // When send a ack command we add the request to ackFlushRequest
    private final ConcurrentLongHashMap<List<OpForAckCallBack>> ackFlushRequests;
    // if immediate ack we will only carry one  OpForAckCallBack
    private final ConcurrentLongHashMap<OpForAckCallBack> ackImmediateRequests;
    private final ConcurrentLinkedDeque<OpForAckResponseTimeout> timeoutDeque;

    /**
     * This is a set of all the individual acks that the application has issued and that were not already sent to
     * broker.
     */
    private final ConcurrentSkipListSet<MessageIdImpl> pendingIndividualAcks;
    private final ConcurrentHashMap<MessageIdImpl, ConcurrentBitSetRecyclable> pendingIndividualBatchIndexAcks;

    private final ScheduledFuture<?> scheduledTask;
    // when we need ack response, we should async process flush
    private final EventLoopGroup eventExecutors;

    public PersistentAcknowledgmentsWithResponseGroupingTracker(ConsumerImpl<?> consumer,
                                                                ConsumerConfigurationData<?> conf,
                                                                EventLoopGroup eventLoopGroup) {
        this.consumer = consumer;
        this.timeoutDeque = new ConcurrentLinkedDeque<>();
        this.ackImmediateRequests = new ConcurrentLongHashMap<>();
        this.acknowledgementGroupTimeMicros = conf.getAcknowledgementsGroupTimeMicros();
        this.pendingIndividualAcks = new ConcurrentSkipListSet<>();
        this.pendingIndividualBatchIndexAcks = new ConcurrentHashMap<>();
        this.currentIndividualAckRequest = new ConcurrentOpenHashSet<>();
        this.currentCumulativeAckRequest = new ConcurrentOpenHashSet<>();
        this.ackFlushRequests = new ConcurrentLongHashMap<>();
        this.intervalTime = conf.getAckResponseTimeout();
        this.lastCumulativeAck = LastCumulativeAck.create((MessageIdImpl) MessageIdImpl.earliest, null);
        this.timer = consumer.getClient().timer();
        this.timer.newTimeout(this, intervalTime, TimeUnit.MILLISECONDS);
        this.eventExecutors = eventLoopGroup.next();
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
                return pendingIndividualAcks.contains(messageId);
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
                    if (messageId instanceof BatchMessageIdImpl) {
                        BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
                        pendingIndividualAcks.add(new MessageIdImpl(batchMessageId.getLedgerId(),
                                batchMessageId.getEntryId(), batchMessageId.getPartitionIndex()));
                    } else {
                        pendingIndividualAcks.add(messageId);
                    }
                    pendingIndividualBatchIndexAcks.remove(messageId);
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    completableFutureList.add(completableFuture);
                    this.currentIndividualAckRequest.add(OpForAckCallBack.create(completableFuture, messageId));
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
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            try {
                this.lock.readLock().lock();
                if (msgId instanceof BatchMessageIdImpl) {
                    MessageIdImpl messageId = new MessageIdImpl(msgId.getLedgerId(),
                            msgId.getEntryId(), msgId.getPartitionIndex());
                    pendingIndividualAcks.add(messageId);
                    currentIndividualAckRequest.add(OpForAckCallBack.create(completableFuture, messageId));
                } else {
                    pendingIndividualAcks.add(msgId);
                    currentIndividualAckRequest.add(OpForAckCallBack.create(completableFuture, msgId));
                }

                pendingIndividualBatchIndexAcks.remove(msgId);
            } finally {
                this.lock.readLock().unlock();
            }
            if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                eventExecutors.execute(this::flush);
            }
            return completableFuture;
        }
    }



    @Override
    public CompletableFuture<Void> addBatchIndexAcknowledgment(BatchMessageIdImpl msgId, int batchIndex, int batchSize, AckType ackType,
                                            Map<String, Long> properties, TransactionImpl txn) {
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
                ConcurrentBitSetRecyclable bitSet;
                bitSet = pendingIndividualBatchIndexAcks.computeIfAbsent(
                        new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId(), msgId.getPartitionIndex()), (v) -> {
                            ConcurrentBitSetRecyclable value;
                            if (msgId.getAcker() != null && !(msgId.getAcker() instanceof BatchMessageAckerDisabled)) {
                                value = ConcurrentBitSetRecyclable.create(msgId.getAcker().getBitSet());
                            } else {
                                value = ConcurrentBitSetRecyclable.create();
                                value.set(0, batchSize);
                            }
                            return value;
                        });
                bitSet.clear(batchIndex);
                this.currentIndividualAckRequest.add(OpForAckCallBack.create(completableFuture, msgId));
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

    @Override
    public void ackReceipt(long requestId) {
        if (acknowledgementGroupTimeMicros > 0) {
            List<OpForAckCallBack> callBacks = this.ackFlushRequests.remove(requestId);
            if (callBacks != null) {
                for (OpForAckCallBack callBack : callBacks) {
                    callBack.callBackFuture.complete(null);
                    callBack.recycle();
                }
            }
        }

        OpForAckCallBack callBack = this.ackImmediateRequests.remove(requestId);
        if (callBack != null) {
            callBack.callBackFuture.complete(null);
            callBack.recycle();
        }
    }

    @Override
    public void ackError(long requestId, PulsarClientException pulsarClientException) {

        if (acknowledgementGroupTimeMicros > 0) {
            List<OpForAckCallBack> callBacks = this.ackFlushRequests.remove(requestId);
            if (callBacks != null) {
                for (OpForAckCallBack callBack : callBacks) {
                    callBack.callBackFuture.completeExceptionally(pulsarClientException);
                    callBack.recycle();
                }
            }
        }

        OpForAckCallBack callBack = this.ackImmediateRequests.remove(requestId);
        if (callBack != null) {
            callBack.callBackFuture.completeExceptionally(pulsarClientException);
            callBack.recycle();
        }
    }

    private CompletableFuture<Void> doCumulativeAck(MessageIdImpl msgId, BitSetRecyclable bitSet) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        LastCumulativeAck cumulativeAck = LastCumulativeAck.create(msgId, bitSet);
        try {
            this.lock.readLock().lock();
            this.currentCumulativeAckRequest.add(OpForAckCallBack.create(completableFuture, msgId));
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
            CompletableFuture<Void> completableFuture = addRequest(requestId, msgId, cnx, cmd);
            this.consumer.unAckedChunkedMessageIdSequenceMap.remove(msgId);
            return CompletableFuture.completedFuture(null);
        } else {
            return singleMessageAckCommand(msgId, properties, cnx, AckType.Individual);
        }
    }

    private CompletableFuture<Void> singleMessageAckCommand(MessageIdImpl msgId, Map<String, Long> properties,
                                                                 ClientCnx cnx, AckType ackType) {
        long requestId = consumer.getClient().newRequestId();
        ByteBuf cmd = Commands.newAck(consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(), null,
                ackType, null, properties, -1, -1, requestId);
        return addRequest(requestId, msgId, cnx, cmd);
    }

    private CompletableFuture<Void> addRequest(long requestId, MessageId msgId, ClientCnx cnx, ByteBuf cmd) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        this.ackImmediateRequests.put(requestId, OpForAckCallBack.create(completableFuture, msgId));
        this.timeoutDeque.add(OpForAckResponseTimeout
                .create(System.currentTimeMillis() + intervalTime, requestId));
        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
        return completableFuture;
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
        this.ackImmediateRequests.put(requestId, OpForAckCallBack.create(completableFuture, msgId));
        this.timeoutDeque.add(OpForAckResponseTimeout
                .create(System.currentTimeMillis() + intervalTime, requestId));
        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
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

        boolean shouldFlush = false;
        boolean shouldWriteIndividualAck = false;
        if (cumulativeAckFlushRequired) {
            final ByteBuf cmd;
            try {
                this.lock.writeLock().lock();
                long requestId = consumer.getClient().newRequestId();
                cmd = Commands.newAck(consumer.consumerId, lastCumulativeAck.messageId.getLedgerId(),
                        lastCumulativeAck.messageId.getEntryId(), lastCumulativeAck.bitSetRecyclable,
                        AckType.Cumulative, null, Collections.emptyMap(),
                        -1, -1, requestId);

                currentCumulativeAckRequest.values();
                this.ackFlushRequests.put(requestId, currentCumulativeAckRequest.values());
                currentCumulativeAckRequest.clear();
                this.timeoutDeque.add(OpForAckResponseTimeout
                        .create(System.currentTimeMillis() + intervalTime, requestId));
            } finally {
                this.lock.writeLock().unlock();
            }
            cnx.ctx().write(cmd, cnx.ctx().voidPromise());
            shouldFlush = true;
            cumulativeAckFlushRequired = false;
        }

        // Flush all individual acks
        final List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck;
        final List<OpForAckCallBack> callBacks;
        try {
            this.lock.writeLock().lock();
            entriesToAck = new ArrayList<>(
                    pendingIndividualAcks.size() + pendingIndividualBatchIndexAcks.size());
            if (!pendingIndividualAcks.isEmpty()) {
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
                callBacks = this.currentIndividualAckRequest.values();
                this.currentIndividualAckRequest.clear();
                shouldWriteIndividualAck = true;
            } else {
                callBacks = null;
            }
        } finally {
            this.lock.writeLock().unlock();
        }

        if (shouldWriteIndividualAck) {
            long requestId = consumer.getClient().newRequestId();
            ackFlushRequests.put(requestId, callBacks);
            this.timeoutDeque.add(OpForAckResponseTimeout
                    .create(System.currentTimeMillis() + intervalTime, requestId));
            cnx.ctx().write(Commands.newMultiMessageAck(consumer.consumerId, entriesToAck, requestId),
                    cnx.ctx().voidPromise());
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
        // we will async close the timer because any lock in the timer, so we can't get the true timeoutTask
        this.isCloseTimer = true;
        if (scheduledTask != null && !scheduledTask.isCancelled()) {
            scheduledTask.cancel(true);
        }
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        while (true) {
            if (isCloseTimer) {
                break;
            }
            OpForAckResponseTimeout op = timeoutDeque.peekFirst();
            if (op == null) {
                this.timer.newTimeout(this, intervalTime, TimeUnit.MILLISECONDS);
                break;
            }
            long timeoutTime = op.timeout - System.currentTimeMillis();
            if (timeoutTime <= 0) {
                // we don't need to lock this logical, we should handle every timeout, if the callBack not in
                // ackFlushRequests and ackImmediateRequests when timeout, mean that callBack have handle by response
                List<OpForAckCallBack> callBacks = ackFlushRequests.remove(op.requestId);
                if (callBacks != null) {
                    for (OpForAckCallBack callBack : callBacks) {
                        callBack.callBackFuture.completeExceptionally(new AckResponseTimeoutException(
                                "Ack messageId messageId : [" + callBack.messageId + "] timeout!"));
                        callBack.recycle();
                    }
                }
                OpForAckCallBack callBack = ackImmediateRequests.remove(op.requestId);
                if (callBack != null) {
                    callBack.callBackFuture.completeExceptionally(new AckResponseTimeoutException(
                            "Ack messageId messageId : [" + callBack.messageId + "] timeout!"));
                    callBack.recycle();
                }
                timeoutDeque.removeFirst();
                op.recycle();
            } else {
                this.timer.newTimeout(this, timeoutTime, TimeUnit.MILLISECONDS);
                break;
            }
        }
    }

    private static class OpForAckCallBack {
        protected CompletableFuture<Void> callBackFuture;
        protected MessageIdImpl messageId;
        static OpForAckCallBack create(CompletableFuture<Void> callback, MessageId messageId) {
            OpForAckCallBack op = RECYCLER.get();
            op.callBackFuture = callback;
            op.messageId = (MessageIdImpl) messageId;
            return op;
        }

        private OpForAckCallBack(Recycler.Handle<OpForAckCallBack> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        void recycle() {
            recyclerHandle.recycle(this);
        }

        private final Recycler.Handle<OpForAckCallBack> recyclerHandle;
        private static final Recycler<OpForAckCallBack> RECYCLER = new Recycler<OpForAckCallBack>() {
            @Override
            protected OpForAckCallBack newObject(Handle<OpForAckCallBack> handle) {
                return new OpForAckCallBack(handle);
            }
        };
    }

    private static class OpForAckResponseTimeout {
        private long timeout;
        private long requestId;

        static OpForAckResponseTimeout create(long timeout, long requestId) {
            OpForAckResponseTimeout op = RECYCLER.get();
            op.timeout = timeout;
            op.requestId = requestId;
            return op;
        }

        private OpForAckResponseTimeout(Recycler.Handle<OpForAckResponseTimeout> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        void recycle() {
            recyclerHandle.recycle(this);
        }

        private final Recycler.Handle<OpForAckResponseTimeout> recyclerHandle;
        private static final Recycler<OpForAckResponseTimeout> RECYCLER = new Recycler<OpForAckResponseTimeout>() {
            @Override
            protected OpForAckResponseTimeout newObject(Handle<OpForAckResponseTimeout> handle) {
                return new OpForAckResponseTimeout(handle);
            }
        };
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
