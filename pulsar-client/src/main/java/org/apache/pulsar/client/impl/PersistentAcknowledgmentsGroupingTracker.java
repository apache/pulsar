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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.ValidationError;
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

    /**
     * Latest cumulative ack sent to broker
     */
    private volatile MessageIdImpl lastCumulativeAck = (MessageIdImpl) MessageId.earliest;
    private volatile BitSetRecyclable lastCumulativeAckSet = null;
    private volatile boolean cumulativeAckFlushRequired = false;

    private static final AtomicReferenceFieldUpdater<PersistentAcknowledgmentsGroupingTracker, MessageIdImpl> LAST_CUMULATIVE_ACK_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(PersistentAcknowledgmentsGroupingTracker.class, MessageIdImpl.class, "lastCumulativeAck");
    private static final AtomicReferenceFieldUpdater<PersistentAcknowledgmentsGroupingTracker, BitSetRecyclable> LAST_CUMULATIVE_ACK_SET_UPDATER = AtomicReferenceFieldUpdater
        .newUpdater(PersistentAcknowledgmentsGroupingTracker.class, BitSetRecyclable.class, "lastCumulativeAckSet");


    /**
     * This is a set of all the individual acks that the application has issued and that were not already sent to
     * broker.
     */
    private final ConcurrentSkipListSet<MessageIdImpl> pendingIndividualAcks;
    private final ConcurrentHashMap<MessageIdImpl, ConcurrentBitSetRecyclable> pendingIndividualBatchIndexAcks;

    private final ScheduledFuture<?> scheduledTask;

    public PersistentAcknowledgmentsGroupingTracker(ConsumerImpl<?> consumer, ConsumerConfigurationData<?> conf,
                                                    EventLoopGroup eventLoopGroup) {
        this.consumer = consumer;
        this.pendingIndividualAcks = new ConcurrentSkipListSet<>();
        this.pendingIndividualBatchIndexAcks = new ConcurrentHashMap<>();
        this.acknowledgementGroupTimeMicros = conf.getAcknowledgementsGroupTimeMicros();

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
    public boolean isDuplicate(MessageId messageId) {
        if (messageId.compareTo(lastCumulativeAck) <= 0) {
            // Already included in a cumulative ack
            return true;
        } else {
            return pendingIndividualAcks.contains(messageId);
        }
    }

    @Override
    public void addListAcknowledgment(List<MessageIdImpl> messageIds, AckType ackType, Map<String, Long> properties) {
        if (ackType == AckType.Cumulative) {
            messageIds.forEach(messageId -> doCumulativeAck(messageId, null));
            return;
        }
        messageIds.forEach(messageId -> {
            if (messageId instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
                pendingIndividualAcks.add(new MessageIdImpl(batchMessageId.getLedgerId(),
                        batchMessageId.getEntryId(), batchMessageId.getPartitionIndex()));
            } else {
                pendingIndividualAcks.add(messageId);
            }
            pendingIndividualBatchIndexAcks.remove(messageId);
            if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                flush();
            }
        });
        if (acknowledgementGroupTimeMicros == 0) {
            flush();
        }
    }

    @Override
    public void addAcknowledgment(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties,
                                  long txnidMostBits, long txnidLeastBits) {
        if (txnidMostBits != -1 && txnidLeastBits != -1) {
            doImmediateAck(msgId, ackType, properties, txnidMostBits, txnidLeastBits);
        } else if (acknowledgementGroupTimeMicros == 0 || !properties.isEmpty()) {
            // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
            // uncommon condition since it's only used for the compaction subscription.
            doImmediateAck(msgId, ackType, properties, -1, -1);
        } else if (ackType == AckType.Cumulative) {
            doCumulativeAck(msgId, null);
        } else {
            // Individual ack
            if (msgId instanceof BatchMessageIdImpl) {
                pendingIndividualAcks.add(new MessageIdImpl(msgId.getLedgerId(),
                        msgId.getEntryId(), msgId.getPartitionIndex()));
            } else {
                pendingIndividualAcks.add(msgId);
            }
            pendingIndividualBatchIndexAcks.remove(msgId);
            if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                flush();
            }
        }
    }

    @Override
    public void addBatchIndexAcknowledgment(BatchMessageIdImpl msgId, int batchIndex, int batchSize, AckType ackType,
                                            Map<String, Long> properties, long txnidMostBits, long txnidLeastBits) {
        if (txnidMostBits != -1 && txnidLeastBits != -1) {
            doImmediateBatchIndexAck(msgId, batchIndex, batchSize, ackType, properties, txnidMostBits, txnidLeastBits);
        } else if (acknowledgementGroupTimeMicros == 0 || !properties.isEmpty()) {
            doImmediateBatchIndexAck(msgId, batchIndex, batchSize, ackType, properties, -1, -1);
        } else if (ackType == AckType.Cumulative) {
            BitSetRecyclable bitSet = BitSetRecyclable.create();
            bitSet.set(0, batchSize);
            bitSet.clear(0, batchIndex + 1);
            doCumulativeAck(msgId, bitSet);
        } else if (ackType == AckType.Individual) {
            ConcurrentBitSetRecyclable bitSet = pendingIndividualBatchIndexAcks.computeIfAbsent(
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
            if (pendingIndividualBatchIndexAcks.size() >= MAX_ACK_GROUP_SIZE) {
                flush();
            }
        }
    }

    private void doCumulativeAck(MessageIdImpl msgId, BitSetRecyclable bitSet) {
        // Handle concurrent updates from different threads
        while (true) {
            MessageIdImpl lastCumlativeAck = this.lastCumulativeAck;
            BitSetRecyclable lastBitSet = this.lastCumulativeAckSet;
            if (msgId.compareTo(lastCumlativeAck) > 0) {
                if (LAST_CUMULATIVE_ACK_UPDATER.compareAndSet(this, lastCumlativeAck, msgId) && LAST_CUMULATIVE_ACK_SET_UPDATER.compareAndSet(this, lastBitSet, bitSet)) {
                    if (lastBitSet != null) {
                        try {
                            lastBitSet.recycle();
                        } catch (Exception ignore) {
                            // no-op
                        }
                    }
                    // Successfully updated the last cumulative ack. Next flush iteration will send this to broker.
                    cumulativeAckFlushRequired = true;
                    return;
                }
            } else {
                // message id acknowledging an before the current last cumulative ack
                return;
            }
        }
    }

    private boolean doImmediateAck(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties,
                                   long txnidMostBits, long txnidLeastBits) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return false;
        }

        newAckCommand(consumer.consumerId, msgId, null, ackType, null,
                properties, cnx, true /* flush */, txnidMostBits, txnidLeastBits);
        return true;
    }

    private boolean doImmediateBatchIndexAck(BatchMessageIdImpl msgId, int batchIndex, int batchSize, AckType ackType,
                                             Map<String, Long> properties, long txnidMostBits, long txnidLeastBits) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return false;
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

        final ByteBuf cmd = Commands.newAck(consumer.consumerId, msgId.ledgerId, msgId.entryId, bitSet, ackType,
                null, properties, txnidLeastBits, txnidMostBits);
        bitSet.recycle();
        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
        return true;
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
        if (cumulativeAckFlushRequired) {
            newAckCommand(consumer.consumerId, lastCumulativeAck, lastCumulativeAckSet, AckType.Cumulative, null, Collections.emptyMap(), cnx, false /* flush */, -1, -1);
            shouldFlush=true;
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
                    MessageIdImpl[] chunkMsgIds = this.consumer.unAckedChunckedMessageIdSequenceMap.get(msgId);
                    if (chunkMsgIds != null && chunkMsgIds.length > 1) {
                        for (MessageIdImpl cMsgId : chunkMsgIds) {
                            if (cMsgId != null) {
                                entriesToAck.add(Triple.of(cMsgId.getLedgerId(), cMsgId.getEntryId(), null));
                            }
                        }
                        // messages will be acked so, remove checked message sequence
                        this.consumer.unAckedChunckedMessageIdSequenceMap.remove(msgId);
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

                    newAckCommand(consumer.consumerId, msgId, null, AckType.Individual, null, Collections.emptyMap(), cnx, false, -1, -1);
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
            cnx.ctx().write(Commands.newMultiMessageAck(consumer.consumerId, entriesToAck),
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
        lastCumulativeAck = (MessageIdImpl) MessageId.earliest;
        pendingIndividualAcks.clear();
    }

    @Override
    public void close() {
        flush();
        if (scheduledTask != null && !scheduledTask.isCancelled()) {
            scheduledTask.cancel(true);
        }
    }

    private void newAckCommand(long consumerId, MessageIdImpl msgId, BitSetRecyclable lastCumulativeAckSet,
            AckType ackType, ValidationError validationError, Map<String, Long> map, ClientCnx cnx,
                               boolean flush, long txnidMostBits, long txnidLeastBits) {

        MessageIdImpl[] chunkMsgIds = this.consumer.unAckedChunckedMessageIdSequenceMap.get(msgId);
        if (chunkMsgIds != null && txnidLeastBits < 0 && txnidMostBits < 0) {
            if (Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion())
                    && ackType != AckType.Cumulative) {
                List<Triple<Long, Long, ConcurrentBitSetRecyclable>> entriesToAck = new ArrayList<>(chunkMsgIds.length);
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    if (cMsgId != null && chunkMsgIds.length > 1) {
                        entriesToAck.add(Triple.of(cMsgId.getLedgerId(), cMsgId.getEntryId(), null));
                    }
                }
                ByteBuf cmd = Commands.newMultiMessageAck(consumer.consumerId, entriesToAck);
                if (flush) {
                    cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                } else {
                    cnx.ctx().write(cmd, cnx.ctx().voidPromise());
                }
            } else {
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    ByteBuf cmd = Commands.newAck(consumerId, cMsgId.getLedgerId(), cMsgId.getEntryId(),
                            lastCumulativeAckSet, ackType, validationError, map);
                    if (flush) {
                        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                    } else {
                        cnx.ctx().write(cmd, cnx.ctx().voidPromise());
                    }
                }
            }
            this.consumer.unAckedChunckedMessageIdSequenceMap.remove(msgId);
        } else {
            ByteBuf cmd = Commands.newAck(consumerId, msgId.getLedgerId(), msgId.getEntryId(), lastCumulativeAckSet,
                    ackType, validationError, map, txnidLeastBits, txnidMostBits);
            if (flush) {
                cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
            } else {
                cnx.ctx().write(cmd, cnx.ctx().voidPromise());
            }
        }
    }
}