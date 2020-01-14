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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;

import java.util.ArrayList;
import java.util.BitSet;
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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.api.proto.PulsarApi.IntRange;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.protocol.Commands.SingleBatchMessageIndexesAck;
import org.apache.pulsar.common.util.collections.ConcurrentBitSet;

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
    private volatile boolean cumulativeAckFlushRequired = false;

    private static final AtomicReferenceFieldUpdater<PersistentAcknowledgmentsGroupingTracker, MessageIdImpl> LAST_CUMULATIVE_ACK_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(PersistentAcknowledgmentsGroupingTracker.class, MessageIdImpl.class, "lastCumulativeAck");

    /**
     * This is a set of all the individual acks that the application has issued and that were not already sent to
     * broker.
     */
    private final ConcurrentSkipListSet<MessageIdImpl> pendingIndividualAcks;
    private final ConcurrentHashMap<MessageIdImpl, Pair<Integer, ConcurrentBitSet>> pendingIndividualBatchIndexAcks;

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
    public boolean isDuplicate(MessageId messageId) {
        if (messageId.compareTo(lastCumulativeAck) <= 0) {
            // Already included in a cumulative ack
            return true;
        } else {
            return pendingIndividualAcks.contains(messageId);
        }
    }

    public void addAcknowledgment(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties) {
        if (acknowledgementGroupTimeMicros == 0 || !properties.isEmpty()) {
            // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
            // uncommon condition since it's only used for the compaction subscription.
            doImmediateAck(msgId, ackType, properties);
        } else if (ackType == AckType.Cumulative) {
            doCumulativeAck(msgId);
        } else {
            // Individual ack
            if (msgId instanceof BatchMessageIdImpl) {
                pendingIndividualAcks.add(new MessageIdImpl(msgId.getLedgerId(),
                        msgId.getEntryId(), msgId.getPartitionIndex()));
            } else {
                pendingIndividualAcks.add(msgId);
            }
            if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                flush();
            }
        }
    }

    @Override
    public void addBatchIndexAcknowledgment(BatchMessageIdImpl msgId, int batchIndex, int batchSize, AckType ackType, Map<String, Long> properties) {
        if (acknowledgementGroupTimeMicros == 0 || !properties.isEmpty() || ackType == AckType.Cumulative) {
            doImmediateBatchIndexAck(msgId, batchIndex, batchSize, ackType, properties);
        } else if (ackType == AckType.Individual) {
            ConcurrentBitSet bitSet = pendingIndividualBatchIndexAcks.computeIfAbsent(
                new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId(), msgId.getPartitionIndex()), (v) ->
                Pair.of(batchSize, new ConcurrentBitSet(msgId.getBatchSize()))).getRight();
            bitSet.set(batchIndex);
            if (pendingIndividualBatchIndexAcks.size() >= MAX_ACK_GROUP_SIZE) {
                flush();
            }
        }
    }

    private void doCumulativeAck(MessageIdImpl msgId) {
        // Handle concurrent updates from different threads
        while (true) {
            MessageIdImpl lastCumlativeAck = this.lastCumulativeAck;
            if (msgId.compareTo(lastCumlativeAck) > 0) {
                if (LAST_CUMULATIVE_ACK_UPDATER.compareAndSet(this, lastCumlativeAck, msgId)) {
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

    private boolean doImmediateAck(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return false;
        }

        final ByteBuf cmd = Commands.newAck(consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(), ackType, null,
                properties);

        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
        return true;
    }

    private boolean doImmediateBatchIndexAck(BatchMessageIdImpl msgId, int batchIndex, int batchSize, AckType ackType, Map<String, Long> properties) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return false;
        }
        IntRange.Builder rangeBuilder = IntRange.newBuilder();
        final ByteBuf cmd = Commands.newBatchIndexAck(consumer.consumerId,
            Collections.singletonList(
                new SingleBatchMessageIndexesAck(
                    msgId.getLedgerId(),
                    msgId.getEntryId(),
                    batchSize,
                    Collections.singletonList(rangeBuilder.setStart(0).setEnd(batchIndex).build())
                )), ackType, null, properties);
        rangeBuilder.recycle();
        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
        return true;
    }

    /**
     * Flush all the pending acks and send them to the broker
     */
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
            ByteBuf cmd = Commands.newAck(consumer.consumerId, lastCumulativeAck.ledgerId, lastCumulativeAck.entryId,
                    AckType.Cumulative, null, Collections.emptyMap());
            cnx.ctx().write(cmd, cnx.ctx().voidPromise());
            shouldFlush=true;
            cumulativeAckFlushRequired = false;
        }

        // Flush all individual acks
        if (!pendingIndividualAcks.isEmpty()) {
            if (Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion())) {
                // We can send 1 single protobuf command with all individual acks
                List<Pair<Long, Long>> entriesToAck = new ArrayList<>(pendingIndividualAcks.size());
                while (true) {
                    MessageIdImpl msgId = pendingIndividualAcks.pollFirst();
                    if (msgId == null) {
                        break;
                    }

                    entriesToAck.add(Pair.of(msgId.getLedgerId(), msgId.getEntryId()));
                }

                cnx.ctx().write(Commands.newMultiMessageAck(consumer.consumerId, entriesToAck),
                        cnx.ctx().voidPromise());
                shouldFlush = true;
            } else {
                // When talking to older brokers, send the acknowledgements individually
                while (true) {
                    MessageIdImpl msgId = pendingIndividualAcks.pollFirst();
                    if (msgId == null) {
                        break;
                    }

                    cnx.ctx().write(Commands.newAck(consumer.consumerId, msgId.getLedgerId(), msgId.getEntryId(),
                            AckType.Individual, null, Collections.emptyMap()), cnx.ctx().voidPromise());
                    shouldFlush = true;
                }
            }
        }

        if (!pendingIndividualBatchIndexAcks.isEmpty()) {
            Iterator<Map.Entry<MessageIdImpl, Pair<Integer, ConcurrentBitSet>>> iterator = pendingIndividualBatchIndexAcks.entrySet().iterator();
            List<SingleBatchMessageIndexesAck> batchIndexesAcks = new ArrayList<>(pendingIndividualBatchIndexAcks.size());
            IntRange.Builder rangeBuilder = IntRange.newBuilder();
            while (iterator.hasNext()) {
                Map.Entry<MessageIdImpl, Pair<Integer, ConcurrentBitSet>> entry = iterator.next();
                SingleBatchMessageIndexesAck ack = new SingleBatchMessageIndexesAck();
                ack.setLedgerId(entry.getKey().getLedgerId());
                ack.setEntryId(entry.getKey().getEntryId());
                ack.setBatchSize(entry.getValue().getLeft());
                ack.setIndexRangesToAck(Lists.newArrayList());
                BitSet bitSet = entry.getValue().getRight();
                int nextSetBit = bitSet.nextSetBit(0);
                while (nextSetBit != -1) {
                    int nextClearBit = bitSet.nextClearBit(nextSetBit);
                    ack.getIndexRangesToAck().add(rangeBuilder.setStart(nextSetBit).setEnd(nextClearBit - 1).build());
                    nextSetBit = bitSet.nextSetBit(nextClearBit);
                }
                batchIndexesAcks.add(ack);
                iterator.remove();
            }
            cnx.ctx().write(Commands.newBatchIndexAck(consumer.consumerId, batchIndexesAcks, AckType.Individual, null,
                null));
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
}
