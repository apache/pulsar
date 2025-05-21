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

import static org.apache.pulsar.client.impl.UnAckedMessageTracker.addChunkedMessageIdsAndRemoveFromSequenceMap;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NegativeAcksTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(NegativeAcksTracker.class);

    // timestamp -> ledgerId -> entryId, no need to batch index, if different messages have
    // different timestamp, there will be multiple entries in the map
    // RB Tree -> LongOpenHashMap -> Roaring64Bitmap
    private Long2ObjectSortedMap<Long2ObjectMap<Roaring64Bitmap>> nackedMessages = null;

    private final ConsumerBase<?> consumer;
    private final Timer timer;
    private final long nackDelayMs;
    private final RedeliveryBackoff negativeAckRedeliveryBackoff;
    private final int negativeAckPrecisionBitCnt;

    private Timeout timeout;

    // Set a min delay to allow for grouping nacks within a single batch
    private static final long MIN_NACK_DELAY_MS = 100;
    private static final int DUMMY_PARTITION_INDEX = -2;

    public NegativeAcksTracker(ConsumerBase<?> consumer, ConsumerConfigurationData<?> conf) {
        this.consumer = consumer;
        this.timer = consumer.getClient().timer();
        this.nackDelayMs = Math.max(TimeUnit.MICROSECONDS.toMillis(conf.getNegativeAckRedeliveryDelayMicros()),
                MIN_NACK_DELAY_MS);
        this.negativeAckRedeliveryBackoff = conf.getNegativeAckRedeliveryBackoff();
        this.negativeAckPrecisionBitCnt = conf.getNegativeAckPrecisionBitCnt();
    }

    private void triggerRedelivery(Timeout t) {
        Set<MessageId> messagesToRedeliver = new HashSet<>();
        synchronized (this) {
            if (nackedMessages.isEmpty()) {
                this.timeout = null;
                return;
            }

            long currentTimestamp = System.currentTimeMillis();
            for (long timestamp : nackedMessages.keySet()) {
                if (timestamp > currentTimestamp) {
                    // We are done with all the messages that need to be redelivered
                    break;
                }

                Long2ObjectMap<Roaring64Bitmap> ledgerMap = nackedMessages.get(timestamp);
                for (Long2ObjectMap.Entry<Roaring64Bitmap> ledgerEntry : ledgerMap.long2ObjectEntrySet()) {
                    long ledgerId = ledgerEntry.getLongKey();
                    Roaring64Bitmap entrySet = ledgerEntry.getValue();
                    entrySet.forEach(entryId -> {
                        MessageId msgId = new MessageIdImpl(ledgerId, entryId, DUMMY_PARTITION_INDEX);
                        addChunkedMessageIdsAndRemoveFromSequenceMap(msgId, messagesToRedeliver, this.consumer);
                        messagesToRedeliver.add(msgId);
                    });
                }
            }

            // remove entries from the nackedMessages map
            LongBidirectionalIterator iterator = nackedMessages.keySet().iterator();
            while (iterator.hasNext()) {
                long timestamp = iterator.nextLong();
                if (timestamp <= currentTimestamp) {
                    iterator.remove();
                } else {
                    break;
                }
            }

            // Schedule the next redelivery if there are still messages to redeliver
            if (!nackedMessages.isEmpty()) {
                long nextTriggerTimestamp = nackedMessages.firstLongKey();
                long delayMs = Math.max(nextTriggerTimestamp - currentTimestamp, 0);
                if (delayMs > 0) {
                    this.timeout = timer.newTimeout(this::triggerRedelivery, delayMs, TimeUnit.MILLISECONDS);
                } else {
                    this.timeout = timer.newTimeout(this::triggerRedelivery, 0, TimeUnit.MILLISECONDS);
                }
            } else {
                this.timeout = null;
            }
        }

        // release the lock of NegativeAcksTracker before calling consumer.redeliverUnacknowledgedMessages,
        // in which we may acquire the lock of consumer, leading to potential deadlock.
        if (!messagesToRedeliver.isEmpty()) {
            consumer.onNegativeAcksSend(messagesToRedeliver);
            log.info("[{}] {} messages will be re-delivered", consumer, messagesToRedeliver.size());
            consumer.redeliverUnacknowledgedMessages(messagesToRedeliver);
        }
    }

    public synchronized void add(MessageId messageId) {
        add(messageId, 0);
    }

    public synchronized void add(Message<?> message) {
        add(message.getMessageId(), message.getRedeliveryCount());
    }

    static long trimLowerBit(long timestamp, int bits) {
        return timestamp & (-1L << bits);
    }

    private synchronized void add(MessageId messageId, int redeliveryCount) {
        if (nackedMessages == null) {
            nackedMessages = new Long2ObjectAVLTreeMap<>();
        }

        long backoffMs;
        if (negativeAckRedeliveryBackoff != null) {
            backoffMs = TimeUnit.MILLISECONDS.toMillis(negativeAckRedeliveryBackoff.next(redeliveryCount));
        } else {
            backoffMs = nackDelayMs;
        }
        MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
        long timestamp = trimLowerBit(System.currentTimeMillis() + backoffMs, negativeAckPrecisionBitCnt);
        nackedMessages.computeIfAbsent(timestamp, k -> new Long2ObjectOpenHashMap<>())
                .computeIfAbsent(messageIdAdv.getLedgerId(), k -> new Roaring64Bitmap())
                .add(messageIdAdv.getEntryId());

        if (this.timeout == null) {
            // Schedule a task and group all the redeliveries for same period. Leave a small buffer to allow for
            // nack immediately following the current one will be batched into the same redeliver request.
            this.timeout = timer.newTimeout(this::triggerRedelivery, backoffMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Discard the batch index and partition index from the message id.
     *
     * @param messageId
     * @return
     */
    public static MessageIdAdv discardBatchAndPartitionIndex(MessageId messageId) {
        if (messageId instanceof ChunkMessageIdImpl) {
            return (MessageIdAdv) messageId;
        }
        MessageIdAdv msgId = (MessageIdAdv) messageId;
        return new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId(), DUMMY_PARTITION_INDEX);
    }

    @VisibleForTesting
    synchronized long getNackedMessagesCount() {
        if (nackedMessages == null) {
            return 0;
        }
        return nackedMessages.values().stream().mapToLong(
                ledgerMap -> ledgerMap.values().stream().mapToLong(
                        Roaring64Bitmap::getLongCardinality).sum()).sum();
    }

    @Override
    public synchronized void close() {
        if (timeout != null && !timeout.isCancelled()) {
            timeout.cancel();
            timeout = null;
        }

        if (nackedMessages != null) {
            nackedMessages.clear();
            nackedMessages = null;
        }
    }
}
