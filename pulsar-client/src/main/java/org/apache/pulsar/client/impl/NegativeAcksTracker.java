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
import java.io.Closeable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.util.collections.ConcurrentLongLongPairHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NegativeAcksTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(NegativeAcksTracker.class);

    private ConcurrentLongLongPairHashMap nackedMessages = null;

    private final ConsumerBase<?> consumer;
    private final Timer timer;
    private final long nackDelayNanos;
    private final long timerIntervalNanos;
    private final RedeliveryBackoff negativeAckRedeliveryBackoff;

    private Timeout timeout;

    // Set a min delay to allow for grouping nacks within a single batch
    private static final long MIN_NACK_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long NON_PARTITIONED_TOPIC_PARTITION_INDEX = Long.MAX_VALUE;

    public NegativeAcksTracker(ConsumerBase<?> consumer, ConsumerConfigurationData<?> conf) {
        this.consumer = consumer;
        this.timer = consumer.getClient().timer();
        this.nackDelayNanos = Math.max(TimeUnit.MICROSECONDS.toNanos(conf.getNegativeAckRedeliveryDelayMicros()),
                MIN_NACK_DELAY_NANOS);
        this.negativeAckRedeliveryBackoff = conf.getNegativeAckRedeliveryBackoff();
        if (negativeAckRedeliveryBackoff != null) {
            this.timerIntervalNanos = Math.max(
                    TimeUnit.MILLISECONDS.toNanos(negativeAckRedeliveryBackoff.next(0)),
                    MIN_NACK_DELAY_NANOS) / 3;
        } else {
            this.timerIntervalNanos = nackDelayNanos / 3;
        }
    }

    private void triggerRedelivery(Timeout t) {
        Set<MessageId> messagesToRedeliver = new HashSet<>();
        synchronized (this) {
            if (nackedMessages.isEmpty()) {
                this.timeout = null;
                return;
            }

            long now = System.nanoTime();
            nackedMessages.forEach((ledgerId, entryId, partitionIndex, timestamp) -> {
                if (timestamp < now) {
                    MessageId msgId = new MessageIdImpl(ledgerId, entryId,
                            // need to covert non-partitioned topic partition index to -1
                            (int) (partitionIndex == NON_PARTITIONED_TOPIC_PARTITION_INDEX ? -1 : partitionIndex));
                    addChunkedMessageIdsAndRemoveFromSequenceMap(msgId, messagesToRedeliver, this.consumer);
                    messagesToRedeliver.add(msgId);
                }
            });
            for (MessageId messageId : messagesToRedeliver) {
                nackedMessages.remove(((MessageIdImpl) messageId).getLedgerId(),
                        ((MessageIdImpl) messageId).getEntryId());
            }
            this.timeout = timer.newTimeout(this::triggerRedelivery, timerIntervalNanos, TimeUnit.NANOSECONDS);
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

    private synchronized void add(MessageId messageId, int redeliveryCount) {
        if (nackedMessages == null) {
            nackedMessages = ConcurrentLongLongPairHashMap.newBuilder()
                    .autoShrink(true)
                    .concurrencyLevel(1)
                    .build();
        }

        long backoffNs;
        if (negativeAckRedeliveryBackoff != null) {
            backoffNs = TimeUnit.MILLISECONDS.toNanos(negativeAckRedeliveryBackoff.next(redeliveryCount));
        } else {
            backoffNs = nackDelayNanos;
        }
        MessageIdAdv messageIdAdv = MessageIdAdvUtils.discardBatch(messageId);
        // ConcurrentLongLongPairHashMap requires the key and value >=0.
        // partitionIndex is -1 if the message is from a non-partitioned topic, but we don't use
        // partitionIndex actually, so we can set it to Long.MAX_VALUE in the case of non-partitioned topic to
        // avoid exception from ConcurrentLongLongPairHashMap.
        nackedMessages.put(messageIdAdv.getLedgerId(), messageIdAdv.getEntryId(),
                messageIdAdv.getPartitionIndex() >= 0 ? messageIdAdv.getPartitionIndex() :
                        NON_PARTITIONED_TOPIC_PARTITION_INDEX, System.nanoTime() + backoffNs);

        if (this.timeout == null) {
            // Schedule a task and group all the redeliveries for same period. Leave a small buffer to allow for
            // nack immediately following the current one will be batched into the same redeliver request.
            this.timeout = timer.newTimeout(this::triggerRedelivery, timerIntervalNanos, TimeUnit.NANOSECONDS);
        }
    }

    @VisibleForTesting
    Optional<Long> getNackedMessagesCount() {
        return Optional.ofNullable(nackedMessages).map(ConcurrentLongLongPairHashMap::size);
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
