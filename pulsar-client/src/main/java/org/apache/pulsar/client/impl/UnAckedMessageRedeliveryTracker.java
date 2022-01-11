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

import io.netty.util.Timeout;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnAckedMessageRedeliveryTracker extends UnAckedMessageTracker {

    protected final HashMap<MessageId, Long> ackTimeoutMessages;
    private static final Logger log = LoggerFactory.getLogger(UnAckedMessageRedeliveryTracker.class);

    private final long unAckDelayNanos;
    private final ConsumerBase<?> consumer;

    private final long timerIntervalNanos;

    private final RedeliveryBackoff ackTimeoutRedeliveryBackoff;

    // Set a min delay to allow for grouping unack within a single batch
    private static final long MIN_NACK_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    public UnAckedMessageRedeliveryTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase,
                                           ConsumerConfigurationData<?> conf) {
        super(client, consumerBase, conf);
        this.consumer = consumerBase;
        this.ackTimeoutRedeliveryBackoff = conf.getAckTimeoutRedeliveryBackoff();
        this.ackTimeoutMessages = new HashMap<MessageId, Long>();
        this.unAckDelayNanos = Math.max(TimeUnit.MICROSECONDS.toNanos(tickDurationInMs),
                MIN_NACK_DELAY_NANOS);
        this.timerIntervalNanos = Math.max(
                TimeUnit.MILLISECONDS.toNanos(ackTimeoutRedeliveryBackoff.next(0)),
                MIN_NACK_DELAY_NANOS) / 3;
    }

    private static final FastThreadLocal<HashSet<MessageId>> TL_MESSAGE_IDS_SET = new FastThreadLocal<HashSet<MessageId>>() {
        @Override
        protected HashSet<MessageId> initialValue() throws Exception {
            return new HashSet<>();
        }
    };

    private void triggerRedelivery(Timeout t) {

        if (ackTimeoutMessages.isEmpty()) {
            this.timeout = null;
            return;
        }

        Set<MessageId> messageIds = TL_MESSAGE_IDS_SET.get();
        messageIds.clear();

        writeLock.lock();
        try {
            long now = System.nanoTime();
            ackTimeoutMessages.forEach((messageId, timestamp) -> {
                if (timestamp < now) {
                    addChunkedMessageIdsAndRemoveFromSequenceMap(messageId, messageIds, consumer);
                    messageIds.add(messageId);
                }
            });
            if (!messageIds.isEmpty()) {
                log.info("[{}] {} messages will be re-delivered", consumer, messageIds.size());
                messageIds.forEach(ackTimeoutMessages::remove);
            }
        } finally {
            writeLock.unlock();
            if (messageIds.size() > 0) {
                consumer.onAckTimeoutSend(messageIds);
                consumer.redeliverUnacknowledgedMessages(messageIds);
            }
            this.timeout = consumer.getClient().timer().newTimeout(this::triggerRedelivery, timerIntervalNanos,
                    TimeUnit.NANOSECONDS);
        }
    }

    boolean isEmpty() {
        readLock.lock();
        try {
            return ackTimeoutMessages.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void clear() {
        writeLock.lock();
        try {
            ackTimeoutMessages.clear();
        } finally {
            writeLock.unlock();
        }
    }

    long size() {
        readLock.lock();
        try {
            return ackTimeoutMessages.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean add(MessageId messageId) {
        writeLock.lock();
        try {
            ackTimeoutMessages.put(messageId,
                    System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.ackTimeoutMillis) + unAckDelayNanos);
            if (this.timeout == null) {
                // Schedule a task and group all the redeliveries for same period. Leave a small buffer to allow for
                // nack immediately following the current one will be batched into the same redeliver request.
                this.timeout = consumer.getClient().timer().newTimeout(this::triggerRedelivery, timerIntervalNanos,
                        TimeUnit.NANOSECONDS);
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean add(MessageId messageId, int redeliveryCount) {
        writeLock.lock();
        try {
            long backoffNs =
                    TimeUnit.MILLISECONDS.toNanos(ackTimeoutMillis + ackTimeoutRedeliveryBackoff.next(redeliveryCount));
            ackTimeoutMessages.put(messageId, System.nanoTime() + backoffNs);
            if (this.timeout == null) {
                // Schedule a task and group all the redeliveries for same period. Leave a small buffer to allow for
                // nack immediately following the current one will be batched into the same redeliver request.
                this.timeout = consumer.getClient().timer().newTimeout(this::triggerRedelivery, timerIntervalNanos,
                        TimeUnit.NANOSECONDS);
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean remove(MessageId messageId) {
        writeLock.lock();
        try {
            return ackTimeoutMessages.remove(messageId) == null ? false : true;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public int removeMessagesTill(MessageId msgId) {
        writeLock.lock();
        try {
            Set<MessageId> messageIds = TL_MESSAGE_IDS_SET.get();
            messageIds.clear();
            ackTimeoutMessages.forEach((messageId, timestamp) -> {
                if (messageId.compareTo(msgId) <= 0) {
                    addChunkedMessageIdsAndRemoveFromSequenceMap(messageId, messageIds, consumer);
                    messageIds.add(messageId);
                }
            });
            messageIds.forEach(ackTimeoutMessages::remove);
            return messageIds.size();
        } finally {
            writeLock.unlock();
        }
    }

}
