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

import static org.apache.pulsar.client.impl.UnAckedMessageTracker.addChunkedMessageIdsAndRemoveFromSequenceMap;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NegativeAcksTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(NegativeAcksTracker.class);

    private HashMap<MessageId, Long> nackedMessages = null;

    private final ConsumerBase<?> consumer;
    private final Timer timer;
    private final long nackDelayNanos;
    private final long timerIntervalNanos;
    private final RedeliveryBackoff negativeAckRedeliveryBackoff;

    private Timeout timeout;

    // Set a min delay to allow for grouping nacks within a single batch
    private static final long MIN_NACK_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

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

    private synchronized void triggerRedelivery(Timeout t) {
        if (nackedMessages.isEmpty()) {
            this.timeout = null;
            return;
        }

        // Group all the nacked messages into one single re-delivery request
        Set<MessageId> messagesToRedeliver = new HashSet<>();
        long now = System.nanoTime();
        nackedMessages.forEach((msgId, timestamp) -> {
            if (timestamp < now) {
                addChunkedMessageIdsAndRemoveFromSequenceMap(msgId, messagesToRedeliver, this.consumer);
                messagesToRedeliver.add(msgId);
            }
        });

        if (!messagesToRedeliver.isEmpty()) {
            messagesToRedeliver.forEach(nackedMessages::remove);
            consumer.onNegativeAcksSend(messagesToRedeliver);
            log.info("[{}] {} messages will be re-delivered", consumer, messagesToRedeliver.size());
            consumer.redeliverUnacknowledgedMessages(messagesToRedeliver);
        }

        this.timeout = timer.newTimeout(this::triggerRedelivery, timerIntervalNanos, TimeUnit.NANOSECONDS);
    }

    public synchronized void add(MessageId messageId) {
        add(messageId, 0);
    }

    public synchronized void add(Message<?> message) {
        add(message.getMessageId(), message.getRedeliveryCount());
    }

    private synchronized void add(MessageId messageId, int redeliveryCount) {
        if (messageId instanceof TopicMessageIdImpl) {
            TopicMessageIdImpl topicMessageId = (TopicMessageIdImpl) messageId;
            messageId = topicMessageId.getInnerMessageId();
        }

        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            messageId = new MessageIdImpl(batchMessageId.getLedgerId(), batchMessageId.getEntryId(),
                    batchMessageId.getPartitionIndex());
        }

        if (nackedMessages == null) {
            nackedMessages = new HashMap<>();
        }

        long backoffNs;
        if (negativeAckRedeliveryBackoff != null) {
            backoffNs = TimeUnit.MILLISECONDS.toNanos(negativeAckRedeliveryBackoff.next(redeliveryCount));
        } else {
            backoffNs = nackDelayNanos;
        }
        nackedMessages.put(messageId, System.nanoTime() + backoffNs);

        if (this.timeout == null) {
            // Schedule a task and group all the redeliveries for same period. Leave a small buffer to allow for
            // nack immediately following the current one will be batched into the same redeliver request.
            this.timeout = timer.newTimeout(this::triggerRedelivery, timerIntervalNanos, TimeUnit.NANOSECONDS);
        }
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
