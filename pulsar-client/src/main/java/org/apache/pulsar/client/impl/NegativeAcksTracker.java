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
import io.netty.util.Timer;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

class NegativeAcksTracker {

    private final Set<MessageId> nackedMessages = new HashSet<>();

    private final ConsumerBase<?> consumer;
    private final Timer timer;
    private final long nackDelayMicros;

    private Timeout timeout;

    // Set a min delay to allow for grouping nacks within a single batch
    private static final long MIN_NACK_DELAY_MICROS = 100_000;

    public NegativeAcksTracker(ConsumerBase<?> consumer, ConsumerConfigurationData<?> conf) {
        this.consumer = consumer;
        this.timer = ((PulsarClientImpl) consumer.getClient()).timer();
        this.nackDelayMicros = Math.max(conf.getNegativeAckRedeliveryDelayMicros(), MIN_NACK_DELAY_MICROS);
    }

    private synchronized void triggerRedelivery() {
        // Group all the nacked messages into one single re-delivery request
        consumer.redeliverUnacknowledgedMessages(nackedMessages);
        nackedMessages.clear();

        this.timeout = null;
    }

    public synchronized void add(MessageId messageId) {
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            messageId = new MessageIdImpl(batchMessageId.getLedgerId(), batchMessageId.getEntryId(),
                    batchMessageId.getPartitionIndex());
        }

        nackedMessages.add(messageId);

        if (this.timeout == null) {
            // Schedule a task and group all the redeliveries for same period
            this.timeout = timer.newTimeout(timeout -> triggerRedelivery(), nackDelayMicros, TimeUnit.MICROSECONDS);
        }
    }
}
