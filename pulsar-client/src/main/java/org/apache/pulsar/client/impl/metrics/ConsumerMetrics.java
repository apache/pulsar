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
package org.apache.pulsar.client.impl.metrics;

import io.opentelemetry.api.common.Attributes;

public class ConsumerMetrics {

    private final Counter messagesReceivedCounter;
    private final Counter bytesReceivedCounter;
    private final UpDownCounter messagesPrefetchedGauge;
    private final UpDownCounter bytesPrefetchedGauge;
    private final Counter consumersOpenedCounter;
    private final Counter consumersClosedCounter;
    private final Counter consumerAcksCounter;
    private final Counter consumerNacksCounter;
    private final Counter consumerDlqMessagesCounter;

    public ConsumerMetrics(InstrumentProvider ip, String topic, String subscription) {
        Attributes attrs = Attributes.builder().put("pulsar.subscription", subscription).build();

        consumersOpenedCounter = ip.newCounter("pulsar.client.consumer.opened", Unit.Sessions,
                "The number of consumer sessions opened", topic, attrs);
        consumersClosedCounter = ip.newCounter("pulsar.client.consumer.closed", Unit.Sessions,
                "The number of consumer sessions closed", topic, attrs);
        messagesReceivedCounter = ip.newCounter("pulsar.client.consumer.message.received.count", Unit.Messages,
                "The number of messages explicitly received by the consumer application", topic, attrs);
        bytesReceivedCounter = ip.newCounter("pulsar.client.consumer.message.received.size", Unit.Bytes,
                "The number of bytes explicitly received by the consumer application", topic, attrs);
        messagesPrefetchedGauge = ip.newUpDownCounter("pulsar.client.consumer.receive_queue.count", Unit.Messages,
                "The number of messages currently sitting in the consumer receive queue", topic, attrs);
        bytesPrefetchedGauge = ip.newUpDownCounter("pulsar.client.consumer.receive_queue.size", Unit.Bytes,
                "The total size in bytes of messages currently sitting in the consumer receive queue", topic, attrs);
        consumerAcksCounter = ip.newCounter("pulsar.client.consumer.message.ack", Unit.Messages,
                "The number of acknowledged messages", topic, attrs);
        consumerNacksCounter = ip.newCounter("pulsar.client.consumer.message.nack", Unit.Messages,
                "The number of negatively acknowledged messages", topic, attrs);
        consumerDlqMessagesCounter = ip.newCounter("pulsar.client.consumer.message.dlq", Unit.Messages,
                "The number of messages sent to DLQ", topic, attrs);
    }

    public void recordMessagePrefetched(int msgSize) {
        messagesPrefetchedGauge.increment();
        bytesPrefetchedGauge.add(msgSize);
    }

    public void recordMessageReceived(int msgSize) {
        messagesPrefetchedGauge.decrement();
        bytesPrefetchedGauge.subtract(msgSize);
        messagesReceivedCounter.increment();
        bytesReceivedCounter.add(msgSize);
    }

    public void recordAck() {
        consumerAcksCounter.increment();
    }

    public void recordNack() {
        consumerNacksCounter.increment();
    }

    public void recordDlq() {
        consumerDlqMessagesCounter.increment();
    }

    public void recordConsumerOpened() {
        consumersOpenedCounter.increment();
    }

    public void recordConsumerClosed() {
        consumersClosedCounter.increment();
    }
}
