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

public class ProducerMetrics {

    private final LatencyHistogram sendLatencyHistogram;
    private final LatencyHistogram rpcLatencyHistogram;
    private final Counter publishedBytesCounter;
    private final UpDownCounter pendingMessagesUpDownCounter;
    private final UpDownCounter pendingBytesUpDownCounter;
    private final Counter producersOpenedCounter;
    private final Counter producersClosedCounter;

    public ProducerMetrics(InstrumentProvider ip, String topic) {
        sendLatencyHistogram = ip.newLatencyHistogram(
                "pulsar.client.producer.message.send.duration",
                "Publish latency experienced by the application, includes client batching time",
                topic, Attributes.empty());

        rpcLatencyHistogram = ip.newLatencyHistogram(
                "pulsar.client.producer.rpc.send.duration",
                "Publish RPC latency experienced internally by the client when sending data and receiving an ack",
                topic, Attributes.empty());

        publishedBytesCounter = ip.newCounter(
                "pulsar.client.producer.message.send.size",
                Unit.Bytes, "The number of bytes published",
                topic, Attributes.empty());

        pendingMessagesUpDownCounter = ip.newUpDownCounter(
                "pulsar.client.producer.message.pending.count", Unit.Messages,
                "The number of messages in the producer internal send queue, waiting to be sent",
                topic, Attributes.empty());

        pendingBytesUpDownCounter = ip.newUpDownCounter(
                "pulsar.client.producer.message.pending.size", Unit.Bytes,
                "The size of the messages in the producer internal queue, waiting to be sent",
                topic, Attributes.empty());

        producersOpenedCounter = ip.newCounter(
                "pulsar.client.producer.opened", Unit.Sessions,
                "The number of producer sessions opened",
                topic, Attributes.empty());

        producersClosedCounter = ip.newCounter(
                "pulsar.client.producer.closed", Unit.Sessions,
                "The number of producer sessions closed",
                topic, Attributes.empty());
    }

    public void recordPendingMessage(int msgSize) {
        pendingMessagesUpDownCounter.increment();
        pendingBytesUpDownCounter.add(msgSize);
    }

    public void recordSendSuccess(long latencyNanos, int msgSize) {
        pendingMessagesUpDownCounter.decrement();
        pendingBytesUpDownCounter.subtract(msgSize);
        sendLatencyHistogram.recordSuccess(latencyNanos);
        publishedBytesCounter.add(msgSize);
    }

    public void recordSendFailed(long latencyNanos, int msgSize) {
        pendingMessagesUpDownCounter.decrement();
        pendingBytesUpDownCounter.subtract(msgSize);
        sendLatencyHistogram.recordFailure(latencyNanos);
    }

    public void recordRpcLatencySuccess(long latencyNanos) {
        rpcLatencyHistogram.recordSuccess(latencyNanos);
    }

    public void recordRpcLatencyFailure(long latencyNanos) {
        rpcLatencyHistogram.recordFailure(latencyNanos);
    }

    public void recordProducerOpened() {
        producersOpenedCounter.increment();
    }

    public void recordProducerClosed() {
        producersClosedCounter.increment();
    }
}
