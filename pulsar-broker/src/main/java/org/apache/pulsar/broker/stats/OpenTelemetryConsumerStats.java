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
package org.apache.pulsar.broker.stats;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Collection;
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryConsumerStats implements AutoCloseable {

    // Replaces pulsar_consumer_msg_rate_out
    public static final String MESSAGE_OUT_COUNTER = "pulsar.broker.consumer.message.outgoing.count";
    private final ObservableLongMeasurement messageOutCounter;

    // Replaces pulsar_consumer_msg_throughput_out
    public static final String BYTES_OUT_COUNTER = "pulsar.broker.consumer.message.outgoing.size";
    private final ObservableLongMeasurement bytesOutCounter;

    // Replaces pulsar_consumer_msg_ack_rate
    public static final String MESSAGE_ACK_COUNTER = "pulsar.broker.consumer.message.ack.count";
    private final ObservableLongMeasurement messageAckCounter;

    // Replaces pulsar_consumer_msg_rate_redeliver
    public static final String MESSAGE_REDELIVER_COUNTER = "pulsar.broker.consumer.message.redeliver.count";
    private final ObservableLongMeasurement messageRedeliverCounter;

    // Replaces pulsar_consumer_unacked_messages
    public static final String MESSAGE_UNACKNOWLEDGED_COUNTER = "pulsar.broker.consumer.message.unack.count";
    private final ObservableLongMeasurement messageUnacknowledgedCounter;

    // Replaces pulsar_consumer_blocked_on_unacked_messages
    public static final String MESSAGE_BLOCKED_ON_UNACKNOWLEDGED_MESSAGES_GAUGE = "pulsar.broker.consumer.message.unack.blocked";
    private final ObservableLongMeasurement messageBlockedOnUnacknowledgedMessagesGauge;

    // Replaces pulsar_consumer_available_permits
    public static final String MESSAGE_PERMITS_COUNTER = "pulsar.broker.consumer.permit.count";
    private final ObservableLongMeasurement messagePermitsCounter;


    private final BatchCallback batchCallback;
    private final PulsarService pulsar;

    public OpenTelemetryConsumerStats(PulsarService pulsar) {
        this.pulsar = pulsar;
        var meter = pulsar.getOpenTelemetry().getMeter();

        messageOutCounter = meter
                .counterBuilder(MESSAGE_OUT_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages dispatched to this consumer.")
                .buildObserver();

        bytesOutCounter = meter
                .counterBuilder(BYTES_OUT_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes dispatched to this consumer.")
                .buildObserver();

        messageAckCounter = meter
                .counterBuilder(MESSAGE_ACK_COUNTER)
                .setUnit("{ack}")
                .setDescription("The total number of message acknowledgments received from this consumer.")
                .buildObserver();

        messageRedeliverCounter = meter
                .counterBuilder(MESSAGE_REDELIVER_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of message acknowledgments that have been redelivered to this consumer.")
                .buildObserver();

        messageUnacknowledgedCounter = meter
                .counterBuilder(MESSAGE_UNACKNOWLEDGED_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages unacknowledged by this consumer.")
                .buildObserver();

        messageBlockedOnUnacknowledgedMessagesGauge = meter
                .gaugeBuilder(MESSAGE_BLOCKED_ON_UNACKNOWLEDGED_MESSAGES_GAUGE)
                .ofLongs()
                .setUnit("Boolean")
                .setDescription("TODO: Indicates whether a consumer is blocked on acknowledged messages or not.")
                .buildObserver();

        messagePermitsCounter = meter
                .counterBuilder(MESSAGE_PERMITS_COUNTER)
                .setUnit("{permit}")
                .setDescription("The total number of available permits for this consumer.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> pulsar.getBrokerService()
                        .getTopics()
                        .values()
                        .stream()
                        .map(topicFuture -> topicFuture.getNow(Optional.empty()))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .map(Topic::getSubscriptions)
                        .flatMap(s -> s.values().stream())
                        .map(Subscription::getConsumers)
                        .flatMap(Collection::stream)
                        .forEach(this::recordMetricsForConsumer),
                messageOutCounter,
                bytesOutCounter,
                messageAckCounter,
                messageRedeliverCounter,
                messageUnacknowledgedCounter,
                messageBlockedOnUnacknowledgedMessagesGauge,
                messagePermitsCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForConsumer(Consumer consumer) {
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic.getName())
                .build();

        messageOutCounter.record(dummyValue, attributes);
        bytesOutCounter.record(dummyValue, attributes);
        messageAckCounter.record(dummyValue, attributes);
        messageRedeliverCounter.record(dummyValue, attributes);
        messageUnacknowledgedCounter.record(dummyValue, attributes);
        messageBlockedOnUnacknowledgedMessagesGauge.record(dummyValue, attributes);
        messagePermitsCounter.record(dummyValue, attributes);

    }
}
