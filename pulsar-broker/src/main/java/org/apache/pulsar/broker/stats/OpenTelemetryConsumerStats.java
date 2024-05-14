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
import org.apache.pulsar.common.naming.TopicName;
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

    // Replaces pulsar_consumer_available_permits
    public static final String MESSAGE_PERMITS_COUNTER = "pulsar.broker.consumer.permit.count";
    private final ObservableLongMeasurement messagePermitsCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryConsumerStats(PulsarService pulsar) {
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
                .setDescription("The total number of messages that have been redelivered to this consumer.")
                .buildObserver();

        messageUnacknowledgedCounter = meter
                .upDownCounterBuilder(MESSAGE_UNACKNOWLEDGED_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages unacknowledged by this consumer.")
                .buildObserver();

        messagePermitsCounter = meter
                .upDownCounterBuilder(MESSAGE_PERMITS_COUNTER)
                .setUnit("{permit}")
                .setDescription("The number of permits currently available for this consumer.")
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
                messagePermitsCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForConsumer(Consumer consumer) {
        var subscription = consumer.getSubscription();
        var topicName = TopicName.get(subscription.getTopic().getName());

        var builder = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_CONSUMER_NAME, consumer.consumerName())
                .put(OpenTelemetryAttributes.PULSAR_CONSUMER_ID, consumer.consumerId())
                .put(OpenTelemetryAttributes.PULSAR_CONSUMER_CONNECTED_SINCE,
                        consumer.getConnectedSince().getEpochSecond())
                .put(OpenTelemetryAttributes.PULSAR_SUBSCRIPTION_NAME, subscription.getName())
                .put(OpenTelemetryAttributes.PULSAR_SUBSCRIPTION_TYPE, consumer.subType().toString())
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, topicName.getDomain().toString())
                .put(OpenTelemetryAttributes.PULSAR_TENANT, topicName.getTenant())
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, topicName.getNamespace())
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName.getPartitionedTopicName());
        if (topicName.isPartitioned()) {
            builder.put(OpenTelemetryAttributes.PULSAR_PARTITION_INDEX, topicName.getPartitionIndex());
        }
        var clientAddress = consumer.getClientAddressAndPort();
        if (clientAddress != null) {
            builder.put(OpenTelemetryAttributes.PULSAR_CLIENT_ADDRESS, clientAddress);
        }
        var clientVersion = consumer.getClientVersion();
        if (clientVersion != null) {
            builder.put(OpenTelemetryAttributes.PULSAR_CLIENT_VERSION, clientVersion);
        }
        var metadataList = consumer.getMetadata()
                .entrySet()
                .stream()
                .map(e -> String.format("%s:%s", e.getKey(), e.getValue()))
                .toList();
        builder.put(OpenTelemetryAttributes.PULSAR_CONSUMER_METADATA, metadataList);
        var attributes = builder.build();

        messageOutCounter.record(consumer.getMsgOutCounter(), attributes);
        bytesOutCounter.record(consumer.getBytesOutCounter(), attributes);
        messageAckCounter.record(consumer.getMessageAckCounter(), attributes);
        messageRedeliverCounter.record(consumer.getMessageRedeliverCounter(), attributes);
        messageUnacknowledgedCounter.record(consumer.getUnackedMessages(),
                Attributes.builder()
                        .putAll(attributes)
                        .put(OpenTelemetryAttributes.PULSAR_CONSUMER_BLOCKED, consumer.isBlocked())
                        .build());
        messagePermitsCounter.record(consumer.getAvailablePermits(), attributes);
    }
}
