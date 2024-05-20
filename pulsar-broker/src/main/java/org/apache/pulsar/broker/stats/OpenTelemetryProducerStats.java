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
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.stats.NonPersistentPublisherStatsImpl;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryProducerStats implements AutoCloseable {

    // Replaces pulsar_producer_msg_rate_in
    public static final String MESSAGE_IN_COUNTER = "pulsar.broker.producer.message.incoming.count";
    // Replaces pulsar_producer_msg_throughput_in
    public static final String BYTES_IN_COUNTER = "pulsar.broker.consumer.message.incoming.size";
    public static final String MESSAGE_DROP_COUNTER = "pulsar.broker.producer.message.drop.count";
    private final ObservableLongMeasurement messageInCounter;
    private final ObservableLongMeasurement bytesInCounter;
    private final ObservableLongMeasurement messageDropCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryProducerStats(PulsarService pulsar) {
        var meter = pulsar.getOpenTelemetry().getMeter();

        messageInCounter = meter
                .counterBuilder(MESSAGE_IN_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages received from this producer.")
                .buildObserver();

        bytesInCounter = meter
                .counterBuilder(BYTES_IN_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes received from this producer.")
                .buildObserver();

        messageDropCounter = meter
                .counterBuilder(MESSAGE_DROP_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages dropped from this producer.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> pulsar.getBrokerService()
                        .getTopics()
                        .values()
                        .stream()
                        .map(topicFuture -> topicFuture.getNow(Optional.empty()))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .map(Topic::getProducers)
                        .flatMap(p -> p.values().stream()).forEach(this::recordMetricsForProducer),
                messageInCounter,
                bytesInCounter,
                messageDropCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForProducer(Producer producer) {
        var topicName = TopicName.get(producer.getTopic().getName());

        var builder = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_PRODUCER_NAME, producer.getProducerName())
                .put(OpenTelemetryAttributes.PULSAR_PRODUCER_ID, producer.getProducerId())
                .put(OpenTelemetryAttributes.PULSAR_PRODUCER_ACCESS_MODE, producer.getAccessMode().toString())
                .put(OpenTelemetryAttributes.PULSAR_PRODUCER_CONNECTED_SINCE,
                        producer.getConnectedSince().getEpochSecond())
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, topicName.getDomain().toString())
                .put(OpenTelemetryAttributes.PULSAR_TENANT, topicName.getTenant())
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, topicName.getNamespace())
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName.getPartitionedTopicName());
        if (topicName.isPartitioned()) {
            builder.put(OpenTelemetryAttributes.PULSAR_PARTITION_INDEX, topicName.getPartitionIndex());
        }

        var clientAddress = producer.getClientAddressAndPort();
        if (clientAddress != null) {
            builder.put(OpenTelemetryAttributes.PULSAR_CLIENT_ADDRESS, clientAddress);
        }
        var clientVersion = producer.getClientVersion();
        if (clientVersion != null) {
            builder.put(OpenTelemetryAttributes.PULSAR_CLIENT_VERSION, clientVersion);
        }
        var metadataList = producer.getMetadata()
                .entrySet()
                .stream()
                .map(e -> String.format("%s:%s", e.getKey(), e.getValue()))
                .toList();
        builder.put(OpenTelemetryAttributes.PULSAR_PRODUCER_METADATA, metadataList);
        var attributes = builder.build();

        var stats = producer.getStats();
        messageInCounter.record(stats.getMsgInCounter(), attributes);
        bytesInCounter.record(stats.getBytesInCounter(), attributes);

        if (stats instanceof NonPersistentPublisherStatsImpl nonPersistentStats) {
            messageDropCounter.record(nonPersistentStats.getMsgDropCount(), attributes);
        }
    }
}
