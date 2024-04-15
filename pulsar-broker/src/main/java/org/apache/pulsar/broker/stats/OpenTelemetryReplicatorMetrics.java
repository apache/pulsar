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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryReplicatorStats implements AutoCloseable {

    // Replaces pulsar_replication_rate_in
    public static final String MESSAGE_IN_COUNTER = "pulsar.broker.replication.message.incoming.count";
    private final ObservableLongMeasurement messageInCounter;

    // Replaces pulsar_replication_rate_out
    public static final String MESSAGE_OUT_COUNTER = "pulsar.broker.replication.message.outgoing.count";
    private final ObservableLongMeasurement messageOutCounter;

    // Replaces pulsar_replication_throughput_in
    public static final String BYTES_IN_COUNTER = "pulsar.broker.replication.message.incoming.size";
    private final ObservableLongMeasurement bytesInCounter;

    // Replaces pulsar_replication_throughput_out
    public static final String BYTES_OUT_COUNTER = "pulsar.broker.replication.message.outgoing.size";
    private final ObservableLongMeasurement bytesOutCounter;

    // Replaces pulsar_replication_backlog
    public static final String BACKLOG_COUNTER = "pulsar.broker.replication.message.backlog";
    private final ObservableLongMeasurement backlogCounter;

    // Replaces pulsar_replication_rate_expired
    public static final String EXPIRED_COUNTER = "pulsar.broker.replication.message.expired";
    private final ObservableLongMeasurement expiredCounter;

    // Replaces pulsar_replication_connected_count
    public static final String CONNECTED_COUNTER = "pulsar.broker.replication.connected.count";
    private final ObservableLongMeasurement connectedCounter;

    // Replaces pulsar_replication_delay_in_seconds
    public static final String DELAY_GAUGE = "pulsar.broker.replication.message.age";
    private final ObservableLongMeasurement delayGauge;

    private final BatchCallback batchCallback;
    private final PulsarService pulsar;

    public OpenTelemetryReplicatorStats(PulsarService pulsar) {
        this.pulsar = pulsar;
        var meter = pulsar.getOpenTelemetry().getMeter();

        messageInCounter = meter
                .upDownCounterBuilder(MESSAGE_IN_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages received from the remote cluster through this replicator.")
                .buildObserver();

        messageOutCounter = meter
                .upDownCounterBuilder(MESSAGE_OUT_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages sent to the remote cluster through this replicator.")
                .buildObserver();

        bytesInCounter = meter
                .upDownCounterBuilder(BYTES_IN_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total number of messages bytes received from the remote cluster through this replicator.")
                .buildObserver();

        bytesOutCounter = meter
                .upDownCounterBuilder(BYTES_OUT_COUNTER)
                .setUnit("{byte}")
                .setDescription("The total number of messages bytes sent to the remote cluster through this replicator.")
                .buildObserver();

        backlogCounter = meter
                .upDownCounterBuilder(BACKLOG_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages in the backlog for this replicator.")
                .buildObserver();

        expiredCounter = meter
                .upDownCounterBuilder(EXPIRED_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages that expired for this replicator.")
                .buildObserver();

        connectedCounter = meter
                .upDownCounterBuilder(CONNECTED_COUNTER)
                .setUnit("{subscriber}")
                .setDescription("The total number of replication subscribers that are running.")
                .buildObserver();

        delayGauge = meter
                .upDownCounterBuilder(DELAY_GAUGE)
                .setUnit("{second}")
                .setDescription("The total number of messages that expired for this replicator.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> pulsar.getBrokerService()
                        .getTopics()
                        .values()
                        .stream()
                        .map(topicFuture -> topicFuture.getNow(Optional.empty()))
                        .forEach(topic -> topic.ifPresent(this::recordMetricsForTopic)),
                messageInCounter,
                messageOutCounter,
                bytesInCounter,
                bytesOutCounter,
                backlogCounter,
                expiredCounter,
                connectedCounter,
                delayGauge);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForTopic(Topic topic) {
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic.getName())
                .build();
        var replicators = topic.getReplicators();

        topic.getReplicators().values().stream().mapMulti(new BiConsumer<Replicator, Consumer<? extends Object>>() {
            @Override
            public void accept(Replicator replicator, Consumer<?> consumer) {

            }
        });

        messageInCounter.record(dummyValue, attributes);
        messageOutCounter.record(dummyValue, attributes);
        bytesInCounter.record(dummyValue, attributes);
        bytesOutCounter.record(dummyValue, attributes);
        backlogCounter.record(dummyValue, attributes);
        expiredCounter.record(dummyValue, attributes);
        connectedCounter.record(dummyValue, attributes);
        delayGauge.record(dummyValue, attributes);
    }
}
