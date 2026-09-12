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

import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.common.policies.data.stats.NonPersistentPublisherStatsImpl;

public class OpenTelemetryProducerStats implements AutoCloseable {

    // Replaces pulsar_producer_msg_rate_in
    public static final String MESSAGE_IN_COUNTER = "pulsar.broker.producer.message.incoming.count";
    private final ObservableLongMeasurement messageInCounter;

    // Replaces pulsar_producer_msg_throughput_in
    public static final String BYTES_IN_COUNTER = "pulsar.broker.producer.message.incoming.size";
    private final ObservableLongMeasurement bytesInCounter;

    public static final String MESSAGE_DROP_COUNTER = "pulsar.broker.producer.message.drop.count";
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
                        .filter(future -> future.isDone() && !future.isCompletedExceptionally())
                        .map(CompletableFuture::join)
                        .filter(Optional::isPresent)
                        .flatMap(topic -> topic.get().getProducers().values().stream())
                        .forEach(this::recordMetricsForProducer),
                messageInCounter,
                bytesInCounter,
                messageDropCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForProducer(Producer producer) {
        var attributes = producer.getOpenTelemetryAttributes();
        var stats = producer.getStats();

        messageInCounter.record(stats.getMsgInCounter(), attributes);
        bytesInCounter.record(stats.getBytesInCounter(), attributes);

        if (stats instanceof NonPersistentPublisherStatsImpl nonPersistentStats) {
            messageDropCounter.record(nonPersistentStats.getMsgDropCount(), attributes);
        }
    }
}
