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
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.AbstractReplicator;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.common.stats.MetricsUtil;

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
    public static final String BACKLOG_COUNTER = "pulsar.broker.replication.message.backlog.count";
    private final ObservableLongMeasurement backlogCounter;

    // Replaces pulsar_replication_delay_in_seconds
    public static final String DELAY_GAUGE = "pulsar.broker.replication.message.backlog.age";
    private final ObservableDoubleMeasurement delayGauge;

    // Replaces pulsar_replication_rate_expired
    public static final String EXPIRED_COUNTER = "pulsar.broker.replication.message.expired.count";
    private final ObservableLongMeasurement expiredCounter;

    public static final String DROPPED_COUNTER = "pulsar.broker.replication.message.dropped.count";
    private final ObservableLongMeasurement droppedCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryReplicatorStats(PulsarService pulsar) {
        var meter = pulsar.getOpenTelemetry().getMeter();

        messageInCounter = meter
                .upDownCounterBuilder(MESSAGE_IN_COUNTER)
                .setUnit("{message}")
                .setDescription(
                        "The total number of messages received from the remote cluster through this replicator.")
                .buildObserver();

        messageOutCounter = meter
                .upDownCounterBuilder(MESSAGE_OUT_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages sent to the remote cluster through this replicator.")
                .buildObserver();

        bytesInCounter = meter
                .upDownCounterBuilder(BYTES_IN_COUNTER)
                .setUnit("{By}")
                .setDescription(
                        "The total number of messages bytes received from the remote cluster through this replicator.")
                .buildObserver();

        bytesOutCounter = meter
                .upDownCounterBuilder(BYTES_OUT_COUNTER)
                .setUnit("{By}")
                .setDescription(
                        "The total number of messages bytes sent to the remote cluster through this replicator.")
                .buildObserver();

        backlogCounter = meter
                .upDownCounterBuilder(BACKLOG_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages in the backlog for this replicator.")
                .buildObserver();

        delayGauge = meter
                .gaugeBuilder(DELAY_GAUGE)
                .setUnit("s")
                .setDescription("The age of the oldest message in the replicator backlog.")
                .buildObserver();

        expiredCounter = meter
                .upDownCounterBuilder(EXPIRED_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages that expired for this replicator.")
                .buildObserver();

        droppedCounter = meter
                .upDownCounterBuilder(DROPPED_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages dropped by this replicator.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> pulsar.getBrokerService()
                        .getTopics()
                        .values()
                        .stream()
                        .filter(topicFuture -> topicFuture.isDone() && !topicFuture.isCompletedExceptionally())
                        .map(CompletableFuture::join)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .flatMap(topic -> topic.getReplicators().values().stream())
                        .map(AbstractReplicator.class::cast)
                        .forEach(this::recordMetricsForReplicator),
                messageInCounter,
                messageOutCounter,
                bytesInCounter,
                bytesOutCounter,
                backlogCounter,
                delayGauge,
                expiredCounter,
                droppedCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForReplicator(AbstractReplicator replicator) {
        var attributes = replicator.getAttributes();
        var stats = replicator.getStats();

        messageInCounter.record(stats.getMsgInCount(), attributes);
        messageOutCounter.record(stats.getMsgOutCount(), attributes);
        bytesInCounter.record(stats.getBytesInCount(), attributes);
        bytesOutCounter.record(stats.getBytesOutCount(), attributes);
        var delaySeconds = MetricsUtil.convertToSeconds(replicator.getReplicationDelayMs(), TimeUnit.MILLISECONDS);
        delayGauge.record(delaySeconds, attributes);

        if (replicator instanceof PersistentReplicator persistentReplicator) {
            expiredCounter.record(persistentReplicator.getMessageExpiredCount(), attributes);
            backlogCounter.record(persistentReplicator.getNumberOfEntriesInBacklog(), attributes);
        } else if (replicator instanceof NonPersistentReplicator nonPersistentReplicator) {
            droppedCounter.record(nonPersistentReplicator.getStats().getMsgDropCount(), attributes);
        }
    }
}
