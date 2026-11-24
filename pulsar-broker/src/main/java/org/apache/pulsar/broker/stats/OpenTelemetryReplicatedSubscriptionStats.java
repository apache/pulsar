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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.stats.MetricsUtil;

public class OpenTelemetryReplicatedSubscriptionStats {

    public static final AttributeKey<String> SNAPSHOT_OPERATION_RESULT =
            AttributeKey.stringKey("pulsar.replication.subscription.snapshot.operation.result");
    public enum SnapshotOperationResult {
        SUCCESS,
        TIMEOUT;
        private final Attributes attributes = Attributes.of(SNAPSHOT_OPERATION_RESULT, name().toLowerCase());
    }

    public static final String SNAPSHOT_OPERATION_COUNT_METRIC_NAME =
            "pulsar.broker.replication.subscription.snapshot.operation.count";
    private final LongCounter snapshotOperationCounter;

    public static final String SNAPSHOT_DURATION_METRIC_NAME =
            "pulsar.broker.replication.subscription.snapshot.operation.duration";
    private final DoubleHistogram snapshotDuration;

    public OpenTelemetryReplicatedSubscriptionStats(PulsarService pulsar) {
        var meter = pulsar.getOpenTelemetry().getMeter();
        snapshotOperationCounter = meter.counterBuilder(SNAPSHOT_OPERATION_COUNT_METRIC_NAME)
                .setDescription("The number of snapshot operations attempted")
                .setUnit("{operation}")
                .build();
        snapshotDuration = meter.histogramBuilder(SNAPSHOT_DURATION_METRIC_NAME)
                .setDescription("Time taken to complete a consistent snapshot operation across clusters")
                .setUnit("s")
                .build();
    }

    public void recordSnapshotStarted() {
        snapshotOperationCounter.add(1);
    }

    public void recordSnapshotTimedOut(long durationMs) {
        snapshotDuration.record(MetricsUtil.convertToSeconds(durationMs, TimeUnit.MILLISECONDS),
                SnapshotOperationResult.TIMEOUT.attributes);
    }

    public void recordSnapshotCompleted(long durationMs) {
        snapshotDuration.record(MetricsUtil.convertToSeconds(durationMs, TimeUnit.MILLISECONDS),
                SnapshotOperationResult.SUCCESS.attributes);
    }
}
