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
package org.apache.bookkeeper.mledger.impl.cache;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.prometheus.client.Gauge;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.opentelemetry.Constants;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.InflightReadLimiterUtilization;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;

@Slf4j
public class InflightReadsLimiter implements AutoCloseable {

    public static final String INFLIGHT_READS_LIMITER_LIMIT_METRIC_NAME =
            "pulsar.broker.managed_ledger.inflight.read.limit";
    private final ObservableLongCounter inflightReadsLimitCounter;

    @PulsarDeprecatedMetric(newMetricName = INFLIGHT_READS_LIMITER_LIMIT_METRIC_NAME)
    @Deprecated
    private static final Gauge PULSAR_ML_READS_BUFFER_SIZE = Gauge
            .build()
            .name("pulsar_ml_reads_inflight_bytes")
            .help("Estimated number of bytes retained by data read from storage or cache")
            .register();

    public static final String INFLIGHT_READS_LIMITER_USAGE_METRIC_NAME =
            "pulsar.broker.managed_ledger.inflight.read.usage";
    private final ObservableLongCounter inflightReadsUsageCounter;

    @PulsarDeprecatedMetric(newMetricName = INFLIGHT_READS_LIMITER_USAGE_METRIC_NAME)
    @Deprecated
    private static final Gauge PULSAR_ML_READS_AVAILABLE_BUFFER_SIZE = Gauge
            .build()
            .name("pulsar_ml_reads_available_inflight_bytes")
            .help("Available space for inflight data read from storage or cache")
            .register();

    private final long maxReadsInFlightSize;
    private long remainingBytes;

    public InflightReadsLimiter(long maxReadsInFlightSize, OpenTelemetry openTelemetry) {
        if (maxReadsInFlightSize <= 0) {
            // set it to -1 in order to show in the metrics that the metric is not available
            PULSAR_ML_READS_BUFFER_SIZE.set(-1);
            PULSAR_ML_READS_AVAILABLE_BUFFER_SIZE.set(-1);
        }
        this.maxReadsInFlightSize = maxReadsInFlightSize;
        this.remainingBytes = maxReadsInFlightSize;

        var meter = openTelemetry.getMeter(Constants.BROKER_INSTRUMENTATION_SCOPE_NAME);
        inflightReadsLimitCounter = meter.counterBuilder(INFLIGHT_READS_LIMITER_LIMIT_METRIC_NAME)
                .setDescription("Maximum number of bytes that can be retained by managed ledger data read from storage "
                        + "or cache.")
                .setUnit("By")
                .buildWithCallback(measurement -> {
                    if (!isDisabled()) {
                        measurement.record(maxReadsInFlightSize);
                    }
                });
        inflightReadsUsageCounter = meter.counterBuilder(INFLIGHT_READS_LIMITER_USAGE_METRIC_NAME)
                .setDescription("Estimated number of bytes retained by managed ledger data read from storage or cache.")
                .setUnit("By")
                .buildWithCallback(measurement -> {
                    if (!isDisabled()) {
                        var freeBytes = getRemainingBytes();
                        var usedBytes = maxReadsInFlightSize - freeBytes;
                        measurement.record(freeBytes, InflightReadLimiterUtilization.FREE.attributes);
                        measurement.record(usedBytes, InflightReadLimiterUtilization.USED.attributes);
                    }
                });
    }

    @VisibleForTesting
    public synchronized long getRemainingBytes() {
        return remainingBytes;
    }

    @Override
    public void close() {
        inflightReadsLimitCounter.close();
        inflightReadsUsageCounter.close();
    }

    @AllArgsConstructor
    @ToString
    static class Handle {
        final long acquiredPermits;
        final boolean success;
        final int trials;

        final long creationTime;
    }

    private static final Handle DISABLED = new Handle(0, true, 0, -1);

    Handle acquire(long permits, Handle current) {
        if (maxReadsInFlightSize <= 0) {
            // feature is disabled
            return DISABLED;
        }
        synchronized (this) {
            try {
                if (current == null) {
                    if (remainingBytes == 0) {
                        return new Handle(0, false, 1, System.currentTimeMillis());
                    }
                    if (remainingBytes >= permits) {
                        remainingBytes -= permits;
                        return new Handle(permits, true, 1, System.currentTimeMillis());
                    } else {
                        long possible = remainingBytes;
                        remainingBytes = 0;
                        return new Handle(possible, false, 1, System.currentTimeMillis());
                    }
                } else {
                    if (current.trials >= 4 && current.acquiredPermits > 0) {
                        remainingBytes += current.acquiredPermits;
                        return new Handle(0, false, 1, current.creationTime);
                    }
                    if (remainingBytes == 0) {
                        return new Handle(current.acquiredPermits, false, current.trials + 1,
                                current.creationTime);
                    }
                    long needed = permits - current.acquiredPermits;
                    if (remainingBytes >= needed) {
                        remainingBytes -= needed;
                        return new Handle(permits, true, current.trials + 1, current.creationTime);
                    } else {
                        long possible = remainingBytes;
                        remainingBytes = 0;
                        return new Handle(current.acquiredPermits + possible, false,
                                current.trials + 1, current.creationTime);
                    }
                }
            } finally {
                updateMetrics();
            }
        }
    }

    void release(Handle handle) {
        if (handle == DISABLED) {
            return;
        }
        synchronized (this) {
            remainingBytes += handle.acquiredPermits;
            updateMetrics();
        }
    }

    private synchronized void updateMetrics() {
        PULSAR_ML_READS_BUFFER_SIZE.set(maxReadsInFlightSize - remainingBytes);
        PULSAR_ML_READS_AVAILABLE_BUFFER_SIZE.set(remainingBytes);
    }

    public boolean isDisabled() {
        return maxReadsInFlightSize <= 0;
    }


}
