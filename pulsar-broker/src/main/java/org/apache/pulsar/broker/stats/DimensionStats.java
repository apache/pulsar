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

import io.prometheus.metrics.core.metrics.Summary;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Quantile;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;

/**
 */
@CustomLog
public class DimensionStats implements AutoCloseable {

    private final Summary summary;
    /** Whether this instance's summary made it into the default registry, so {@link #close()} removes only its own. */
    private final boolean registered;
    private static final double[] QUANTILES = {0.50, 0.75, 0.95, 0.99, 0.999, 0.9999};

    public DimensionStats(String name, long updateDurationInSec) {
        this(name, updateDurationInSec, true);
    }

    DimensionStats(String name, long updateDurationInSec, boolean register) {
        Summary.Builder summaryBuilder = Summary.builder().name(name).help("-").maxAgeSeconds(updateDurationInSec);
        for (double quantile : QUANTILES) {
            summaryBuilder.quantile(quantile, 0.01);
        }
        this.summary = summaryBuilder.build();
        boolean registered = false;
        if (register) {
            try {
                PrometheusRegistry.defaultRegistry.register(summary);
                registered = true;
            } catch (IllegalArgumentException ie) {
                // it only happens in test-cases when try to register summary multiple times in registry
                log.warn().attr("name", name).exceptionMessage(ie).log("is already registered");
            }
        }
        this.registered = registered;
    }

    /** Whether the scrape of the default registry reads this instance's summary rather than an earlier one's. */
    boolean isRegistered() {
        return registered;
    }

    public void recordDimensionTimeValue(long latency, TimeUnit unit) {
        summary.observe(unit.toMillis(latency));
    }

    public DimensionStatsSnapshot getSnapshot() {
        // A reset() racing with this collect() can leave the summary without its data point for a moment; the
        // snapshot then reads as zero, which is what the simpleclient version returned for a missing sample.
        List<SummarySnapshot.SummaryDataPointSnapshot> dataPoints = summary.collect().getDataPoints();
        return new DimensionStatsSnapshot(dataPoints.isEmpty() ? null : dataPoints.get(0));
    }

    public static class DimensionStatsSnapshot {
        private final SummarySnapshot.SummaryDataPointSnapshot dataPoint;

        public DimensionStatsSnapshot(SummarySnapshot.SummaryDataPointSnapshot dataPoint) {
            this.dataPoint = dataPoint;
        }

        public double getMeanDimension() {
            double sum = getDimensionSum();
            double count = getDimensionCount();
            if (!Double.isNaN(sum) && !Double.isNaN(count)) {
                return sum / count;
            }
            return 0;
        }

        public double getMedianDimension() {
            return getQuantile(QUANTILES[0]);
        }

        public double getDimension75() {
            return getQuantile(QUANTILES[1]);
        }

        public double getDimension95() {
            return getQuantile(QUANTILES[2]);
        }

        public double getDimension99() {
            return getQuantile(QUANTILES[3]);
        }

        public double getDimension999() {
            return getQuantile(QUANTILES[4]);
        }

        public double getDimension9999() {
            return getQuantile(QUANTILES[5]);
        }

        public double getDimensionSum() {
            return dataPoint == null ? 0 : dataPoint.getSum();
        }

        public double getDimensionCount() {
            return dataPoint == null ? 0 : dataPoint.getCount();
        }

        private double getQuantile(double quantile) {
            if (dataPoint == null) {
                return 0;
            }
            for (Quantile q : dataPoint.getQuantiles()) {
                if (q.getQuantile() == quantile) {
                    return q.getValue();
                }
            }
            return 0;
        }
    }

    public void reset() {
        summary.clear();
    }

    @Override
    public void close() {
        if (registered) {
            PrometheusRegistry.defaultRegistry.unregister(summary);
        }
    }
}
