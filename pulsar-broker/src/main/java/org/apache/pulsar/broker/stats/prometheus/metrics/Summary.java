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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import io.prometheus.metrics.model.registry.Collector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricFamilyDescriptor;
import io.prometheus.metrics.model.snapshots.Quantile;
import io.prometheus.metrics.model.snapshots.Quantiles;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import io.prometheus.metrics.model.snapshots.SummarySnapshot.SummaryDataPointSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Summary implements Collector {

    private static final List<DataSketchesSummaryLogger> LOGGERS = new ArrayList<>();

    public static class Builder {
        private final String name;
        private final String help;
        private final List<Double> quantiles = new ArrayList<>();

        private Builder(String name, String help) {
            this.name = name;
            this.help = help;
        }

        public Builder quantile(double quantile) {
            this.quantiles.add(quantile);
            return this;
        }

        public Summary register() {
            Summary summary = new Summary(this);
            PrometheusRegistry.defaultRegistry.register(summary);
            return summary;
        }
    }

    public static Builder build(String name, String help) {
        return new Builder(name, help);
    }

    private final MetricFamilyDescriptor descriptor;
    private final List<Double> quantiles;
    private final DataSketchesSummaryLogger logger = new DataSketchesSummaryLogger();

    private Summary(Builder builder) {
        this.descriptor = MetricFamilyDescriptor.summary(builder.name).help(builder.help).build();
        this.quantiles = List.copyOf(builder.quantiles);
        // Rejects a quantile outside [0, 1] and a duplicate here, where the summary is built. A scrape builds the
        // same quantiles, and there the exception would fail the whole scrape.
        Quantiles.of(quantiles.stream().map(quantile -> new Quantile(quantile, 0)).toList());
        synchronized (LOGGERS) {
            LOGGERS.add(logger);
        }
    }

    public void observe(long eventLatency, TimeUnit unit) {
        logger.registerEvent(eventLatency, unit);
    }

    public static void rotateLatencyCollection() {
        synchronized (LOGGERS) {
            for (int i = 0, n = LOGGERS.size(); i < n; i++) {
                LOGGERS.get(i).rotateLatencyCollection();
            }
        }
    }

    @Override
    public SummarySnapshot collect() {
        List<Quantile> quantileValues = new ArrayList<>(quantiles.size());
        for (double quantile : quantiles) {
            quantileValues.add(new Quantile(quantile, logger.getQuantileValue(quantile)));
        }
        return new SummarySnapshot(descriptor.getMetadata(), List.of(SummaryDataPointSnapshot.builder()
                .count(logger.getCount())
                .sum(logger.getSum())
                .quantiles(Quantiles.of(quantileValues))
                .build()));
    }

    @Override
    public MetricFamilyDescriptor getMetricFamilyDescriptor() {
        return descriptor;
    }
}
