/**
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

import static io.prometheus.client.CollectorRegistry.defaultRegistry;
import io.prometheus.client.Collector;
import io.prometheus.client.Summary;
import io.prometheus.client.Summary.Builder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class DimensionStats {

    private final String name;
    private final String dimensionSumLabel;
    private final String dimensionCountLabel;
    private final Summary summary;
    private static final double[] QUANTILES = {0.50, 0.75, 0.95, 0.99, 0.999, 0.9999};
    private static final List<String> QUANTILE_LABEL = Collections.unmodifiableList(Arrays.asList("quantile"));
    private static final List<List<String>> QUANTILE_LABEL_VALUES = Collections.unmodifiableList(
            Arrays.stream(QUANTILES).mapToObj(Collector::doubleToGoString)
                    .map(Collections::singletonList)
                    .collect(Collectors.toList()));


    public DimensionStats(String name, long updateDurationInSec) {
        this(name, updateDurationInSec, true);
    }

    DimensionStats(String name, long updateDurationInSec, boolean register) {
        this.name = name;
        this.dimensionSumLabel = name + "_sum";
        this.dimensionCountLabel = name + "_count";
        Builder summaryBuilder = Summary.build().name(name).help("-");
        for (int i = 0; i < QUANTILES.length; i++) {
            summaryBuilder.quantile(QUANTILES[i], 0.01);
        }
        this.summary = summaryBuilder.maxAgeSeconds(updateDurationInSec).create();
        if (register) {
            try {
                defaultRegistry.register(summary);
            } catch (IllegalArgumentException ie) {
                // it only happens in test-cases when try to register summary multiple times in registry
                log.warn("{} is already registred {}", name, ie.getMessage());
            }
        }
    }

    public void recordDimensionTimeValue(long latency, TimeUnit unit) {
        summary.observe(unit.toMillis(latency));
    }

    public DimensionStatsSnapshot getSnapshot() {
        return new DimensionStatsSnapshot(summary.collect());
    }


    public class DimensionStatsSnapshot {
        private final List<Collector.MetricFamilySamples> samples;

        public DimensionStatsSnapshot(List<Collector.MetricFamilySamples> samples) {
            this.samples = samples;
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
            return getQuantile(QUANTILE_LABEL_VALUES.get(0));
        }

        public double getDimension75() {
            return getQuantile(QUANTILE_LABEL_VALUES.get(1));
        }

        public double getDimension95() {
            return getQuantile(QUANTILE_LABEL_VALUES.get(2));
        }

        public double getDimension99() {
            return getQuantile(QUANTILE_LABEL_VALUES.get(3));
        }

        public double getDimension999() {
            return getQuantile(QUANTILE_LABEL_VALUES.get(4));
        }

        public double getDimension9999() {
            return getQuantile(QUANTILE_LABEL_VALUES.get(5));
        }

        public double getDimensionSum() {
            return getSampleValue(dimensionSumLabel);
        }

        public double getDimensionCount() {
            return getSampleValue(dimensionCountLabel);
        }

        private double getQuantile(List<String> labelValues) {
            return getSampleValue(name, QUANTILE_LABEL, labelValues);
        }

        private double getSampleValue(String name) {
            return getSampleValue(name, Collections.emptyList(), Collections.emptyList());
        }

        private double getSampleValue(String name, List<String> labelNames, List<String> labelValues) {
            for (Collector.MetricFamilySamples metricFamilySamples : samples) {
                for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
                    if (sample.name.equals(name)
                            && sample.labelNames.equals(labelNames)
                            && sample.labelValues.equals(labelValues)) {
                        return sample.value;
                    }
                }
            }
            return 0;
        }
    }

    public void reset() {
        summary.clear();
    }

    private static final Logger log = LoggerFactory.getLogger(DimensionStats.class);
}
