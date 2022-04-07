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

package org.apache.pulsar.functions.instance.stats;

import io.prometheus.client.CollectorRegistry;
import java.util.Arrays;
import org.apache.pulsar.functions.api.metrics.CounterBuilder;
import org.apache.pulsar.functions.api.metrics.GaugeBuilder;
import org.apache.pulsar.functions.api.metrics.HistogramBuilder;
import org.apache.pulsar.functions.api.metrics.MetricProvider;
import org.apache.pulsar.functions.api.metrics.SummaryBuilder;

public class PrometheusMetricProviderImpl implements MetricProvider {

    private final CollectorRegistry collectorRegistry;
    private final String customMetricNamePrefix;
    private final String[] systemMetricsLabelNames;
    private final String[] systemMetricsLabels;

    public PrometheusMetricProviderImpl(CollectorRegistry collectorRegistry,
                                        String customMetricNamePrefix,
                                        String[] systemMetricsLabelNames,
                                        String[] systemMetricsLabels) {
        this.collectorRegistry = collectorRegistry;
        this.customMetricNamePrefix = customMetricNamePrefix;
        this.systemMetricsLabels = Arrays.copyOf(systemMetricsLabels, systemMetricsLabels.length);
        this.systemMetricsLabelNames = Arrays.copyOf(systemMetricsLabelNames, systemMetricsLabelNames.length);
    }

    @Override
    public CounterBuilder registerCounter() {
        return new PrometheusCounterBuilder(collectorRegistry, customMetricNamePrefix, systemMetricsLabelNames,
                systemMetricsLabels);
    }

    @Override
    public GaugeBuilder registerGauge() {
        return new PrometheusGaugeBuilder(collectorRegistry, customMetricNamePrefix, systemMetricsLabelNames,
                systemMetricsLabels);
    }

    @Override
    public HistogramBuilder registerHistogram() {
        return new PrometheusHistogramBuilder(collectorRegistry, customMetricNamePrefix, systemMetricsLabelNames,
                systemMetricsLabels);
    }

    @Override
    public SummaryBuilder registerSummary() {
        return new PrometheusSummaryBuilder(collectorRegistry, customMetricNamePrefix, systemMetricsLabelNames,
                systemMetricsLabels);
    }
}
