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

import static com.google.common.base.Preconditions.checkArgument;
import io.prometheus.client.CollectorRegistry;
import java.util.Arrays;
import org.apache.pulsar.functions.api.metrics.Gauge;
import org.apache.pulsar.functions.api.metrics.GaugeBuilder;

public class PrometheusGaugeBuilder implements GaugeBuilder, MetricLabels {

    private final CollectorRegistry collectorRegistry;
    private final String customMetricNamePrefix;
    private final String[] systemMetricsLabelNames;
    private final String[] systemMetricsLabels;

    private String metricName;
    private String[] userDefinedLabels;
    private String[] userDefinedLabelNames;
    private String helpMsg;

    private Gauge gauge;

    public PrometheusGaugeBuilder(CollectorRegistry collectorRegistry,
                                  String customMetricNamePrefix,
                                  String[] systemMetricsLabelNames,
                                  String[] systemMetricsLabels) {
        this.collectorRegistry = collectorRegistry;
        this.customMetricNamePrefix = customMetricNamePrefix;
        this.systemMetricsLabelNames = Arrays.copyOf(systemMetricsLabelNames, systemMetricsLabelNames.length);
        this.systemMetricsLabels = Arrays.copyOf(systemMetricsLabels, systemMetricsLabels.length);
    }

    @Override
    public GaugeBuilder name(String metricName) {
        this.metricName = metricName;
        return this;
    }

    @Override
    public GaugeBuilder labelNames(String... labelNames) {
        this.userDefinedLabelNames = labelNames;
        return this;
    }

    @Override
    public GaugeBuilder labels(String... labels) {
        this.userDefinedLabels = labels;
        return this;
    }

    @Override
    public GaugeBuilder help(String helpMsg) {
        this.helpMsg = helpMsg;
        return this;
    }

    @Override
    public GaugeBuilder gauge(Gauge gauge) {
        this.gauge = gauge;
        return this;
    }

    @Override
    public Gauge register() {
        checkArgument(this.metricName != null, "Metric name has not been set");
        checkArgument(this.helpMsg != null, "Metric help message has not been set");
        String[] labelNames = combineLabels(systemMetricsLabelNames, userDefinedLabelNames);
        String[] labels = combineLabels(systemMetricsLabels, userDefinedLabels);
        checkLabels(labelNames, labels);
        Gauge gauge = getGauge();

        io.prometheus.client.Gauge gaugeCollector = createGaugeCollector(
                this.customMetricNamePrefix + metricName, labelNames, helpMsg);
        collectorRegistry.register(gaugeCollector);
        // needs to setChild because Prometheus collector implementation only collects metrics through a metric Child
        gaugeCollector.setChild(new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
                return gauge.get();
            }
        }, labels);
        return gauge;
    }

    private Gauge getGauge() {
        if (gauge == null) {
            return new PrometheusGaugeImpl(customMetricNamePrefix + metricName, helpMsg);
        } else {
            return gauge;
        }
    }

    private io.prometheus.client.Gauge createGaugeCollector(String name, String[] labelNames, String helpMsg) {
        return io.prometheus.client.Gauge.build()
                .name(name)
                .labelNames(labelNames)
                .help(helpMsg)
                .create();
    }

}
