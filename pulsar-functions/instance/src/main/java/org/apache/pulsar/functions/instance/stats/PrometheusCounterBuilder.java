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
import org.apache.pulsar.functions.api.metrics.Counter;
import org.apache.pulsar.functions.api.metrics.CounterBuilder;

public class PrometheusCounterBuilder implements CounterBuilder, MetricLabels {

    private final CollectorRegistry collectorRegistry;
    private final String customMetricNamePrefix;
    private final String[] systemMetricsLabelNames;
    private final String[] systemMetricsLabels;

    private String metricName;
    private String[] userDefinedLabels;
    private String[] userDefinedLabelNames;
    private String helpMsg;

    private Counter counter;

    PrometheusCounterBuilder(CollectorRegistry collectorRegistry,
                             String customMetricNamePrefix,
                             String[] systemMetricsLabelNames,
                             String[] systemMetricsLabels) {
        this.collectorRegistry = collectorRegistry;
        this.customMetricNamePrefix = customMetricNamePrefix;
        this.systemMetricsLabelNames = Arrays.copyOf(systemMetricsLabelNames, systemMetricsLabelNames.length);
        this.systemMetricsLabels = Arrays.copyOf(systemMetricsLabels, systemMetricsLabels.length);
    }

    @Override
    public CounterBuilder name(String metricName) {
        this.metricName = metricName;
        return this;
    }

    @Override
    public CounterBuilder labelNames(String... labelNames) {
        this.userDefinedLabelNames = labelNames;
        return this;
    }

    @Override
    public CounterBuilder labels(String... labels) {
        this.userDefinedLabels = labels;
        return this;
    }

    @Override
    public CounterBuilder help(String helpMsg) {
        this.helpMsg = helpMsg;
        return this;
    }

    @Override
    public CounterBuilder counter(Counter counter) {
        this.counter = counter;
        return this;
    }

    @Override
    public Counter register() {
        checkArgument(this.metricName != null, "Metric name has not been set");
        checkArgument(this.helpMsg != null, "Metric help message has not been set");
        String[] labelNames = combineLabels(systemMetricsLabelNames, userDefinedLabelNames);
        String[] labels = combineLabels(systemMetricsLabels, userDefinedLabels);
        checkLabels(labelNames, labels);
        Counter counter = getCounter();

        io.prometheus.client.Counter counterCollector = createCounterCollector(
                this.customMetricNamePrefix + metricName, labelNames, helpMsg);
        collectorRegistry.register(counterCollector);
        // needs to setChild because Prometheus collector implementation only collects metrics through a metric Child
        counterCollector.setChild(new io.prometheus.client.Counter.Child() {
            @Override
            public double get() {
                return counter.get();
            }
        }, labels);
        return counter;
    }

    private Counter getCounter() {
        if (counter == null) {
            return new PrometheusCounterImpl(customMetricNamePrefix + metricName, helpMsg);
        } else {
            return counter;
        }
    }

    private io.prometheus.client.Counter createCounterCollector(String name, String[] labelNames, String helpMsg) {
        return io.prometheus.client.Counter.build()
                .name(name)
                .labelNames(labelNames)
                .help(helpMsg)
                .create();
    }



}
