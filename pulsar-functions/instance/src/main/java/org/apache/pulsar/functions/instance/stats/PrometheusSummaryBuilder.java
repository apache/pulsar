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
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.functions.api.metrics.Summary;
import org.apache.pulsar.functions.api.metrics.SummaryBuilder;

public class PrometheusSummaryBuilder implements SummaryBuilder, MetricLabels {

    private final CollectorRegistry collectorRegistry;
    private final String customMetricNamePrefix;
    private final String[] systemMetricsLabelNames;
    private final String[] systemMetricsLabels;
    private final List<Summary.Quantile> quantiles;

    private String metricName;
    private String[] userDefinedLabels;
    private String[] userDefinedLabelNames;
    private String helpMsg;

    private Summary summary;

    PrometheusSummaryBuilder(CollectorRegistry collectorRegistry,
                             String customMetricNamePrefix,
                             String[] systemMetricsLabelNames,
                             String[] systemMetricsLabels) {
        this.collectorRegistry = collectorRegistry;
        this.customMetricNamePrefix = customMetricNamePrefix;
        this.systemMetricsLabelNames = Arrays.copyOf(systemMetricsLabelNames, systemMetricsLabelNames.length);
        this.systemMetricsLabels = Arrays.copyOf(systemMetricsLabels, systemMetricsLabels.length);
        this.quantiles = new LinkedList<>();
    }

    @Override
    public SummaryBuilder name(String metricName) {
        this.metricName = metricName;
        return this;
    }

    @Override
    public SummaryBuilder labelNames(String... labelNames) {
        this.userDefinedLabelNames = labelNames;
        return this;
    }

    @Override
    public SummaryBuilder labels(String... labels) {
        this.userDefinedLabels = labels;
        return this;
    }

    @Override
    public SummaryBuilder help(String helpMsg) {
        this.helpMsg = helpMsg;
        return this;
    }

    @Override
    public SummaryBuilder quantile(double quantile, double error) {
        this.quantiles.add(Summary.Quantile.of(quantile, error));
        return this;
    }

    @Override
    public SummaryBuilder summary(Summary summary) {
        this.summary = summary;
        return this;
    }

    @Override
    public Summary register() {
        checkArgument(this.metricName != null, "Metric name has not been set");
        checkArgument(this.helpMsg != null, "Metric help message has not been set");
        String[] labelNames = combineLabels(systemMetricsLabelNames, userDefinedLabelNames);
        String[] labels = combineLabels(systemMetricsLabels, userDefinedLabels);
        checkLabels(labelNames, labels);
        Summary summary = getSummary();
        // no need for setChild since we use our own Summary collector implementation
        createSummaryCollector(customMetricNamePrefix + metricName, labelNames, labels, helpMsg, summary)
                .register(collectorRegistry);
        return summary;
    }

    private SummaryCollector createSummaryCollector(String metricName, String[] labelNames, String[] labels,
                                                    String helpMsg, Summary summary) {
        return new SummaryCollector(metricName, labelNames, labels, helpMsg, summary);
    }

    private Summary getSummary() {
        if (summary == null) {
            return new PrometheusSummaryImpl(customMetricNamePrefix + metricName, helpMsg, quantiles);
        } else {
            return summary;
        }
    }

    // Implementing our own Summary collector since Prometheus Summary marks its Child's constructor as private
    static class SummaryCollector extends Collector {
        private final String metricName;
        private final String[] labelsNames;
        private final String[] labels;
        private final String helpMsg;
        private final Summary summary;

        SummaryCollector(String metricName, String[] labelsNames, String[] labels, String helpMsg, Summary summary) {
            this.metricName = metricName;
            this.labelsNames = labelsNames;
            this.labels = labels;
            this.helpMsg = helpMsg;
            this.summary = summary;
        }

        @Override
        public List<MetricFamilySamples> collect() {
            List<MetricFamilySamples.Sample> samples = new LinkedList<>();
            List<String> labelNamesList = Arrays.asList(labelsNames);
            List<String> labelNamesWithQuantile = new ArrayList<>(labelNamesList);
            labelNamesWithQuantile.add("quantile");
            List<String> labelValuesList = Arrays.asList(labels);

            for (Map.Entry<Double, Double> q: summary.getQuantileValues().entrySet()) {
                List<String> labelValuesWithQuantiles = new ArrayList<>(labelValuesList);
                labelValuesWithQuantiles.add(doubleToGoString(q.getKey()));
                samples.add(new MetricFamilySamples.Sample(metricName, labelNamesWithQuantile,
                        labelValuesWithQuantiles, q.getValue()));
            }
            samples.add(new MetricFamilySamples.Sample(metricName + "_count", labelNamesList, labelValuesList,
                    summary.getCount()));
            samples.add(new MetricFamilySamples.Sample(metricName + "_sum", labelNamesList, labelValuesList,
                    summary.getSum()));
            return Collections.singletonList(new MetricFamilySamples(metricName, Type.SUMMARY, helpMsg, samples));
        }
    }

}
