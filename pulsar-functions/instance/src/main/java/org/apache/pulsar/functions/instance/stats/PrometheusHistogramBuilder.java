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
import org.apache.pulsar.functions.api.metrics.Histogram;
import org.apache.pulsar.functions.api.metrics.HistogramBuilder;

public class PrometheusHistogramBuilder implements HistogramBuilder, MetricLabels {

    private final CollectorRegistry collectorRegistry;
    private final String customMetricNamePrefix;
    private final String[] systemMetricsLabelNames;
    private final String[] systemMetricsLabels;

    private String metricName;
    private String[] userDefinedLabels;
    private String[] userDefinedLabelNames;
    private String helpMsg;

    private Histogram histogram;
    private double[] buckets;

    PrometheusHistogramBuilder(CollectorRegistry collectorRegistry,
                               String customMetricNamePrefix,
                               String[] systemMetricsLabelNames,
                               String[] systemMetricsLabels) {
        this.collectorRegistry = collectorRegistry;
        this.customMetricNamePrefix = customMetricNamePrefix;
        this.systemMetricsLabelNames = Arrays.copyOf(systemMetricsLabelNames, systemMetricsLabelNames.length);
        this.systemMetricsLabels = Arrays.copyOf(systemMetricsLabels, systemMetricsLabels.length);
    }

    @Override
    public HistogramBuilder name(String metricName) {
        this.metricName = metricName;
        return this;
    }

    @Override
    public HistogramBuilder labelNames(String... labelNames) {
        this.userDefinedLabelNames = labelNames;
        return this;
    }

    @Override
    public HistogramBuilder labels(String... labels) {
        this.userDefinedLabels = labels;
        return this;
    }

    @Override
    public HistogramBuilder help(String helpMsg) {
        this.helpMsg = helpMsg;
        return this;
    }

    @Override
    public HistogramBuilder buckets(double[] buckets) {
        this.buckets = Arrays.copyOf(buckets, buckets.length);
        return this;
    }

    @Override
    public HistogramBuilder histogram(Histogram histogram) {
        this.histogram = histogram;
        return this;
    }

    @Override
    public Histogram register() {
        checkArgument(this.metricName != null, "Metric name has not been set");
        checkArgument(this.helpMsg != null, "Metric help message has not been set");
        String[] labelNames = combineLabels(systemMetricsLabelNames, userDefinedLabelNames);
        String[] labels = combineLabels(systemMetricsLabels, userDefinedLabels);
        checkLabels(labelNames, labels);
        if (histogram == null) {
            checkArgument(this.buckets != null, "Histogram buckets are not set");
        } else {
            checkArgument(this.histogram.getBuckets() != null, "Histogram buckets are not set");
        }
        Histogram histogram = getHistogram();
        // no need for setChild since we use our own Histogram collector implementation
        createHistogramCollector(customMetricNamePrefix + metricName, labelNames, labels, helpMsg, histogram)
                .register(collectorRegistry);
        return histogram;
    }

    private HistogramCollector createHistogramCollector(String name, String[] labelNames, String[] labels,
                                                        String helpMsg, Histogram histogram) {
        return new HistogramCollector(name, labelNames, labels, helpMsg, histogram);
    }

    private Histogram getHistogram() {
        if (histogram == null) {
            return new PrometheusHistogramImpl(customMetricNamePrefix + metricName, helpMsg, buckets);
        } else {
            return histogram;
        }
    }

     // Implementing our own Histogram collector since Prometheus Histogram marks its Child's constructor as private
    static class HistogramCollector extends Collector {
        private final String metricName;
        private final String[] labelsNames;
        private final String[] labels;
        private final String helpMsg;
        private final Histogram histogram;

        HistogramCollector(String metricName, String[] labelNames, String[] labels, String helpMsg,
                           Histogram histogram) {
            this.metricName = metricName;
            this.labelsNames = labelNames;
            this.labels = labels;
            this.helpMsg = helpMsg;
            this.histogram = histogram;
        }

        @Override
        public List<MetricFamilySamples> collect() {
            List<MetricFamilySamples.Sample> samples = new LinkedList<>();
            double[] buckets = histogram.getBuckets();
            double[] values = histogram.getValues();
            List<String> labelNamesList = Arrays.asList(labelsNames);
            List<String> labelNamesWithLe = new ArrayList<>(labelNamesList);
            labelNamesWithLe.add("le");
            List<String> labelValuesList = Arrays.asList(labels);

            for (int i = 0; i < buckets.length; i++) {
                List<String> labelValuesWithLe = new ArrayList<>(labelValuesList);
                labelValuesWithLe.add(doubleToGoString(buckets[i]));
                samples.add(new MetricFamilySamples.Sample(metricName + "_bucket", labelNamesWithLe,
                        labelValuesWithLe, values[i]));
            }
            samples.add(new MetricFamilySamples.Sample(metricName + "_count", labelNamesList, labelValuesList,
                    histogram.getCount()));
            samples.add(new MetricFamilySamples.Sample(metricName + "_sum", labelNamesList, labelValuesList,
                    histogram.getSum()));
            return Collections.singletonList(new MetricFamilySamples(metricName, Type.HISTOGRAM, helpMsg, samples));
        }
    }

}
