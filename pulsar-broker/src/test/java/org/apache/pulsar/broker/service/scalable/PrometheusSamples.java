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
package org.apache.pulsar.broker.service.scalable;

import io.prometheus.metrics.model.registry.MetricType;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.ClassicHistogramBucket;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Flattens a Prometheus Java client 1.x registry scrape into the samples the text exposition prints, so tests can
 * assert on the exposed sample names ({@code _total}, {@code _bucket}, {@code _count}, {@code _sum}) and label sets
 * the way {@code CollectorRegistry.getSampleValue} allowed with the simpleclient. Families without data points are
 * omitted, as the exposition omits them.
 */
public final class PrometheusSamples {
    public record Sample(String name, List<String> labelNames, List<String> labelValues, double value) {
        private Sample(String name, Labels labels, double value) {
            this(name, labelNames(labels), labelValues(labels), value);
        }

        private Sample(String name, Labels labels, String extraName, String extraValue, double value) {
            this(name, append(labelNames(labels), extraName), append(labelValues(labels), extraValue), value);
        }

        private static List<String> labelNames(Labels labels) {
            List<String> names = new ArrayList<>(labels.size());
            for (int i = 0; i < labels.size(); i++) {
                names.add(labels.getPrometheusName(i));
            }
            return names;
        }

        private static List<String> labelValues(Labels labels) {
            List<String> values = new ArrayList<>(labels.size());
            for (int i = 0; i < labels.size(); i++) {
                values.add(labels.getValue(i));
            }
            return values;
        }

        private static List<String> append(List<String> list, String value) {
            list.add(value);
            return list;
        }
    }

    public record Family(String name, MetricType type, String help, List<Sample> samples) { }

    private PrometheusSamples() {
    }

    /**
     * Names of every family a registered collector reports, including families without data points. Use this to
     * assert that a collector was unregistered: {@link #families} would also be empty for a registered collector
     * that merely has nothing to report.
     */
    public static List<String> registeredFamilies(PrometheusRegistry registry) {
        return registry.scrape().stream().map(snapshot -> snapshot.getMetadata().getPrometheusName()).toList();
    }

    public static List<Family> families(PrometheusRegistry registry) {
        List<Family> families = new ArrayList<>();
        for (MetricSnapshot snapshot : registry.scrape()) {
            if (snapshot.getDataPoints().isEmpty()) {
                continue;
            }
            String name = snapshot.getMetadata().getPrometheusName();
            List<Sample> samples = new ArrayList<>();
            MetricType type;
            if (snapshot instanceof CounterSnapshot counter) {
                type = MetricType.COUNTER;
                for (CounterSnapshot.CounterDataPointSnapshot point : counter.getDataPoints()) {
                    samples.add(new Sample(name + "_total", point.getLabels(), point.getValue()));
                }
            } else if (snapshot instanceof GaugeSnapshot gauge) {
                type = MetricType.GAUGE;
                for (GaugeSnapshot.GaugeDataPointSnapshot point : gauge.getDataPoints()) {
                    samples.add(new Sample(name, point.getLabels(), point.getValue()));
                }
            } else if (snapshot instanceof HistogramSnapshot histogram) {
                type = MetricType.HISTOGRAM;
                for (HistogramSnapshot.HistogramDataPointSnapshot point : histogram.getDataPoints()) {
                    long cumulative = 0;
                    for (ClassicHistogramBucket bucket : point.getClassicBuckets()) {
                        cumulative += bucket.getCount();
                        samples.add(new Sample(name + "_bucket", point.getLabels(), "le",
                                upperBound(bucket.getUpperBound()), cumulative));
                    }
                    samples.add(new Sample(name + "_count", point.getLabels(), point.getCount()));
                    samples.add(new Sample(name + "_sum", point.getLabels(), point.getSum()));
                }
            } else if (snapshot instanceof SummarySnapshot summary) {
                type = MetricType.SUMMARY;
                for (SummarySnapshot.SummaryDataPointSnapshot point : summary.getDataPoints()) {
                    samples.add(new Sample(name + "_count", point.getLabels(), point.getCount()));
                    samples.add(new Sample(name + "_sum", point.getLabels(), point.getSum()));
                }
            } else {
                type = MetricType.UNKNOWN;
                for (DataPointSnapshot point : snapshot.getDataPoints()) {
                    samples.add(new Sample(name, point.getLabels(), Double.NaN));
                }
            }
            families.add(new Family(name, type, snapshot.getMetadata().getHelp(), List.copyOf(samples)));
        }
        return families;
    }

    /** The {@code le} label value as the text exposition prints it. */
    public static String upperBound(double bound) {
        return bound == Double.POSITIVE_INFINITY ? "+Inf" : Double.toString(bound);
    }

    public static Double getSampleValue(PrometheusRegistry registry, String name) {
        return getSampleValue(registry, name, new String[0], new String[0]);
    }

    /** Returns the exposed sample with exactly these labels, in any order, or null when it is absent. */
    public static Double getSampleValue(PrometheusRegistry registry, String name, String[] labelNames,
                                        String[] labelValues) {
        List<String> names = Arrays.asList(labelNames);
        List<String> values = Arrays.asList(labelValues);
        for (Family family : families(registry)) {
            for (Sample sample : family.samples) {
                if (sample.name.equals(name) && sameLabels(sample, names, values)) {
                    return sample.value;
                }
            }
        }
        return null;
    }

    private static boolean sameLabels(Sample sample, List<String> names, List<String> values) {
        if (sample.labelNames.size() != names.size()) {
            return false;
        }
        for (int i = 0; i < names.size(); i++) {
            int index = sample.labelNames.indexOf(names.get(i));
            if (index < 0 || !sample.labelValues.get(index).equals(values.get(i))) {
                return false;
            }
        }
        return true;
    }
}
