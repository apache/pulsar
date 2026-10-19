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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.Meter;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.apache.pulsar.broker.stats.prometheus.PrometheusCounterMetricFamily;

/** Broker-level segment-owner metrics, with a single source for both export paths. */
public final class SegmentOwnerMetrics implements AutoCloseable {
    public static final String OTEL_PREFIX = "pulsar.broker.scalable_topic.";
    public static final String PROMETHEUS_PREFIX = "pulsar_scalable_topic_";
    private static final String ATTRIBUTE_PREFIX = "pulsar.scalable_topic.";
    // A JVM can host several brokers (notably in tests). Share registration, not broker state,
    // and sum brokers of the same cluster so the registry never emits duplicate label sets.
    private static final Map<CollectorRegistry, OwnerCollector> COLLECTORS = new HashMap<>();

    public enum EntryBucketViolation {
        STRADDLING("straddling"), UNSTAMPED_BATCH("unstamped_batch");

        private final String label;
        private final Attributes attributes;

        EntryBucketViolation(String label) {
            this.label = label;
            attributes = Attributes.builder().put(ATTRIBUTE_PREFIX + "entry_bucket.violation", label).build();
        }
    }

    public enum LoadReportResult {
        WRITTEN("written"), UNCHANGED("unchanged"), FAILED("failed");

        private final String label;
        private final Attributes attributes;

        LoadReportResult(String label) {
            this.label = label;
            attributes = Attributes.builder().put(ATTRIBUTE_PREFIX + "load_report.result", label).build();
        }
    }

    public record Gauges(long leaderless, long unwritable, long drainingBuckets) { }

    private final String cluster;
    private final Supplier<Gauges> gauges;
    private final LongAdder[] violations = {new LongAdder(), new LongAdder()};
    private final LongAdder[] loadReports = {new LongAdder(), new LongAdder(), new LongAdder()};
    private final BatchCallback callback;
    private final CollectorRegistry registry;
    private boolean closed;

    public SegmentOwnerMetrics(Meter meter, String cluster, Supplier<Gauges> gauges) {
        this(meter, CollectorRegistry.defaultRegistry, cluster, gauges);
    }

    SegmentOwnerMetrics(Meter meter, CollectorRegistry registry, String cluster, Supplier<Gauges> gauges) {
        this.cluster = cluster;
        this.gauges = gauges;
        this.registry = registry;
        var leaderless = meter.upDownCounterBuilder(OTEL_PREFIX + "leaderless.count")
                .setDescription("Hosted scalable topics without a controller leader.")
                .setUnit("{topic}").buildObserver();
        var unwritable = meter.upDownCounterBuilder(OTEL_PREFIX + "segment.unwritable.count")
                .setDescription("Hosted active segments whose backing topic is terminated.")
                .setUnit("{segment}").buildObserver();
        var draining = meter.upDownCounterBuilder(OTEL_PREFIX + "entry_bucket.draining.count")
                .setDescription("Entry buckets withheld until their previous owner acknowledges in-flight messages.")
                .setUnit("{bucket}").buildObserver();
        var violation = meter.counterBuilder(OTEL_PREFIX + "entry_bucket.violation.count")
                .setDescription("Entries that cannot be routed by a single entry bucket.")
                .setUnit("{entry}").buildObserver();
        var loadReport = meter.counterBuilder(OTEL_PREFIX + "segment.load_report.count")
                .setDescription("Segment load samples by reporting result.")
                .setUnit("{sample}").buildObserver();
        callback = meter.batchCallback(() -> {
            Gauges current = gauges.get();
            leaderless.record(current.leaderless());
            unwritable.record(current.unwritable());
            draining.record(current.drainingBuckets());
            for (EntryBucketViolation value : EntryBucketViolation.values()) {
                violation.record(violations[value.ordinal()].sum(), value.attributes);
            }
            for (LoadReportResult value : LoadReportResult.values()) {
                loadReport.record(loadReports[value.ordinal()].sum(), value.attributes);
            }
        }, leaderless, unwritable, draining, violation, loadReport);
        synchronized (COLLECTORS) {
            OwnerCollector collector = COLLECTORS.computeIfAbsent(registry, key -> {
                OwnerCollector created = new OwnerCollector();
                key.register(created);
                return created;
            });
            collector.owners.add(this);
        }
    }

    public void recordEntryBucketViolation(EntryBucketViolation violation) {
        violations[violation.ordinal()].increment();
    }

    public void recordLoadReport(LoadReportResult result) {
        loadReports[result.ordinal()].increment();
    }

    @Override
    public void close() {
        synchronized (COLLECTORS) {
            if (closed) {
                return;
            }
            closed = true;
            OwnerCollector collector = COLLECTORS.get(registry);
            collector.owners.remove(this);
            if (collector.owners.isEmpty()) {
                registry.unregister(collector);
                COLLECTORS.remove(registry);
            }
        }
        callback.close();
    }

    private static final class Totals {
        private long leaderless;
        private long unwritable;
        private long draining;
        private final long[] violations = new long[2];
        private final long[] loadReports = new long[3];

        private void add(SegmentOwnerMetrics owner) {
            Gauges current = owner.gauges.get();
            leaderless += current.leaderless();
            unwritable += current.unwritable();
            draining += current.drainingBuckets();
            for (int i = 0; i < violations.length; i++) {
                violations[i] += owner.violations[i].sum();
            }
            for (int i = 0; i < loadReports.length; i++) {
                loadReports[i] += owner.loadReports[i].sum();
            }
        }
    }

    private static final class OwnerCollector extends Collector implements Collector.Describable {
        private final List<SegmentOwnerMetrics> owners = new CopyOnWriteArrayList<>();

        @Override
        public List<MetricFamilySamples> describe() {
            return families(Map.of());
        }

        @Override
        public List<MetricFamilySamples> collect() {
            Map<String, Totals> totals = new HashMap<>();
            owners.forEach(owner -> totals.computeIfAbsent(owner.cluster, __ -> new Totals()).add(owner));
            return families(totals);
        }

        private static List<MetricFamilySamples> families(Map<String, Totals> totals) {
            var leaderless = new GaugeMetricFamily(PROMETHEUS_PREFIX + "leaderless_count",
                    "Hosted scalable topics without a controller leader.", List.of("cluster"));
            var unwritable = new GaugeMetricFamily(PROMETHEUS_PREFIX + "segment_unwritable_count",
                    "Hosted active segments whose backing topic is terminated.", List.of("cluster"));
            var draining = new GaugeMetricFamily(PROMETHEUS_PREFIX + "entry_bucket_draining_count",
                    "Entry buckets withheld until their previous owner acknowledges in-flight messages.",
                    List.of("cluster"));
            var violations = new PrometheusCounterMetricFamily(PROMETHEUS_PREFIX + "entry_bucket_violation",
                    "Entries that cannot be routed by a single entry bucket.",
                    List.of("cluster", "entry_bucket_violation"));
            var loadReports = new PrometheusCounterMetricFamily(PROMETHEUS_PREFIX + "segment_load_report",
                    "Segment load samples by reporting result.", List.of("cluster", "load_report_result"));
            totals.forEach((cluster, total) -> {
                leaderless.addMetric(List.of(cluster), total.leaderless);
                unwritable.addMetric(List.of(cluster), total.unwritable);
                draining.addMetric(List.of(cluster), total.draining);
                for (EntryBucketViolation value : EntryBucketViolation.values()) {
                    violations.addMetric(List.of(cluster, value.label), total.violations[value.ordinal()]);
                }
                for (LoadReportResult value : LoadReportResult.values()) {
                    loadReports.addMetric(List.of(cluster, value.label), total.loadReports[value.ordinal()]);
                }
            });
            return new ArrayList<>(List.of(leaderless, unwritable, draining, violations, loadReports));
        }
    }
}
