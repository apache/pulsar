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
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricFamilyDescriptor;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/** Broker-level segment-owner metrics, with a single source for both export paths. */
public final class SegmentOwnerMetrics implements AutoCloseable {
    public static final String OTEL_PREFIX = "pulsar.broker.scalable_topic.";
    public static final String PROMETHEUS_PREFIX = "pulsar_scalable_topic_";
    private static final String ATTRIBUTE_PREFIX = "pulsar.scalable_topic.";
    // A JVM can host several brokers (notably in tests). Share registration, not broker state,
    // and sum brokers of the same cluster so the registry never emits duplicate label sets.
    private static final Map<PrometheusRegistry, OwnerCollector> COLLECTORS = new HashMap<>();

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
    private final PrometheusRegistry registry;
    private boolean closed;

    public SegmentOwnerMetrics(Meter meter, String cluster, Supplier<Gauges> gauges) {
        this(meter, PrometheusRegistry.defaultRegistry, cluster, gauges);
    }

    SegmentOwnerMetrics(Meter meter, PrometheusRegistry registry, String cluster, Supplier<Gauges> gauges) {
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

    private static final class OwnerCollector implements MultiCollector {
        // Counters are declared without "_total"; the exposition format appends it.
        private static final MetricFamilyDescriptor LEADERLESS = MetricFamilyDescriptor
                .gauge(PROMETHEUS_PREFIX + "leaderless_count")
                .help("Hosted scalable topics without a controller leader.").labelNames("cluster").build();
        private static final MetricFamilyDescriptor UNWRITABLE = MetricFamilyDescriptor
                .gauge(PROMETHEUS_PREFIX + "segment_unwritable_count")
                .help("Hosted active segments whose backing topic is terminated.").labelNames("cluster").build();
        private static final MetricFamilyDescriptor DRAINING = MetricFamilyDescriptor
                .gauge(PROMETHEUS_PREFIX + "entry_bucket_draining_count")
                .help("Entry buckets withheld until their previous owner acknowledges in-flight messages.")
                .labelNames("cluster").build();
        private static final MetricFamilyDescriptor VIOLATIONS = MetricFamilyDescriptor
                .counter(PROMETHEUS_PREFIX + "entry_bucket_violation")
                .help("Entries that cannot be routed by a single entry bucket.")
                .labelNames("cluster", "entry_bucket_violation").build();
        private static final MetricFamilyDescriptor LOAD_REPORTS = MetricFamilyDescriptor
                .counter(PROMETHEUS_PREFIX + "segment_load_report")
                .help("Segment load samples by reporting result.").labelNames("cluster", "load_report_result").build();
        private static final List<MetricFamilyDescriptor> DESCRIPTORS =
                List.of(LEADERLESS, UNWRITABLE, DRAINING, VIOLATIONS, LOAD_REPORTS);

        private final List<SegmentOwnerMetrics> owners = new CopyOnWriteArrayList<>();

        @Override
        public List<MetricFamilyDescriptor> getMetricFamilyDescriptors() {
            return DESCRIPTORS;
        }

        @Override
        public MetricSnapshots collect() {
            Map<String, Totals> totals = new HashMap<>();
            owners.forEach(owner -> totals.computeIfAbsent(owner.cluster, __ -> new Totals()).add(owner));
            List<GaugeSnapshot.GaugeDataPointSnapshot> leaderless = new ArrayList<>();
            List<GaugeSnapshot.GaugeDataPointSnapshot> unwritable = new ArrayList<>();
            List<GaugeSnapshot.GaugeDataPointSnapshot> draining = new ArrayList<>();
            List<CounterSnapshot.CounterDataPointSnapshot> violations = new ArrayList<>();
            List<CounterSnapshot.CounterDataPointSnapshot> loadReports = new ArrayList<>();
            totals.forEach((cluster, total) -> {
                leaderless.add(gauge(Labels.of("cluster", cluster), total.leaderless));
                unwritable.add(gauge(Labels.of("cluster", cluster), total.unwritable));
                draining.add(gauge(Labels.of("cluster", cluster), total.draining));
                for (EntryBucketViolation value : EntryBucketViolation.values()) {
                    violations.add(counter(Labels.of("cluster", cluster, "entry_bucket_violation", value.label),
                            total.violations[value.ordinal()]));
                }
                for (LoadReportResult value : LoadReportResult.values()) {
                    loadReports.add(counter(Labels.of("cluster", cluster, "load_report_result", value.label),
                            total.loadReports[value.ordinal()]));
                }
            });
            return MetricSnapshots.of(new GaugeSnapshot(LEADERLESS.getMetadata(), leaderless),
                    new GaugeSnapshot(UNWRITABLE.getMetadata(), unwritable),
                    new GaugeSnapshot(DRAINING.getMetadata(), draining),
                    new CounterSnapshot(VIOLATIONS.getMetadata(), violations),
                    new CounterSnapshot(LOAD_REPORTS.getMetadata(), loadReports));
        }

        private static GaugeSnapshot.GaugeDataPointSnapshot gauge(Labels labels, long value) {
            return new GaugeSnapshot.GaugeDataPointSnapshot(value, labels, null);
        }

        private static CounterSnapshot.CounterDataPointSnapshot counter(Labels labels, long value) {
            return new CounterSnapshot.CounterDataPointSnapshot(value, labels, null, 0L);
        }
    }
}
