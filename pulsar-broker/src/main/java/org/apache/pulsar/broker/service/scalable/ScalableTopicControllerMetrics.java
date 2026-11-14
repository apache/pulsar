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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.prometheus.metrics.model.registry.MetricType;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.ClassicHistogramBuckets;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricFamilyDescriptor;
import io.prometheus.metrics.model.snapshots.MetricMetadata;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import lombok.CustomLog;

/**
 * Broker-level controller metrics, with shared state for Prometheus and OpenTelemetry.
 *
 * <p>The ordinary deployment scope is one broker SDK {@link Meter}, one Prometheus registry and one cluster.
 * Real SDK histogram history is Meter-wide and survives provider recreation. Sharing that Meter across distinct
 * registries or clusters does not provide independent histogram isolation: each registry with a live provider
 * exposes the Meter's shared history, including its other clusters. OpenTelemetry cluster scope comes from the
 * broker SDK resource, not an instrument attribute. Use separate SDK Meters for independent scopes.
 *
 * <p>Embedded providers sharing a registry and cluster aggregate counters from live providers only. Membership
 * changes, including partial close or recreation, may lower or reset cumulative counter series, even though the
 * OpenTelemetry observers report monotonic sums. This is not general monotonic multi-broker aggregation.
 * With the API no-op Meter, Prometheus histogram history is provider-local instead of Meter-wide.
 */
@CustomLog
public final class ScalableTopicControllerMetrics implements AutoCloseable {
    private static final String OTEL_PREFIX = "pulsar.broker.scalable_topic.";
    private static final String ATTRIBUTE_PREFIX = "pulsar.scalable_topic.";
    private static final Meter NOOP_METER = MeterProvider.noop().get(ScalableTopicControllerMetrics.class.getName());

    /** Shared explicit histogram boundaries in seconds, from 1 ms through 5 minutes. */
    static final List<Double> DURATION_BUCKETS =
            List.of(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0);

    private static final Set<String> OPERATIONS = Set.of("split", "merge", "rebucket", "prune");
    private static final Set<String> TRIGGERS = Set.of("manual", "auto");
    private static final Set<String> STATUSES = Set.of("success", "failure", "partial");
    private static final Set<String> BLOCKED_REASONS = Set.of("max_segments", "max_entry_buckets", "max_dag_depth",
            "unsplittable", "invalid_policy");

    // A registry can serve several in-JVM brokers. One collector merges identical series, without broker labels.
    // Likewise, one callback per Meter avoids duplicate observations when services share an SDK Meter.
    private static final Map<PrometheusRegistry, RegistryCollector> COLLECTORS = new IdentityHashMap<>();
    private static final Map<Meter, MeterCallbacks> CALLBACKS = new IdentityHashMap<>();
    // SDK histograms outlive service recreation. Values must not reference their weak Meter keys.
    private static final Map<Meter, Map<HistogramKey, Histogram>> HISTOGRAMS = new WeakHashMap<>();

    public static final ScalableTopicControllerMetrics NOOP = new ScalableTopicControllerMetrics();

    public record Counts(long controllers, long active, long sealed) { }

    private enum Metric {
        CONTROLLERS("controller.count", MetricType.GAUGE),
        TAKEOVER("controller.takeover.duration", MetricType.HISTOGRAM, "operation.status"),
        SEGMENTS("segment.count", MetricType.GAUGE, "segment.state"),
        OPERATIONS("layout.operation.count", MetricType.COUNTER, "operation", "operation.trigger",
                "operation.status"),
        OPERATION_DURATION("layout.operation.duration", MetricType.HISTOGRAM, "operation", "operation.status"),
        SEAL_DURATION("layout.seal.duration", MetricType.HISTOGRAM, "operation"),
        BLOCKED("auto_scale.blocked.count", MetricType.COUNTER, "auto_scale.reason");

        private final String name;
        private final MetricType type;
        private final List<String> attributes;
        private final List<String> labelNames;
        private final MetricFamilyDescriptor descriptor;

        Metric(String name, MetricType type, String... attributes) {
            this.name = name;
            this.type = type;
            this.attributes = List.of(attributes);
            // Counters are declared without "_total"; the exposition format appends it.
            String suffix = type == MetricType.COUNTER ? name.substring(0, name.length() - ".count".length())
                    : name;
            String prometheusName = "pulsar_scalable_topic_" + suffix.replace('.', '_')
                    + (type == MetricType.HISTOGRAM ? "_seconds" : "");
            List<String> names = new ArrayList<>(List.of("cluster"));
            for (String attribute : attributes) {
                names.add(attribute.replace('.', '_'));
            }
            this.labelNames = List.copyOf(names);
            this.descriptor = MetricFamilyDescriptor.of(type,
                    new MetricMetadata(prometheusName, "Scalable topic " + name), labelNames);
        }

        private Labels labels(String cluster, Attributes attributes) {
            String[] values = new String[labelNames.size()];
            values[0] = cluster;
            for (int i = 0; i < this.attributes.size(); i++) {
                values[i + 1] = attributes.get(AttributeKey.stringKey(ATTRIBUTE_PREFIX + this.attributes.get(i)));
            }
            return Labels.of(labelNames.toArray(String[]::new), values);
        }
    }

    private record Point(Metric metric, Attributes attributes) { }

    private record HistogramKey(String cluster, Point point) { }

    private final String cluster;
    private final Supplier<Counts> counts;
    private final PrometheusRegistry registry;
    private final Meter meter;
    private final Map<Point, LongAdder> counters = new ConcurrentHashMap<>();
    private final Map<HistogramKey, Histogram> histograms;
    private final Map<Metric, DoubleHistogram> otelHistograms = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private ScalableTopicControllerMetrics() {
        cluster = null;
        counts = null;
        registry = null;
        meter = null;
        histograms = null;
        closed.set(true);
    }

    /**
     * Creates metrics for the ordinary one-broker SDK Meter, default registry and cluster scope.
     * Real SDK histogram history is Meter-wide, not independently isolated by distinct registries or clusters;
     * use separate SDK Meters for independent scopes. Embedded providers sharing the registry and cluster aggregate
     * live-provider counters only: membership changes or partial close may lower or reset cumulative series,
     * rather than provide general monotonic multi-broker aggregation.
     */
    public ScalableTopicControllerMetrics(Meter meter, String cluster, Supplier<Counts> counts) {
        this(meter, cluster, counts, PrometheusRegistry.defaultRegistry);
    }

    /**
     * Creates metrics with an explicit registry for embedded deployments and tests; the ordinary deployment
     * scope remains one broker SDK Meter, registry and cluster. A distinct registry or cluster does not isolate
     * real SDK Meter-wide histogram history; use separate SDK Meters for independent scopes. Providers sharing a
     * registry and cluster contribute live-provider counters, so membership changes or partial close may lower or
     * reset cumulative series. This is not general monotonic multi-broker aggregation.
     */
    ScalableTopicControllerMetrics(Meter meter, String cluster, Supplier<Counts> counts, PrometheusRegistry registry) {
        this.meter = Objects.requireNonNull(meter);
        this.cluster = Objects.requireNonNull(cluster);
        this.counts = Objects.requireNonNull(counts);
        this.registry = Objects.requireNonNull(registry);
        if (meter == NOOP_METER) {
            // The API no-op Meter is shared across brokers and retains no SDK observations.
            // Its Prometheus histograms must belong to this provider, not the singleton Meter's lifetime.
            histograms = new ConcurrentHashMap<>();
        } else {
            synchronized (HISTOGRAMS) {
                histograms = HISTOGRAMS.computeIfAbsent(meter, ignored -> new ConcurrentHashMap<>());
            }
        }
        for (Metric metric : List.of(Metric.TAKEOVER, Metric.OPERATION_DURATION, Metric.SEAL_DURATION)) {
            otelHistograms.put(metric, meter.histogramBuilder(OTEL_PREFIX + metric.name)
                    .setUnit("s").setDescription("Scalable topic " + metric.name)
                    .setExplicitBucketBoundariesAdvice(DURATION_BUCKETS).build());
        }
        synchronized (COLLECTORS) {
            RegistryCollector existing = COLLECTORS.get(registry);
            if (existing == null) {
                existing = new RegistryCollector();
                registry.register(existing);
                COLLECTORS.put(registry, existing);
            }
            existing.providers.add(this);
        }
        try {
            synchronized (CALLBACKS) {
                CALLBACKS.computeIfAbsent(meter, MeterCallbacks::new).providers.add(this);
            }
        } catch (RuntimeException e) {
            // Callback setup failed after publishing this provider: remove only its registry ownership.
            synchronized (COLLECTORS) {
                RegistryCollector collector = COLLECTORS.get(registry);
                collector.providers.remove(this);
                if (collector.providers.isEmpty()) {
                    registry.unregister(collector);
                    COLLECTORS.remove(registry);
                }
            }
            throw e;
        }
    }

    public void recordTakeover(double seconds, boolean success) {
        if (!closed.get()) {
            recordHistogram(Metric.TAKEOVER, seconds, attributes("operation.status", success ? "success" : "failure"));
        }
    }

    public void recordOperation(String operation, String trigger, String status, double seconds) {
        if (!closed.get()) {
            validate(operation, OPERATIONS);
            validate(trigger, TRIGGERS);
            validate(status, STATUSES);
            validateDuration(seconds);
            counters.computeIfAbsent(new Point(Metric.OPERATIONS, attributes("operation", operation,
                    "operation.trigger", trigger, "operation.status", status)), ignored -> new LongAdder()).increment();
            recordHistogram(Metric.OPERATION_DURATION, seconds,
                    attributes("operation", operation, "operation.status", status));
        }
    }

    public void recordSeal(String operation, double seconds) {
        if (!closed.get()) {
            validate(operation, Set.of("split", "merge", "rebucket"));
            recordHistogram(Metric.SEAL_DURATION, seconds, attributes("operation", operation));
        }
    }

    public void recordBlocked(String reason) {
        if (!closed.get()) {
            validate(reason, BLOCKED_REASONS);
            counters.computeIfAbsent(new Point(Metric.BLOCKED, attributes("auto_scale.reason", reason)),
                    ignored -> new LongAdder()).increment();
        }
    }

    private static void validate(String value, Set<String> allowed) {
        if (value == null || !allowed.contains(value)) {
            throw new IllegalArgumentException("Invalid scalable topic metric label: " + value);
        }
    }

    private static void validateDuration(double seconds) {
        if (!Double.isFinite(seconds) || seconds < 0) {
            throw new IllegalArgumentException("Duration must be finite and nonnegative");
        }
    }

    private static Attributes attributes(String... pairs) {
        var builder = Attributes.builder();
        for (int i = 0; i < pairs.length; i += 2) {
            builder.put(ATTRIBUTE_PREFIX + pairs[i], pairs[i + 1]);
        }
        return builder.build();
    }

    private void recordHistogram(Metric metric, double seconds, Attributes attributes) {
        validateDuration(seconds);
        histograms.computeIfAbsent(new HistogramKey(cluster, new Point(metric, attributes)),
                ignored -> new Histogram()).record(seconds);
        otelHistograms.get(metric).record(seconds, attributes);
    }

    private void collectState(Map<Point, Long> values) {
        try {
            Counts current = Objects.requireNonNull(counts.get());
            values.merge(new Point(Metric.CONTROLLERS, Attributes.empty()), current.controllers(), Long::sum);
            values.merge(new Point(Metric.SEGMENTS, attributes("segment.state", "active")),
                    current.active(), Long::sum);
            values.merge(new Point(Metric.SEGMENTS, attributes("segment.state", "sealed")),
                    current.sealed(), Long::sum);
        } catch (RuntimeException e) {
            // A failed new state read must not fail the shared scrape or suppress other providers' metrics.
            log.debug().exceptionMessage(e).log("Failed to collect scalable topic controller counts");
        }
        counters.forEach((point, value) -> values.merge(point, value.sum(), Long::sum));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        synchronized (COLLECTORS) {
            RegistryCollector collector = COLLECTORS.get(registry);
            collector.providers.remove(this);
            if (collector.providers.isEmpty()) {
                registry.unregister(collector);
                COLLECTORS.remove(registry);
            }
        }
        synchronized (CALLBACKS) {
            MeterCallbacks callbacks = CALLBACKS.get(meter);
            callbacks.providers.remove(this);
            if (callbacks.providers.isEmpty()) {
                callbacks.callback.close();
                CALLBACKS.remove(meter);
            }
        }
    }

    private static final class Histogram {
        private final LongAdder[] buckets = new LongAdder[DURATION_BUCKETS.size() + 1];
        private final DoubleAdder sum = new DoubleAdder();

        private Histogram() {
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] = new LongAdder();
            }
        }

        private void record(double seconds) {
            int bucket = 0;
            while (bucket < DURATION_BUCKETS.size() && seconds > DURATION_BUCKETS.get(bucket)) {
                bucket++;
            }
            buckets[bucket].increment();
            sum.add(seconds);
        }
    }

    private static final class MeterCallbacks {
        private final Set<ScalableTopicControllerMetrics> providers = ConcurrentHashMap.newKeySet();
        private final BatchCallback callback;

        private MeterCallbacks(Meter meter) {
            Map<Metric, ObservableLongMeasurement> observers = new HashMap<>();
            observers.put(Metric.CONTROLLERS, meter.upDownCounterBuilder(OTEL_PREFIX + Metric.CONTROLLERS.name)
                    .setUnit("{controller}").buildObserver());
            observers.put(Metric.SEGMENTS, meter.upDownCounterBuilder(OTEL_PREFIX + Metric.SEGMENTS.name)
                    .setUnit("{segment}").buildObserver());
            observers.put(Metric.OPERATIONS, meter.counterBuilder(OTEL_PREFIX + Metric.OPERATIONS.name)
                    .setUnit("{operation}").buildObserver());
            observers.put(Metric.BLOCKED, meter.counterBuilder(OTEL_PREFIX + Metric.BLOCKED.name)
                    .setUnit("{evaluation}").buildObserver());
            callback = meter.batchCallback(() -> {
                Map<Point, Long> values = new HashMap<>();
                providers.forEach(provider -> provider.collectState(values));
                values.forEach((point, value) -> observers.get(point.metric).record(value, point.attributes));
            }, observers.get(Metric.CONTROLLERS), observers.get(Metric.SEGMENTS),
                    observers.get(Metric.OPERATIONS), observers.get(Metric.BLOCKED));
        }
    }

    /**
     * Merges live-provider state and counters by cluster in one registry, without general monotonic multi-broker
     * guarantees: provider membership changes or partial close can lower or reset counters. The ordinary scope is
     * one broker SDK Meter, registry and cluster. Each real SDK Meter's histogram history is included once while
     * any provider using it remains in this registry, including history recorded through other registries or clusters.
     * Distinct registries or clusters sharing that Meter are not independent histogram scopes; use separate SDK
     * Meters for isolation. API no-op Meter histograms instead follow each provider's lifetime.
     */
    private static final class RegistryCollector implements MultiCollector {
        private static final List<MetricFamilyDescriptor> DESCRIPTORS =
                Arrays.stream(Metric.values()).map(metric -> metric.descriptor).toList();
        private static final double[] UPPER_BOUNDS;

        static {
            UPPER_BOUNDS = new double[DURATION_BUCKETS.size() + 1];
            for (int i = 0; i < DURATION_BUCKETS.size(); i++) {
                UPPER_BOUNDS[i] = DURATION_BUCKETS.get(i);
            }
            UPPER_BOUNDS[DURATION_BUCKETS.size()] = Double.POSITIVE_INFINITY;
        }

        private final Set<ScalableTopicControllerMetrics> providers = ConcurrentHashMap.newKeySet();

        @Override
        public List<MetricFamilyDescriptor> getMetricFamilyDescriptors() {
            return DESCRIPTORS;
        }

        @Override
        public MetricSnapshots collect() {
            Map<Metric, Map<Labels, Double>> values = new EnumMap<>(Metric.class);
            Map<Metric, Map<Labels, Distribution>> distributions = new EnumMap<>(Metric.class);
            Set<Map<HistogramKey, Histogram>> histograms = Collections.newSetFromMap(new IdentityHashMap<>());
            for (ScalableTopicControllerMetrics provider : providers) {
                Map<Point, Long> state = new HashMap<>();
                provider.collectState(state);
                state.forEach((point, value) -> values.computeIfAbsent(point.metric, ignored -> new HashMap<>())
                        .merge(point.metric.labels(provider.cluster, point.attributes), (double) value, Double::sum));
                histograms.add(provider.histograms);
            }
            histograms.forEach(state -> state.forEach((key, histogram) ->
                    distributions.computeIfAbsent(key.point.metric, ignored -> new HashMap<>())
                            .computeIfAbsent(key.point.metric.labels(key.cluster, key.point.attributes),
                                    ignored -> new Distribution()).add(histogram)));
            List<MetricSnapshot> result = new ArrayList<>();
            for (Metric metric : Metric.values()) {
                MetricMetadata metadata = metric.descriptor.getMetadata();
                result.add(switch (metric.type) {
                    case GAUGE -> new GaugeSnapshot(metadata, values.getOrDefault(metric, Map.of()).entrySet()
                            .stream().map(entry -> new GaugeSnapshot.GaugeDataPointSnapshot(
                                    entry.getValue(), entry.getKey(), null)).toList());
                    case COUNTER -> new CounterSnapshot(metadata, values.getOrDefault(metric, Map.of()).entrySet()
                            .stream().map(entry -> new CounterSnapshot.CounterDataPointSnapshot(
                                    entry.getValue(), entry.getKey(), null, 0L)).toList());
                    case HISTOGRAM -> new HistogramSnapshot(metadata,
                            distributions.getOrDefault(metric, Map.of()).entrySet().stream()
                                    .map(entry -> new HistogramSnapshot.HistogramDataPointSnapshot(
                                            ClassicHistogramBuckets.of(UPPER_BOUNDS, entry.getValue().buckets),
                                            entry.getValue().sum, entry.getKey(), null, 0L)).toList());
                    default -> throw new IllegalStateException("Unexpected metric type " + metric.type);
                });
            }
            return new MetricSnapshots(result);
        }

        /** Per-bucket counts of one series summed over the live histogram states that share its labels. */
        private static final class Distribution {
            private final long[] buckets = new long[UPPER_BOUNDS.length];
            private double sum;

            private void add(Histogram histogram) {
                for (int i = 0; i < buckets.length; i++) {
                    buckets[i] += histogram.buckets[i].sum();
                }
                sum += histogram.sum.sum();
            }
        }
    }
}
