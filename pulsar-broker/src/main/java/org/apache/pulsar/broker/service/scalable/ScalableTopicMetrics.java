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

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
import io.prometheus.metrics.config.PrometheusProperties;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.ClassicHistogramBuckets;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricFamilyDescriptor;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Broker-level scalable-topic session and control-plane request metrics, exported on both metrics paths. */
public final class ScalableTopicMetrics implements AutoCloseable {
    public static final String REQUEST_DURATION_METRIC_NAME = "pulsar.broker.scalable_topic.request.duration";
    public static final String WATCH_SESSION_COUNT_METRIC_NAME = "pulsar.broker.scalable_topic.watch.session.count";
    public static final String WATCH_UPDATE_COUNT_METRIC_NAME = "pulsar.broker.scalable_topic.watch.update.count";

    private static final String REQUEST_DURATION = "pulsar_scalable_topic_request_duration_seconds";
    private static final String WATCH_SESSION_COUNT = "pulsar_scalable_topic_watch_session_count";
    private static final String WATCH_UPDATE = "pulsar_scalable_topic_watch_update";
    private static final String ATTRIBUTE_PREFIX = "pulsar.scalable_topic.";
    private static final String REQUEST_HELP = "Scalable-topic request duration from arrival to the first response.";
    private static final String SESSION_HELP = "Open scalable-topic watch sessions.";
    private static final String UPDATE_HELP = "Scalable-topic layout update pushes, excluding initial snapshots.";
    private static final List<Double> DURATION_BUCKETS =
            List.of(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0, 30.0);
    /** The classic histogram bounds including the mandatory +Inf bucket, as the snapshots report them. */
    private static final double[] UPPER_BOUNDS;

    static {
        UPPER_BOUNDS = new double[DURATION_BUCKETS.size() + 1];
        for (int i = 0; i < DURATION_BUCKETS.size(); i++) {
            UPPER_BOUNDS[i] = DURATION_BUCKETS.get(i);
        }
        UPPER_BOUNDS[DURATION_BUCKETS.size()] = Double.POSITIVE_INFINITY;
    }

    // One collector per registry aggregates brokers with the same cluster label. A broker must not unregister
    // another broker's metrics, or publish duplicate samples when several brokers run in the same JVM.
    private static final Map<PrometheusRegistry, SharedCollector> COLLECTORS = new IdentityHashMap<>();

    public enum RequestType {
        LOOKUP("lookup"),
        WATCH_TOPICS("watch_topics"),
        SUBSCRIBE("subscribe"),
        UNSUBSCRIBE("unsubscribe");

        private final String label;
        private final Attributes successAttributes;
        private final Attributes failureAttributes;

        RequestType(String label) {
            this.label = label;
            successAttributes = Attributes.builder()
                    .put(ATTRIBUTE_PREFIX + "request.type", label)
                    .put(ATTRIBUTE_PREFIX + "operation.status", "success").build();
            failureAttributes = Attributes.builder()
                    .put(ATTRIBUTE_PREFIX + "request.type", label)
                    .put(ATTRIBUTE_PREFIX + "operation.status", "failure").build();
        }
    }

    public enum WatchType {
        TOPIC("topic"),
        NAMESPACE("namespace");

        private final String label;
        private final Attributes attributes;

        WatchType(String label) {
            this.label = label;
            attributes = Attributes.builder().put(ATTRIBUTE_PREFIX + "watch.type", label).build();
        }
    }

    private static final Attributes SUCCESS_ATTRIBUTES =
            Attributes.builder().put(ATTRIBUTE_PREFIX + "operation.status", "success").build();
    private static final Attributes FAILURE_ATTRIBUTES =
            Attributes.builder().put(ATTRIBUTE_PREFIX + "operation.status", "failure").build();

    private final String cluster;
    private final PrometheusRegistry registry;
    private final LongSupplier nanoTime;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object cumulativeLock = new Object();
    private final AtomicLong[] watchSessions = {new AtomicLong(), new AtomicLong()};
    private final AtomicLong successfulUpdates = new AtomicLong();
    private final AtomicLong failedUpdates = new AtomicLong();
    private final Histogram prometheusRequestDuration;
    private final DoubleHistogram requestDuration;
    private final ObservableLongUpDownCounter sessionCount;
    private final ObservableLongCounter updateCount;

    public ScalableTopicMetrics(Meter meter, String cluster) {
        this(meter, cluster, PrometheusRegistry.defaultRegistry);
    }

    public ScalableTopicMetrics(Meter meter, String cluster, PrometheusRegistry registry) {
        this(meter, cluster, registry, System::nanoTime);
    }

    @VisibleForTesting
    ScalableTopicMetrics(Meter meter, String cluster, PrometheusRegistry registry, LongSupplier nanoTime) {
        this(meter, cluster, registry, nanoTime, PrometheusProperties.get());
    }

    /**
     * @param clientProperties the Prometheus client runtime configuration. Its metric layout settings (default
     *                         and per-metric, such as {@code histogramClassicUpperBounds}) are ignored for the
     *                         request histogram: the shared collector rebuilds that snapshot with the fixed
     *                         {@link #DURATION_BUCKETS}, and a different live layout would fail every scrape.
     */
    @VisibleForTesting
    ScalableTopicMetrics(Meter meter, String cluster, PrometheusRegistry registry, LongSupplier nanoTime,
                         PrometheusProperties clientProperties) {
        this.cluster = Objects.requireNonNull(cluster);
        this.registry = Objects.requireNonNull(registry);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        Objects.requireNonNull(meter);
        // The shared collector exports no exemplars. Disable sampling here so recording under the freeze lock
        // cannot invoke a globally configured exemplar sampler. The histogram is never registered itself; the
        // shared collector reads its snapshot.
        // Never use the plain Histogram.builder() here: it reads the global client configuration, whose per-metric
        // bucket override would change the live layout while the shared collector rebuilds it with the fixed bounds.
        prometheusRequestDuration = Histogram.builder(withoutMetricOverrides(clientProperties))
                .name(REQUEST_DURATION).help(REQUEST_HELP)
                .labelNames("cluster", "request_type", "operation_status")
                .classicOnly()
                .classicUpperBounds(DURATION_BUCKETS.stream().mapToDouble(Double::doubleValue).toArray())
                .withoutExemplars()
                .build();
        requestDuration = meter.histogramBuilder(REQUEST_DURATION_METRIC_NAME)
                .setDescription(REQUEST_HELP).setUnit("s")
                .setExplicitBucketBoundariesAdvice(DURATION_BUCKETS).build();
        sessionCount = meter.upDownCounterBuilder(WATCH_SESSION_COUNT_METRIC_NAME)
                .setDescription(SESSION_HELP).setUnit("{session}")
                .buildWithCallback(measurement -> {
                    for (WatchType type : WatchType.values()) {
                        measurement.record(watchSessions[type.ordinal()].get(), type.attributes);
                    }
                });
        updateCount = meter.counterBuilder(WATCH_UPDATE_COUNT_METRIC_NAME)
                .setDescription(UPDATE_HELP).setUnit("{update}")
                .buildWithCallback(measurement -> {
                    measurement.record(successfulUpdates.get(), SUCCESS_ATTRIBUTES);
                    measurement.record(failedUpdates.get(), FAILURE_ATTRIBUTES);
                });
        try {
            synchronized (COLLECTORS) {
                SharedCollector collector = COLLECTORS.get(registry);
                if (collector == null) {
                    collector = new SharedCollector();
                    registry.register(collector);
                    COLLECTORS.put(registry, collector);
                }
                collector.add(this);
            }
        } catch (RuntimeException e) {
            sessionCount.close();
            updateCount.close();
            throw e;
        }
    }

    /** The client configuration without its metric layout settings, which would override the fixed buckets. */
    private static PrometheusProperties withoutMetricOverrides(PrometheusProperties properties) {
        return PrometheusProperties.builder()
                .exemplarProperties(properties.getExemplarProperties())
                .exporterProperties(properties.getExporterProperties())
                .build();
    }

    /** Starts timing at request arrival; only the first response records an outcome. */
    public Request startRequest(RequestType type) {
        return new Request(Objects.requireNonNull(type));
    }

    public void sessionOpened(WatchType type) {
        if (!closed.get()) {
            watchSessions[type.ordinal()].incrementAndGet();
        }
    }

    public void sessionClosed(WatchType type) {
        if (!closed.get()) {
            watchSessions[type.ordinal()].decrementAndGet();
        }
    }

    public void recordWatchUpdate(boolean success) {
        synchronized (cumulativeLock) {
            if (!closed.get()) {
                (success ? successfulUpdates : failedUpdates).incrementAndGet();
            }
        }
    }

    public final class Request {
        private final RequestType type;
        private final long startedAt;
        private final AtomicBoolean completed = new AtomicBoolean();

        private Request(RequestType type) {
            this.type = type;
            startedAt = nanoTime.getAsLong();
        }

        public void success() {
            complete(true);
        }

        public void failure() {
            complete(false);
        }

        private void complete(boolean success) {
            if (!completed.compareAndSet(false, true) || closed.get()) {
                return;
            }
            double seconds = (nanoTime.getAsLong() - startedAt) / 1_000_000_000.0;
            synchronized (cumulativeLock) {
                if (closed.get()) {
                    return;
                }
                prometheusRequestDuration.labelValues(cluster, type.label, success ? "success" : "failure")
                        .observe(seconds);
            }
            // An accepted observation must reach both paths even if close() runs here. Keep SDK calls outside the lock.
            requestDuration.record(seconds, success ? type.successAttributes : type.failureAttributes);
        }
    }

    @Override
    public void close() {
        Map<SampleKey, Series> cumulative;
        synchronized (cumulativeLock) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            // Freeze cumulative state before transferring it; an in-flight recording cannot be lost at retirement.
            cumulative = collectCumulative();
        }
        sessionCount.close();
        updateCount.close();
        synchronized (COLLECTORS) {
            SharedCollector collector = COLLECTORS.get(registry);
            collector.retire(this, cumulative);
            if (collector.isEmpty()) {
                registry.unregister(collector);
                COLLECTORS.remove(registry);
            }
        }
    }

    private Map<SampleKey, Series> collect() {
        Map<SampleKey, Series> series = collectCumulative();
        for (WatchType type : WatchType.values()) {
            series.put(new SampleKey(WATCH_SESSION_COUNT, Labels.of("cluster", cluster, "watch_type", type.label)),
                    Series.scalar(watchSessions[type.ordinal()].get()));
        }
        return series;
    }

    private Map<SampleKey, Series> collectCumulative() {
        Map<SampleKey, Series> series = new LinkedHashMap<>();
        for (HistogramSnapshot.HistogramDataPointSnapshot point : prometheusRequestDuration.collect().getDataPoints()) {
            ClassicHistogramBuckets buckets = point.getClassicBuckets();
            long[] counts = new long[buckets.size()];
            for (int i = 0; i < counts.length; i++) {
                counts[i] = buckets.getCount(i);
            }
            series.put(new SampleKey(REQUEST_DURATION, point.getLabels()),
                    new Series(counts, BigDecimal.valueOf(point.getSum())));
        }
        series.put(new SampleKey(WATCH_UPDATE, Labels.of("cluster", cluster, "operation_status", "success")),
                Series.scalar(successfulUpdates.get()));
        series.put(new SampleKey(WATCH_UPDATE, Labels.of("cluster", cluster, "operation_status", "failure")),
                Series.scalar(failedUpdates.get()));
        return series;
    }

    private record SampleKey(String family, Labels labels) {
        private String cluster() {
            return labels.get("cluster");
        }
    }

    /** Exact totals of one series: per-bucket counts and the sum for a histogram, or a scalar with no buckets. */
    private record Series(long[] buckets, BigDecimal value) {
        private static Series scalar(double value) {
            return new Series(null, BigDecimal.valueOf(value));
        }

        private Series plus(Series other) {
            long[] counts = null;
            if (buckets != null) {
                counts = buckets.clone();
                for (int i = 0; i < counts.length; i++) {
                    counts[i] += other.buckets[i];
                }
            }
            return new Series(counts, value.add(other.value));
        }
    }

    private static final class SharedCollector implements MultiCollector {
        private static final MetricFamilyDescriptor REQUEST_DURATION_FAMILY = MetricFamilyDescriptor
                .histogram(REQUEST_DURATION).help(REQUEST_HELP)
                .labelNames("cluster", "request_type", "operation_status").build();
        private static final MetricFamilyDescriptor WATCH_SESSION_COUNT_FAMILY = MetricFamilyDescriptor
                .gauge(WATCH_SESSION_COUNT).help(SESSION_HELP).labelNames("cluster", "watch_type").build();
        // Declared without "_total"; the exposition format appends it.
        private static final MetricFamilyDescriptor WATCH_UPDATE_FAMILY = MetricFamilyDescriptor
                .counter(WATCH_UPDATE).help(UPDATE_HELP).labelNames("cluster", "operation_status").build();
        private static final List<MetricFamilyDescriptor> DESCRIPTORS =
                List.of(REQUEST_DURATION_FAMILY, WATCH_SESSION_COUNT_FAMILY, WATCH_UPDATE_FAMILY);

        // Publish membership and retired totals together, so a scrape sees each broker's contribution exactly once.
        // Writers hold COLLECTORS; scrapes read one immutable snapshot without taking that lock.
        private record State(List<ScalableTopicMetrics> brokers, Map<SampleKey, Series> retired) {}
        private volatile State state = new State(List.of(), Map.of());

        private void add(ScalableTopicMetrics broker) {
            State current = state;
            List<ScalableTopicMetrics> brokers = new ArrayList<>(current.brokers);
            brokers.add(broker);
            state = new State(List.copyOf(brokers), current.retired);
        }

        private void retire(ScalableTopicMetrics broker, Map<SampleKey, Series> cumulative) {
            State current = state;
            List<ScalableTopicMetrics> brokers = new ArrayList<>(current.brokers);
            brokers.remove(broker);
            Map<SampleKey, Series> retired = new LinkedHashMap<>(current.retired);
            if (brokers.stream().anyMatch(active -> active.cluster.equals(broker.cluster))) {
                merge(retired, cumulative);
            } else {
                // The last broker ends this cluster's series lifetime. Retain no history for inactive clusters.
                retired.keySet().removeIf(key -> key.cluster().equals(broker.cluster));
            }
            // Only fixed-label sample totals are retained, never retired brokers or their SDK callbacks.
            state = new State(List.copyOf(brokers), Map.copyOf(retired));
        }

        private boolean isEmpty() {
            return state.brokers.isEmpty();
        }

        @Override
        public List<MetricFamilyDescriptor> getMetricFamilyDescriptors() {
            return DESCRIPTORS;
        }

        private static void merge(Map<SampleKey, Series> totals, Map<SampleKey, Series> series) {
            series.forEach((key, value) -> totals.merge(key, value, Series::plus));
        }

        @Override
        public MetricSnapshots collect() {
            State current = state;
            // Exact aggregation avoids a floating-point _sum decrease when retirement changes the grouping.
            Map<SampleKey, Series> totals = new LinkedHashMap<>(current.retired);
            for (ScalableTopicMetrics broker : current.brokers) {
                // Frozen brokers remain readable by scrapes that captured the pre-retirement snapshot.
                merge(totals, broker.collect());
            }
            List<HistogramSnapshot.HistogramDataPointSnapshot> durations = new ArrayList<>();
            List<GaugeSnapshot.GaugeDataPointSnapshot> sessions = new ArrayList<>();
            List<CounterSnapshot.CounterDataPointSnapshot> updates = new ArrayList<>();
            totals.forEach((key, series) -> {
                switch (key.family) {
                    case REQUEST_DURATION -> durations.add(new HistogramSnapshot.HistogramDataPointSnapshot(
                            ClassicHistogramBuckets.of(UPPER_BOUNDS, series.buckets), series.value.doubleValue(),
                            key.labels, null, 0L));
                    case WATCH_SESSION_COUNT -> sessions.add(new GaugeSnapshot.GaugeDataPointSnapshot(
                            series.value.doubleValue(), key.labels, null));
                    case WATCH_UPDATE -> updates.add(new CounterSnapshot.CounterDataPointSnapshot(
                            series.value.doubleValue(), key.labels, null, 0L));
                    default -> throw new IllegalStateException("Unexpected metric family " + key.family);
                }
            });
            return MetricSnapshots.of(new HistogramSnapshot(REQUEST_DURATION_FAMILY.getMetadata(), durations),
                    new GaugeSnapshot(WATCH_SESSION_COUNT_FAMILY.getMetadata(), sessions),
                    new CounterSnapshot(WATCH_UPDATE_FAMILY.getMetadata(), updates));
        }
    }
}
