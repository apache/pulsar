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
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
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

    // One collector per registry aggregates brokers with the same cluster label. A broker must not unregister
    // another broker's metrics, or publish duplicate samples when several brokers run in the same JVM.
    private static final Map<CollectorRegistry, SharedCollector> COLLECTORS = new IdentityHashMap<>();

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
    private final CollectorRegistry registry;
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
        this(meter, cluster, CollectorRegistry.defaultRegistry);
    }

    public ScalableTopicMetrics(Meter meter, String cluster, CollectorRegistry registry) {
        this(meter, cluster, registry, System::nanoTime);
    }

    @VisibleForTesting
    ScalableTopicMetrics(Meter meter, String cluster, CollectorRegistry registry, LongSupplier nanoTime) {
        this.cluster = Objects.requireNonNull(cluster);
        this.registry = Objects.requireNonNull(registry);
        this.nanoTime = Objects.requireNonNull(nanoTime);
        Objects.requireNonNull(meter);
        // The shared collector exports no exemplars. Disable sampling here so recording under the freeze lock
        // cannot invoke a globally configured exemplar sampler.
        prometheusRequestDuration = Histogram.build(REQUEST_DURATION, REQUEST_HELP)
                .labelNames("cluster", "request_type", "operation_status")
                .withoutExemplars()
                .buckets(DURATION_BUCKETS.stream().mapToDouble(Double::doubleValue).toArray()).create();
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
                prometheusRequestDuration.labels(cluster, type.label, success ? "success" : "failure").observe(seconds);
            }
            // An accepted observation must reach both paths even if close() runs here. Keep SDK calls outside the lock.
            requestDuration.record(seconds, success ? type.successAttributes : type.failureAttributes);
        }
    }

    @Override
    public void close() {
        List<Collector.MetricFamilySamples> cumulative;
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

    private List<Collector.MetricFamilySamples> collect() {
        List<Collector.MetricFamilySamples.Sample> sessions = new ArrayList<>();
        for (WatchType type : WatchType.values()) {
            sessions.add(new Collector.MetricFamilySamples.Sample(WATCH_SESSION_COUNT,
                    List.of("cluster", "watch_type"), List.of(cluster, type.label),
                    watchSessions[type.ordinal()].get()));
        }
        List<Collector.MetricFamilySamples> families = collectCumulative();
        families.add(new Collector.MetricFamilySamples(
                WATCH_SESSION_COUNT, Collector.Type.GAUGE, SESSION_HELP, sessions));
        return families;
    }

    private List<Collector.MetricFamilySamples> collectCumulative() {
        List<Collector.MetricFamilySamples.Sample> updates = List.of(
                new Collector.MetricFamilySamples.Sample(WATCH_UPDATE + "_total",
                        List.of("cluster", "operation_status"), List.of(cluster, "success"), successfulUpdates.get()),
                new Collector.MetricFamilySamples.Sample(WATCH_UPDATE + "_total",
                        List.of("cluster", "operation_status"), List.of(cluster, "failure"), failedUpdates.get()));
        List<Collector.MetricFamilySamples> families = new ArrayList<>(prometheusRequestDuration.collect());
        families.add(new Collector.MetricFamilySamples(WATCH_UPDATE, Collector.Type.COUNTER, UPDATE_HELP, updates));
        return families;
    }

    private record SampleKey(String family, String name, List<String> labelNames, List<String> labelValues) {
        private SampleKey(Collector.MetricFamilySamples family, Collector.MetricFamilySamples.Sample sample) {
            this(family.name, sample.name, List.copyOf(sample.labelNames), List.copyOf(sample.labelValues));
        }

        private String cluster() {
            return labelValues.get(labelNames.indexOf("cluster"));
        }
    }

    private static final class SharedCollector extends Collector implements Collector.Describable {
        // Publish membership and retired totals together, so a scrape sees each broker's contribution exactly once.
        // Writers hold COLLECTORS; scrapes read one immutable snapshot without taking that lock.
        private record State(List<ScalableTopicMetrics> brokers, Map<SampleKey, BigDecimal> retired) {}
        private volatile State state = new State(List.of(), Map.of());

        private void add(ScalableTopicMetrics broker) {
            State current = state;
            List<ScalableTopicMetrics> brokers = new ArrayList<>(current.brokers);
            brokers.add(broker);
            state = new State(List.copyOf(brokers), current.retired);
        }

        private void retire(ScalableTopicMetrics broker, List<MetricFamilySamples> cumulative) {
            State current = state;
            List<ScalableTopicMetrics> brokers = new ArrayList<>(current.brokers);
            brokers.remove(broker);
            Map<SampleKey, BigDecimal> retired = new LinkedHashMap<>(current.retired);
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
        public List<MetricFamilySamples> describe() {
            return List.of(new MetricFamilySamples(REQUEST_DURATION, Type.HISTOGRAM, REQUEST_HELP, List.of()),
                    new MetricFamilySamples(WATCH_SESSION_COUNT, Type.GAUGE, SESSION_HELP, List.of()),
                    new MetricFamilySamples(WATCH_UPDATE, Type.COUNTER, UPDATE_HELP, List.of()));
        }

        private static void merge(Map<SampleKey, BigDecimal> samples, List<MetricFamilySamples> families) {
            for (MetricFamilySamples family : families) {
                for (MetricFamilySamples.Sample sample : family.samples) {
                    // Creation timestamps cannot be summed, and are not part of these metric families.
                    if (!sample.name.endsWith("_created")) {
                        samples.merge(new SampleKey(family, sample), BigDecimal.valueOf(sample.value), BigDecimal::add);
                    }
                }
            }
        }

        @Override
        public List<MetricFamilySamples> collect() {
            State current = state;
            // Exact aggregation avoids a floating-point _sum decrease when retirement changes the grouping.
            Map<SampleKey, BigDecimal> totals = new LinkedHashMap<>(current.retired);
            for (ScalableTopicMetrics broker : current.brokers) {
                // Frozen brokers remain readable by scrapes that captured the pre-retirement snapshot.
                merge(totals, broker.collect());
            }
            List<MetricFamilySamples> result = new ArrayList<>();
            for (MetricFamilySamples description : describe()) {
                List<MetricFamilySamples.Sample> samples = new ArrayList<>();
                totals.forEach((key, value) -> {
                    if (key.family.equals(description.name)) {
                        samples.add(new MetricFamilySamples.Sample(
                                key.name, key.labelNames, key.labelValues, value.doubleValue()));
                    }
                });
                result.add(new MetricFamilySamples(description.name, description.type, description.help, samples));
            }
            return result;
        }
    }
}
