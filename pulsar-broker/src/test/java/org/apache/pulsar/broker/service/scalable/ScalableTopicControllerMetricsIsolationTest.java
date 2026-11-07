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

import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.service.scalable.ScalableTopicMetrics.RequestType;
import org.apache.pulsar.broker.service.scalable.ScalableTopicMetrics.WatchType;
import org.apache.pulsar.broker.stats.BrokerOperabilityMetrics;
import org.apache.pulsar.broker.stats.OpenTelemetryTopicStats;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ScalableTopicControllerMetricsIsolationTest {
    private static final String PREFIX = "pulsar_metrics_isolation_";
    private static final String OTEL_PREFIX = "pulsar.broker.scalable_topic.";
    private static final Set<String> CONTROLLER_METRICS = Set.of(
            OTEL_PREFIX + "controller.count", OTEL_PREFIX + "controller.takeover.duration",
            OTEL_PREFIX + "segment.count", OTEL_PREFIX + "layout.operation.count",
            OTEL_PREFIX + "layout.operation.duration", OTEL_PREFIX + "layout.seal.duration",
            OTEL_PREFIX + "auto_scale.blocked.count");
    private static final String REQUEST_DURATION = "pulsar_scalable_topic_request_duration_seconds";
    private static final String WATCH_SESSIONS = "pulsar_scalable_topic_watch_session_count";
    private static final String WATCH_UPDATES = "pulsar_scalable_topic_watch_update";
    private static final Set<String> SESSION_METRICS = Set.of(REQUEST_DURATION, WATCH_SESSIONS, WATCH_UPDATES);
    private static final String CLUSTER = "metrics-isolation";
    private static final ScalableTopicControllerMetrics.Counts COUNTS =
            new ScalableTopicControllerMetrics.Counts(1, 2, 3);

    @DataProvider
    public Object[][] registries() {
        return new Object[][]{{new CollectorRegistry()}, {CollectorRegistry.defaultRegistry}};
    }

    @Test(dataProvider = "registries")
    public void testExistingPrometheusCollectorsRemainUnchanged(CollectorRegistry registry) {
        Counter counter = Counter.build(PREFIX + "counter_total", "Existing counter")
                .labelNames("cluster", "topic").register(registry);
        Gauge gauge = Gauge.build(PREFIX + "gauge", "Existing gauge")
                .labelNames("cluster", "topic").register(registry);
        Histogram histogram = Histogram.build(PREFIX + "histogram_seconds", "Existing histogram")
                .labelNames("cluster", "topic").buckets(0.01, 0.1, 1).register(registry);
        Summary summary = Summary.build(PREFIX + "summary_seconds", "Existing summary")
                .labelNames("cluster", "topic").quantile(0.5, 0.01).register(registry);
        List<Collector> existing = List.of(counter, gauge, histogram, summary);
        try (var sdk = SdkMeterProvider.builder().build()) {
            Meter meter = sdk.get("shared-broker-meter");
            counter.labels(CLUSTER, "persistent://tenant/ns/topic").inc(7);
            gauge.labels(CLUSTER, "persistent://tenant/ns/topic").set(3);
            histogram.labels(CLUSTER, "persistent://tenant/ns/topic").observe(0.05);
            summary.labels(CLUSTER, "persistent://tenant/ns/topic").observe(0.02);
            List<MetricFamilySamples> before = existingPrometheus(registry);
            assertThat(before).hasSize(4);
            try (var added = new ScalableTopicControllerMetrics(meter, CLUSTER, () -> COUNTS, registry)) {
                recordAllMetrics(added);
                assertThat(existingPrometheus(registry)).containsExactlyInAnyOrderElementsOf(before);
                assertThat(existingPrometheus(registry)).containsExactlyInAnyOrderElementsOf(before);
                counter.labels(CLUSTER, "persistent://tenant/ns/topic").inc(2);
                gauge.labels(CLUSTER, "persistent://tenant/ns/topic").dec();
                histogram.labels(CLUSTER, "persistent://tenant/ns/topic").observe(0.5);
                summary.labels(CLUSTER, "persistent://tenant/ns/topic").observe(0.2);
            }
            List<MetricFamilySamples> after = existingPrometheus(registry);
            assertThat(after).isNotEqualTo(before);
            assertThat(counter.labels(CLUSTER, "persistent://tenant/ns/topic").get()).isEqualTo(9);
            assertThat(gauge.labels(CLUSTER, "persistent://tenant/ns/topic").get()).isEqualTo(2);
            try (var replacement = new ScalableTopicControllerMetrics(meter, CLUSTER, () -> COUNTS, registry)) {
                recordAllMetrics(replacement);
                assertThat(existingPrometheus(registry)).containsExactlyInAnyOrderElementsOf(after);
            }
            assertThat(existingPrometheus(registry)).containsExactlyInAnyOrderElementsOf(after);
        } finally {
            existing.forEach(registry::unregister);
        }
    }

    @Test
    public void testExistingOpenTelemetryInstrumentsRemainUnchanged() {
        var reader = InMemoryMetricReader.create();
        try (var sdk = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            Meter meter = sdk.get("shared-broker-meter");
            Attributes attributes = Attributes.builder().put("pulsar.topic", "persistent://tenant/ns/topic").build();
            AtomicLong connections = new AtomicLong(7);
            AtomicLong producers = new AtomicLong(3);
            // Use the existing names and kinds on the very same SDK Meter as the added instruments.
            try (var counter = meter.counterBuilder(BrokerOperabilityMetrics.CONNECTION_COUNTER_METRIC_NAME)
                         .setDescription("Existing connection counter").setUnit("{connection}")
                         .buildWithCallback(m -> m.record(connections.get(), attributes));
                 var upDownCounter = meter.upDownCounterBuilder(OpenTelemetryTopicStats.PRODUCER_COUNTER)
                         .setDescription("Existing producer count").setUnit("{producer}")
                         .buildWithCallback(m -> m.record(producers.get(), attributes))) {
                var histogram = meter.histogramBuilder(BrokerOperabilityMetrics.TOPIC_PUBLISH_LATENCY_METRIC_NAME)
                        .setDescription("Existing publish latency").setUnit("s")
                        .setExplicitBucketBoundariesAdvice(List.of(0.01, 0.1, 1.0)).build();
                histogram.record(0.05, attributes);
                Map<String, Object> before = existingOpenTelemetry(reader.collectAllMetrics());
                assertThat(before).hasSize(3);
                Map<String, Object> updated;
                try (var added = new ScalableTopicControllerMetrics(
                        meter, CLUSTER, () -> COUNTS, new CollectorRegistry())) {
                    recordAllMetrics(added);
                    assertThat(existingOpenTelemetry(reader.collectAllMetrics())).isEqualTo(before);
                    assertThat(existingOpenTelemetry(reader.collectAllMetrics())).isEqualTo(before);
                    connections.addAndGet(2);
                    producers.decrementAndGet();
                    histogram.record(0.5, attributes);
                    updated = existingOpenTelemetry(reader.collectAllMetrics());
                    assertThat(updated).isNotEqualTo(before);
                }
                assertThat(existingOpenTelemetry(reader.collectAllMetrics())).isEqualTo(updated);
                try (var replacement = new ScalableTopicControllerMetrics(
                        meter, CLUSTER, () -> COUNTS, new CollectorRegistry())) {
                    recordAllMetrics(replacement);
                    assertThat(existingOpenTelemetry(reader.collectAllMetrics())).isEqualTo(updated);
                }
                assertThat(existingOpenTelemetry(reader.collectAllMetrics())).isEqualTo(updated);
            }
        }
    }

    @DataProvider
    public Object[][] sessionMetricsModes() {
        return new Object[][]{
                {true, new CollectorRegistry()}, {true, CollectorRegistry.defaultRegistry},
                {false, new CollectorRegistry()}, {false, CollectorRegistry.defaultRegistry}
        };
    }

    @DataProvider
    public Object[][] residentSessionMetricsModes() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "residentSessionMetricsModes")
    public void testCoexistenceWithResidentSessionProvider(boolean telemetryEnabled) {
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        String residentCluster = "resident-session-" + UUID.randomUUID();
        var reader = InMemoryMetricReader.create();
        try (var sdk = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            Meter meter = telemetryEnabled ? sdk.get("resident-broker") : MeterProvider.noop().get("resident-broker");
            try (var resident = new ScalableTopicMetrics(meter, residentCluster, registry)) {
                resident.sessionOpened(WatchType.TOPIC);
                resident.recordWatchUpdate(true);
                AtomicLong successfulUpdates = new AtomicLong(1);
                Runnable residentActivity = () -> {
                    resident.startRequest(RequestType.LOOKUP).success();
                    resident.recordWatchUpdate(true);
                    successfulUpdates.incrementAndGet();
                };
                for (int attempt = 0; attempt < 2; attempt++) {
                    assertSessionMetricsSurviveControllerLifecycle(telemetryEnabled, registry, residentActivity);
                    assertThat(registry.getSampleValue(WATCH_SESSIONS, new String[]{"cluster", "watch_type"},
                            new String[]{residentCluster, "topic"})).isEqualTo(1);
                    assertThat(registry.getSampleValue(WATCH_UPDATES + "_total",
                            new String[]{"cluster", "operation_status"}, new String[]{residentCluster, "success"}))
                            .isEqualTo((double) successfulUpdates.get());
                    assertThat(Collections.list(registry.metricFamilySamples())).extracting(family -> family.name)
                            .containsAll(SESSION_METRICS);
                    if (telemetryEnabled) {
                        assertThat(reader.collectAllMetrics()).anySatisfy(metric -> {
                            assertThat(metric.getName()).isEqualTo(ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME);
                            assertThat(metric.getLongSumData().getPoints()).filteredOn(point -> point.getAttributes()
                                            .equals(Attributes.builder()
                                                    .put("pulsar.scalable_topic.operation.status", "success").build()))
                                    .singleElement().satisfies(point ->
                                            assertThat(point.getValue()).isEqualTo(successfulUpdates.get()));
                        });
                        assertThat(reader.collectAllMetrics()).anySatisfy(metric -> {
                            assertThat(metric.getName()).isEqualTo(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME);
                            assertThat(metric.getHistogramData().getPoints()).singleElement().satisfies(point ->
                                    assertThat(point.getCount()).isEqualTo(successfulUpdates.get() - 1));
                        });
                    } else {
                        assertThat(reader.collectAllMetrics()).isEmpty();
                    }
                }
            }
        }
    }

    @Test(dataProvider = "sessionMetricsModes")
    public void testUpstreamSessionMetricsSurviveControllerLifecycle(boolean telemetryEnabled,
                                                                   CollectorRegistry registry) {
        assertSessionMetricsSurviveControllerLifecycle(telemetryEnabled, registry, () -> { });
    }

    private void assertSessionMetricsSurviveControllerLifecycle(boolean telemetryEnabled, CollectorRegistry registry,
                                                              Runnable residentActivity) {
        String cluster = "session-isolation-" + UUID.randomUUID();
        var reader = InMemoryMetricReader.create();
        try (var sdk = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            Meter meter = telemetryEnabled ? sdk.get("shared-broker-meter")
                    : MeterProvider.noop().get("shared-broker-meter");
            try (var sessions = new ScalableTopicMetrics(meter, cluster, registry)) {
                for (RequestType type : RequestType.values()) {
                    sessions.startRequest(type).success();
                    sessions.startRequest(type).failure();
                }
                sessions.sessionOpened(WatchType.TOPIC);
                sessions.sessionOpened(WatchType.TOPIC);
                sessions.sessionClosed(WatchType.TOPIC);
                sessions.sessionOpened(WatchType.NAMESPACE);
                sessions.recordWatchUpdate(true);
                sessions.recordWatchUpdate(true);
                sessions.recordWatchUpdate(false);
                SessionMetricsSnapshot before = sessionMetricsSnapshot(registry, reader, cluster);
                assertThat(before.prometheus()).extracting(family -> family.name)
                        .containsExactlyInAnyOrderElementsOf(SESSION_METRICS);
                assertThat(before.prometheus()).filteredOn(family -> family.name.equals(REQUEST_DURATION))
                        .singleElement().satisfies(family -> assertThat(family.samples)
                                .filteredOn(sample -> sample.name.equals(REQUEST_DURATION + "_count"))
                                .hasSize(8).allSatisfy(sample -> {
                                    assertThat(sample.value).isEqualTo(1);
                                    assertThat(sample.labelNames)
                                            .containsExactly("cluster", "request_type", "operation_status");
                                }));
                assertThat(registry.getSampleValue(WATCH_SESSIONS,
                        new String[]{"cluster", "watch_type"}, new String[]{cluster, "topic"})).isEqualTo(1);
                assertThat(registry.getSampleValue(WATCH_SESSIONS,
                        new String[]{"cluster", "watch_type"}, new String[]{cluster, "namespace"})).isEqualTo(1);
                assertThat(registry.getSampleValue(WATCH_UPDATES + "_total",
                        new String[]{"cluster", "operation_status"}, new String[]{cluster, "success"})).isEqualTo(2);
                assertThat(registry.getSampleValue(WATCH_UPDATES + "_total",
                        new String[]{"cluster", "operation_status"}, new String[]{cluster, "failure"})).isEqualTo(1);
                if (telemetryEnabled) {
                    assertThat(before.openTelemetry()).containsOnlyKeys(
                            ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME,
                            ScalableTopicMetrics.WATCH_SESSION_COUNT_METRIC_NAME,
                            ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME);
                    assertThat(reader.collectAllMetrics()).anySatisfy(metric -> {
                        assertThat(metric.getName()).isEqualTo(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME);
                        assertThat(metric.getHistogramData().getPoints()).hasSize(8).allSatisfy(point -> {
                            assertThat(point.getCount()).isEqualTo(1);
                            assertThat(point.getBoundaries()).containsExactly(
                                    0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0, 30.0);
                        });
                    });
                } else {
                    assertThat(before.openTelemetry()).isEmpty();
                }
                SessionMetricsSnapshot updated;
                residentActivity.run();
                try (var added = new ScalableTopicControllerMetrics(meter, cluster, () -> COUNTS, registry)) {
                    assertThat(sessionMetricsSnapshot(registry, reader, cluster)).isEqualTo(before);
                    recordAllMetrics(added);
                    assertThat(sessionMetricsSnapshot(registry, reader, cluster)).isEqualTo(before);
                    assertThat(sessionMetricsSnapshot(registry, reader, cluster)).isEqualTo(before);
                    // Actual upstream instruments must still record while controller instruments are present.
                    sessions.startRequest(RequestType.LOOKUP).success();
                    sessions.startRequest(RequestType.WATCH_TOPICS).failure();
                    sessions.sessionOpened(WatchType.TOPIC);
                    sessions.sessionClosed(WatchType.NAMESPACE);
                    sessions.recordWatchUpdate(true);
                    sessions.recordWatchUpdate(false);
                    updated = sessionMetricsSnapshot(registry, reader, cluster);
                    assertThat(updated.prometheus()).isNotEqualTo(before.prometheus());
                    if (telemetryEnabled) {
                        assertThat(updated.openTelemetry()).isNotEqualTo(before.openTelemetry());
                    }
                    residentActivity.run();
                    recordAllMetrics(added);
                    assertThat(sessionMetricsSnapshot(registry, reader, cluster)).isEqualTo(updated);
                }
                residentActivity.run();
                assertThat(sessionMetricsSnapshot(registry, reader, cluster)).isEqualTo(updated);
                try (var replacement = new ScalableTopicControllerMetrics(meter, cluster, () -> COUNTS, registry)) {
                    residentActivity.run();
                    recordAllMetrics(replacement);
                    assertThat(sessionMetricsSnapshot(registry, reader, cluster)).isEqualTo(updated);
                    assertThat(sessionMetricsSnapshot(registry, reader, cluster)).isEqualTo(updated);
                }
                assertThat(sessionMetricsSnapshot(registry, reader, cluster)).isEqualTo(updated);
                sessions.sessionClosed(WatchType.TOPIC);
                sessions.startRequest(RequestType.UNSUBSCRIBE).success();
                sessions.recordWatchUpdate(false);
                SessionMetricsSnapshot after = sessionMetricsSnapshot(registry, reader, cluster);
                assertThat(after.prometheus()).isNotEqualTo(updated.prometheus());
                if (telemetryEnabled) {
                    assertThat(after.openTelemetry()).isNotEqualTo(updated.openTelemetry());
                }
            }
            // Other brokers may keep the shared collector and these families registered.
            assertThat(sessionPrometheusSnapshot(registry, cluster)).isEmpty();
        }
    }

    private record SessionMetricsSnapshot(List<MetricFamilySamples> prometheus, Map<String, Object> openTelemetry) { }

    private static SessionMetricsSnapshot sessionMetricsSnapshot(CollectorRegistry registry,
                                                                 InMemoryMetricReader reader, String cluster) {
        // The SDK reader belongs to this fixture; only the Prometheus registry is shared with other brokers.
        return new SessionMetricsSnapshot(sessionPrometheusSnapshot(registry, cluster),
                existingOpenTelemetry(reader.collectAllMetrics()));
    }

    private static List<MetricFamilySamples> sessionPrometheusSnapshot(CollectorRegistry registry, String cluster) {
        return Collections.list(registry.metricFamilySamples()).stream()
                .filter(family -> SESSION_METRICS.contains(family.name))
                .map(family -> new MetricFamilySamples(family.name, family.unit, family.type, family.help,
                        family.samples.stream().filter(sample -> {
                            int index = sample.labelNames.indexOf("cluster");
                            return index >= 0 && cluster.equals(sample.labelValues.get(index));
                        }).toList()))
                .filter(family -> !family.samples.isEmpty())
                .toList();
    }

    @DataProvider
    public Object[][] brokenStates() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "brokenStates")
    public void testBrokenNewStateDoesNotBreakExistingMetrics(boolean nullState) throws Exception {
        var reader = InMemoryMetricReader.create();
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        Counter existing = Counter.build(PREFIX + "scrape_total", "Existing scrape sentinel").register(registry);
        try (var sdk = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            Meter meter = sdk.get("shared-broker-meter");
            var counter = meter.counterBuilder(BrokerOperabilityMetrics.CONNECTION_COUNTER_METRIC_NAME).build();
            counter.add(11);
            existing.inc(7);
            Map<String, Object> before = existingOpenTelemetry(reader.collectAllMetrics());
            try (var broken = new ScalableTopicControllerMetrics(meter, CLUSTER, () -> {
                if (nullState) {
                    return null;
                }
                throw new IllegalStateException("Cannot read new controller state");
            });
                 var healthy = new ScalableTopicControllerMetrics(meter, CLUSTER, () -> COUNTS)) {
                broken.recordBlocked("invalid_policy");
                recordAllMetrics(healthy);
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                PrometheusMetricsGeneratorUtils.generate(CLUSTER, output, List.of());
                assertThat(output.toString()).contains(PREFIX + "scrape_total{cluster=\"" + CLUSTER + "\"} 7.0");
                assertThat(registry.getSampleValue("pulsar_scalable_topic_controller_count",
                        new String[]{"cluster"}, new String[]{CLUSTER})).isEqualTo(1);
                Collection<MetricData> collected = reader.collectAllMetrics();
                assertThat(existingOpenTelemetry(collected)).isEqualTo(before);
                assertThat(collected).anySatisfy(metric -> {
                    assertThat(metric.getName()).isEqualTo(OTEL_PREFIX + "controller.count");
                    assertThat(metric.getLongSumData().getPoints()).singleElement()
                            .satisfies(point -> assertThat(point.getValue()).isEqualTo(1));
                });
            }
            assertThat(existing.get()).isEqualTo(7);
            counter.add(2);
            assertThat(reader.collectAllMetrics().stream()
                    .filter(metric -> !metric.getName().startsWith(OTEL_PREFIX)).toList())
                    .singleElement().satisfies(metric ->
                    assertThat(metric.getLongSumData().getPoints()).singleElement()
                            .satisfies(point -> assertThat(point.getValue()).isEqualTo(13)));
        } finally {
            registry.unregister(existing);
        }
    }

    private static void recordAllMetrics(ScalableTopicControllerMetrics metrics) {
        metrics.recordTakeover(0.005, true);
        metrics.recordOperation("split", "auto", "success", 0.1);
        metrics.recordSeal("split", 0.01);
        metrics.recordBlocked("max_segments");
    }

    private static List<MetricFamilySamples> existingPrometheus(CollectorRegistry registry) {
        return Collections.list(registry.metricFamilySamples()).stream()
                .filter(metric -> metric.name.startsWith(PREFIX)).toList();
    }

    /**
     * Exclude only controller/layout instruments, not upstream request/watch instruments sharing their prefix.
     * Ignore collection timestamps, but preserve descriptors, start times, labels, values and histogram buckets.
     */
    private static Map<String, Object> existingOpenTelemetry(Collection<MetricData> metrics) {
        Map<String, Object> result = new HashMap<>();
        for (MetricData metric : metrics) {
            if (CONTROLLER_METRICS.contains(metric.getName())) {
                continue;
            }
            Object data = switch (metric.getType()) {
                case LONG_SUM -> List.of(metric.getLongSumData().isMonotonic(),
                        metric.getLongSumData().getAggregationTemporality(),
                        metric.getLongSumData().getPoints().stream().map(point ->
                                List.of(point.getAttributes(), point.getValue(), point.getStartEpochNanos())).toList());
                case HISTOGRAM -> List.of(metric.getHistogramData().getAggregationTemporality(),
                        metric.getHistogramData().getPoints().stream().map(point -> List.of(point.getAttributes(),
                                point.getStartEpochNanos(), point.getCount(), point.getSum(), point.getMin(),
                                point.getMax(), point.getBoundaries(), point.getCounts())).toList());
                default -> throw new AssertionError("Unexpected existing metric type: " + metric.getType());
            };
            result.put(metric.getName(), List.of(metric.getDescription(), metric.getUnit(), metric.getType(),
                    metric.getResource(), metric.getInstrumentationScopeInfo(), data));
        }
        return result;
    }
}
