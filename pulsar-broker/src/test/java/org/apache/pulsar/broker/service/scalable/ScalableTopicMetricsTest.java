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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.service.scalable.ScalableTopicMetrics.RequestType;
import org.apache.pulsar.broker.service.scalable.ScalableTopicMetrics.WatchType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ScalableTopicMetricsTest {
    private static final String CLUSTER = "test-cluster";
    private static final String PROM_DURATION = "pulsar_scalable_topic_request_duration_seconds";
    private static final String PROM_SESSIONS = "pulsar_scalable_topic_watch_session_count";
    private static final String PROM_UPDATES = "pulsar_scalable_topic_watch_update_total";
    private static final List<Double> BUCKETS =
            List.of(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0, 30.0);

    @Test
    public void testSessionAndUpdateExporterParity() {
        CollectorRegistry registry = new CollectorRegistry();
        try (Telemetry telemetry = new Telemetry();
             ScalableTopicMetrics metrics = new ScalableTopicMetrics(telemetry.meter, CLUSTER, registry)) {
            assertSessionsAndUpdates(telemetry, registry, 0, 0, 0, 0);
            metrics.sessionOpened(WatchType.TOPIC);
            metrics.sessionOpened(WatchType.TOPIC);
            metrics.sessionOpened(WatchType.NAMESPACE);
            metrics.sessionClosed(WatchType.TOPIC);
            metrics.recordWatchUpdate(true);
            metrics.recordWatchUpdate(false);
            metrics.recordWatchUpdate(true);
            assertSessionsAndUpdates(telemetry, registry, 1, 1, 2, 1);

            metrics.sessionClosed(WatchType.TOPIC);
            metrics.sessionClosed(WatchType.NAMESPACE);
            assertSessionsAndUpdates(telemetry, registry, 0, 0, 2, 1);
            MetricData sessions = telemetry.metric(ScalableTopicMetrics.WATCH_SESSION_COUNT_METRIC_NAME);
            assertThat(sessions.getType()).isEqualTo(MetricDataType.LONG_SUM);
            assertThat(sessions.getLongSumData().isMonotonic()).isFalse();
            assertThat(sessions.getUnit()).isEqualTo("{session}");
            MetricData updates = telemetry.metric(ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME);
            assertThat(updates.getLongSumData().isMonotonic()).isTrue();
            assertThat(updates.getUnit()).isEqualTo("{update}");
            assertThat(family(registry, PROM_SESSIONS).type).isEqualTo(Collector.Type.GAUGE);
            assertThat(family(registry, "pulsar_scalable_topic_watch_update").type)
                    .isEqualTo(Collector.Type.COUNTER);
        }
    }

    @Test
    public void testRequestHistogramExporterParityLabelsAndBuckets() {
        CollectorRegistry registry = new CollectorRegistry();
        AtomicLong nanoTime = new AtomicLong();
        try (Telemetry telemetry = new Telemetry();
             ScalableTopicMetrics metrics =
                     new ScalableTopicMetrics(telemetry.meter, CLUSTER, registry, nanoTime::get)) {
            List<Double> durations = new ArrayList<>(BUCKETS);
            durations.add(0.0005);
            durations.add(31.0);
            for (RequestType type : RequestType.values()) {
                for (boolean success : new boolean[]{true, false}) {
                    for (double seconds : durations) {
                        ScalableTopicMetrics.Request request = metrics.startRequest(type);
                        nanoTime.addAndGet((long) (seconds * 1_000_000_000));
                        complete(request, success);
                    }
                }
            }
            MetricData metric = telemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME);
            assertThat(metric.getType()).isEqualTo(MetricDataType.HISTOGRAM);
            assertThat(metric.getUnit()).isEqualTo("s");
            assertThat(metric.getHistogramData().getPoints()).hasSize(8);
            for (RequestType type : RequestType.values()) {
                String requestType = type.name().toLowerCase(Locale.ROOT);
                for (String status : List.of("success", "failure")) {
                    Attributes attributes = requestAttributes(requestType, status);
                    HistogramPointData point = histogramPoint(metric, attributes);
                    assertThat(point.getCount()).isEqualTo(durations.size());
                    assertThat(point.getBoundaries()).containsExactlyElementsOf(BUCKETS);
                    assertThat(point.getSum()).isEqualTo(durationSample(registry, "_sum", requestType, status));
                    assertThat((double) point.getCount())
                            .isEqualTo(durationSample(registry, "_count", requestType, status));
                    long cumulative = 0;
                    for (int i = 0; i < point.getCounts().size(); i++) {
                        cumulative += point.getCounts().get(i);
                        String boundary = i < BUCKETS.size()
                                ? Collector.doubleToGoString(BUCKETS.get(i)) : "+Inf";
                        assertThat(registry.getSampleValue(PROM_DURATION + "_bucket",
                                new String[]{"cluster", "request_type", "operation_status", "le"},
                                new String[]{CLUSTER, requestType, status, boundary}))
                                .as("cumulative bucket %s for %s/%s", boundary, requestType, status)
                                .isEqualTo((double) cumulative);
                        if (i < BUCKETS.size()) {
                            double upperBound = BUCKETS.get(i);
                            assertThat(cumulative).isEqualTo(durations.stream().filter(d -> d <= upperBound).count());
                        }
                    }
                    assertThat(cumulative).isEqualTo(durations.size());
                }
            }
            Collector.MetricFamilySamples histogram = family(registry, PROM_DURATION);
            assertThat(histogram.type).isEqualTo(Collector.Type.HISTOGRAM);
            assertThat(histogram.samples).allSatisfy(sample -> {
                assertThat(sample.labelNames).contains("cluster", "request_type", "operation_status");
                assertThat(sample.labelNames).isSubsetOf("cluster", "request_type", "operation_status", "le");
                assertThat(sample.labelValues.get(sample.labelNames.indexOf("cluster"))).isEqualTo(CLUSTER);
            });
        }
    }

    @DataProvider
    public Object[][] outcomes() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "outcomes")
    public void testOnlyFirstResponseRecordsDuration(boolean success) {
        CollectorRegistry registry = new CollectorRegistry();
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(3));
        try (Telemetry telemetry = new Telemetry();
             ScalableTopicMetrics metrics =
                     new ScalableTopicMetrics(telemetry.meter, CLUSTER, registry, nanoTime::get)) {
            ScalableTopicMetrics.Request request = metrics.startRequest(RequestType.LOOKUP);
            nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(20));
            complete(request, success);
            nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(9));
            complete(request, !success);
            complete(request, success);

            MetricData metric = telemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME);
            assertThat(metric.getHistogramData().getPoints()).hasSize(1);
            String status = success ? "success" : "failure";
            HistogramPointData point = histogramPoint(metric, requestAttributes("lookup", status));
            assertThat(point.getCount()).isEqualTo(1);
            assertThat(point.getSum()).isEqualTo(0.02);
            assertThat(durationSample(registry, "_count", "lookup", status)).isEqualTo(1);
            assertThat(durationSample(registry, "_sum", "lookup", status)).isEqualTo(0.02);
            assertThat(durationSample(registry, "_count", "lookup", success ? "failure" : "success")).isNull();
        }
    }

    @Test
    public void testConcurrentResponsesRecordExactlyOnce() throws Exception {
        CollectorRegistry registry = new CollectorRegistry();
        AtomicLong nanoTime = new AtomicLong();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try (Telemetry telemetry = new Telemetry();
             ScalableTopicMetrics metrics =
                     new ScalableTopicMetrics(telemetry.meter, CLUSTER, registry, nanoTime::get)) {
            ScalableTopicMetrics.Request request = metrics.startRequest(RequestType.SUBSCRIBE);
            nanoTime.set(TimeUnit.MILLISECONDS.toNanos(50));
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> responses = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                boolean success = i % 2 == 0;
                responses.add(executor.submit(() -> {
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    complete(request, success);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> response : responses) {
                response.get(10, TimeUnit.SECONDS);
            }
            MetricData metric = telemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME);
            assertThat(metric.getHistogramData().getPoints()).singleElement().satisfies(point -> {
                assertThat(point.getCount()).isEqualTo(1);
                assertThat(point.getSum()).isEqualTo(0.05);
                String status = point.getAttributes().asMap().values().contains("success") ? "success" : "failure";
                assertThat(durationSample(registry, "_count", "subscribe", status)).isEqualTo(1);
                assertThat(durationSample(registry, "_sum", "subscribe", status)).isEqualTo(0.05);
            });
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void testCloseUnregistersCollectorAndObservableCallbacks() {
        CollectorRegistry registry = new CollectorRegistry();
        try (Telemetry telemetry = new Telemetry();
             ScalableTopicMetrics metrics = new ScalableTopicMetrics(telemetry.meter, CLUSTER, registry)) {
            metrics.sessionOpened(WatchType.TOPIC);
            metrics.recordWatchUpdate(true);
            metrics.startRequest(RequestType.LOOKUP).success();
            ScalableTopicMetrics.Request pending = metrics.startRequest(RequestType.SUBSCRIBE);
            assertSessionsAndUpdates(telemetry, registry, 1, 0, 1, 0);
            metrics.close();
            metrics.close();
            pending.failure();
            metrics.sessionOpened(WatchType.TOPIC);
            metrics.sessionClosed(WatchType.TOPIC);
            metrics.recordWatchUpdate(false);
            assertThat(Collections.list(registry.metricFamilySamples())).isEmpty();
            assertThat(telemetry.reader.collectAllMetrics()).extracting(MetricData::getName)
                    .doesNotContain(ScalableTopicMetrics.WATCH_SESSION_COUNT_METRIC_NAME,
                            ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME);
            // Synchronous OTel histograms have no close method and retain their already recorded values.
            assertThat(telemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME)
                    .getHistogramData().getPoints()).singleElement().satisfies(point ->
                            assertThat(point.getCount()).isEqualTo(1));
        }
    }

    @Test
    public void testDefaultRegistrySharedBrokerLifecycle() {
        String cluster = "scalable-metrics-" + UUID.randomUUID();
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        try (Telemetry firstTelemetry = new Telemetry();
             Telemetry secondTelemetry = new Telemetry();
             Telemetry thirdTelemetry = new Telemetry();
             ScalableTopicMetrics first = new ScalableTopicMetrics(firstTelemetry.meter, cluster);
             ScalableTopicMetrics second = new ScalableTopicMetrics(secondTelemetry.meter, cluster)) {
            first.sessionOpened(WatchType.TOPIC);
            second.sessionOpened(WatchType.TOPIC);
            second.sessionOpened(WatchType.NAMESPACE);
            first.recordWatchUpdate(true);
            second.recordWatchUpdate(true);
            first.startRequest(RequestType.LOOKUP).success();
            second.startRequest(RequestType.LOOKUP).success();
            assertThat(sessionSample(registry, cluster, "topic")).isEqualTo(2);
            assertThat(sessionSample(registry, cluster, "namespace")).isEqualTo(1);
            assertThat(updateSample(registry, cluster, "success")).isEqualTo(2);
            assertThat(registry.getSampleValue(PROM_DURATION + "_count",
                    new String[]{"cluster", "request_type", "operation_status"},
                    new String[]{cluster, "lookup", "success"})).isEqualTo(2);
            assertThat(family(registry, PROM_SESSIONS).samples)
                    .filteredOn(sample -> sample.labelValues.contains(cluster)).hasSize(2);

            first.close();
            first.close();
            assertThat(sessionSample(registry, cluster, "topic")).isEqualTo(1);
            assertThat(updateSample(registry, cluster, "success")).isEqualTo(1);
            assertThat(registry.getSampleValue(PROM_DURATION + "_count",
                    new String[]{"cluster", "request_type", "operation_status"},
                    new String[]{cluster, "lookup", "success"})).isEqualTo(1);
            assertThat(longPoint(secondTelemetry, ScalableTopicMetrics.WATCH_SESSION_COUNT_METRIC_NAME,
                    watchAttributes("topic"))).isEqualTo(1);

            // Distinct clusters share registration, but never share state or labels.
            String otherCluster = cluster + "-other";
            try (ScalableTopicMetrics third = new ScalableTopicMetrics(thirdTelemetry.meter, otherCluster)) {
                third.sessionOpened(WatchType.NAMESPACE);
                second.close();
                assertThat(sessionSample(registry, cluster, "topic")).isNull();
                assertThat(sessionSample(registry, otherCluster, "namespace")).isEqualTo(1);
                assertThat(longPoint(thirdTelemetry, ScalableTopicMetrics.WATCH_SESSION_COUNT_METRIC_NAME,
                        watchAttributes("namespace"))).isEqualTo(1);
            }
            assertThat(sessionSample(registry, otherCluster, "namespace")).isNull();
            assertThat(sessionSample(registry, cluster, "topic")).isNull();

            // Last close releases registration, and a new broker starts with fresh state.
            try (ScalableTopicMetrics restarted = new ScalableTopicMetrics(thirdTelemetry.meter, cluster)) {
                first.close();
                second.close();
                assertThat(sessionSample(registry, cluster, "topic")).isEqualTo(0);
                assertThat(updateSample(registry, cluster, "success")).isEqualTo(0);
                restarted.sessionOpened(WatchType.TOPIC);
                assertThat(sessionSample(registry, cluster, "topic")).isEqualTo(1);
            }
        }
        assertThat(sessionSample(registry, cluster, "topic")).isNull();
    }

    @Test
    public void testSeparateRegistriesAreIndependent() {
        CollectorRegistry firstRegistry = new CollectorRegistry();
        CollectorRegistry secondRegistry = new CollectorRegistry();
        try (Telemetry firstTelemetry = new Telemetry();
             Telemetry secondTelemetry = new Telemetry();
             ScalableTopicMetrics first = new ScalableTopicMetrics(firstTelemetry.meter, CLUSTER, firstRegistry);
             ScalableTopicMetrics second = new ScalableTopicMetrics(secondTelemetry.meter, CLUSTER, secondRegistry)) {
            first.sessionOpened(WatchType.TOPIC);
            second.sessionOpened(WatchType.NAMESPACE);
            assertThat(sessionSample(firstRegistry, CLUSTER, "topic")).isEqualTo(1);
            assertThat(sessionSample(secondRegistry, CLUSTER, "topic")).isEqualTo(0);
            first.close();
            assertThat(Collections.list(firstRegistry.metricFamilySamples())).isEmpty();
            assertThat(sessionSample(secondRegistry, CLUSTER, "namespace")).isEqualTo(1);
        }
    }

    @Test
    public void testFailedRegistrationDoesNotLeakCallbacksOrUnregisterOtherCollector() {
        CollectorRegistry registry = new CollectorRegistry();
        Gauge existing = Gauge.build(PROM_SESSIONS, "Existing collector").register(registry);
        try (Telemetry telemetry = new Telemetry()) {
            assertThatThrownBy(() -> new ScalableTopicMetrics(telemetry.meter, CLUSTER, registry))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(telemetry.reader.collectAllMetrics()).isEmpty();
            assertThat(registry.getSampleValue(PROM_SESSIONS)).isEqualTo(0);
        } finally {
            registry.unregister(existing);
        }
    }

    private static void complete(ScalableTopicMetrics.Request request, boolean success) {
        if (success) {
            request.success();
        } else {
            request.failure();
        }
    }

    private static Attributes requestAttributes(String type, String status) {
        return Attributes.builder().put("pulsar.scalable_topic.request.type", type)
                .put("pulsar.scalable_topic.operation.status", status).build();
    }

    private static Attributes watchAttributes(String type) {
        return Attributes.builder().put("pulsar.scalable_topic.watch.type", type).build();
    }

    private static Attributes statusAttributes(String status) {
        return Attributes.builder().put("pulsar.scalable_topic.operation.status", status).build();
    }

    private static long longPoint(Telemetry telemetry, String name, Attributes attributes) {
        return telemetry.metric(name).getLongSumData().getPoints().stream()
                .filter(point -> point.getAttributes().equals(attributes)).findFirst().orElseThrow().getValue();
    }

    private static HistogramPointData histogramPoint(MetricData metric, Attributes attributes) {
        return metric.getHistogramData().getPoints().stream()
                .filter(point -> point.getAttributes().equals(attributes)).findFirst().orElseThrow();
    }

    private static Collector.MetricFamilySamples family(CollectorRegistry registry, String name) {
        return Collections.list(registry.metricFamilySamples()).stream()
                .filter(family -> family.name.equals(name)).findFirst().orElseThrow();
    }

    private static Double durationSample(CollectorRegistry registry, String suffix, String type, String status) {
        return registry.getSampleValue(PROM_DURATION + suffix,
                new String[]{"cluster", "request_type", "operation_status"}, new String[]{CLUSTER, type, status});
    }

    private static Double sessionSample(CollectorRegistry registry, String cluster, String type) {
        return registry.getSampleValue(PROM_SESSIONS,
                new String[]{"cluster", "watch_type"}, new String[]{cluster, type});
    }

    private static Double updateSample(CollectorRegistry registry, String cluster, String status) {
        return registry.getSampleValue(PROM_UPDATES,
                new String[]{"cluster", "operation_status"}, new String[]{cluster, status});
    }

    private static void assertSessionsAndUpdates(Telemetry telemetry, CollectorRegistry registry,
                                                long topics, long namespaces, long successes, long failures) {
        assertThat(longPoint(telemetry, ScalableTopicMetrics.WATCH_SESSION_COUNT_METRIC_NAME, watchAttributes("topic")))
                .isEqualTo(topics);
        assertThat(longPoint(telemetry, ScalableTopicMetrics.WATCH_SESSION_COUNT_METRIC_NAME,
                watchAttributes("namespace"))).isEqualTo(namespaces);
        assertThat(longPoint(telemetry, ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME,
                statusAttributes("success"))).isEqualTo(successes);
        assertThat(longPoint(telemetry, ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME,
                statusAttributes("failure"))).isEqualTo(failures);
        assertThat(sessionSample(registry, CLUSTER, "topic")).isEqualTo((double) topics);
        assertThat(sessionSample(registry, CLUSTER, "namespace")).isEqualTo((double) namespaces);
        assertThat(updateSample(registry, CLUSTER, "success")).isEqualTo((double) successes);
        assertThat(updateSample(registry, CLUSTER, "failure")).isEqualTo((double) failures);
        assertThat(telemetry.metric(ScalableTopicMetrics.WATCH_SESSION_COUNT_METRIC_NAME)
                .getLongSumData().getPoints()).hasSize(2);
        assertThat(telemetry.metric(ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME)
                .getLongSumData().getPoints()).hasSize(2);
    }

    private static final class Telemetry implements AutoCloseable {
        private final InMemoryMetricReader reader = InMemoryMetricReader.create();
        private final SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        private final Meter meter = provider.get("scalable-topic-metrics-test");

        private MetricData metric(String name) {
            return reader.collectAllMetrics().stream().filter(metric -> metric.getName().equals(name))
                    .findFirst().orElseThrow();
        }

        @Override
        public void close() {
            provider.close();
        }
    }
}
