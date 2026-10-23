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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
            assertThat(updateSample(registry, cluster, "success")).isEqualTo(2);
            assertThat(registry.getSampleValue(PROM_DURATION + "_count",
                    new String[]{"cluster", "request_type", "operation_status"},
                    new String[]{cluster, "lookup", "success"})).isEqualTo(2);
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
    public void testSharedClusterRetainsCumulativeSamplesUntilLastBrokerCloses() {
        CollectorRegistry registry = new CollectorRegistry();
        AtomicLong firstClock = new AtomicLong();
        AtomicLong secondClock = new AtomicLong();
        AtomicLong replacementClock = new AtomicLong();
        AtomicLong restartedClock = new AtomicLong();
        String otherCluster = CLUSTER + "-other";
        try (Telemetry firstTelemetry = new Telemetry();
             Telemetry secondTelemetry = new Telemetry();
             Telemetry replacementTelemetry = new Telemetry();
             Telemetry restartedTelemetry = new Telemetry();
             Telemetry otherTelemetry = new Telemetry();
             ScalableTopicMetrics first =
                     new ScalableTopicMetrics(firstTelemetry.meter, CLUSTER, registry, firstClock::get);
             ScalableTopicMetrics second =
                     new ScalableTopicMetrics(secondTelemetry.meter, CLUSTER, registry, secondClock::get);
             ScalableTopicMetrics other = new ScalableTopicMetrics(otherTelemetry.meter, otherCluster, registry)) {
            first.sessionOpened(WatchType.TOPIC);
            first.sessionOpened(WatchType.NAMESPACE);
            recordAllOutcomesAndBoundaries(first, firstClock);
            Map<CumulativeSample, Double> oneBroker = cumulativeSamples(registry, CLUSTER);
            assertThat(oneBroker).hasSize(RequestType.values().length * 2 * (BUCKETS.size() + 3) + 2);
            second.sessionOpened(WatchType.TOPIC);
            recordAllOutcomesAndBoundaries(second, secondClock);
            recordAllOutcomesAndBoundaries(second, secondClock);
            other.sessionOpened(WatchType.NAMESPACE);
            other.recordWatchUpdate(true);
            Map<CumulativeSample, Double> beforeClose = cumulativeSamples(registry, CLUSTER);
            Map<CumulativeSample, Double> otherBefore = cumulativeSamples(registry, otherCluster);
            MetricData secondHistogram = secondTelemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME);
            assertThat(secondHistogram.getHistogramData().getPoints()).hasSize(8).allSatisfy(point ->
                    assertThat(point.getCount()).isEqualTo(2L * (BUCKETS.size() + 2)));

            first.close();
            assertThat(cumulativeSamples(registry, CLUSTER))
                    .as("closing one broker must not subtract cumulative totals, buckets, counts or sums")
                    .isEqualTo(beforeClose);
            assertThat(sessionSample(registry, CLUSTER, "topic")).isEqualTo(1);
            assertThat(sessionSample(registry, CLUSTER, "namespace")).isEqualTo(0);
            first.close();
            first.recordWatchUpdate(true);
            first.startRequest(RequestType.LOOKUP).failure();
            assertThat(cumulativeSamples(registry, CLUSTER)).isEqualTo(beforeClose);
            assertThat(longPoint(secondTelemetry, ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME,
                    statusAttributes("success"))).isEqualTo(8);
            assertThat(secondTelemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME)
                    .getHistogramData().getPoints()).hasSize(8).allSatisfy(point -> {
                        assertThat(point.getCount()).isEqualTo(2L * (BUCKETS.size() + 2));
                        assertThat(point.getBoundaries()).isEqualTo(BUCKETS);
                    });

            recordAllOutcomesAndBoundaries(second, secondClock);
            assertCumulativeMultiple(registry, CLUSTER, oneBroker, 4);
            assertThat(longPoint(secondTelemetry, ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME,
                    statusAttributes("success"))).isEqualTo(12);
            assertThat(secondTelemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME)
                    .getHistogramData().getPoints()).hasSize(8).allSatisfy(point ->
                            assertThat(point.getCount()).isEqualTo(3L * (BUCKETS.size() + 2)));

            Map<CumulativeSample, Double> beforeReplacement = cumulativeSamples(registry, CLUSTER);
            try (ScalableTopicMetrics replacement = new ScalableTopicMetrics(
                    replacementTelemetry.meter, CLUSTER, registry, replacementClock::get)) {
                assertThat(cumulativeSamples(registry, CLUSTER)).isEqualTo(beforeReplacement);
                replacement.sessionOpened(WatchType.TOPIC);
                recordAllOutcomesAndBoundaries(replacement, replacementClock);
                assertCumulativeMultiple(registry, CLUSTER, oneBroker, 5);
                Map<CumulativeSample, Double> beforeSecondClose = cumulativeSamples(registry, CLUSTER);
                second.close();
                second.close();
                assertThat(cumulativeSamples(registry, CLUSTER)).isEqualTo(beforeSecondClose);
                assertThat(sessionSample(registry, CLUSTER, "topic")).isEqualTo(1);
            }
            assertThat(cumulativeSamples(registry, CLUSTER)).isEmpty();
            assertThat(sessionSample(registry, CLUSTER, "topic")).isNull();
            assertThat(cumulativeSamples(registry, otherCluster)).isEqualTo(otherBefore);
            assertThat(sessionSample(registry, otherCluster, "namespace")).isEqualTo(1);

            // A cluster that starts a new lifetime must not inherit history while another cluster remains live.
            try (ScalableTopicMetrics restarted = new ScalableTopicMetrics(
                    restartedTelemetry.meter, CLUSTER, registry, restartedClock::get)) {
                assertThat(cumulativeSamples(registry, CLUSTER)).hasSize(2)
                        .allSatisfy((sample, value) -> assertThat(value).isZero());
                recordAllOutcomesAndBoundaries(restarted, restartedClock);
                assertCumulativeMultiple(registry, CLUSTER, oneBroker, 1);
            }
            assertThat(cumulativeSamples(registry, CLUSTER)).isEmpty();
        }
        assertThat(Collections.list(registry.metricFamilySamples())).isEmpty();
        try (Telemetry telemetry = new Telemetry();
             ScalableTopicMetrics fresh = new ScalableTopicMetrics(telemetry.meter, CLUSTER, registry)) {
            assertThat(cumulativeSamples(registry, CLUSTER)).hasSize(2)
                    .allSatisfy((sample, value) -> assertThat(value).isZero());
            assertThat(sessionSample(registry, CLUSTER, "topic")).isEqualTo(0);
        }
    }

    @DataProvider
    public Object[][] brokerCloseOrders() {
        return new Object[][]{{new int[]{0, 1, 2}}, {new int[]{0, 2, 1}}, {new int[]{1, 0, 2}},
                {new int[]{1, 2, 0}}, {new int[]{2, 0, 1}}, {new int[]{2, 1, 0}}};
    }

    @Test(dataProvider = "brokerCloseOrders")
    public void testThreeBrokerCloseOrderPreservesExactHistogramSum(int[] closeOrder) {
        CollectorRegistry registry = new CollectorRegistry();
        AtomicLong firstClock = new AtomicLong();
        AtomicLong secondClock = new AtomicLong();
        AtomicLong thirdClock = new AtomicLong();
        try (Telemetry firstTelemetry = new Telemetry();
             Telemetry secondTelemetry = new Telemetry();
             Telemetry thirdTelemetry = new Telemetry();
             ScalableTopicMetrics first =
                     new ScalableTopicMetrics(firstTelemetry.meter, CLUSTER, registry, firstClock::get);
             ScalableTopicMetrics second =
                     new ScalableTopicMetrics(secondTelemetry.meter, CLUSTER, registry, secondClock::get);
             ScalableTopicMetrics third =
                     new ScalableTopicMetrics(thirdTelemetry.meter, CLUSTER, registry, thirdClock::get)) {
            List<ScalableTopicMetrics> brokers = List.of(first, second, third);
            List<AtomicLong> clocks = List.of(firstClock, secondClock, thirdClock);
            for (int i = 0; i < brokers.size(); i++) {
                brokers.get(i).sessionOpened(WatchType.TOPIC);
                brokers.get(i).recordWatchUpdate(true);
                recordLookup(brokers.get(i), clocks.get(i), TimeUnit.MILLISECONDS.toNanos(100L * (i + 1)));
            }
            Map<CumulativeSample, Double> before = cumulativeSamples(registry, CLUSTER);
            // Compare the collected sum itself, not a decimal literal with a different floating-point rounding.
            for (int i = 0; i < closeOrder.length - 1; i++) {
                brokers.get(closeOrder[i]).close();
                assertThat(cumulativeSamples(registry, CLUSTER)).as("close order %s, step %s", closeOrder, i)
                        .isEqualTo(before);
                assertThat(sessionSample(registry, CLUSTER, "topic")).isEqualTo(2 - i);
            }
            brokers.get(closeOrder[2]).close();
            assertThat(cumulativeSamples(registry, CLUSTER)).isEmpty();
            assertThat(Collections.list(registry.metricFamilySamples())).isEmpty();
        }
    }

    @Test
    public void testRecordCloseAndScrapeHandoffPreservesCumulativeSamples() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch clockEntered = new CountDownLatch(1);
        CountDownLatch releaseClock = new CountDownLatch(1);
        AtomicBoolean blockClock = new AtomicBoolean();
        AtomicLong firstClock = new AtomicLong();
        AtomicLong survivorClock = new AtomicLong();
        CollectorRegistry registry = new CollectorRegistry();
        try (Telemetry firstTelemetry = new Telemetry();
             Telemetry survivorTelemetry = new Telemetry();
             Telemetry stressTelemetry = new Telemetry();
             ScalableTopicMetrics first = new ScalableTopicMetrics(firstTelemetry.meter, CLUSTER, registry, () -> {
                 if (blockClock.compareAndSet(true, false)) {
                     clockEntered.countDown();
                     awaitLatch(releaseClock);
                 }
                 return firstClock.get();
             });
             ScalableTopicMetrics survivor =
                     new ScalableTopicMetrics(survivorTelemetry.meter, CLUSTER, registry, survivorClock::get)) {
            recordLookup(first, firstClock, TimeUnit.MILLISECONDS.toNanos(100));
            recordLookup(survivor, survivorClock, TimeUnit.MILLISECONDS.toNanos(200));
            Map<CumulativeSample, Double> before = cumulativeSamples(registry, CLUSTER);
            blockClock.set(true);
            Future<?> lateRecord = executor.submit(() -> first.startRequest(RequestType.SUBSCRIBE).success());
            awaitLatch(clockEntered);
            try {
                // The pending request has not reached acceptance; closing must finish without waiting for its clock.
                executor.submit(first::close).get(10, TimeUnit.SECONDS);
                assertThat(cumulativeSamples(registry, CLUSTER)).isEqualTo(before);
            } finally {
                releaseClock.countDown();
            }
            lateRecord.get(10, TimeUnit.SECONDS);
            assertThat(cumulativeSamples(registry, CLUSTER)).isEqualTo(before);
            assertThat(firstTelemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME)
                    .getHistogramData().getPoints()).singleElement().satisfies(point -> {
                        assertThat(point.getAttributes()).isEqualTo(requestAttributes("lookup", "success"));
                        assertThat(point.getCount()).isEqualTo(1);
                    });
            assertThat(durationSample(registry, "_count", "subscribe", "success")).isNull();
            recordLookup(survivor, survivorClock, TimeUnit.MILLISECONDS.toNanos(100));
            assertCumulativeNondecreasing(before, cumulativeSamples(registry, CLUSTER));
            assertThat(durationSample(registry, "_count", "lookup", "success")).isEqualTo(3);
            assertAcceptedSdkRecordDoesNotBlockClose(executor);

            for (int iteration = 0; iteration < 100; iteration++) {
                CollectorRegistry sharedRegistry = new CollectorRegistry();
                AtomicLong clock = new AtomicLong();
                try (ScalableTopicMetrics closing = new ScalableTopicMetrics(
                        stressTelemetry.meter, CLUSTER, sharedRegistry, clock::get);
                     ScalableTopicMetrics live = new ScalableTopicMetrics(
                             stressTelemetry.meter, CLUSTER, sharedRegistry, clock::get)) {
                    recordLookup(closing, clock, TimeUnit.MILLISECONDS.toNanos(100));
                    recordLookup(live, clock, TimeUnit.MILLISECONDS.toNanos(100));
                    closing.recordWatchUpdate(true);
                    live.recordWatchUpdate(true);
                    Map<CumulativeSample, Double> baseline = cumulativeSamples(sharedRegistry, CLUSTER);
                    CountDownLatch start = new CountDownLatch(1);
                    Future<?> scraping = executor.submit(() -> {
                        awaitLatch(start);
                        for (int scrape = 0; scrape < 30; scrape++) {
                            assertThat(cumulativeSamples(sharedRegistry, CLUSTER)).isEqualTo(baseline);
                        }
                    });
                    Future<?> closingBroker = executor.submit(() -> {
                        start.countDown();
                        closing.close();
                    });
                    scraping.get(10, TimeUnit.SECONDS);
                    closingBroker.get(10, TimeUnit.SECONDS);
                    assertThat(cumulativeSamples(sharedRegistry, CLUSTER)).isEqualTo(baseline);
                    recordLookup(live, clock, TimeUnit.MILLISECONDS.toNanos(100));
                    live.recordWatchUpdate(true);
                    assertCumulativeNondecreasing(baseline, cumulativeSamples(sharedRegistry, CLUSTER));
                    assertThat(durationSample(sharedRegistry, "_count", "lookup", "success")).isEqualTo(3);
                    assertThat(updateSample(sharedRegistry, CLUSTER, "success")).isEqualTo(3);
                }
                assertThat(Collections.list(sharedRegistry.metricFamilySamples())).isEmpty();
            }
        } finally {
            releaseClock.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
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

    private static void assertAcceptedSdkRecordDoesNotBlockClose(ExecutorService executor) throws Exception {
        CollectorRegistry registry = new CollectorRegistry();
        AtomicLong clock = new AtomicLong();
        CountDownLatch sdkEntered = new CountDownLatch(1);
        CountDownLatch releaseSdk = new CountDownLatch(1);
        try (Telemetry firstTelemetry = new Telemetry();
             Telemetry survivorTelemetry = new Telemetry()) {
            DoubleHistogram histogram = spy(firstTelemetry.meter
                    .histogramBuilder(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME)
                    .setDescription("Scalable-topic request duration from arrival to the first response.")
                    .setUnit("s").setExplicitBucketBoundariesAdvice(BUCKETS).build());
            doAnswer(invocation -> {
                sdkEntered.countDown();
                awaitLatch(releaseSdk);
                return invocation.callRealMethod();
            }).when(histogram).record(anyDouble(), any(Attributes.class));
            DoubleHistogramBuilder builder = mock(DoubleHistogramBuilder.class, RETURNS_SELF);
            when(builder.build()).thenReturn(histogram);
            Meter meter = spy(firstTelemetry.meter);
            doReturn(builder).when(meter).histogramBuilder(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME);
            try (ScalableTopicMetrics first = new ScalableTopicMetrics(meter, CLUSTER, registry, clock::get);
                 ScalableTopicMetrics survivor = new ScalableTopicMetrics(survivorTelemetry.meter, CLUSTER, registry)) {
                first.sessionOpened(WatchType.TOPIC);
                survivor.sessionOpened(WatchType.TOPIC);
                ScalableTopicMetrics.Request request = first.startRequest(RequestType.LOOKUP);
                clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100));
                Future<?> accepted = executor.submit(request::success);
                try {
                    awaitLatch(sdkEntered);
                    assertThat(durationSample(registry, "_count", "lookup", "success")).isEqualTo(1);
                    Map<CumulativeSample, Double> acceptedProm = cumulativeSamples(registry, CLUSTER);
                    executor.submit(first::close).get(10, TimeUnit.SECONDS);
                    assertThat(cumulativeSamples(registry, CLUSTER)).isEqualTo(acceptedProm);
                    assertThat(sessionSample(registry, CLUSTER, "topic")).isEqualTo(1);
                } finally {
                    releaseSdk.countDown();
                }
                accepted.get(10, TimeUnit.SECONDS);
                assertThat(firstTelemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME)
                        .getHistogramData().getPoints()).singleElement().satisfies(point -> {
                            assertThat(point.getCount()).isEqualTo(1);
                            assertThat(point.getSum()).isEqualTo(0.1);
                        });
                first.startRequest(RequestType.SUBSCRIBE).success();
                assertThat(durationSample(registry, "_count", "lookup", "success")).isEqualTo(1);
                assertThat(durationSample(registry, "_count", "subscribe", "success")).isNull();
                assertThat(firstTelemetry.metric(ScalableTopicMetrics.REQUEST_DURATION_METRIC_NAME)
                        .getHistogramData().getPoints()).singleElement().satisfies(point ->
                                assertThat(point.getCount()).isEqualTo(1));
            }
        } finally {
            releaseSdk.countDown();
        }
    }

    private static void recordLookup(ScalableTopicMetrics metrics, AtomicLong clock, long nanos) {
        ScalableTopicMetrics.Request request = metrics.startRequest(RequestType.LOOKUP);
        clock.addAndGet(nanos);
        request.success();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).as("latch reached before deadline").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted waiting for test handoff", e);
        }
    }

    private static void assertCumulativeNondecreasing(Map<CumulativeSample, Double> before,
                                                     Map<CumulativeSample, Double> after) {
        assertThat(after.keySet()).isEqualTo(before.keySet());
        before.forEach((sample, value) -> assertThat(after.get(sample)).as("%s", sample)
                .isGreaterThanOrEqualTo(value));
    }

    private record CumulativeSample(String name, List<String> labels, List<String> values) {}

    private static Map<CumulativeSample, Double> cumulativeSamples(CollectorRegistry registry, String cluster) {
        Map<CumulativeSample, Double> samples = new LinkedHashMap<>();
        for (Collector.MetricFamilySamples family : Collections.list(registry.metricFamilySamples())) {
            if (family.type != Collector.Type.COUNTER && family.type != Collector.Type.HISTOGRAM) {
                continue;
            }
            for (Collector.MetricFamilySamples.Sample sample : family.samples) {
                int clusterIndex = sample.labelNames.indexOf("cluster");
                if (clusterIndex >= 0 && sample.labelValues.get(clusterIndex).equals(cluster)
                        && !sample.name.endsWith("_created")) {
                    CumulativeSample key = new CumulativeSample(sample.name,
                            List.copyOf(sample.labelNames), List.copyOf(sample.labelValues));
                    assertThat(samples).as("one cumulative series per label set").doesNotContainKey(key);
                    samples.put(key, sample.value);
                }
            }
        }
        return Map.copyOf(samples);
    }

    private static void recordAllOutcomesAndBoundaries(ScalableTopicMetrics metrics, AtomicLong nanoTime) {
        List<Double> durations = new ArrayList<>(BUCKETS);
        durations.add(0.0005);
        durations.add(31.0);
        for (RequestType type : RequestType.values()) {
            for (boolean success : new boolean[]{true, false}) {
                metrics.recordWatchUpdate(success);
                for (double seconds : durations) {
                    ScalableTopicMetrics.Request request = metrics.startRequest(type);
                    nanoTime.addAndGet((long) (seconds * 1_000_000_000));
                    complete(request, success);
                }
            }
        }
    }

    private static void assertCumulativeMultiple(CollectorRegistry registry, String cluster,
                                                Map<CumulativeSample, Double> oneBroker, int multiple) {
        Map<CumulativeSample, Double> actual = cumulativeSamples(registry, cluster);
        assertThat(actual.keySet()).isEqualTo(oneBroker.keySet());
        oneBroker.forEach((sample, value) -> {
            // Histogram sums can differ by a few rounding bits when aggregation groups brokers differently.
            if (sample.name().endsWith("_sum")) {
                assertThat(actual.get(sample)).isBetween(value * multiple - 1e-9, value * multiple + 1e-9);
            } else {
                assertThat(actual.get(sample)).as("%s", sample).isEqualTo(value * multiple);
            }
        });
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
