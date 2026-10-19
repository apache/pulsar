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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.stats.BrokerOperabilityMetrics;
import org.apache.pulsar.broker.stats.OpenTelemetryTopicStats;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ScalableTopicMetricsIsolationTest {
    private static final String PREFIX = "pulsar_metrics_isolation_";
    private static final String OTEL_PREFIX = "pulsar.broker.scalable_topic.";
    private static final String CLUSTER = "metrics-isolation";
    private static final ScalableTopicMetrics.Counts COUNTS = new ScalableTopicMetrics.Counts(1, 2, 3);

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
            try (var added = new ScalableTopicMetrics(meter, CLUSTER, () -> COUNTS, registry)) {
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
            try (var replacement = new ScalableTopicMetrics(meter, CLUSTER, () -> COUNTS, registry)) {
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
                try (var added = new ScalableTopicMetrics(meter, CLUSTER, () -> COUNTS, new CollectorRegistry())) {
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
                try (var replacement =
                             new ScalableTopicMetrics(meter, CLUSTER, () -> COUNTS, new CollectorRegistry())) {
                    recordAllMetrics(replacement);
                    assertThat(existingOpenTelemetry(reader.collectAllMetrics())).isEqualTo(updated);
                }
                assertThat(existingOpenTelemetry(reader.collectAllMetrics())).isEqualTo(updated);
            }
        }
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
            try (var broken = new ScalableTopicMetrics(meter, CLUSTER, () -> {
                if (nullState) {
                    return null;
                }
                throw new IllegalStateException("Cannot read new controller state");
            });
                 var healthy = new ScalableTopicMetrics(meter, CLUSTER, () -> COUNTS)) {
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

    private static void recordAllMetrics(ScalableTopicMetrics metrics) {
        metrics.recordTakeover(0.005, true);
        metrics.recordOperation("split", "auto", "success", 0.1);
        metrics.recordSeal("split", 0.01);
        metrics.recordBlocked("max_segments");
    }

    private static List<MetricFamilySamples> existingPrometheus(CollectorRegistry registry) {
        return Collections.list(registry.metricFamilySamples()).stream()
                .filter(metric -> metric.name.startsWith(PREFIX)).toList();
    }

    /** Ignore collection timestamps, but preserve descriptors, start times, labels, values and histogram buckets. */
    private static Map<String, Object> existingOpenTelemetry(Collection<MetricData> metrics) {
        Map<String, Object> result = new HashMap<>();
        for (MetricData metric : metrics) {
            if (metric.getName().startsWith(OTEL_PREFIX)) {
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
