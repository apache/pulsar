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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator.ConsumerStats;
import org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator.RebalanceReason;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.broker.stats.PulsarBrokerOpenTelemetry;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ScalableTopicConsumerMetricsTest {
    @Test
    public void testBothExportPathsReadTheSameState() throws Exception {
        var registry = new CollectorRegistry();
        var state = new AtomicReference<>(new ConsumerStats(3, 2, 1, 4));
        try (var reader = InMemoryMetricReader.create();
             var provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
             var metrics = new ScalableTopicConsumerMetrics(provider.get("test"), registry, "test", state::get)) {
            metrics.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            metrics.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            metrics.recordRebalance(RebalanceReason.LAYOUT_CHANGE);
            assertExports(reader, registry, state.get(), 2, 1);

            state.set(new ConsumerStats(0, 1, 5, 0));
            metrics.recordRebalance(RebalanceReason.LAYOUT_CHANGE);
            assertExports(reader, registry, state.get(), 2, 2);
            // Scraping must not itself change counters.
            assertExports(reader, registry, state.get(), 2, 2);
        }
        assertThat(registry.metricFamilySamples().hasMoreElements()).isFalse();
    }

    @Test
    public void testPrometheusWorksWithDisabledOpenTelemetryAndReleasesRegistration() {
        var registry = new CollectorRegistry();
        var meter = OpenTelemetry.noop().getMeter("test");
        try (var first = new ScalableTopicConsumerMetrics(meter, registry, "test",
                     () -> new ConsumerStats(1, 0, 0, 2));
             var second = new ScalableTopicConsumerMetrics(meter, registry, "test",
                     () -> new ConsumerStats(0, 1, 0, 3))) {
            first.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            second.recordRebalance(RebalanceReason.CONSUMER_LEAVE);
            assertThat(sample(registry, "consumer_count", "consumer_state", "active")).isEqualTo(1);
            assertThat(sample(registry, "consumer_count", "consumer_state", "idle")).isEqualTo(1);
            assertThat(sample(registry, "segment_blocked_count", null, null)).isEqualTo(5);
            first.close();
            first.close();
            assertThat(sample(registry, "consumer_count", "consumer_state", "active")).isZero();
            assertThat(sample(registry, "segment_blocked_count", null, null)).isEqualTo(3);
        }
        assertThat(registry.metricFamilySamples().hasMoreElements()).isFalse();
        // A restarted broker can register the exact same metric names.
        try (var metrics = new ScalableTopicConsumerMetrics(meter, registry, "test", () -> ConsumerStats.EMPTY)) {
            assertThat(sample(registry, "rebalance_total", "rebalance_reason", "consumer_join")).isZero();
        }
    }

    @Test
    public void testSharedRegistryRetainsCountersWhenSourceClosesAndIsReplaced() {
        var registry = new CollectorRegistry();
        var meter = OpenTelemetry.noop().getMeter("test");
        var retired = new AtomicBoolean();
        try (var first = new ScalableTopicConsumerMetrics(meter, registry, "test", () -> {
                 assertThat(retired.get()).as("Retired gauge suppliers must not be collected").isFalse();
                 return new ConsumerStats(2, 0, 0, 2);
             });
             var second = new ScalableTopicConsumerMetrics(meter, registry, "test",
                     () -> new ConsumerStats(0, 1, 0, 3))) {
            for (RebalanceReason reason : RebalanceReason.values()) {
                first.recordRebalance(reason);
                second.recordRebalance(reason);
            }
            assertReasonTotals(registry, 2);
            first.close();
            first.close();
            retired.set(true);
            for (RebalanceReason reason : RebalanceReason.values()) {
                first.recordRebalance(reason); // Late events cannot alter this source's final retained totals.
            }
            assertReasonTotals(registry, 2);
            assertThat(sample(registry, "consumer_count", "consumer_state", "active")).isZero();
            assertThat(sample(registry, "consumer_count", "consumer_state", "idle")).isEqualTo(1);
            assertThat(sample(registry, "segment_blocked_count", null, null)).isEqualTo(3);
            for (RebalanceReason reason : RebalanceReason.values()) {
                second.recordRebalance(reason);
            }
            assertReasonTotals(registry, 3);
            try (var replacement = new ScalableTopicConsumerMetrics(meter, registry, "test",
                    () -> new ConsumerStats(1, 0, 0, 1))) {
                assertReasonTotals(registry, 3);
                for (RebalanceReason reason : RebalanceReason.values()) {
                    replacement.recordRebalance(reason);
                }
                assertReasonTotals(registry, 4);
                assertThat(sample(registry, "consumer_count", "consumer_state", "active")).isEqualTo(1);
            }
            assertReasonTotals(registry, 4);
        }
        assertThat(registry.metricFamilySamples().hasMoreElements()).isFalse();
        try (var restarted = new ScalableTopicConsumerMetrics(meter, registry, "test", () -> ConsumerStats.EMPTY)) {
            assertReasonTotals(registry, 0);
        }
    }

    @Test
    public void testGaugeFailureDoesNotRemoveActiveRebalanceCountsAfterRetirement() {
        var registry = new CollectorRegistry();
        var meter = OpenTelemetry.noop().getMeter("test");
        var fail = new AtomicBoolean();
        try (var first = new ScalableTopicConsumerMetrics(meter, registry, "test", () -> ConsumerStats.EMPTY);
             var second = new ScalableTopicConsumerMetrics(meter, registry, "test", () -> {
                 if (fail.get()) {
                     throw new IllegalStateException("Injected gauge source failure after retirement");
                 }
                 return new ConsumerStats(1, 0, 0, 0);
             })) {
            first.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            first.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            for (int i = 0; i < 3; i++) {
                second.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            }
            first.close();
            assertThat(sample(registry, "rebalance_total", "rebalance_reason", "consumer_join")).isEqualTo(5);
            fail.set(true);
            assertThat(sample(registry, "rebalance_total", "rebalance_reason", "consumer_join")).isEqualTo(5);
            assertThat(registry.getSampleValue("pulsar_scalable_topic_consumer_count",
                    new String[]{"cluster", "consumer_state"}, new String[]{"test", "active"})).isNull();
            second.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            assertThat(sample(registry, "rebalance_total", "rebalance_reason", "consumer_join")).isEqualTo(6);
            fail.set(false);
            assertThat(sample(registry, "consumer_count", "consumer_state", "active")).isEqualTo(1);
        }
    }

    @Test
    public void testOpenTelemetryCountersSurviveGaugeFailureAndRetiredCallbacksStop() throws IOException {
        var registry = new CollectorRegistry();
        var firstCalls = new AtomicInteger();
        var fail = new AtomicBoolean();
        try (var reader = InMemoryMetricReader.create();
             var provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
             var first = new ScalableTopicConsumerMetrics(provider.get("first"), registry, "test", () -> {
                 firstCalls.incrementAndGet();
                 return ConsumerStats.EMPTY;
             });
             var second = new ScalableTopicConsumerMetrics(provider.get("second"), registry, "test", () -> {
                 if (fail.get()) {
                     throw new IllegalStateException("Injected OpenTelemetry gauge failure");
                 }
                 return ConsumerStats.EMPTY;
             })) {
            first.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            second.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            reader.collectAllMetrics();
            first.close();
            int callsAtClose = firstCalls.get();
            fail.set(true);
            for (long expected = 1; expected <= 2; expected++) {
                var metrics = reader.collectAllMetrics();
                assertThat(metrics).extracting(MetricData::getName).containsOnly(
                        ScalableTopicConsumerMetrics.REBALANCE_COUNT);
                long joins = metrics.stream().flatMap(metric -> metric.getLongSumData().getPoints().stream())
                        .filter(point -> "consumer_join".equals(point.getAttributes().get(
                                AttributeKey.stringKey("pulsar.scalable_topic.rebalance.reason"))))
                        .mapToLong(point -> point.getValue()).sum();
                assertThat(joins).isEqualTo(expected);
                assertThat(firstCalls.get()).as("Closed OTel callbacks must not be retained").isEqualTo(callsAtClose);
                assertThat(sample(registry, "rebalance_total", "rebalance_reason", "consumer_join"))
                        .isEqualTo(expected + 1);
                second.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            }
        }
    }

    @Test
    public void testRetiredClusterKeepsOnlyCountersUntilCollectorIsUnregistered() {
        var registry = new CollectorRegistry();
        var meter = OpenTelemetry.noop().getMeter("test");
        try (var first = new ScalableTopicConsumerMetrics(meter, registry, "retired",
                     () -> new ConsumerStats(2, 0, 0, 4));
             var second = new ScalableTopicConsumerMetrics(meter, registry, "test", () -> ConsumerStats.EMPTY)) {
            first.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            second.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            second.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            first.close();
            assertThat(registry.getSampleValue("pulsar_scalable_topic_rebalance_total",
                    new String[]{"cluster", "rebalance_reason"}, new String[]{"retired", "consumer_join"}))
                    .isEqualTo(1);
            assertThat(registry.getSampleValue("pulsar_scalable_topic_consumer_count",
                    new String[]{"cluster", "consumer_state"}, new String[]{"retired", "active"})).isNull();
            assertThat(registry.getSampleValue("pulsar_scalable_topic_segment_blocked_count",
                    new String[]{"cluster"}, new String[]{"retired"})).isNull();
            assertThat(sample(registry, "rebalance_total", "rebalance_reason", "consumer_join")).isEqualTo(2);
            try (var replacement = new ScalableTopicConsumerMetrics(meter, registry, "retired",
                    () -> new ConsumerStats(1, 0, 0, 0))) {
                replacement.recordRebalance(RebalanceReason.CONSUMER_JOIN);
                assertThat(registry.getSampleValue("pulsar_scalable_topic_rebalance_total",
                        new String[]{"cluster", "rebalance_reason"}, new String[]{"retired", "consumer_join"}))
                        .isEqualTo(2);
            }
        }
        assertThat(registry.metricFamilySamples().hasMoreElements()).isFalse();
    }

    @Test
    public void testCollectionConcurrentWithRetirementDoesNotDoubleCount() throws Exception {
        var registry = new CollectorRegistry();
        var meter = OpenTelemetry.noop().getMeter("test");
        var block = new AtomicBoolean();
        var collecting = new CompletableFuture<Void>();
        var resume = new CompletableFuture<ConsumerStats>();
        try (var first = new ScalableTopicConsumerMetrics(meter, registry, "test", () -> {
                 if (block.get()) {
                     collecting.complete(null);
                     return resume.join();
                 }
                 return ConsumerStats.EMPTY;
             });
             var second = new ScalableTopicConsumerMetrics(meter, registry, "test", () -> ConsumerStats.EMPTY);
             var executor = Executors.newSingleThreadExecutor()) {
            first.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            second.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            block.set(true);
            try {
                var sample = CompletableFuture.supplyAsync(() ->
                        sample(registry, "rebalance_total", "rebalance_reason", "consumer_join"), executor);
                collecting.get(5, TimeUnit.SECONDS);
                CompletableFuture.runAsync(first::close).get(5, TimeUnit.SECONDS);
                resume.complete(ConsumerStats.EMPTY);
                assertThat(sample.get(5, TimeUnit.SECONDS)).isEqualTo(2);
                assertThat(sample(registry, "rebalance_total", "rebalance_reason", "consumer_join")).isEqualTo(2);
            } finally {
                resume.complete(ConsumerStats.EMPTY);
            }
        }
    }

    private static void assertReasonTotals(CollectorRegistry registry, long expected) {
        for (String reason : List.of("consumer_join", "consumer_leave", "consumer_evict",
                "layout_change", "segment_drained")) {
            assertThat(sample(registry, "rebalance_total", "rebalance_reason", reason)).isEqualTo(expected);
        }
    }

    @Test
    public void testExistingPrometheusMetricsSurviveLifecycle() throws IOException {
        var registry = new CollectorRegistry();
        var counter = Counter.build("audit_existing_counter", "Existing counter").labelNames("owner")
                .register(registry);
        var gauge = Gauge.build("audit_existing_gauge", "Existing gauge").labelNames("owner")
                .register(registry);
        var histogram = Histogram.build("audit_existing_histogram", "Existing histogram")
                .labelNames("owner").buckets(1, 2, 4).register(registry);
        counter.labels("broker").inc(7);
        gauge.labels("broker").set(3);
        histogram.labels("broker").observe(1.5);
        String baseline = existingPrometheusSnapshot(registry);
        var meter = OpenTelemetry.noop().getMeter("test");
        try (var metrics = new ScalableTopicConsumerMetrics(meter, registry, "test",
                () -> new ConsumerStats(2, 1, 0, 3))) {
            assertThat(existingPrometheusSnapshot(registry)).isEqualTo(baseline);
            metrics.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            assertThat(sample(registry, "rebalance_total", "rebalance_reason", "consumer_join")).isEqualTo(1);
            assertThat(existingPrometheusSnapshot(registry)).isEqualTo(baseline);
            assertThat(existingPrometheusSnapshot(registry)).isEqualTo(baseline);
        }
        assertThat(existingPrometheusSnapshot(registry)).isEqualTo(baseline);
        counter.labels("broker").inc();
        gauge.labels("broker").set(5);
        histogram.labels("broker").observe(3);
        assertThat(registry.getSampleValue("audit_existing_counter_total", new String[]{"owner"},
                new String[]{"broker"})).isEqualTo(8);
        assertThat(registry.getSampleValue("audit_existing_gauge", new String[]{"owner"},
                new String[]{"broker"})).isEqualTo(5);
        assertThat(registry.getSampleValue("audit_existing_histogram_count", new String[]{"owner"},
                new String[]{"broker"})).isEqualTo(2);
        String updated = existingPrometheusSnapshot(registry);
        assertThat(updated).isNotEqualTo(baseline);
        try (var restarted = new ScalableTopicConsumerMetrics(meter, registry, "test", () -> ConsumerStats.EMPTY)) {
            restarted.recordRebalance(RebalanceReason.LAYOUT_CHANGE);
            assertThat(existingPrometheusSnapshot(registry)).isEqualTo(updated);
        }
        assertThat(existingPrometheusSnapshot(registry)).isEqualTo(updated);
    }

    @Test
    public void testExistingOpenTelemetryMetricsSurviveLifecycle() throws IOException {
        var registry = new CollectorRegistry();
        var gaugeValue = new AtomicLong(9);
        try (var reader = InMemoryMetricReader.create();
             var provider = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            var meter = provider.get("test");
            PulsarService pulsar = mock(PulsarService.class);
            PulsarBrokerOpenTelemetry telemetry = mock(PulsarBrokerOpenTelemetry.class);
            when(pulsar.getOpenTelemetry()).thenReturn(telemetry);
            when(telemetry.getMeter()).thenReturn(meter);
            var existing = new OpenTelemetryReplicatedSubscriptionStats(pulsar);
            try (var gauge = meter.gaugeBuilder("audit.existing.gauge").ofLongs().setUnit("{session}")
                    .setDescription("Existing asynchronous gauge")
                    .buildWithCallback(measurement -> measurement.record(gaugeValue.get(),
                            Attributes.of(AttributeKey.stringKey("owner"), "broker")))) {
                existing.recordSnapshotStarted();
                existing.recordSnapshotCompleted(125);
                existing.recordSnapshotTimedOut(250);
                Map<String, List<String>> baseline = existingOpenTelemetrySnapshot(reader);
                assertThat(baseline).hasSize(3);
                try (var metrics = new ScalableTopicConsumerMetrics(meter, registry, "test",
                        () -> new ConsumerStats(2, 1, 0, 3))) {
                    assertThat(existingOpenTelemetrySnapshot(reader)).isEqualTo(baseline);
                    metrics.recordRebalance(RebalanceReason.CONSUMER_JOIN);
                    assertThat(existingOpenTelemetrySnapshot(reader)).isEqualTo(baseline);
                    assertThat(existingOpenTelemetrySnapshot(reader)).isEqualTo(baseline);
                }
                assertThat(existingOpenTelemetrySnapshot(reader)).isEqualTo(baseline);
                existing.recordSnapshotStarted();
                existing.recordSnapshotCompleted(500);
                gaugeValue.set(12);
                Map<String, List<String>> updated = existingOpenTelemetrySnapshot(reader);
                assertThat(updated).isNotEqualTo(baseline);
                assertThat(updated).allSatisfy((name, snapshot) ->
                        assertThat(snapshot).isNotEqualTo(baseline.get(name)));
                try (var restarted = new ScalableTopicConsumerMetrics(meter, registry, "test",
                        () -> ConsumerStats.EMPTY)) {
                    restarted.recordRebalance(RebalanceReason.LAYOUT_CHANGE);
                    assertThat(existingOpenTelemetrySnapshot(reader)).isEqualTo(updated);
                }
                assertThat(existingOpenTelemetrySnapshot(reader)).isEqualTo(updated);
            }
        }
    }

    @Test
    public void testFailedSourceDoesNotBreakExistingMetricsOrHealthySource() throws IOException {
        var registry = new CollectorRegistry();
        Gauge.build("audit_existing_gauge", "Existing gauge").register(registry).set(17);
        String baseline = existingPrometheusSnapshot(registry);
        var fail = new AtomicBoolean();
        var gaugeValue = new AtomicLong(19);
        try (var reader = InMemoryMetricReader.create();
             var provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
             var gauge = provider.get("test").gaugeBuilder("audit.existing.gauge").ofLongs()
                     .buildWithCallback(measurement -> measurement.record(gaugeValue.get()));
             var healthy = new ScalableTopicConsumerMetrics(provider.get("healthy"), registry, "test",
                     () -> new ConsumerStats(2, 0, 0, 4));
             var failing = new ScalableTopicConsumerMetrics(provider.get("failing"), registry, "test", () -> {
                 if (fail.get()) {
                     throw new IllegalStateException("Injected new metrics source failure");
                 }
                 return ConsumerStats.EMPTY;
             })) {
            healthy.recordRebalance(RebalanceReason.CONSUMER_JOIN);
            Map<String, List<String>> telemetryBaseline = existingOpenTelemetrySnapshot(reader);
            fail.set(true);
            // Serialize the whole shared registry: checking only a healthy collector would hide propagation.
            assertThat(existingPrometheusSnapshot(registry)).isEqualTo(baseline);
            assertThat(sample(registry, "consumer_count", "consumer_state", "active")).isEqualTo(2);
            assertThat(sample(registry, "segment_blocked_count", null, null)).isEqualTo(4);
            assertThat(sample(registry, "rebalance_total", "rebalance_reason", "consumer_join")).isEqualTo(1);
            assertThat(existingOpenTelemetrySnapshot(reader)).isEqualTo(telemetryBaseline);
            gaugeValue.set(23);
            assertThat(existingOpenTelemetrySnapshot(reader)).isNotEqualTo(telemetryBaseline);
        }
        assertThat(existingPrometheusSnapshot(registry)).isEqualTo(baseline);
    }

    @Test
    public void testRegistrationCollisionDoesNotLeakOpenTelemetryCallback() throws IOException {
        var registry = new CollectorRegistry();
        var existing = Gauge.build("pulsar_scalable_topic_consumer_count", "Pre-existing collector")
                .register(registry);
        existing.set(29);
        var supplierCalls = new AtomicInteger();
        try (var reader = InMemoryMetricReader.create();
             var provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
             var gauge = provider.get("test").gaugeBuilder("audit.existing.gauge").ofLongs()
                     .buildWithCallback(measurement -> measurement.record(31))) {
            Map<String, List<String>> baseline = existingOpenTelemetrySnapshot(reader);
            assertThatThrownBy(() -> new ScalableTopicConsumerMetrics(provider.get("test"), registry, "test", () -> {
                supplierCalls.incrementAndGet();
                return ConsumerStats.EMPTY;
            })).isInstanceOf(IllegalArgumentException.class);
            assertThat(existingOpenTelemetrySnapshot(reader)).isEqualTo(baseline);
            assertThat(existingOpenTelemetrySnapshot(reader)).isEqualTo(baseline);
            assertThat(supplierCalls.get()).as("Failed registration must remove its asynchronous callback").isZero();
            assertThat(registry.getSampleValue("pulsar_scalable_topic_consumer_count")).isEqualTo(29);
            existing.inc();
            assertThat(registry.getSampleValue("pulsar_scalable_topic_consumer_count")).isEqualTo(30);
            assertThat(serialize(registry)).contains("Pre-existing collector");
        }
    }

    private static String serialize(CollectorRegistry registry) throws IOException {
        var output = new StringBuilder();
        var families = registry.metricFamilySamples();
        while (families.hasMoreElements()) {
            var family = families.nextElement();
            output.append(family.name).append(" HELP ").append(family.help)
                    .append(" TYPE ").append(family.type).append('\n');
            for (var sample : family.samples) {
                output.append(sample.name).append(sample.labelNames).append(sample.labelValues)
                        .append('=').append(sample.value).append('\n');
            }
        }
        return output.toString();
    }

    private static String existingPrometheusSnapshot(CollectorRegistry registry) throws IOException {
        return serialize(registry).lines().filter(line -> line.contains("audit_existing_"))
                .sorted().collect(Collectors.joining("\n"));
    }

    private static Map<String, List<String>> existingOpenTelemetrySnapshot(InMemoryMetricReader reader) {
        Set<String> names = Set.of(OpenTelemetryReplicatedSubscriptionStats.SNAPSHOT_OPERATION_COUNT_METRIC_NAME,
                OpenTelemetryReplicatedSubscriptionStats.SNAPSHOT_DURATION_METRIC_NAME, "audit.existing.gauge");
        return reader.collectAllMetrics().stream().filter(metric -> names.contains(metric.getName()))
                .collect(Collectors.toMap(MetricData::getName, metric -> {
                    List<String> snapshot = new ArrayList<>();
                    snapshot.add(metric.getDescription());
                    snapshot.add(metric.getUnit());
                    snapshot.add(metric.getType().name());
                    switch (metric.getType()) {
                        case LONG_SUM -> {
                            var sum = metric.getLongSumData();
                            snapshot.add(sum.getAggregationTemporality().name());
                            snapshot.add(Boolean.toString(sum.isMonotonic()));
                            sum.getPoints().forEach(point ->
                                    snapshot.add(point.getAttributes() + "=" + point.getValue()));
                        }
                        case LONG_GAUGE -> metric.getLongGaugeData().getPoints()
                                .forEach(point -> snapshot.add(point.getAttributes() + "=" + point.getValue()));
                        case HISTOGRAM -> {
                            var histogram = metric.getHistogramData();
                            snapshot.add(histogram.getAggregationTemporality().name());
                            histogram.getPoints().forEach(point -> snapshot.add(point.getAttributes() + "="
                                    + point.getCount() + ":" + point.getSum() + ":" + point.getMin() + ":"
                                    + point.getMax() + ":" + point.getBoundaries() + ":" + point.getCounts()));
                        }
                        default -> throw new AssertionError("Unexpected existing instrument: " + metric.getType());
                    }
                    return snapshot;
                }));
    }

    private static void assertExports(InMemoryMetricReader reader, CollectorRegistry registry,
                                      ConsumerStats expected, long joins, long layouts) {
        Map<String, MetricData> data = reader.collectAllMetrics().stream()
                .collect(Collectors.toMap(MetricData::getName, metric -> metric));
        assertThat(data).containsOnlyKeys(ScalableTopicConsumerMetrics.CONSUMER_COUNT,
                ScalableTopicConsumerMetrics.BLOCKED_COUNT, ScalableTopicConsumerMetrics.REBALANCE_COUNT);
        var consumers = data.get(ScalableTopicConsumerMetrics.CONSUMER_COUNT).getLongSumData();
        assertThat(consumers.isMonotonic()).isFalse();
        assertThat(consumers.getPoints()).hasSize(3);
        Map<String, Long> expectedConsumers = Map.of("active", expected.active(), "idle", expected.idle(),
                "disconnected", expected.disconnected());
        consumers.getPoints().forEach(point -> {
            assertThat(point.getAttributes().size()).isEqualTo(1);
            String state = point.getAttributes().get(AttributeKey.stringKey("pulsar.scalable_topic.consumer.state"));
            assertThat(point.getValue()).isEqualTo(expectedConsumers.get(state));
            assertThat(sample(registry, "consumer_count", "consumer_state", state)).isEqualTo(point.getValue());
        });
        String blockedDescription = "Segment/subscription pairs waiting for ancestors to drain, "
                + "summed across subscriptions served by this broker.";
        assertThat(data.get(ScalableTopicConsumerMetrics.BLOCKED_COUNT).getDescription()).isEqualTo(blockedDescription);
        assertThat(Collections.list(registry.metricFamilySamples()))
                .filteredOn(family -> family.name.equals("pulsar_scalable_topic_segment_blocked_count"))
                .singleElement().satisfies(family -> assertThat(family.help).isEqualTo(blockedDescription));
        var blocked = data.get(ScalableTopicConsumerMetrics.BLOCKED_COUNT).getLongSumData();
        assertThat(blocked.isMonotonic()).isFalse();
        assertThat(blocked.getPoints()).singleElement().satisfies(point -> {
            assertThat(point.getAttributes().isEmpty()).isTrue();
            assertThat(point.getValue()).isEqualTo(expected.blocked());
            assertThat(sample(registry, "segment_blocked_count", null, null)).isEqualTo(point.getValue());
        });
        var rebalances = data.get(ScalableTopicConsumerMetrics.REBALANCE_COUNT).getLongSumData();
        assertThat(rebalances.isMonotonic()).isTrue();
        assertThat(rebalances.getPoints()).hasSize(5);
        rebalances.getPoints().forEach(point -> {
            assertThat(point.getAttributes().size()).isEqualTo(1);
            String reason = point.getAttributes().get(AttributeKey.stringKey("pulsar.scalable_topic.rebalance.reason"));
            long count = switch (reason) {
                case "consumer_join" -> joins;
                case "layout_change" -> layouts;
                default -> 0;
            };
            assertThat(point.getValue()).isEqualTo(count);
            assertThat(sample(registry, "rebalance_total", "rebalance_reason", reason)).isEqualTo(count);
        });
    }

    private static double sample(CollectorRegistry registry, String name, String label, String value) {
        String[] labels = label == null ? new String[]{"cluster"} : new String[]{"cluster", label};
        String[] values = label == null ? new String[]{"test"} : new String[]{"test", value};
        Double sample = registry.getSampleValue("pulsar_scalable_topic_" + name, labels, values);
        assertThat(sample).as("Prometheus sample %s with %s=%s", name, label, value).isNotNull();
        return sample;
    }
}
