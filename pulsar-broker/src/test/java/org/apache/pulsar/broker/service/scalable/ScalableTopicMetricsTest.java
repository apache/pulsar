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
import static org.assertj.core.data.Offset.offset;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ScalableTopicMetricsTest {
    private static final String OTEL_PREFIX = "pulsar.broker.scalable_topic.";
    private static final String PROM_PREFIX = "pulsar_scalable_topic_";
    private static final String ATTRIBUTE_PREFIX = "pulsar.scalable_topic.";
    private static final String CLUSTER = "test-cluster";
    private static final ScalableTopicMetrics.Counts EMPTY = new ScalableTopicMetrics.Counts(0, 0, 0);

    private InMemoryMetricReader reader;
    private SdkMeterProvider sdk;
    private Meter meter;
    private CollectorRegistry registry;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        reader = InMemoryMetricReader.create();
        sdk = SdkMeterProvider.builder().registerMetricReader(reader).build();
        meter = sdk.get("scalable-topic-metrics-test");
        registry = new CollectorRegistry();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        sdk.close();
    }

    @Test
    public void testCounterAndHistogramParity() {
        try (var metrics = new ScalableTopicMetrics(meter, CLUSTER, () -> EMPTY, registry)) {
            metrics.recordTakeover(0.005, true);
            metrics.recordTakeover(0.2, false);
            metrics.recordOperation("split", "auto", "success", 0.001);
            metrics.recordOperation("split", "auto", "success", 0.05);
            metrics.recordOperation("merge", "manual", "failure", 0.1);
            metrics.recordOperation("rebucket", "manual", "partial", 350);
            metrics.recordOperation("prune", "auto", "success", 0);
            metrics.recordSeal("split", 0.002);
            metrics.recordSeal("merge", 2);
            metrics.recordSeal("rebucket", 31);
            for (String reason : List.of("max_segments", "max_entry_buckets", "max_dag_depth", "unsplittable",
                    "invalid_policy")) {
                metrics.recordBlocked(reason);
                metrics.recordBlocked(reason);
            }

            Collection<MetricData> otel = reader.collectAllMetrics();
            List<MetricFamilySamples> prometheus = Collections.list(registry.metricFamilySamples());
            assertThat(otel).extracting(MetricData::getName).containsExactlyInAnyOrder(
                    OTEL_PREFIX + "controller.count", OTEL_PREFIX + "segment.count",
                    OTEL_PREFIX + "controller.takeover.duration", OTEL_PREFIX + "layout.operation.count",
                    OTEL_PREFIX + "layout.operation.duration", OTEL_PREFIX + "layout.seal.duration",
                    OTEL_PREFIX + "auto_scale.blocked.count");
            assertThat(prometheus).hasSize(7);
            for (MetricFamilySamples family : prometheus) {
                assertThat(family.type).isIn(Collector.Type.COUNTER, Collector.Type.GAUGE, Collector.Type.HISTOGRAM);
                for (Sample sample : family.samples) {
                    assertThat(labels(sample)).containsEntry("cluster", CLUSTER);
                    assertThat(sample.labelNames).doesNotContain("topic", "broker", "namespace", "segment");
                }
            }
            assertCounterParity(otel, prometheus, "layout.operation.count", "layout_operation_total", 5);
            assertCounterParity(otel, prometheus, "auto_scale.blocked.count", "auto_scale_blocked_total", 10);
            assertHistogramParity(otel, prometheus, "controller.takeover.duration",
                    "controller_takeover_duration_seconds", 2, 0.205);
            assertHistogramParity(otel, prometheus, "layout.operation.duration",
                    "layout_operation_duration_seconds", 5, 350.151);
            assertHistogramParity(otel, prometheus, "layout.seal.duration", "layout_seal_duration_seconds", 3, 33.002);

            assertThat(sample(prometheus, "layout_operation_total", Map.of("cluster", CLUSTER,
                    "operation", "split", "operation_trigger", "auto", "operation_status", "success")))
                    .isEqualTo(2);
            assertThat(prometheus.stream().flatMap(family -> family.samples.stream())
                    .filter(value -> value.name.endsWith("_bucket")))
                    .allSatisfy(value -> assertThat(value.labelNames).contains("le"));
        }
    }

    @Test
    public void testGaugesReadCurrentCountsAtCollection() {
        AtomicReference<ScalableTopicMetrics.Counts> counts = new AtomicReference<>(EMPTY);
        AtomicInteger reads = new AtomicInteger();
        try (var metrics = new ScalableTopicMetrics(meter, CLUSTER, () -> {
            reads.incrementAndGet();
            return counts.get();
        }, registry)) {
            assertThat(reads).hasValue(0);
            counts.set(new ScalableTopicMetrics.Counts(2, 5, 7));
            assertGauges(reader.collectAllMetrics(), Collections.list(registry.metricFamilySamples()), 2, 5, 7);
            assertThat(reads).hasValue(2);
            counts.set(new ScalableTopicMetrics.Counts(1, 3, 11));
            assertGauges(reader.collectAllMetrics(), Collections.list(registry.metricFamilySamples()), 1, 3, 11);
            assertThat(reads).hasValue(4);
        }
    }

    @Test
    public void testMultipleInstancesAggregateAndCleanUp() {
        AtomicInteger firstReads = new AtomicInteger();
        var first = new ScalableTopicMetrics(meter, CLUSTER, () -> {
            firstReads.incrementAndGet();
            return new ScalableTopicMetrics.Counts(1, 2, 3);
        }, registry);
        var second = new ScalableTopicMetrics(meter, CLUSTER,
                () -> new ScalableTopicMetrics.Counts(4, 5, 6), registry);
        try {
            first.recordOperation("split", "manual", "success", 0.005);
            second.recordOperation("split", "manual", "success", 0.01);
            var otel = reader.collectAllMetrics();
            var prometheus = Collections.list(registry.metricFamilySamples());
            assertGauges(otel, prometheus, 5, 7, 9);
            assertCounterParity(otel, prometheus, "layout.operation.count", "layout_operation_total", 2);
            assertHistogramParity(otel, prometheus, "layout.operation.duration",
                    "layout_operation_duration_seconds", 2, 0.015);
            assertThat(prometheus.stream().flatMap(family -> family.samples.stream())
                    .filter(value -> value.name.equals(PROM_PREFIX + "layout_operation_total"))).hasSize(1);

            first.close();
            first.close();
            int readsBefore = firstReads.get();
            assertGauges(reader.collectAllMetrics(), Collections.list(registry.metricFamilySamples()), 4, 5, 6);
            assertThat(firstReads).hasValue(readsBefore);
            assertHistogramParity(reader.collectAllMetrics(), Collections.list(registry.metricFamilySamples()),
                    "layout.operation.duration", "layout_operation_duration_seconds", 2, 0.015);
            second.close();
            assertThat(Collections.list(registry.metricFamilySamples())).isEmpty();
            assertThat(reader.collectAllMetrics()).extracting(MetricData::getName)
                    .doesNotContain(OTEL_PREFIX + "controller.count", OTEL_PREFIX + "segment.count",
                            OTEL_PREFIX + "layout.operation.count", OTEL_PREFIX + "auto_scale.blocked.count");
            // SDK synchronous histograms retain their observations; callbacks and Prom collectors do not.
            try (var replacement = new ScalableTopicMetrics(meter, CLUSTER, () -> EMPTY, registry)) {
                assertGauges(reader.collectAllMetrics(), Collections.list(registry.metricFamilySamples()), 0, 0, 0);
            }
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    public void testRecreatedServiceKeepsHistogramParity() {
        try (var first = new ScalableTopicMetrics(meter, CLUSTER, () -> EMPTY, registry)) {
            first.recordTakeover(0.005, true);
            assertHistogramParity(reader.collectAllMetrics(), Collections.list(registry.metricFamilySamples()),
                    "controller.takeover.duration", "controller_takeover_duration_seconds", 1, 0.005);
        }
        assertThat(Collections.list(registry.metricFamilySamples())).isEmpty();
        try (var replacement = new ScalableTopicMetrics(meter, CLUSTER, () -> EMPTY, registry)) {
            replacement.recordTakeover(0.01, true);
            assertHistogramParity(reader.collectAllMetrics(), Collections.list(registry.metricFamilySamples()),
                    "controller.takeover.duration", "controller_takeover_duration_seconds", 2, 0.015);
        }
        assertThat(Collections.list(registry.metricFamilySamples())).isEmpty();
    }

    @Test
    public void testDistinctMetersRemoveClosedBrokerHistograms() {
        var otherReader = InMemoryMetricReader.create();
        try (var otherSdk = SdkMeterProvider.builder().registerMetricReader(otherReader).build();
             var first = new ScalableTopicMetrics(meter, CLUSTER, () -> EMPTY, registry);
             var second = new ScalableTopicMetrics(otherSdk.get("other-broker"), CLUSTER, () -> EMPTY, registry)) {
            first.recordTakeover(0.005, true);
            second.recordTakeover(0.01, true);
            var before = Collections.list(registry.metricFamilySamples());
            assertThat(sample(before, "controller_takeover_duration_seconds_count",
                    Map.of("cluster", CLUSTER, "operation_status", "success"))).isEqualTo(2);
            first.close();
            assertHistogramParity(otherReader.collectAllMetrics(), Collections.list(registry.metricFamilySamples()),
                    "controller.takeover.duration", "controller_takeover_duration_seconds", 1, 0.01);
        }
    }

    @Test
    public void testDefaultRegistryAndClusterIsolationWithDisabledOpenTelemetry() {
        Meter disabled = OpenTelemetry.noop().getMeter("disabled");
        try (var first = new ScalableTopicMetrics(disabled, "first", () -> new ScalableTopicMetrics.Counts(1, 2, 3));
             var second = new ScalableTopicMetrics(disabled, "second",
                     () -> new ScalableTopicMetrics.Counts(4, 5, 6))) {
            first.recordBlocked("max_segments");
            first.recordTakeover(0.5, true);
            second.recordOperation("merge", "auto", "partial", 1);
            var samples = Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples());
            assertThat(sample(samples, "controller_count", Map.of("cluster", "first"))).isEqualTo(1);
            assertThat(sample(samples, "controller_count", Map.of("cluster", "second"))).isEqualTo(4);
            assertThat(sample(samples, "auto_scale_blocked_total",
                    Map.of("cluster", "first", "auto_scale_reason", "max_segments"))).isEqualTo(1);
            assertThat(sample(samples, "controller_takeover_duration_seconds_count",
                    Map.of("cluster", "first", "operation_status", "success"))).isEqualTo(1);
            assertThat(sample(samples, "layout_operation_total", Map.of("cluster", "second",
                    "operation", "merge", "operation_trigger", "auto", "operation_status", "partial"))).isEqualTo(1);
            assertThat(reader.collectAllMetrics()).isEmpty();
        }
        assertThat(Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples()))
                .flatExtracting(family -> family.samples)
                .noneSatisfy(value -> assertThat(labels(value)).containsEntry("cluster", "first"));
    }

    @Test
    public void testRegistriesAreIsolatedAndNoopNeverRegisters() {
        var otherRegistry = new CollectorRegistry();
        try (var first = new ScalableTopicMetrics(meter, CLUSTER, () -> EMPTY, registry);
             var second = new ScalableTopicMetrics(meter, CLUSTER, () -> EMPTY, otherRegistry)) {
            first.recordBlocked("invalid_policy");
            assertThat(sample(Collections.list(registry.metricFamilySamples()), "auto_scale_blocked_total",
                    Map.of("cluster", CLUSTER, "auto_scale_reason", "invalid_policy"))).isEqualTo(1);
            assertThat(Collections.list(otherRegistry.metricFamilySamples())).hasSize(2);
            ScalableTopicMetrics.NOOP.recordBlocked("arbitrary");
            ScalableTopicMetrics.NOOP.recordOperation(null, null, null, Double.NaN);
            ScalableTopicMetrics.NOOP.recordTakeover(Double.NaN, false);
            ScalableTopicMetrics.NOOP.recordSeal(null, Double.NaN);
            ScalableTopicMetrics.NOOP.close();
            assertThat(Collections.list(otherRegistry.metricFamilySamples())).hasSize(2);
        }
    }

    @Test
    public void testRejectsUnboundedLabelsAndInvalidDurations() {
        try (var metrics = new ScalableTopicMetrics(meter, CLUSTER, () -> EMPTY, registry)) {
            assertThatThrownBy(() -> metrics.recordOperation("topic-name", "auto", "success", 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> metrics.recordOperation("split", "unknown", "success", 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> metrics.recordOperation("split", "auto", "unknown", 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> metrics.recordBlocked("topic-name")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> metrics.recordSeal("prune", 1)).isInstanceOf(IllegalArgumentException.class);
            for (double duration : new double[]{-1, Double.NaN, Double.POSITIVE_INFINITY}) {
                assertThatThrownBy(() -> metrics.recordTakeover(duration, true))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> metrics.recordOperation("split", "auto", "success", duration))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> metrics.recordSeal("split", duration))
                        .isInstanceOf(IllegalArgumentException.class);
            }
            assertThat(Collections.list(registry.metricFamilySamples())).hasSize(2);
        }
    }

    private static void assertCounterParity(Collection<MetricData> otel, List<MetricFamilySamples> prometheus,
                                            String otelName, String prometheusName, long expectedTotal) {
        var data = metric(otel, otelName).getLongSumData();
        assertThat(data.isMonotonic()).isTrue();
        long total = 0;
        for (var point : data.getPoints()) {
            assertThat(sample(prometheus, prometheusName, labels(point.getAttributes()))).isEqualTo(point.getValue());
            total += point.getValue();
        }
        assertThat(total).isEqualTo(expectedTotal);
    }

    private static void assertHistogramParity(Collection<MetricData> otel, List<MetricFamilySamples> prometheus,
                                              String otelName, String prometheusName, long expectedCount,
                                              double expectedSum) {
        var metric = metric(otel, otelName);
        assertThat(metric.getUnit()).isEqualTo("s");
        long totalCount = 0;
        double totalSum = 0;
        for (var point : metric.getHistogramData().getPoints()) {
            assertThat(point.getBoundaries()).containsExactlyElementsOf(ScalableTopicMetrics.DURATION_BUCKETS);
            Map<String, String> labels = labels(point.getAttributes());
            assertThat(sample(prometheus, prometheusName + "_count", labels)).isEqualTo(point.getCount());
            assertThat(sample(prometheus, prometheusName + "_sum", labels)).isEqualTo(point.getSum());
            long cumulative = 0;
            for (int i = 0; i < point.getCounts().size(); i++) {
                cumulative += point.getCounts().get(i);
                Map<String, String> bucketLabels = new HashMap<>(labels);
                bucketLabels.put("le", i < point.getBoundaries().size()
                        ? Collector.doubleToGoString(point.getBoundaries().get(i)) : "+Inf");
                assertThat(sample(prometheus, prometheusName + "_bucket", bucketLabels)).isEqualTo(cumulative);
            }
            totalCount += point.getCount();
            totalSum += point.getSum();
        }
        assertThat(totalCount).isEqualTo(expectedCount);
        assertThat(totalSum).isCloseTo(expectedSum, offset(0.000001));
    }

    private static void assertGauges(Collection<MetricData> otel, List<MetricFamilySamples> prometheus,
                                     long controllers, long active, long sealed) {
        var controllerData = metric(otel, "controller.count").getLongSumData();
        assertThat(controllerData.isMonotonic()).isFalse();
        assertThat(controllerData.getPoints()).singleElement().satisfies(point -> {
            assertThat(point.getAttributes()).isEqualTo(Attributes.empty());
            assertThat(point.getValue()).isEqualTo(controllers);
        });
        var segmentData = metric(otel, "segment.count").getLongSumData();
        assertThat(segmentData.isMonotonic()).isFalse();
        assertThat(segmentData.getPoints()).hasSize(2).allSatisfy(point -> assertThat(point.getValue())
                .isEqualTo(point.getAttributes().equals(Attributes.builder()
                        .put(ATTRIBUTE_PREFIX + "segment.state", "active").build()) ? active : sealed));
        assertThat(sample(prometheus, "controller_count", Map.of("cluster", CLUSTER))).isEqualTo(controllers);
        assertThat(sample(prometheus, "segment_count", Map.of("cluster", CLUSTER, "segment_state", "active")))
                .isEqualTo(active);
        assertThat(sample(prometheus, "segment_count", Map.of("cluster", CLUSTER, "segment_state", "sealed")))
                .isEqualTo(sealed);
    }

    private static MetricData metric(Collection<MetricData> metrics, String name) {
        return metrics.stream().filter(metric -> metric.getName().equals(OTEL_PREFIX + name)).findFirst().orElseThrow();
    }

    private static Map<String, String> labels(Attributes attributes) {
        Map<String, String> result = new HashMap<>(Map.of("cluster", CLUSTER));
        attributes.forEach((key, value) -> {
            assertThat(key.getKey()).startsWith(ATTRIBUTE_PREFIX);
            result.put(key.getKey().substring(ATTRIBUTE_PREFIX.length()).replace('.', '_'), value.toString());
        });
        return result;
    }

    private static Map<String, String> labels(Sample sample) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < sample.labelNames.size(); i++) {
            result.put(sample.labelNames.get(i), sample.labelValues.get(i));
        }
        return result;
    }

    private static double sample(List<MetricFamilySamples> families, String name, Map<String, String> labels) {
        return families.stream().flatMap(family -> family.samples.stream())
                .filter(sample -> sample.name.equals(PROM_PREFIX + name) && labels(sample).equals(labels))
                .mapToDouble(sample -> sample.value).findFirst().orElseThrow();
    }
}
