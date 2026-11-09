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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.stats.OpenTelemetryMessageFinderStats;
import org.apache.pulsar.broker.stats.OpenTelemetryMessageFinderStats.EntryStorage;
import org.apache.pulsar.broker.stats.OpenTelemetryMessageFinderStats.FindReason;
import org.apache.pulsar.broker.stats.OpenTelemetryMessageFinderStats.FindResult;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.Test;

/** Compatibility checks for unrelated metrics sharing the registry and meter with scalable-topic metrics. */
@Test(groups = "broker")
public class ScalableTopicMetricsIsolationTest {
    private static final String CLUSTER = "isolation-cluster";
    private static final String PROM_SESSION = "pulsar_scalable_topic_watch_session_count";
    private static final String OLD_CALLBACK = "pulsar.broker.non.recoverable.entries.skipped.count";
    private static final String OLD_LOOKUPS = "pulsar.broker.request.topic.lookup.concurrent.usage";

    @Test
    public void existingMetricsSurviveCreateUpdateCollectCloseAndRestart() {
        try (ExistingMetrics existing = new ExistingMetrics(new CollectorRegistry(), "")) {
            List<MetricSnapshot> before = existing.snapshot();
            try (ScalableTopicMetrics metrics = new ScalableTopicMetrics(existing.meter, CLUSTER, existing.registry)) {
                existing.assertUnchanged(before);
                exercise(metrics);
                existing.assertUnchanged(before);
                existing.assertUnchanged(before); // Repeated collection must not change unrelated instruments.
                ScalableTopicMetrics.Request pending = metrics.startRequest(ScalableTopicMetrics.RequestType.SUBSCRIBE);
                metrics.close();
                metrics.close();
                pending.failure();
                exercise(metrics); // Late completions after close cannot affect existing metrics either.
                existing.assertUnchanged(before);
            }
            try (ScalableTopicMetrics restarted =
                         new ScalableTopicMetrics(existing.meter, CLUSTER, existing.registry)) {
                exercise(restarted);
                existing.assertUnchanged(before);
            }
            existing.assertUnchanged(before);
            existing.assertStillUpdates(before);
        }
    }

    @Test
    public void closingOneOfSeveralBrokersDoesNotDisposeExistingInstruments() {
        try (ExistingMetrics existing = new ExistingMetrics(new CollectorRegistry(), "");
             ScalableTopicMetrics first = new ScalableTopicMetrics(existing.meter, CLUSTER, existing.registry);
             ScalableTopicMetrics second = new ScalableTopicMetrics(existing.meter, CLUSTER, existing.registry)) {
            List<MetricSnapshot> before = existing.snapshot();
            exercise(first);
            exercise(second);
            existing.assertUnchanged(before);
            assertThat(existing.registry.getSampleValue(PROM_SESSION,
                    new String[]{"cluster", "watch_type"}, new String[]{CLUSTER, "topic"})).isEqualTo(2);
            first.close();
            existing.assertUnchanged(before);
            assertThat(existing.registry.getSampleValue(PROM_SESSION,
                    new String[]{"cluster", "watch_type"}, new String[]{CLUSTER, "topic"})).isEqualTo(1);
            second.recordWatchUpdate(false);
            second.close();
            existing.assertUnchanged(before);
            existing.assertStillUpdates(before);
        }
    }

    @Test
    public void failedRegistrationDoesNotUnregisterExistingCollectorsOrCallbacks() {
        try (ExistingMetrics existing = new ExistingMetrics(new CollectorRegistry(), "")) {
            Gauge conflict = Gauge.build(PROM_SESSION, "Pre-existing conflicting collector")
                    .register(existing.registry);
            conflict.set(17);
            try {
                List<MetricSnapshot> before = existing.snapshot();
                List<MetricSnapshot> conflictBefore = prometheusSnapshot(existing.registry, Set.of(PROM_SESSION));
                assertThatThrownBy(() -> new ScalableTopicMetrics(existing.meter, CLUSTER, existing.registry))
                        .isInstanceOf(IllegalArgumentException.class);
                existing.assertUnchanged(before);
                assertThat(prometheusSnapshot(existing.registry, Set.of(PROM_SESSION))).isEqualTo(conflictBefore);
                assertThat(existing.reader.collectAllMetrics()).extracting(MetricData::getName)
                        .doesNotContain(ScalableTopicMetrics.WATCH_SESSION_COUNT_METRIC_NAME,
                                ScalableTopicMetrics.WATCH_UPDATE_COUNT_METRIC_NAME);
            } finally {
                existing.registry.unregister(conflict);
            }
            List<MetricSnapshot> beforeRetry = existing.snapshot();
            try (ScalableTopicMetrics retry = new ScalableTopicMetrics(existing.meter, CLUSTER, existing.registry)) {
                exercise(retry);
                existing.assertUnchanged(beforeRetry);
            }
            existing.assertUnchanged(beforeRetry);
            existing.assertStillUpdates(beforeRetry);
        }
    }

    @Test
    public void defaultRegistryRendererPreservesExistingSamplesAndAddsClusterOnlyOnce() {
        // DefaultRegistry may already contain real broker collectors. Use exclusively owned, uniquely suffixed
        // replicas here; the isolated-registry tests above use the unmodified existing production names.
        String suffix = "_isolation_" + UUID.randomUUID().toString().replace("-", "");
        String cluster = "isolation-" + UUID.randomUUID();
        try (ExistingMetrics existing = new ExistingMetrics(CollectorRegistry.defaultRegistry, suffix)) {
            List<String> before = existingRenderedLines(existing, cluster);
            try (ScalableTopicMetrics metrics = new ScalableTopicMetrics(existing.meter, cluster)) {
                exercise(metrics);
                assertThat(existingRenderedLines(existing, cluster)).isEqualTo(before);
                List<String> scalable = render(cluster).lines()
                        .filter(line -> line.startsWith("pulsar_scalable_topic_")
                                && line.contains("cluster=\"" + cluster + "\""))
                        .toList();
                assertThat(scalable).isNotEmpty();
                assertThat(scalable).allSatisfy(line ->
                        assertThat(line.split("cluster=", -1)).as("single cluster label: %s", line).hasSize(2));
                assertThat(scalable.stream().map(line -> line.substring(0, line.indexOf('}') + 1)).toList())
                        .doesNotHaveDuplicates();
                assertThat(render(cluster).lines()
                        .filter(line -> line.equals("# TYPE " + PROM_SESSION + " gauge")).count()).isEqualTo(1);
                metrics.close();
                assertThat(existingRenderedLines(existing, cluster)).isEqualTo(before);
            }
            try (ScalableTopicMetrics restarted = new ScalableTopicMetrics(existing.meter, cluster)) {
                exercise(restarted);
                assertThat(existingRenderedLines(existing, cluster)).isEqualTo(before);
            }
            assertThat(existingRenderedLines(existing, cluster)).isEqualTo(before);
        }
    }

    private static void exercise(ScalableTopicMetrics metrics) {
        metrics.sessionOpened(ScalableTopicMetrics.WatchType.TOPIC);
        metrics.sessionOpened(ScalableTopicMetrics.WatchType.NAMESPACE);
        metrics.sessionClosed(ScalableTopicMetrics.WatchType.NAMESPACE);
        metrics.recordWatchUpdate(true);
        metrics.recordWatchUpdate(false);
        metrics.startRequest(ScalableTopicMetrics.RequestType.LOOKUP).success();
        metrics.startRequest(ScalableTopicMetrics.RequestType.UNSUBSCRIBE).failure();
    }

    private static List<String> existingRenderedLines(ExistingMetrics existing, String cluster) {
        List<String> lines = render(cluster).lines().filter(line -> existing.promNames.stream()
                .anyMatch(name -> line.startsWith(name + "{") || line.startsWith(name + "_")
                        || line.startsWith("# TYPE " + name + " "))).sorted().toList();
        assertThat(lines).contains("# TYPE " + existing.counterName + " counter",
                "# TYPE " + existing.gaugeName + " gauge", "# TYPE " + existing.histogramName + " histogram");
        assertThat(lines).filteredOn(line -> !line.startsWith("#")).allSatisfy(line -> {
            assertThat(line).contains("cluster=\"" + cluster + "\"");
            assertThat(line.split("cluster=", -1)).hasSize(2);
        });
        return lines;
    }

    private static String render(String cluster) {
        ByteBuf buffer = Unpooled.buffer();
        try {
            PrometheusMetricsGeneratorUtils.generateSystemMetrics(new SimpleTextOutputStream(buffer), cluster);
            return buffer.toString(StandardCharsets.UTF_8);
        } finally {
            buffer.release();
        }
    }

    private record MetricSnapshot(String name, String type, String help, String unit, List<?> points) {}
    private record PrometheusPoint(String name, List<String> labels, List<String> values, double value) {}
    private record LongPoint(Attributes attributes, long value) {}
    private record HistogramPoint(Attributes attributes, long count, double sum, List<Double> boundaries,
                                  List<Long> counts, double min, double max) {}

    private static List<MetricSnapshot> prometheusSnapshot(CollectorRegistry registry, Set<String> names) {
        return Collections.list(registry.metricFamilySamples()).stream().filter(family -> names.contains(family.name))
                .map(family -> new MetricSnapshot(family.name, family.type.name(), family.help, "",
                        family.samples.stream()
                        .map(sample -> new PrometheusPoint(sample.name, List.copyOf(sample.labelNames),
                                List.copyOf(sample.labelValues), sample.value))
                        .sorted(Comparator.comparing(PrometheusPoint::name)
                                .thenComparing(point -> point.values().toString())).toList()))
                .sorted(Comparator.comparing(MetricSnapshot::name)).toList();
    }

    // Omit only collection timestamps: all exposed metadata, labels, values and bucket boundaries are preserved.
    private static MetricSnapshot otelSnapshot(MetricData metric) {
        String type = metric.getType().name();
        List<?> points;
        switch (metric.getType()) {
            case LONG_SUM -> {
                type += ":" + metric.getLongSumData().isMonotonic() + ":"
                        + metric.getLongSumData().getAggregationTemporality();
                points = metric.getLongSumData().getPoints().stream()
                        .map(point -> new LongPoint(point.getAttributes(), point.getValue())).toList();
            }
            case HISTOGRAM -> {
                type += ":" + metric.getHistogramData().getAggregationTemporality();
                points = metric.getHistogramData().getPoints().stream()
                        .map(point -> new HistogramPoint(point.getAttributes(), point.getCount(), point.getSum(),
                                List.copyOf(point.getBoundaries()), List.copyOf(point.getCounts()),
                                point.getMin(), point.getMax())).toList();
            }
            default -> throw new AssertionError("Unexpected existing instrument: " + metric.getType());
        }
        return new MetricSnapshot(metric.getName(), type, metric.getDescription(), metric.getUnit(), points);
    }

    private static final class ExistingMetrics implements AutoCloseable {
        private final CollectorRegistry registry;
        private final InMemoryMetricReader reader = InMemoryMetricReader.create();
        private final SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        private final Meter meter = provider.get("isolation-test");
        private final AtomicLong callbackValue = new AtomicLong();
        private final AtomicLong lookupValue = new AtomicLong(9);
        private final String counterName;
        private final String gaugeName;
        private final String histogramName;
        private final Set<String> promNames;
        private final Counter counter;
        private final Gauge gauge;
        private final Histogram histogram;
        private final OpenTelemetryMessageFinderStats finder = new OpenTelemetryMessageFinderStats(meter);
        private final ObservableLongCounter callback;
        private final ObservableLongUpDownCounter lookups;
        private final Set<String> otelNames = Set.of(OLD_CALLBACK, OLD_LOOKUPS,
                OpenTelemetryMessageFinderStats.FIND_DURATION_METRIC_NAME,
                OpenTelemetryMessageFinderStats.FIND_ENTRY_READ_COUNT_METRIC_NAME,
                OpenTelemetryMessageFinderStats.FIND_ENTRY_READ_SIZE_METRIC_NAME);

        private ExistingMetrics(CollectorRegistry registry, String suffix) {
            this.registry = registry;
            // Existing Prometheus names, help, labels and histogram buckets from SchemaRegistryStats/BrokerService.
            counterName = "pulsar_schema_get_ops_failed" + suffix;
            gaugeName = "pulsar_broker_lookup_pending_requests" + suffix;
            histogramName = "pulsar_storage_backlog_quota_check_duration_seconds" + suffix;
            promNames = Set.of(counterName, gaugeName, histogramName);
            counter = Counter.build(counterName + "_total", "-").labelNames("namespace").register(registry);
            gauge = Gauge.build(gaugeName, "-").register(registry);
            histogram = Histogram.build(histogramName, "The duration of the backlog quota check process.")
                    .buckets(5, 10, 30, 60, 300).register(registry);
            callback = meter.counterBuilder(OLD_CALLBACK)
                    .setDescription("The number of non-recoverable entries skipped.")
                    .setUnit("{entry}").buildWithCallback(measurement -> measurement.record(callbackValue.get()));
            lookups = meter.upDownCounterBuilder(OLD_LOOKUPS)
                    .setDescription("The number of pending lookup operations in the broker. "
                            + "When it reaches threshold \"maxConcurrentLookupRequest\" defined in broker.conf, "
                            + "new requests are rejected.")
                    .setUnit("{operation}").buildWithCallback(measurement -> measurement.record(lookupValue.get()));
            advance();
        }

        private void advance() {
            counter.labels("tenant/namespace").inc(2);
            gauge.set(lookupValue.addAndGet(2));
            histogram.observe(8);
            callbackValue.addAndGet(2);
            finder.recordEntryRead(FindReason.SEEK, EntryStorage.BOOKKEEPER, 1024);
            finder.recordFindCompleted(FindReason.SEEK, FindResult.FOUND, TimeUnit.MILLISECONDS.toNanos(125));
        }

        private List<MetricSnapshot> snapshot() {
            List<MetricSnapshot> result = new ArrayList<>(prometheusSnapshot(registry, promNames));
            result.addAll(reader.collectAllMetrics().stream().filter(metric -> otelNames.contains(metric.getName()))
                    .map(ScalableTopicMetricsIsolationTest::otelSnapshot).toList());
            assertThat(result).as("all three Prometheus and five existing OTel instruments remain exported").hasSize(8);
            return result.stream().sorted(Comparator.comparing(MetricSnapshot::name)).toList();
        }

        private void assertUnchanged(List<MetricSnapshot> before) {
            assertThat(snapshot()).as("existing metric metadata, labels, buckets and values").isEqualTo(before);
        }

        private void assertStillUpdates(List<MetricSnapshot> before) {
            advance();
            List<MetricSnapshot> after = snapshot();
            assertThat(after).extracting(metric -> List.of(metric.name(), metric.type(), metric.help(), metric.unit()))
                    .isEqualTo(before.stream().map(metric ->
                            List.of(metric.name(), metric.type(), metric.help(), metric.unit())).toList());
            assertThat(registry.getSampleValue(counterName + "_total",
                    new String[]{"namespace"}, new String[]{"tenant/namespace"})).isEqualTo(4);
            assertThat(registry.getSampleValue(gaugeName)).isEqualTo(13);
            assertThat(registry.getSampleValue(histogramName + "_count")).isEqualTo(2);
            assertThat(after).filteredOn(metric -> metric.name().equals(OLD_CALLBACK)).singleElement()
                    .satisfies(metric -> assertThat(metric.points())
                            .isEqualTo(List.of(new LongPoint(Attributes.empty(), 4))));
            assertThat(after).filteredOn(metric -> metric.name().equals(OLD_LOOKUPS)).singleElement()
                    .satisfies(metric -> assertThat(metric.points())
                            .isEqualTo(List.of(new LongPoint(Attributes.empty(), 13))));
            assertThat(after).filteredOn(metric ->
                    metric.name().equals(OpenTelemetryMessageFinderStats.FIND_ENTRY_READ_COUNT_METRIC_NAME))
                    .singleElement().satisfies(metric -> assertThat(((LongPoint) metric.points().getFirst()).value())
                            .isEqualTo(2));
            assertThat(after).filteredOn(metric ->
                    metric.name().equals(OpenTelemetryMessageFinderStats.FIND_ENTRY_READ_SIZE_METRIC_NAME))
                    .singleElement().satisfies(metric -> assertThat(((LongPoint) metric.points().getFirst()).value())
                            .isEqualTo(2048));
            assertThat(after).filteredOn(metric ->
                    metric.name().equals(OpenTelemetryMessageFinderStats.FIND_DURATION_METRIC_NAME)).singleElement()
                    .satisfies(metric -> assertThat(metric.points()).singleElement().satisfies(point -> {
                        HistogramPoint histogramPoint = (HistogramPoint) point;
                        assertThat(histogramPoint.count()).isEqualTo(2);
                        assertThat(histogramPoint.sum()).isEqualTo(0.25);
                    }));
        }

        @Override
        public void close() {
            callback.close();
            lookups.close();
            registry.unregister(counter);
            registry.unregister(gauge);
            registry.unregister(histogram);
            provider.close();
        }
    }
}
