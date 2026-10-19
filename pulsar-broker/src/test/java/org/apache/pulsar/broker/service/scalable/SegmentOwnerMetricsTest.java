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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.service.scalable.SegmentOwnerMetrics.EntryBucketViolation;
import org.apache.pulsar.broker.service.scalable.SegmentOwnerMetrics.Gauges;
import org.apache.pulsar.broker.service.scalable.SegmentOwnerMetrics.LoadReportResult;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SegmentOwnerMetricsTest {
    @Test
    public void testBothPathsReadTheSameState() throws Exception {
        CollectorRegistry registry = new CollectorRegistry();
        var state = new AtomicReference<>(new Gauges(2, 3, 5));
        try (var reader = InMemoryMetricReader.create();
             var provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
             var metrics = new SegmentOwnerMetrics(provider.get("test"), registry, "test-cluster", state::get)) {
            metrics.recordEntryBucketViolation(EntryBucketViolation.STRADDLING);
            metrics.recordEntryBucketViolation(EntryBucketViolation.UNSTAMPED_BATCH);
            metrics.recordEntryBucketViolation(EntryBucketViolation.UNSTAMPED_BATCH);
            metrics.recordLoadReport(LoadReportResult.WRITTEN);
            metrics.recordLoadReport(LoadReportResult.UNCHANGED);
            metrics.recordLoadReport(LoadReportResult.FAILED);

            assertGauges(registry, reader, new Gauges(2, 3, 5));
            var exported = reader.collectAllMetrics();
            assertThat(exported).hasSize(5).allSatisfy(metric -> {
                assertThat(metric.getLongSumData().getPoints()).allSatisfy(point ->
                        assertThat(point.getAttributes().asMap().keySet()).allSatisfy(key ->
                                assertThat(key.getKey()).isIn("pulsar.scalable_topic.entry_bucket.violation",
                                        "pulsar.scalable_topic.load_report.result")));
            });
            assertThat(exported).allSatisfy(metric -> assertThat(metric.getLongSumData().isMonotonic())
                    .isEqualTo(metric.getName().endsWith("violation.count")
                            || metric.getName().endsWith("load_report.count")));
            for (String value : new String[]{"straddling", "unstamped_batch"}) {
                long expected = value.equals("straddling") ? 1 : 2;
                assertMetricLongSumValue(exported, SegmentOwnerMetrics.OTEL_PREFIX + "entry_bucket.violation.count",
                        Attributes.builder().put("pulsar.scalable_topic.entry_bucket.violation", value).build(),
                        expected);
                assertThat(registry.getSampleValue("pulsar_scalable_topic_entry_bucket_violation_total",
                        new String[]{"cluster", "entry_bucket_violation"},
                        new String[]{"test-cluster", value})).isEqualTo((double) expected);
            }
            for (String value : new String[]{"written", "unchanged", "failed"}) {
                assertMetricLongSumValue(exported, SegmentOwnerMetrics.OTEL_PREFIX + "segment.load_report.count",
                        Attributes.builder().put("pulsar.scalable_topic.load_report.result", value).build(), 1);
                assertThat(registry.getSampleValue("pulsar_scalable_topic_segment_load_report_total",
                        new String[]{"cluster", "load_report_result"},
                        new String[]{"test-cluster", value})).isEqualTo(1.0);
            }
            state.set(new Gauges(0, 0, 0));
            assertGauges(registry, reader, state.get());
        }
        assertThat(registry.metricFamilySamples().hasMoreElements()).isFalse();
    }

    private static void assertGauges(CollectorRegistry registry, InMemoryMetricReader reader, Gauges expected) {
        String[] names = {"leaderless.count", "segment.unwritable.count", "entry_bucket.draining.count"};
        long[] values = {expected.leaderless(), expected.unwritable(), expected.drainingBuckets()};
        var exported = reader.collectAllMetrics();
        for (int i = 0; i < names.length; i++) {
            assertMetricLongSumValue(exported, SegmentOwnerMetrics.OTEL_PREFIX + names[i],
                    Attributes.empty(), values[i]);
            assertThat(registry.getSampleValue(SegmentOwnerMetrics.PROMETHEUS_PREFIX + names[i].replace('.', '_'),
                    new String[]{"cluster"}, new String[]{"test-cluster"})).isEqualTo((double) values[i]);
        }
    }

    @Test
    public void testExistingMetricsSurviveSegmentOwnerCollectorLifecycle() throws Exception {
        String legacyName = "segment_owner_compatibility_" + UUID.randomUUID().toString().replace("-", "");
        Counter legacy = Counter.build(legacyName, "Existing counter").labelNames("subscription").register();
        try (var reader = InMemoryMetricReader.create();
             var provider = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            var legacyChild = legacy.labels("existing-subscription");
            legacyChild.inc(7);
            var meter = provider.get("compatibility-test");
            var legacyOtel = meter.counterBuilder("test.existing.counter").build();
            legacyOtel.add(7);
            List<String> before = legacyOutput(legacyName);
            assertThat(before).contains("# TYPE " + legacyName + " counter");
            var attributes = Attributes.empty();
            assertMetricLongSumValue(reader.collectAllMetrics(), "test.existing.counter", attributes, 7);

            try (var metrics = new SegmentOwnerMetrics(meter, "compatibility-cluster", () -> new Gauges(2, 3, 4))) {
                metrics.recordEntryBucketViolation(EntryBucketViolation.STRADDLING);
                metrics.recordLoadReport(LoadReportResult.WRITTEN);
                for (int scrape = 0; scrape < 3; scrape++) {
                    assertThat(legacyOutput(legacyName)).as("existing Prometheus output during scrape %s", scrape)
                            .containsExactlyElementsOf(before);
                    assertThat(legacyChild.get()).as("existing Prometheus state").isEqualTo(7);
                    assertMetricLongSumValue(reader.collectAllMetrics(), "test.existing.counter", attributes, 7);
                }
            }
            assertThat(legacyOutput(legacyName)).as("existing output after new collector closes")
                    .containsExactlyElementsOf(before);
            assertMetricLongSumValue(reader.collectAllMetrics(), "test.existing.counter", attributes, 7);
            // Closing only the new collector must not prevent the original instruments from updating.
            legacyChild.inc(2);
            legacyOtel.add(2);
            assertThat(legacyChild.get()).isEqualTo(9);
            assertThat(legacyOutput(legacyName)).contains(legacyName
                    + "_total{cluster=\"compatibility-cluster\",subscription=\"existing-subscription\"} 9.0");
            assertMetricLongSumValue(reader.collectAllMetrics(), "test.existing.counter", attributes, 9);
        } finally {
            CollectorRegistry.defaultRegistry.unregister(legacy);
        }
    }

    private static List<String> legacyOutput(String legacyName) throws Exception {
        var out = new ByteArrayOutputStream();
        PrometheusMetricsGeneratorUtils.generate("compatibility-cluster", out, List.of());
        return out.toString(UTF_8).lines()
                .filter(line -> line.startsWith(legacyName) || line.startsWith("# TYPE " + legacyName))
                .sorted().toList();
    }

    @Test
    public void testDisabledTelemetryAndSharedRegistryLifecycle() {
        CollectorRegistry registry = new CollectorRegistry();
        var meter = OpenTelemetry.noop().getMeter("test");
        try (var first = new SegmentOwnerMetrics(meter, registry, "cluster", () -> new Gauges(1, 2, 3))) {
            try (var second = new SegmentOwnerMetrics(meter, registry, "cluster", () -> new Gauges(4, 5, 6))) {
                first.recordLoadReport(LoadReportResult.WRITTEN);
                second.recordLoadReport(LoadReportResult.WRITTEN);
                assertThat(registry.getSampleValue("pulsar_scalable_topic_leaderless_count",
                        new String[]{"cluster"}, new String[]{"cluster"})).isEqualTo(5.0);
                assertThat(registry.getSampleValue("pulsar_scalable_topic_segment_load_report_total",
                        new String[]{"cluster", "load_report_result"},
                        new String[]{"cluster", "written"})).isEqualTo(2.0);
            }
            assertThat(registry.getSampleValue("pulsar_scalable_topic_leaderless_count",
                    new String[]{"cluster"}, new String[]{"cluster"})).isEqualTo(1.0);
        }
        assertThat(registry.metricFamilySamples().hasMoreElements()).isFalse();
        try (var restarted = new SegmentOwnerMetrics(meter, registry, "cluster", () -> new Gauges(0, 0, 0))) {
            assertThat(registry.getSampleValue("pulsar_scalable_topic_segment_load_report_total",
                    new String[]{"cluster", "load_report_result"},
                    new String[]{"cluster", "written"})).isZero();
            restarted.close();
        }
    }
}
