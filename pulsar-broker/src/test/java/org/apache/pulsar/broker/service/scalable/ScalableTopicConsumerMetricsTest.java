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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.client.CollectorRegistry;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator.ConsumerStats;
import org.apache.pulsar.broker.service.scalable.SubscriptionCoordinator.RebalanceReason;
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
