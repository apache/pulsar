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
package org.apache.pulsar.broker.stats;

import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricHistogramValue;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.PULSAR_PARTITIONED_METADATA_ERROR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.PulsarStats;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.PartitionedMetadataResponse;
import org.testng.annotations.Test;

/**
 * The topic load summary is registered per {@link BrokerOperabilityMetrics} instance on a registry that outlives
 * the broker, so closing must hand the family back for the next broker in the same JVM.
 */
public class BrokerOperabilityMetricsTest {
    private static final String TOPIC_LOAD_TIMES = "pulsar_topic_load_times";

    private static PulsarService mockPulsar() {
        return mockPulsar(OpenTelemetry.noop().getMeter("test"));
    }

    private static PulsarService mockPulsar(Meter meter) {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("test");
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getConfig()).thenReturn(config);
        when(pulsar.getAdvertisedAddress()).thenReturn("localhost");
        PulsarBrokerOpenTelemetry openTelemetry = mock(PulsarBrokerOpenTelemetry.class);
        when(openTelemetry.getMeter()).thenReturn(meter);
        when(pulsar.getOpenTelemetry()).thenReturn(openTelemetry);
        return pulsar;
    }

    private static long scrapedTopicLoadCount(PrometheusRegistry registry) {
        MetricSnapshots snapshots = registry.scrape(TOPIC_LOAD_TIMES::equals);
        assertEquals(snapshots.size(), 1, "expected exactly one " + TOPIC_LOAD_TIMES + " family");
        SummarySnapshot summary = (SummarySnapshot) snapshots.get(0);
        assertEquals(summary.getDataPoints().size(), 1);
        return summary.getDataPoints().get(0).getCount();
    }

    @Test
    public void testCloseUnregistersTopicLoadSummaryForTheNextInstance() {
        PrometheusRegistry registry = new PrometheusRegistry();
        PulsarService pulsar = mockPulsar();

        BrokerOperabilityMetrics first = new BrokerOperabilityMetrics(pulsar, registry);
        assertTrue(first.getTopicLoadStats().isRegistered());
        first.recordTopicLoadTimeValue(7);
        assertEquals(scrapedTopicLoadCount(registry), 1);

        // A second broker in the same JVM loses the registration while the first one is open.
        BrokerOperabilityMetrics overlapping = new BrokerOperabilityMetrics(pulsar, registry);
        assertFalse(overlapping.getTopicLoadStats().isRegistered());
        overlapping.recordTopicLoadTimeValue(7);
        assertEquals(scrapedTopicLoadCount(registry), 1, "the scrape must still read the registered instance");
        overlapping.close();
        assertEquals(scrapedTopicLoadCount(registry), 1, "closing a non-owner must not unregister the owner");

        first.close();
        assertFalse(first.getTopicLoadStats().isRegistered());
        assertEquals(registry.scrape(TOPIC_LOAD_TIMES::equals).size(), 0,
                "close() must unregister the topic load summary");

        BrokerOperabilityMetrics second = new BrokerOperabilityMetrics(pulsar, registry);
        assertTrue(second.getTopicLoadStats().isRegistered(), "a new instance must register after close()");
        second.recordTopicLoadTimeValue(7);
        second.recordTopicLoadTimeValue(7);
        assertEquals(scrapedTopicLoadCount(registry), 2, "the scrape must read the new instance's observations");
        second.close();
    }

    @Test
    public void testPulsarStatsCloseReleasesTopicLoadSummary() {
        PrometheusRegistry registry = new PrometheusRegistry();
        PulsarService pulsar = mockPulsar();

        PulsarStats first = new PulsarStats(pulsar, registry);
        assertTrue(first.getBrokerOperabilityMetrics().getTopicLoadStats().isRegistered());
        first.close();
        assertEquals(registry.scrape(TOPIC_LOAD_TIMES::equals).size(), 0,
                "PulsarStats.close() must close BrokerOperabilityMetrics");

        PulsarStats second = new PulsarStats(pulsar, registry);
        assertTrue(second.getBrokerOperabilityMetrics().getTopicLoadStats().isRegistered(),
                "a broker restarted in the same JVM must own the topic load summary again");
        second.getBrokerOperabilityMetrics().recordTopicLoadTimeValue(7);
        assertEquals(scrapedTopicLoadCount(registry), 1);
        second.close();
    }

    /** getMetrics() resets before it reads, so the exported topic load mean comes from an empty summary. */
    @Test
    public void testTopicLoadMeanIsNaNWhenNothingWasRecordedSinceTheLastReset() {
        BrokerOperabilityMetrics metrics = new BrokerOperabilityMetrics(mockPulsar(), new PrometheusRegistry());
        try {
            // two values, so the mean is not also one of the quantiles
            metrics.recordTopicLoadTimeValue(1);
            metrics.recordTopicLoadTimeValue(5);
            Map<String, Object> recorded = metrics.getTopicLoadMetrics().getMetrics();
            assertEquals(recorded.get("brk_topic_load_time_mean_ms"), 3.0);
            assertEquals(recorded.get("brk_topic_load_rate_s"), 2.0);

            Map<String, Object> afterReset = metrics.getMetrics().get(0).getMetrics();
            assertEquals(afterReset.get("brk_topic_load_rate_s"), 0.0);
            assertEquals(afterReset.get("brk_topic_load_time_mean_ms"), Double.NaN);
            assertEquals(afterReset.get("brk_topic_load_time_median_ms"), Double.NaN);
        } finally {
            metrics.close();
        }
    }

    @Test
    public void testPartitionedMetadataRequestMetrics() {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
                .build();
        BrokerOperabilityMetrics metrics =
                new BrokerOperabilityMetrics(mockPulsar(openTelemetry.getMeter("test")), new PrometheusRegistry());
        try {
            metrics.recordPartitionedMetadataRequestDuration(TimeUnit.MILLISECONDS.toNanos(25));
            metrics.recordPartitionedMetadataRequestFailure(TimeUnit.MILLISECONDS.toNanos(50),
                    ServerError.TopicNotFound);
            metrics.recordPartitionedMetadataRequestFailure(TimeUnit.MILLISECONDS.toNanos(75),
                    ServerError.TooManyRequests);
            metrics.recordPartitionedMetadataRequestFailure(TimeUnit.MILLISECONDS.toNanos(100),
                    ServerError.TooManyRequests);

            // Every request is timed in seconds, with its outcome as the attribute.
            String duration = BrokerOperabilityMetrics.PARTITIONED_METADATA_REQUEST_DURATION_METRIC_NAME;
            assertMetricHistogramValue(reader.collectAllMetrics(), duration,
                    PartitionedMetadataResponse.SUCCESS.attributes,
                    count -> assertEquals(count.longValue(), 1L),
                    sum -> assertEquals(sum.doubleValue(), 0.025, 1e-9));
            assertMetricHistogramValue(reader.collectAllMetrics(), duration,
                    PartitionedMetadataResponse.FAILURE.attributes,
                    count -> assertEquals(count.longValue(), 3L),
                    sum -> assertEquals(sum.doubleValue(), 0.225, 1e-9));

            // The buckets are for seconds, not the SDK's defaults for milliseconds: 25 ms lands in (20 ms, 50 ms].
            List<Double> buckets = List.of(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0, 30.0);
            assertEquals(histogramPoint(reader.collectAllMetrics(), duration,
                    PartitionedMetadataResponse.SUCCESS.attributes).getBoundaries(), buckets);
            assertEquals(histogramPoint(reader.collectAllMetrics(), duration,
                    PartitionedMetadataResponse.SUCCESS.attributes).getCounts(),
                    List.of(0L, 0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
            assertEquals(histogramPoint(reader.collectAllMetrics(), duration,
                    PartitionedMetadataResponse.FAILURE.attributes).getCounts(),
                    List.of(0L, 0L, 0L, 0L, 1L, 2L, 0L, 0L, 0L, 0L, 0L, 0L));

            // Every failed request is counted with the error that was returned to the client.
            String failures = BrokerOperabilityMetrics.PARTITIONED_METADATA_REQUEST_FAILURE_COUNTER_METRIC_NAME;
            assertMetricLongSumValue(reader.collectAllMetrics(), failures,
                    Attributes.of(PULSAR_PARTITIONED_METADATA_ERROR, ServerError.TopicNotFound.name()), 1);
            assertMetricLongSumValue(reader.collectAllMetrics(), failures,
                    Attributes.of(PULSAR_PARTITIONED_METADATA_ERROR, ServerError.TooManyRequests.name()), 2);
        } finally {
            metrics.close();
            openTelemetry.close();
        }
    }

    private static HistogramPointData histogramPoint(Collection<MetricData> metrics, String metricName,
                                                     Attributes attributes) {
        return metrics.stream().filter(metric -> metric.getName().equals(metricName))
                .flatMap(metric -> metric.getHistogramData().getPoints().stream())
                .filter(point -> point.getAttributes().equals(attributes))
                .findFirst().orElseThrow();
    }
}
