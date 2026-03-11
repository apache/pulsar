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
package org.apache.pulsar.client.impl.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProducerMetricsTest {

    private InMemoryMetricReader metricReader;
    private InstrumentProvider instrumentProvider;
    private ProducerMetrics producerMetrics;

    @BeforeMethod
    public void setup() {
        metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(metricReader)
                .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .build();
        instrumentProvider = new InstrumentProvider(openTelemetry);
        producerMetrics = new ProducerMetrics(instrumentProvider, "persistent://public/default/test-topic");
    }

    @Test
    public void testRecordProducerOpenedAndClosed() {
        producerMetrics.recordProducerOpened();
        producerMetrics.recordProducerOpened();
        producerMetrics.recordProducerClosed();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        assertLongSumMetric(metrics, "pulsar.client.producer.opened", 2);
        assertLongSumMetric(metrics, "pulsar.client.producer.closed", 1);
    }

    @Test
    public void testRecordPendingMessage() {
        producerMetrics.recordPendingMessage(100);
        producerMetrics.recordPendingMessage(200);

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        // 2 messages pending, 300 bytes pending
        assertLongSumMetric(metrics, "pulsar.client.producer.message.pending.count", 2);
        assertLongSumMetric(metrics, "pulsar.client.producer.message.pending.size", 300);
    }

    @Test
    public void testRecordSendSuccess() {
        producerMetrics.recordPendingMessage(512);
        producerMetrics.recordPendingMessage(256);

        long latencyNanos = TimeUnit.MILLISECONDS.toNanos(5);
        producerMetrics.recordSendSuccess(latencyNanos, 512);

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        assertLongSumMetric(metrics, "pulsar.client.producer.message.pending.count", 1);
        assertLongSumMetric(metrics, "pulsar.client.producer.message.pending.size", 256);
        assertLongSumMetric(metrics, "pulsar.client.producer.message.send.size", 512);
        assertHistogramCount(metrics, "pulsar.client.producer.message.send.duration", 1);
    }

    @Test
    public void testRecordSendFailed() {
        producerMetrics.recordPendingMessage(128);

        long latencyNanos = TimeUnit.MILLISECONDS.toNanos(10);
        producerMetrics.recordSendFailed(latencyNanos, 128);

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        assertLongSumMetric(metrics, "pulsar.client.producer.message.pending.count", 0);
        assertLongSumMetric(metrics, "pulsar.client.producer.message.pending.size", 0);
        assertNull(findMetric(metrics, "pulsar.client.producer.message.send.size"),
                "send.size should not be recorded on failure");
        assertHistogramCount(metrics, "pulsar.client.producer.message.send.duration", 1);
    }

    @Test
    public void testRpcLatencyHistogramIsAccessible() {
        assertNotNull(producerMetrics.getRpcLatencyHistogram());

        producerMetrics.getRpcLatencyHistogram().recordSuccess(TimeUnit.MILLISECONDS.toNanos(3));
        producerMetrics.getRpcLatencyHistogram().recordFailure(TimeUnit.MILLISECONDS.toNanos(1));

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        assertHistogramCount(metrics, "pulsar.client.producer.rpc.send.duration", 2);
    }

    private MetricData findMetric(Collection<MetricData> metrics, String name) {
        return metrics.stream()
                .filter(m -> m.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    private void assertLongSumMetric(Collection<MetricData> metrics, String name, long expectedValue) {
        MetricData metric = findMetric(metrics, name);
        assertNotNull(metric, "Metric should exist: " + name);
        long actual = metric.getLongSumData().getPoints().stream()
                .mapToLong(LongPointData::getValue).sum();
        assertEquals(actual, expectedValue, "Unexpected metric value: " + name);
    }

    private void assertHistogramCount(Collection<MetricData> metrics, String name, long expectedCount) {
        MetricData metric = findMetric(metrics, name);
        assertNotNull(metric, "Histogram metric should exist: " + name);
        long actual = metric.getHistogramData().getPoints().stream()
                .mapToLong(HistogramPointData::getCount).sum();
        assertEquals(actual, expectedCount, "Unexpected histogram count: " + name);
    }
}
