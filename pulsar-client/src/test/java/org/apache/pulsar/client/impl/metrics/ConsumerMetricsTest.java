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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumerMetricsTest {

    private InMemoryMetricReader metricReader;
    private InstrumentProvider instrumentProvider;
    private ConsumerMetrics consumerMetrics;

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
        consumerMetrics = new ConsumerMetrics(instrumentProvider,
                "persistent://public/default/test-topic", "test-sub");
    }

    @Test
    public void testRecordConsumerOpenedAndClosed() {
        consumerMetrics.recordConsumerOpened();
        consumerMetrics.recordConsumerOpened();
        consumerMetrics.recordConsumerClosed();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        assertLongSumMetric(metrics, "pulsar.client.consumer.opened", 2);
        assertLongSumMetric(metrics, "pulsar.client.consumer.closed", 1);
    }

    @Test
    public void testRecordMessagePrefetched() {
        consumerMetrics.recordMessagePrefetched(100);
        consumerMetrics.recordMessagePrefetched(200);

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        // 2 messages in receive queue, 300 bytes total
        assertLongSumMetric(metrics, "pulsar.client.consumer.receive_queue.count", 2);
        assertLongSumMetric(metrics, "pulsar.client.consumer.receive_queue.size", 300);
    }

    @Test
    public void testRecordMessageReceived() {
        consumerMetrics.recordMessagePrefetched(512);
        consumerMetrics.recordMessagePrefetched(256);

        consumerMetrics.recordMessageReceived(512);

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        // 1 message (256 bytes) remaining in receive queue
        assertLongSumMetric(metrics, "pulsar.client.consumer.receive_queue.count", 1);
        assertLongSumMetric(metrics, "pulsar.client.consumer.receive_queue.size", 256);
        // 1 message (512 bytes) delivered to application
        assertLongSumMetric(metrics, "pulsar.client.consumer.message.received.count", 1);
        assertLongSumMetric(metrics, "pulsar.client.consumer.message.received.size", 512);
    }

    @Test
    public void testRecordAck() {
        consumerMetrics.recordAck();
        consumerMetrics.recordAck();
        consumerMetrics.recordAck();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        assertLongSumMetric(metrics, "pulsar.client.consumer.message.ack", 3);
    }

    @Test
    public void testRecordNack() {
        consumerMetrics.recordNack();
        consumerMetrics.recordNack();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        assertLongSumMetric(metrics, "pulsar.client.consumer.message.nack", 2);
    }

    @Test
    public void testRecordDlq() {
        consumerMetrics.recordDlqMessageSent();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        assertLongSumMetric(metrics, "pulsar.client.consumer.message.dlq", 1);
    }

    @Test
    public void testSubscriptionAttributeIsAttached() {
        consumerMetrics.recordAck();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        MetricData ackMetric = findMetric(metrics, "pulsar.client.consumer.message.ack");
        assertNotNull(ackMetric);
        boolean hasSubscriptionAttr = ackMetric.getLongSumData().getPoints().stream()
                .anyMatch(p -> "test-sub".equals(p.getAttributes().get(
                        io.opentelemetry.api.common.AttributeKey.stringKey("pulsar.subscription"))));
        assertEquals(hasSubscriptionAttr, true);
    }

    @Test
    public void testReceiveQueueDrainToZero() {
        consumerMetrics.recordMessagePrefetched(100);
        consumerMetrics.recordMessagePrefetched(200);
        consumerMetrics.recordMessagePrefetched(300);

        consumerMetrics.recordMessageReceived(100);
        consumerMetrics.recordMessageReceived(200);
        consumerMetrics.recordMessageReceived(300);

        Collection<MetricData> metrics = metricReader.collectAllMetrics();

        // receive queue drained to zero
        assertLongSumMetric(metrics, "pulsar.client.consumer.receive_queue.count", 0);
        assertLongSumMetric(metrics, "pulsar.client.consumer.receive_queue.size", 0);
        // 3 messages (600 bytes) delivered to application
        assertLongSumMetric(metrics, "pulsar.client.consumer.message.received.count", 3);
        assertLongSumMetric(metrics, "pulsar.client.consumer.message.received.size", 600);
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
}
