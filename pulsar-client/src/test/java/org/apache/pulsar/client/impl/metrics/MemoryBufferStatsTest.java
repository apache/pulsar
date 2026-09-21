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
import static org.testng.Assert.assertTrue;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.testng.annotations.Test;

public class MemoryBufferStatsTest {

    @Test
    public void testMemoryBufferStatsWithMemoryUsage() {
        long memoryLimit = 1024 * 1024; // 1MB
        MemoryLimitController memoryLimitController = new MemoryLimitController(memoryLimit);

        InMemoryMetricReader metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(metricReader)
                .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .build();

        InstrumentProvider instrumentProvider = new InstrumentProvider(openTelemetry);

        try (MemoryBufferStats memoryBufferStats = new MemoryBufferStats(instrumentProvider, memoryLimitController)) {
            assertNotNull(memoryBufferStats);

            // Test initial state - no memory used
            Collection<MetricData> metrics = metricReader.collectAllMetrics();
            assertUsageMetric(metrics, 0);
            assertLimitMetric(metrics, memoryLimit);

            // Reserve some memory
            long reservedMemory = 512 * 1024; // 512KB
            memoryLimitController.forceReserveMemory(reservedMemory);

            // Collect metrics and verify
            metrics = metricReader.collectAllMetrics();
            assertUsageMetric(metrics, reservedMemory);
            assertLimitMetric(metrics, memoryLimit);

            // Reserve more memory
            long additionalMemory = 256 * 1024; // 256KB
            memoryLimitController.forceReserveMemory(additionalMemory);

            // Verify total usage
            metrics = metricReader.collectAllMetrics();
            assertUsageMetric(metrics, reservedMemory + additionalMemory);
            assertLimitMetric(metrics, memoryLimit);

            // Release some memory
            memoryLimitController.releaseMemory(additionalMemory);

            // Verify usage decreased
            metrics = metricReader.collectAllMetrics();
            assertUsageMetric(metrics, reservedMemory);
            assertLimitMetric(metrics, memoryLimit);

            // Release all memory
            memoryLimitController.releaseMemory(reservedMemory);

            // Verify back to zero
            metrics = metricReader.collectAllMetrics();
            assertUsageMetric(metrics, 0);
            assertLimitMetric(metrics, memoryLimit);
        }
    }

    @Test
    public void testMemoryBufferStatsWithNoMemoryLimit() {
        MemoryLimitController memoryLimitController = new MemoryLimitController(0); // No limit

        InMemoryMetricReader metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(metricReader)
                .build();
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .build();

        InstrumentProvider instrumentProvider = new InstrumentProvider(openTelemetry);

        // When memory limiting is disabled, MemoryBufferStats should not be created at all
        // This test verifies that the callback correctly checks for memory limiting
        try (MemoryBufferStats memoryBufferStats = new MemoryBufferStats(instrumentProvider, memoryLimitController)) {
            assertNotNull(memoryBufferStats);

            // When memory limiting is disabled, no metrics should be recorded
            Collection<MetricData> metrics = metricReader.collectAllMetrics();
            assertTrue(metrics.isEmpty() || metrics.stream().noneMatch(metric ->
                    metric.getName().equals(MemoryBufferStats.BUFFER_USAGE_COUNTER)
                    || metric.getName().equals(MemoryBufferStats.BUFFER_LIMIT_COUNTER)));
        }
    }

    private void assertUsageMetric(Collection<MetricData> metrics, long expectedValue) {
        MetricData usageMetric = metrics.stream()
                .filter(metric -> metric.getName().equals(MemoryBufferStats.BUFFER_USAGE_COUNTER))
                .findFirst()
                .orElse(null);

        assertNotNull(usageMetric, "Usage metric should be present");

        Collection<LongPointData> points = usageMetric.getLongSumData().getPoints();
        assertEquals(points.size(), 1, "Should have exactly one data point");

        LongPointData point = points.iterator().next();
        assertEquals(point.getValue(), expectedValue, "Usage metric value should match expected");
    }

    private void assertLimitMetric(Collection<MetricData> metrics, long expectedValue) {
        MetricData limitMetric = metrics.stream()
                .filter(metric -> metric.getName().equals(MemoryBufferStats.BUFFER_LIMIT_COUNTER))
                .findFirst()
                .orElse(null);

        assertNotNull(limitMetric, "Limit metric should be present");

        Collection<LongPointData> points = limitMetric.getLongSumData().getPoints();
        assertEquals(points.size(), 1, "Should have exactly one data point");

        LongPointData point = points.iterator().next();
        assertEquals(point.getValue(), expectedValue, "Limit metric value should match expected");
    }
}