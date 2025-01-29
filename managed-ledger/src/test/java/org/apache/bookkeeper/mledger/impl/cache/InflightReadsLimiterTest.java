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
package org.apache.bookkeeper.mledger.impl.cache;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.InflightReadLimiterUtilization.FREE;
import static org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.InflightReadLimiterUtilization.USED;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class InflightReadsLimiterTest {
    private static final int ACQUIRE_QUEUE_SIZE = 1000;
    private static final int ACQUIRE_TIMEOUT_MILLIS = 500;

    @DataProvider
    private static Object[][] isDisabled() {
        return new Object[][] {
            {0, true},
            {-1, true},
            {1, false},
        };
    }

    @Test(dataProvider = "isDisabled")
    public void testDisabled(long maxReadsInFlightSize, boolean shouldBeDisabled) throws Exception {
        var otel = buildOpenTelemetryAndReader();
        @Cleanup var openTelemetry = otel.getLeft();
        @Cleanup var metricReader = otel.getRight();

        var limiter = new InflightReadsLimiter(maxReadsInFlightSize, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                mock(ScheduledExecutorService.class), openTelemetry);
        assertEquals(limiter.isDisabled(), shouldBeDisabled);

        if (shouldBeDisabled) {
            // Verify metrics are not present
            var metrics = metricReader.collectAllMetrics();
            assertThat(metrics).noneSatisfy(metricData -> assertThat(metricData)
                    .hasName(InflightReadsLimiter.INFLIGHT_READS_LIMITER_LIMIT_METRIC_NAME));
            assertThat(metrics).noneSatisfy(metricData -> assertThat(metricData)
                    .hasName(InflightReadsLimiter.INFLIGHT_READS_LIMITER_USAGE_METRIC_NAME));
        }
    }

    @Test
    public void testBasicAcquireRelease() throws Exception {
        var otel = buildOpenTelemetryAndReader();
        @Cleanup var openTelemetry = otel.getLeft();
        @Cleanup var metricReader = otel.getRight();

        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        mock(ScheduledExecutorService.class), openTelemetry);
        assertEquals(limiter.getRemainingBytes(), 100);
        assertLimiterMetrics(metricReader, 100, 0, 100);

        Optional<InflightReadsLimiter.Handle> optionalHandle = limiter.acquire(100, null);
        assertEquals(limiter.getRemainingBytes(), 0);
        assertTrue(optionalHandle.isPresent());
        InflightReadsLimiter.Handle handle = optionalHandle.get();
        assertTrue(handle.success());
        assertEquals(handle.permits(), 100);
        assertLimiterMetrics(metricReader, 100, 100, 0);

        limiter.release(handle);
        assertEquals(limiter.getRemainingBytes(), 100);
        assertLimiterMetrics(metricReader, 100, 0, 100);
    }

    @Test
    public void testNotEnoughPermits() throws Exception {
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        mock(ScheduledExecutorService.class), OpenTelemetry.noop());
        assertEquals(limiter.getRemainingBytes(), 100);
        Optional<InflightReadsLimiter.Handle> optionalHandle = limiter.acquire(100, null);
        assertEquals(limiter.getRemainingBytes(), 0);
        assertTrue(optionalHandle.isPresent());
        InflightReadsLimiter.Handle handle = optionalHandle.get();
        assertTrue(handle.success());
        assertEquals(handle.permits(), 100);

        MutableObject<InflightReadsLimiter.Handle> handle2Reference = new MutableObject<>();
        Optional<InflightReadsLimiter.Handle> optionalHandle2 = limiter.acquire(100, handle2Reference::setValue);
        assertEquals(limiter.getRemainingBytes(), 0);
        assertFalse(optionalHandle2.isPresent());

        limiter.release(handle);
        assertNotNull(handle2Reference.getValue());
        assertTrue(handle2Reference.getValue().success());

        limiter.release(handle2Reference.getValue());
        assertEquals(limiter.getRemainingBytes(), 100);
    }

    @Test
    public void testAcquireTimeout() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        executor, OpenTelemetry.noop());
        assertEquals(limiter.getRemainingBytes(), 100);
        limiter.acquire(100, null);

        MutableObject<InflightReadsLimiter.Handle> handle2Reference = new MutableObject<>();
        Optional<InflightReadsLimiter.Handle> optionalHandle2 = limiter.acquire(100, handle2Reference::setValue);
        assertFalse(optionalHandle2.isPresent());

        Thread.sleep(ACQUIRE_TIMEOUT_MILLIS + 100);

        assertNotNull(handle2Reference.getValue());
        assertFalse(handle2Reference.getValue().success());
    }

    @Test
    public void testMultipleQueuedEntriesWithExceptionInFirstCallback() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        executor, OpenTelemetry.noop());
        assertEquals(limiter.getRemainingBytes(), 100);

        // Acquire the initial permits
        Optional<InflightReadsLimiter.Handle> handle1 = limiter.acquire(100, null);
        assertTrue(handle1.isPresent());
        assertEquals(limiter.getRemainingBytes(), 0);

        // Queue the first handle with a callback that throws an exception
        MutableObject<InflightReadsLimiter.Handle> handle2Reference = new MutableObject<>();
        Optional<InflightReadsLimiter.Handle> handle2 = limiter.acquire(50, handle -> {
            handle2Reference.setValue(handle);
            throw new RuntimeException("Callback exception");
        });
        assertFalse(handle2.isPresent());
        assertEquals(limiter.getRemainingBytes(), 0);

        // Queue the second handle with a successful callback
        MutableObject<InflightReadsLimiter.Handle> handle3Reference = new MutableObject<>();
        Optional<InflightReadsLimiter.Handle> handle3 = limiter.acquire(50, handle3Reference::setValue);
        assertFalse(handle3.isPresent());
        assertEquals(limiter.getRemainingBytes(), 0);

        // Release the initial handle to trigger the queued callbacks
        limiter.release(handle1.get());

        // Verify the first callback threw an exception but the second callback was handled successfully
        assertNotNull(handle2Reference.getValue());
        assertTrue(handle2Reference.getValue().success());
        assertNotNull(handle3Reference.getValue());
        assertTrue(handle3Reference.getValue().success());
        assertEquals(limiter.getRemainingBytes(), 0);

        // Release the second handle
        limiter.release(handle3Reference.getValue());
        assertEquals(limiter.getRemainingBytes(), 50);

        // Release the third handle
        limiter.release(handle3Reference.getValue());
        assertEquals(limiter.getRemainingBytes(), 100);
    }

    @Test
    public void testMultipleQueuedEntriesWithTimeoutAndExceptionInFirstCallback() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        executor, OpenTelemetry.noop());
        assertEquals(limiter.getRemainingBytes(), 100);

        // Acquire the initial permits
        Optional<InflightReadsLimiter.Handle> handle1 = limiter.acquire(100, null);
        assertTrue(handle1.isPresent());
        assertEquals(limiter.getRemainingBytes(), 0);

        // Queue the first handle with a callback that times out and throws an exception
        MutableObject<InflightReadsLimiter.Handle> handle2Reference = new MutableObject<>();
        Optional<InflightReadsLimiter.Handle> handle2 = limiter.acquire(50, handle -> {
            handle2Reference.setValue(handle);
            throw new RuntimeException("Callback exception on timeout");
        });
        assertFalse(handle2.isPresent());
        assertEquals(limiter.getRemainingBytes(), 0);

        // Introduce a delay to differentiate operations between queued entries
        Thread.sleep(50);

        // Queue the second handle with a successful callback
        MutableObject<InflightReadsLimiter.Handle> handle3Reference = new MutableObject<>();
        Optional<InflightReadsLimiter.Handle> handle3 = limiter.acquire(50, handle3Reference::setValue);
        assertFalse(handle3.isPresent());
        assertEquals(limiter.getRemainingBytes(), 0);

        // Wait for the timeout to occur
        Thread.sleep(ACQUIRE_TIMEOUT_MILLIS + 100);

        // Verify the first callback timed out and threw an exception, and the second callback was handled
        assertNotNull(handle2Reference.getValue());
        assertFalse(handle2Reference.getValue().success());
        assertNotNull(handle3Reference.getValue());
        assertFalse(handle3Reference.getValue().success());
        assertEquals(limiter.getRemainingBytes(), 0);

        // Release the first handle
        limiter.release(handle1.get());
        assertEquals(limiter.getRemainingBytes(), 100);
    }

    private Pair<OpenTelemetrySdk, InMemoryMetricReader> buildOpenTelemetryAndReader() {
        var metricReader = InMemoryMetricReader.create();
        var openTelemetry = AutoConfiguredOpenTelemetrySdk.builder()
                .disableShutdownHook()
                .addPropertiesSupplier(() -> Map.of("otel.metrics.exporter", "none",
                        "otel.traces.exporter", "none",
                        "otel.logs.exporter", "none"))
                .addMeterProviderCustomizer((builder, __) -> builder.registerMetricReader(metricReader))
                .build()
                .getOpenTelemetrySdk();
        return Pair.of(openTelemetry, metricReader);
    }

    private void assertLimiterMetrics(InMemoryMetricReader metricReader,
                                      long expectedLimit, long expectedUsed, long expectedFree) {
        var metrics = metricReader.collectAllMetrics();
        assertThat(metrics).anySatisfy(metricData -> assertThat(metricData)
                .hasName(InflightReadsLimiter.INFLIGHT_READS_LIMITER_LIMIT_METRIC_NAME)
                .hasLongSumSatisfying(longSum -> longSum.hasPointsSatisfying(point -> point.hasValue(expectedLimit))));
        assertThat(metrics).anySatisfy(metricData -> assertThat(metricData)
                .hasName(InflightReadsLimiter.INFLIGHT_READS_LIMITER_USAGE_METRIC_NAME)
                .hasLongSumSatisfying(longSum -> longSum.hasPointsSatisfying(
                        point -> point.hasValue(expectedFree).hasAttributes(FREE.attributes),
                        point -> point.hasValue(expectedUsed).hasAttributes(USED.attributes))));
    }
}
