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
        assertEquals(100, limiter.getRemainingBytes());
        assertLimiterMetrics(metricReader, 100, 0, 100);

        Optional<InflightReadsLimiter.Handle> optionalHandle = limiter.acquire(100, null);
        assertEquals(0, limiter.getRemainingBytes());
        assertTrue(optionalHandle.isPresent());
        InflightReadsLimiter.Handle handle = optionalHandle.get();
        assertTrue(handle.success);
        assertEquals(handle.permits, 100);
        assertLimiterMetrics(metricReader, 100, 100, 0);

        limiter.release(handle);
        assertEquals(100, limiter.getRemainingBytes());
        assertLimiterMetrics(metricReader, 100, 0, 100);
    }


    @Test
    public void testNotEnoughPermits() throws Exception {
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        mock(ScheduledExecutorService.class), OpenTelemetry.noop());
        assertEquals(100, limiter.getRemainingBytes());
        Optional<InflightReadsLimiter.Handle> optionalHandle = limiter.acquire(100, null);
        assertEquals(0, limiter.getRemainingBytes());
        assertTrue(optionalHandle.isPresent());
        InflightReadsLimiter.Handle handle = optionalHandle.get();
        assertTrue(handle.success);
        assertEquals(handle.permits, 100);

        MutableObject<InflightReadsLimiter.Handle> handle2Reference = new MutableObject<>();
        Optional<InflightReadsLimiter.Handle> optionalHandle2 = limiter.acquire(100, handle2Reference::setValue);
        assertEquals(0, limiter.getRemainingBytes());
        assertFalse(optionalHandle2.isPresent());

        limiter.release(handle);
        assertNotNull(handle2Reference.getValue());
        assertTrue(handle2Reference.getValue().success);

        limiter.release(handle2Reference.getValue());
        assertEquals(100, limiter.getRemainingBytes());
    }

    @Test
    public void testAcquireTimeout() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        executor, OpenTelemetry.noop());
        assertEquals(100, limiter.getRemainingBytes());
        limiter.acquire(100, null);

        MutableObject<InflightReadsLimiter.Handle> handle2Reference = new MutableObject<>();
        Optional<InflightReadsLimiter.Handle> optionalHandle2 = limiter.acquire(100, handle2Reference::setValue);
        assertFalse(optionalHandle2.isPresent());

        Thread.sleep(ACQUIRE_TIMEOUT_MILLIS + 100);

        assertNotNull(handle2Reference.getValue());
        assertFalse(handle2Reference.getValue().success);
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
