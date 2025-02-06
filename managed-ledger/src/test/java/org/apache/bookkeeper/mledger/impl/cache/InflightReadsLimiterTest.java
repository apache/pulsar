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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class InflightReadsLimiterTest {
    private static final int ACQUIRE_QUEUE_SIZE = 1000;
    private static final int ACQUIRE_TIMEOUT_MILLIS = 500;

    @DataProvider
    private static Object[][] isDisabled() {
        return new Object[][]{
                {0, true},
                {-1, true},
                {1, false},
        };
    }

    @DataProvider
    private static Object[] booleanValues() {
        return new Object[]{ true, false };
    }

    @Test(dataProvider = "isDisabled")
    public void testDisabled(long maxReadsInFlightSize, boolean shouldBeDisabled) throws Exception {
        var otel = buildOpenTelemetryAndReader();
        @Cleanup var openTelemetry = otel.getLeft();
        @Cleanup var metricReader = otel.getRight();

        var limiter = new InflightReadsLimiter(maxReadsInFlightSize, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                mock(ScheduledExecutorService.class), openTelemetry);
        assertThat(limiter.isDisabled()).isEqualTo(shouldBeDisabled);

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
        assertThat(limiter.getRemainingBytes()).isEqualTo(100);
        assertLimiterMetrics(metricReader, 100, 0, 100);

        Optional<InflightReadsLimiter.Handle> optionalHandle = limiter.acquire(100, null);
        assertThat(limiter.getRemainingBytes()).isZero();
        assertThat(optionalHandle).isPresent();
        InflightReadsLimiter.Handle handle = optionalHandle.get();
        assertThat(handle.success()).isTrue();
        assertThat(handle.permits()).isEqualTo(100);
        assertLimiterMetrics(metricReader, 100, 100, 0);

        limiter.release(handle);
        assertThat(limiter.getRemainingBytes()).isEqualTo(100);
        assertLimiterMetrics(metricReader, 100, 0, 100);
    }

    @Test
    public void testNotEnoughPermits() throws Exception {
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        mock(ScheduledExecutorService.class), OpenTelemetry.noop());
        assertThat(limiter.getRemainingBytes()).isEqualTo(100);
        Optional<InflightReadsLimiter.Handle> optionalHandle = limiter.acquire(100, null);
        assertThat(limiter.getRemainingBytes()).isZero();
        assertThat(optionalHandle).isPresent();
        InflightReadsLimiter.Handle handle = optionalHandle.get();
        assertThat(handle.success()).isTrue();
        assertThat(handle.permits()).isEqualTo(100);

        AtomicReference<InflightReadsLimiter.Handle> handle2Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> optionalHandle2 = limiter.acquire(100, handle2Reference::set);
        assertThat(limiter.getRemainingBytes()).isZero();
        assertThat(optionalHandle2).isNotPresent();

        limiter.release(handle);
        assertThat(handle2Reference)
                .hasValueSatisfying(h ->
                        assertThat(h.success()).isTrue());

        limiter.release(handle2Reference.get());
        assertThat(limiter.getRemainingBytes()).isEqualTo(100);
    }

    @Test
    public void testAcquireTimeout() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        executor, OpenTelemetry.noop());
        assertThat(limiter.getRemainingBytes()).isEqualTo(100);
        limiter.acquire(100, null);

        AtomicReference<InflightReadsLimiter.Handle> handle2Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> optionalHandle2 = limiter.acquire(100, handle2Reference::set);
        assertThat(optionalHandle2).isNotPresent();

        Thread.sleep(ACQUIRE_TIMEOUT_MILLIS + 100);

        assertThat(handle2Reference).hasValueSatisfying(h -> assertThat(h.success()).isFalse());
    }

    @Test
    public void testMultipleQueuedEntriesWithExceptionInFirstCallback() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        executor, OpenTelemetry.noop());
        assertThat(limiter.getRemainingBytes())
                .as("Initial remaining bytes should be 100")
                .isEqualTo(100);

        // Acquire the initial permits
        Optional<InflightReadsLimiter.Handle> handle1 = limiter.acquire(100, null);
        assertThat(handle1)
                .as("Initial handle should be present")
                .isPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be 0 after acquiring 100 permits")
                .isEqualTo(0);

        // Queue the first handle with a callback that throws an exception
        AtomicReference<InflightReadsLimiter.Handle> handle2Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> handle2 = limiter.acquire(50, handle -> {
            handle2Reference.set(handle);
            throw new RuntimeException("Callback exception");
        });
        assertThat(handle2)
                .as("Second handle should not be present")
                .isNotPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0 after failed acquisition")
                .isEqualTo(0);

        // Queue the second handle with a successful callback
        AtomicReference<InflightReadsLimiter.Handle> handle3Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> handle3 = limiter.acquire(50, handle3Reference::set);
        assertThat(handle3)
                .as("Third handle should not be present as queue is full")
                .isNotPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0")
                .isEqualTo(0);

        // Release the initial handle to trigger the queued callbacks
        limiter.release(handle1.get());

        // Verify the first callback threw an exception but the second callback was handled successfully
        assertThat(handle2Reference)
                .as("Handle2 should have been set in the callback despite the exception")
                .hasValueSatisfying(handle -> assertThat(handle.success())
                        .as("Handle2 should be marked as successful")
                        .isTrue());
        assertThat(handle3Reference)
                .as("Handle3 should have been set successfully")
                .hasValueSatisfying(handle -> assertThat(handle.success())
                        .as("Handle3 should be marked as successful")
                        .isTrue());
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0 after first releases are acquired")
                .isEqualTo(0);

        // Release the second handle
        limiter.release(handle3Reference.get());
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be 50 after releasing handle3")
                .isEqualTo(50);

        // Release the third handle
        limiter.release(handle3Reference.get());
        assertThat(limiter.getRemainingBytes())
                .as("All bytes should be released, so remaining bytes should be 100")
                .isEqualTo(100);
    }

    @Test
    public void testMultipleQueuedEntriesWithTimeoutAndExceptionInFirstCallback() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        executor, OpenTelemetry.noop());
        assertThat(limiter.getRemainingBytes())
                .as("Initial remaining bytes should be 100")
                .isEqualTo(100);

        // Acquire the initial permits
        Optional<InflightReadsLimiter.Handle> handle1 = limiter.acquire(100, null);
        assertThat(handle1)
                .as("The first handle should be present after acquiring 100 permits")
                .isPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be 0 after acquiring all permits")
                .isEqualTo(0);

        // Queue the first handle with a callback that times out and throws an exception
        AtomicReference<InflightReadsLimiter.Handle> handle2Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> handle2 = limiter.acquire(50, handle -> {
            handle2Reference.set(handle);
            throw new RuntimeException("Callback exception on timeout");
        });
        assertThat(handle2)
                .as("The second handle should not be present as the callback throws an exception")
                .isNotPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0 after failed acquisition")
                .isEqualTo(0);

        // Introduce a delay to differentiate operations between queued entries
        Thread.sleep(50);

        // Queue the second handle with a successful callback
        AtomicReference<InflightReadsLimiter.Handle> handle3Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> handle3 = limiter.acquire(50, handle3Reference::set);
        assertThat(handle3)
                .as("The third handle should not be present as permits are still unavailable")
                .isNotPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0 after failed acquisition attempt")
                .isEqualTo(0);

        // Wait for the timeout to occur
        Thread.sleep(ACQUIRE_TIMEOUT_MILLIS + 100);

        // Verify the first callback timed out and threw an exception, and the second callback was handled
        assertThat(handle2Reference)
                .as("Handle2 should have been set in the callback despite the exception")
                .hasValueSatisfying(handle -> assertThat(handle.success())
                        .as("Handle2 should be marked as unsuccessful due to a timeout")
                        .isFalse());
        assertThat(handle3Reference)
                .as("Handle3 should have been set in the callback after the permits became available")
                .hasValueSatisfying(handle -> Assertions.assertThat(handle.success())
                        .as("Handle3 should be marked as unsuccessful due to a timeout")
                        .isFalse());
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0 as no permits were released")
                .isEqualTo(0);

        // Release the first handle
        limiter.release(handle1.get());
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be fully restored to 100 after releasing all permits")
                .isEqualTo(100);
    }

    @Test
    public void testMultipleQueuedEntriesWithTimeoutsThatAreTimedOutWhenPermitsAreAvailable() throws Exception {
        // Use a mock executor to simulate scenarios where timed out queued handles are processed when permits become
        // available
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS,
                        executor, OpenTelemetry.noop());
        assertThat(limiter.getRemainingBytes())
                .as("Initial remaining bytes should be 100")
                .isEqualTo(100);

        // Acquire the initial permits
        Optional<InflightReadsLimiter.Handle> handle1 = limiter.acquire(100, null);
        assertThat(handle1)
                .as("The first handle should be present after acquiring 100 permits")
                .isPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be 0 after acquiring all permits")
                .isEqualTo(0);

        // Queue the first handle
        AtomicReference<InflightReadsLimiter.Handle> handle2Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> handle2 = limiter.acquire(50, handle2Reference::set);
        assertThat(handle2)
                .as("The second handle should not be present as permits are unavailable")
                .isNotPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0 after failed acquisition attempt for handle2")
                .isEqualTo(0);

        // Queue the second handle
        AtomicReference<InflightReadsLimiter.Handle> handle3Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> handle3 = limiter.acquire(50, handle3Reference::set);
        assertThat(handle3)
                .as("The third handle should not be present as permits are unavailable")
                .isNotPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0 after failed acquisition attempt for handle3")
                .isEqualTo(0);

        // Wait for the timeout to occur
        Thread.sleep(ACQUIRE_TIMEOUT_MILLIS + 100);

        // Queue another handle
        AtomicReference<InflightReadsLimiter.Handle> handle4Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> handle4 = limiter.acquire(50, handle4Reference::set);
        assertThat(handle4)
                .as("The fourth handle should not be present because permits are unavailable")
                .isNotPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0 after failed acquisition attempt for handle4")
                .isEqualTo(0);

        // Queue another handle
        AtomicReference<InflightReadsLimiter.Handle> handle5Reference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> handle5 = limiter.acquire(100, handle5Reference::set);
        assertThat(handle5)
                .as("The fifth handle should not be present as permits are unavailable")
                .isNotPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should still be 0 after failed acquisition attempt for handle5")
                .isEqualTo(0);

        // Release the first handle
        limiter.release(handle1.get());

        assertThat(handle2Reference)
                .as("Handle2 should have been set in the callback and marked unsuccessful")
                .hasValueSatisfying(handle -> assertThat(handle.success()).isFalse());

        assertThat(handle3Reference)
                .as("Handle3 should have been set in the callback and marked unsuccessful")
                .hasValueSatisfying(handle -> assertThat(handle.success()).isFalse());

        assertThat(handle4Reference)
                .as("Handle4 should have been set in the callback and marked successful")
                .hasValueSatisfying(handle -> assertThat(handle.success()).isTrue());

        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be 50 after releasing handle4")
                .isEqualTo(50);

        limiter.release(handle4Reference.get());

        assertThat(handle5Reference)
                .as("Handle5 should have been set in the callback and marked successful")
                .hasValueSatisfying(handle -> assertThat(handle.success()).isTrue());

        limiter.release(handle5Reference.get());

        assertThat(limiter.getRemainingBytes())
                .as("All bytes should be released, so remaining bytes should be back to 100")
                .isEqualTo(100);
    }

    @Test
    public void testQueueSizeLimitReached() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        // Minimum queue size is 4.
        final int queueSizeLimit = 4;
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(100, queueSizeLimit, ACQUIRE_TIMEOUT_MILLIS, executor, OpenTelemetry.noop());

        assertThat(limiter.getRemainingBytes())
                .as("Initial remaining bytes should be 100")
                .isEqualTo(100);

        // Acquire all available permits (consume 100 bytes)
        Optional<InflightReadsLimiter.Handle> handle1 = limiter.acquire(100, null);
        assertThat(handle1)
                .as("The first handle should be present after acquiring all available permits")
                .isPresent()
                .hasValueSatisfying(handle -> assertThat(handle.success()).isTrue());
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be zero after acquiring all permits")
                .isEqualTo(0);

        // Queue up to the limit (4 requests)
        AtomicReference<InflightReadsLimiter.Handle> handle2Reference = new AtomicReference<>();
        assertThat(limiter.acquire(50, handle2Reference::set)).isNotPresent();

        AtomicReference<InflightReadsLimiter.Handle> handle3Reference = new AtomicReference<>();
        assertThat(limiter.acquire(50, handle3Reference::set)).isNotPresent();

        AtomicReference<InflightReadsLimiter.Handle> handle4Reference = new AtomicReference<>();
        assertThat(limiter.acquire(50, handle4Reference::set)).isNotPresent();

        AtomicReference<InflightReadsLimiter.Handle> handle5Reference = new AtomicReference<>();
        assertThat(limiter.acquire(50, handle5Reference::set)).isNotPresent();

        // Attempt to add one more request, which should fail as the queue is full
        Optional<InflightReadsLimiter.Handle> handle6 = limiter.acquire(50, null);
        assertThat(handle6)
                .as("The sixth handle should not be successfull since the queue is full")
                .hasValueSatisfying(handle -> assertThat(handle.success()).isFalse());
    }

    @Test(dataProvider = "booleanValues")
    public void testAcquireExceedingMaxReadsInFlightSize(boolean firstInQueue) throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        long maxReadsInFlightSize = 100;
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(maxReadsInFlightSize, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS, executor,
                        OpenTelemetry.noop());

        // Initial state
        assertThat(limiter.getRemainingBytes())
                .as("Initial remaining bytes should match maxReadsInFlightSize")
                .isEqualTo(maxReadsInFlightSize);

        // Acquire all permits (consume 100 bytes)
        Optional<InflightReadsLimiter.Handle> handle1 = limiter.acquire(100, null);
        assertThat(handle1)
                .as("The first handle should be present")
                .isPresent();
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be zero after acquiring all permits")
                .isEqualTo(0);


        AtomicReference<InflightReadsLimiter.Handle> handle2Reference = new AtomicReference<>();

        if (!firstInQueue) {
            Optional<InflightReadsLimiter.Handle> handle2 = limiter.acquire(50, handle2Reference::set);
            assertThat(handle2)
                    .as("The second handle should not be present as remaining permits are zero")
                    .isNotPresent();
        }

        // Attempt to acquire more than maxReadsInFlightSize while all permits are in use
        AtomicReference<InflightReadsLimiter.Handle> handleExceedingMaxReference = new AtomicReference<>();
        Optional<InflightReadsLimiter.Handle> handleExceedingMaxOptional =
                limiter.acquire(200, handleExceedingMaxReference::set);
        assertThat(handleExceedingMaxOptional)
                .as("The second handle should not be present as remaining permits are zero")
                .isNotPresent();

        // Release handle1 permits
        limiter.release(handle1.get());

        if (!firstInQueue) {
            assertThat(handle2Reference)
                    .as("Handle2 should have been set in the callback and marked successful")
                    .hasValueSatisfying(handle -> {
                        assertThat(handle.success()).isTrue();
                        assertThat(handle.permits()).isEqualTo(50);
                    });
            limiter.release(handle2Reference.get());
        }

        assertThat(handleExceedingMaxReference)
                .as("Handle2 should have been set in the callback and marked successful")
                .hasValueSatisfying(handle -> {
                    assertThat(handle.success()).isTrue();
                    assertThat(handle.permits()).isEqualTo(maxReadsInFlightSize);
                });

        limiter.release(handleExceedingMaxReference.get());

        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be fully replenished after releasing all permits")
                .isEqualTo(maxReadsInFlightSize);
    }

    @Test
    public void testAcquireExceedingMaxReadsWhenAllPermitsAvailable() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        long maxReadsInFlightSize = 100;
        InflightReadsLimiter limiter =
                new InflightReadsLimiter(maxReadsInFlightSize, ACQUIRE_QUEUE_SIZE, ACQUIRE_TIMEOUT_MILLIS, executor,
                        OpenTelemetry.noop());

        // Initial state
        assertThat(limiter.getRemainingBytes())
                .as("Initial remaining bytes should match maxReadsInFlightSize")
                .isEqualTo(maxReadsInFlightSize);

        // Acquire permits > maxReadsInFlightSize
        Optional<InflightReadsLimiter.Handle> handleExceedingMaxOptional =
                limiter.acquire(2 * maxReadsInFlightSize, null);
        assertThat(handleExceedingMaxOptional)
                .as("The handle for exceeding max permits should be present")
                .hasValueSatisfying(handle -> {
                    assertThat(handle.success()).isTrue();
                    assertThat(handle.permits()).isEqualTo(maxReadsInFlightSize);
                });
        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be zero after acquiring all permits")
                .isEqualTo(0);

        // Release permits
        limiter.release(handleExceedingMaxOptional.get());

        assertThat(limiter.getRemainingBytes())
                .as("Remaining bytes should be fully replenished after releasing all permits")
                .isEqualTo(maxReadsInFlightSize);
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
