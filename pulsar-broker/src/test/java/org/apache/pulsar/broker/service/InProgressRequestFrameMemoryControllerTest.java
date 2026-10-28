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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.InProgressRequestFrameMemoryLimitMode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InProgressRequestFrameMemoryControllerTest {

    @DataProvider
    public Object[][] enabledModes() {
        return new Object[][] { { InProgressRequestFrameMemoryLimitMode.AUTO },
            { InProgressRequestFrameMemoryLimitMode.ENABLED } };
    }

    @Test
    public void shouldTrackUsageBeforeAutomaticActivation() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> 0, 100);
        controller.acquire(64);
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(64);
        controller.release(64);
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test
    public void shouldEnableAutomaticallyOnlyAboveThresholdAndThrottleOnNextRead() {
        AtomicLong directMemoryUsedBytes = new AtomicLong(84);
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, directMemoryUsedBytes::get, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        limiter.registerThrottleStateListener("connection", () -> { });

        controller.acquire(150);
        directMemoryUsedBytes.set(85);
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isFalse();

        controller.acquire(1);
        directMemoryUsedBytes.set(84);
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isFalse();

        directMemoryUsedBytes.set(85);
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isTrue();
        assertThat(limiter.isThrottled()).isFalse();

        controller.release(1);
        assertThat(limiter.isThrottled()).isFalse();
        // Reusing a cumulation does not allocate a new root; the next read must nevertheless enforce the limit.
        controller.onInboundRead();
        assertThat(limiter.isThrottled()).isTrue();

        directMemoryUsedBytes.set(65);
        controller.release(1);
        assertThat(controller.isAutomaticallyEnabled()).isTrue();
        directMemoryUsedBytes.set(64);
        controller.release(1);
        assertThat(controller.isAutomaticallyEnabled()).isFalse();
        assertThat(limiter.isThrottled()).isFalse();
        controller.release(148);
    }

    @Test(dataProvider = "enabledModes")
    public void shouldApplyLatestLimitToAllConnectionsWithoutResettingUsage(
            InProgressRequestFrameMemoryLimitMode mode) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(mode,
                200, () -> 85, 100);
        InProgressRequestFrameMemoryLimiter existingConnectionLimiter = controller.getLimiter();
        AtomicInteger existingNotifications = new AtomicInteger();
        existingConnectionLimiter.registerThrottleStateListener("existing", existingNotifications::incrementAndGet);
        if (mode == InProgressRequestFrameMemoryLimitMode.AUTO) {
            controller.acquire(301);
            controller.onInboundRead();
            controller.release(301);
            assertThat(controller.isAutomaticallyEnabled()).isTrue();
        }
        controller.acquire(150);
        controller.onInboundRead();
        assertThat(existingConnectionLimiter.isThrottled()).isFalse();

        // Lowering an active limit re-evaluates already-retained memory without requiring another inbound read.
        controller.updateLimitBytes(100);
        assertThat(existingConnectionLimiter.getLimitBytes()).isEqualTo(100);
        assertThat(existingConnectionLimiter.isThrottled()).isTrue();
        assertThat(existingNotifications).hasValue(1);
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(150);
        InProgressRequestFrameMemoryLimiter newConnectionLimiter = controller.getLimiter();
        assertThat(newConnectionLimiter).isSameAs(existingConnectionLimiter);
        AtomicInteger newNotifications = new AtomicInteger();
        newConnectionLimiter.registerThrottleStateListener("new", newNotifications::incrementAndGet);
        assertThat(newConnectionLimiter.isThrottled()).isTrue();
        assertThat(newNotifications).hasValue(1);

        controller.updateLimitBytes(200);
        assertThat(existingConnectionLimiter.isThrottled()).isTrue();
        assertThat(existingNotifications).hasValue(1);
        // The shared hysteresis uses half the latest limit, not half the original connection limit.
        controller.updateLimitBytes(300);
        assertThat(existingConnectionLimiter.isThrottled()).isFalse();
        assertThat(newConnectionLimiter.isThrottled()).isFalse();
        assertThat(existingNotifications).hasValue(2);
        assertThat(newNotifications).hasValue(2);
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(150);
        controller.release(150);
    }

    @Test
    public void shouldClearAutomaticStateAndSkipMemorySamplingWhenManuallyEnabled() {
        AtomicInteger samples = new AtomicInteger();
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> {
                    samples.incrementAndGet();
                    return 85;
                }, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        limiter.registerThrottleStateListener("connection", () -> { });
        controller.acquire(151);
        controller.onInboundRead();
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isTrue();
        assertThat(limiter.isThrottled()).isTrue();

        controller.release(76);
        assertThat(limiter.isThrottled()).isTrue();
        int samplesBeforeManualEnable = samples.get();
        controller.setMode(InProgressRequestFrameMemoryLimitMode.ENABLED);
        assertThat(controller.isAutomaticallyEnabled()).isFalse();
        assertThat(limiter.isThrottled()).isTrue();
        controller.onInboundRead();
        controller.release(25);
        assertThat(limiter.isThrottled()).isFalse();
        assertThat(samples).hasValue(samplesBeforeManualEnable);

        controller.setMode(InProgressRequestFrameMemoryLimitMode.AUTO);
        controller.acquire(101);
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isTrue();
        controller.release(151);
    }

    @Test
    public void shouldUseLatestLimitForAutomaticActivationAndExistingConnections() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> 85, 100);
        InProgressRequestFrameMemoryLimiter oldLimiter = controller.getLimiter();
        oldLimiter.registerThrottleStateListener("old-connection", () -> { });
        controller.updateLimitBytes(200);
        controller.acquire(300);
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isFalse();
        controller.acquire(1);
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isTrue();
        assertThat(oldLimiter.isThrottled()).isFalse();
        controller.onInboundRead();
        assertThat(oldLimiter.isThrottled()).isTrue();
        assertThat(oldLimiter.getLimitBytes()).isEqualTo(200);
        controller.release(301);
    }

    @Test
    public void shouldDisableAutomaticModeWhenTheCurrentLimitIsDisabled() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> 85, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        limiter.registerThrottleStateListener("connection", () -> { });
        controller.acquire(151);
        controller.onInboundRead();
        controller.onInboundRead();
        assertThat(limiter.isThrottled()).isTrue();

        controller.updateLimitBytes(-1);
        assertThat(controller.isAutomaticallyEnabled()).isFalse();
        assertThat(limiter.isThrottled()).isFalse();
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isFalse();
        assertThat(limiter.getLimitBytes()).isEqualTo(-1);
        controller.release(151);
    }

    @DataProvider
    public Object[][] disabledLimits() {
        return new Object[][] {{0L}, {-1L}};
    }

    @Test(dataProvider = "disabledLimits")
    public void shouldDisableAndReenableTheLimitForAllConnections(long disabledLimit) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 85, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        AtomicInteger notifications = new AtomicInteger();
        limiter.registerThrottleStateListener("existing", notifications::incrementAndGet);
        controller.acquire(151);
        controller.onInboundRead();
        assertThat(limiter.isThrottled()).isTrue();
        assertThat(notifications).hasValue(1);

        controller.updateLimitBytes(disabledLimit);
        assertThat(limiter.getLimitBytes()).isEqualTo(disabledLimit);
        assertThat(limiter.isThrottled()).isFalse();
        assertThat(notifications).hasValue(2);
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(151);
        AtomicInteger newNotifications = new AtomicInteger();
        controller.getLimiter().registerThrottleStateListener("new", newNotifications::incrementAndGet);
        controller.onInboundRead();
        assertThat(limiter.isThrottled()).isFalse();
        assertThat(newNotifications).hasValue(0);

        controller.updateLimitBytes(100);
        assertThat(limiter.isThrottled()).isTrue();
        assertThat(notifications).hasValue(3);
        assertThat(newNotifications).hasValue(1);
        controller.release(151);
        assertThat(limiter.isThrottled()).isFalse();
    }

    @Test
    public void shouldReuseLimiterAcrossModeAndNumericLimitChanges() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        controller.setMode(InProgressRequestFrameMemoryLimitMode.ENABLED);
        controller.setMode(InProgressRequestFrameMemoryLimitMode.AUTO);
        controller.updateLimitBytes(100);
        controller.updateLimitBytes(200);
        assertThat(controller.getLimiter()).isSameAs(limiter);
        assertThat(limiter.getLimitBytes()).isEqualTo(200);
    }

    @Test
    public void shouldOnlyNotifyWhenStateChangesAndNotifyNewListenerOnce() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        AtomicInteger notifications = new AtomicInteger();
        limiter.registerThrottleStateListener("first", notifications::incrementAndGet);

        controller.acquire(100);
        controller.onInboundRead();
        assertThat(notifications).hasValue(0);
        controller.acquire(1);
        controller.onInboundRead();
        assertThat(notifications).hasValue(1);
        controller.onInboundRead();
        assertThat(notifications).hasValue(1);

        AtomicInteger newListenerNotifications = new AtomicInteger();
        limiter.registerThrottleStateListener("second", newListenerNotifications::incrementAndGet);
        assertThat(newListenerNotifications).hasValue(1);
        assertThat(notifications).hasValue(1);
        controller.release(51);
        assertThat(notifications).hasValue(2);
        assertThat(newListenerNotifications).hasValue(2);
        controller.release(50);
    }

    @Test
    public void shouldKeepAccountingAfterTheLastConnectionUnregisters() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter oldLimiter = controller.getLimiter();
        AtomicInteger oldNotifications = new AtomicInteger();
        oldLimiter.registerThrottleStateListener("old-connection", oldNotifications::incrementAndGet);
        assertThat(controller.getRegisteredListenerCount()).isEqualTo(1);
        controller.acquire(151);
        oldLimiter.unregisterThrottleStateListener("old-connection");
        assertThat(controller.getRegisteredListenerCount()).isZero();
        controller.updateLimitBytes(200);

        InProgressRequestFrameMemoryLimiter newLimiter = controller.getLimiter();
        newLimiter.registerThrottleStateListener("new-connection", () -> { });
        controller.acquire(50);
        controller.onInboundRead();
        assertThat(newLimiter.isThrottled()).isTrue();
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(201);
        assertThat(oldNotifications).hasValue(0);
        controller.release(151);
        assertThat(newLimiter.isThrottled()).isFalse();
        controller.release(50);
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test
    public void shouldNotifyOutsideTheStateLockAndIsolateListenerFailures() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        AtomicInteger successfulNotifications = new AtomicInteger();
        limiter.registerThrottleStateListener("failing", () -> {
            throw new IllegalStateException("test listener failure");
        });
        limiter.registerThrottleStateListener("healthy", () -> {
            assertThat(controller.getRegisteredListenerCount()).isEqualTo(2);
            successfulNotifications.incrementAndGet();
        });
        controller.acquire(101);
        controller.onInboundRead();
        assertThat(successfulNotifications).hasValue(1);
        controller.release(101);
        assertThat(successfulNotifications).hasValue(2);
    }

    @Test
    public void shouldIgnoreUnavailableDirectMemoryMeasurements() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> -1, 100);
        controller.acquire(151);
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isFalse();
        controller.release(151);
    }

    @Test
    public void shouldCalculateActivationThresholdWithoutOverflow() {
        assertThat(InProgressRequestFrameMemoryController.automaticEnableThresholdBytes(-1)).isEqualTo(-1);
        assertThat(InProgressRequestFrameMemoryController.automaticEnableThresholdBytes(0)).isZero();
        assertThat(InProgressRequestFrameMemoryController.automaticEnableThresholdBytes(100)).isEqualTo(150);
        assertThat(InProgressRequestFrameMemoryController.automaticEnableThresholdBytes(Long.MAX_VALUE))
                .isEqualTo(Long.MAX_VALUE);
        long limit = Long.MAX_VALUE / 3 + 1;
        assertThat(InProgressRequestFrameMemoryController.automaticEnableThresholdBytes(limit))
                .isEqualTo(limit + limit / 2);
    }
    @Test
    public void shouldSkipDirectMemorySamplingUntilAutomaticActivationIsPossible() {
        AtomicInteger samples = new AtomicInteger();
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> {
                    samples.incrementAndGet();
                    return 85;
                }, 100);
        controller.acquire(150);
        controller.onInboundRead();
        controller.release(1);
        controller.onInboundRead();
        assertThat(samples).hasValue(0);
        controller.acquire(2);
        controller.onInboundRead();
        assertThat(samples).hasValue(1);
        assertThat(controller.isAutomaticallyEnabled()).isTrue();
        controller.release(151);
        assertThat(samples).hasValue(2);
    }

    @Test(timeOut = 30_000)
    public void shouldNotLoseLastReleaseRacingWithAnInboundRead() throws Exception {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        limiter.registerThrottleStateListener("connection", () -> { });
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> reader = executor.submit(() -> {
                for (int i = 0; i < 10_000; i++) {
                    barrier.await(10, TimeUnit.SECONDS);
                    controller.onInboundRead();
                    barrier.await(10, TimeUnit.SECONDS);
                }
                return null;
            });
            for (int i = 0; i < 10_000; i++) {
                controller.acquire(101);
                barrier.await(10, TimeUnit.SECONDS);
                controller.release(101);
                barrier.await(10, TimeUnit.SECONDS);
                assertThat(controller.getRetainedFrameBytes()).isZero();
                assertThat(limiter.isThrottled())
                        .as("no future event is available to resume iteration %s", i).isFalse();
            }
            reader.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }


    @Test
    public void shouldNeverLimitOrSampleDirectMemoryWhenForcedDisabled() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.DISABLED, 100, () -> {
                    throw new AssertionError("Metrics-only mode must not sample direct-memory pressure");
                }, 100);
        InProgressRequestFrameMemoryLimiter oldLimiter = controller.getLimiter();
        AtomicInteger notifications = new AtomicInteger();
        oldLimiter.registerThrottleStateListener("old", notifications::incrementAndGet);
        controller.acquire(301);
        controller.onInboundRead();
        controller.updateLimitBytes(200);
        InProgressRequestFrameMemoryLimiter newLimiter = controller.getLimiter();
        newLimiter.registerThrottleStateListener("new", notifications::incrementAndGet);
        controller.onInboundRead();
        controller.release(1);
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(300);
        assertThat(controller.isEnabled()).isFalse();
        assertThat(controller.isAutomaticallyEnabled()).isFalse();
        assertThat(oldLimiter.isThrottled()).isFalse();
        assertThat(newLimiter.isThrottled()).isFalse();
        assertThat(notifications).hasValue(0);
        controller.release(300);
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @DataProvider
    public Object[][] modeTransitions() {
        return new Object[][] {
            { InProgressRequestFrameMemoryLimitMode.DISABLED, InProgressRequestFrameMemoryLimitMode.DISABLED },
            { InProgressRequestFrameMemoryLimitMode.DISABLED, InProgressRequestFrameMemoryLimitMode.AUTO },
            { InProgressRequestFrameMemoryLimitMode.DISABLED, InProgressRequestFrameMemoryLimitMode.ENABLED },
            { InProgressRequestFrameMemoryLimitMode.AUTO, InProgressRequestFrameMemoryLimitMode.DISABLED },
            { InProgressRequestFrameMemoryLimitMode.AUTO, InProgressRequestFrameMemoryLimitMode.AUTO },
            { InProgressRequestFrameMemoryLimitMode.AUTO, InProgressRequestFrameMemoryLimitMode.ENABLED },
            { InProgressRequestFrameMemoryLimitMode.ENABLED, InProgressRequestFrameMemoryLimitMode.DISABLED },
            { InProgressRequestFrameMemoryLimitMode.ENABLED, InProgressRequestFrameMemoryLimitMode.AUTO },
            { InProgressRequestFrameMemoryLimitMode.ENABLED, InProgressRequestFrameMemoryLimitMode.ENABLED }
        };
    }

    @Test(dataProvider = "modeTransitions")
    public void shouldApplyModeChangesToAllConnectionsWithoutResettingUsage(
            InProgressRequestFrameMemoryLimitMode initialMode, InProgressRequestFrameMemoryLimitMode nextMode) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                initialMode, 100, () -> 85, 100);
        InProgressRequestFrameMemoryLimiter oldLimiter = controller.getLimiter();
        oldLimiter.registerThrottleStateListener("old", () -> { });
        controller.updateLimitBytes(200);
        InProgressRequestFrameMemoryLimiter newLimiter = controller.getLimiter();
        newLimiter.registerThrottleStateListener("new", () -> { });
        controller.acquire(301);
        controller.onInboundRead();
        controller.onInboundRead();
        assertThat(oldLimiter.isThrottled()).isEqualTo(initialMode != InProgressRequestFrameMemoryLimitMode.DISABLED);
        assertThat(newLimiter.isThrottled()).isEqualTo(initialMode != InProgressRequestFrameMemoryLimitMode.DISABLED);

        controller.setMode(nextMode);
        boolean enabled = nextMode == InProgressRequestFrameMemoryLimitMode.ENABLED
                || nextMode == InProgressRequestFrameMemoryLimitMode.AUTO && initialMode == nextMode;
        assertThat(oldLimiter.isThrottled()).isEqualTo(enabled);
        assertThat(newLimiter.isThrottled()).isEqualTo(enabled);
        assertThat(controller.isAutomaticallyEnabled())
                .isEqualTo(nextMode == InProgressRequestFrameMemoryLimitMode.AUTO && initialMode == nextMode);
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(301);
        assertThat(controller.getLimiter()).isSameAs(newLimiter);
        assertThat(oldLimiter.getLimitBytes()).isEqualTo(200);

        controller.onInboundRead();
        // Entering AUTO enables protection on this read, but defers new throttling to the following read.
        assertThat(oldLimiter.isThrottled()).isEqualTo(enabled);
        controller.onInboundRead();
        assertThat(oldLimiter.isThrottled()).isEqualTo(nextMode != InProgressRequestFrameMemoryLimitMode.DISABLED);
        assertThat(newLimiter.isThrottled()).isEqualTo(nextMode != InProgressRequestFrameMemoryLimitMode.DISABLED);
        controller.release(251);
        assertThat(oldLimiter.isThrottled()).isFalse();
        assertThat(newLimiter.isThrottled()).isFalse();
        controller.release(50);
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @DataProvider
    public Object[][] forcedModes() {
        return new Object[][] { { InProgressRequestFrameMemoryLimitMode.DISABLED },
            { InProgressRequestFrameMemoryLimitMode.ENABLED } };
    }

    @Test(dataProvider = "forcedModes", timeOut = 30_000)
    public void shouldIgnoreAnAutomaticSampleThatCompletesAfterAModeChange(
            InProgressRequestFrameMemoryLimitMode forcedMode) throws Exception {
        CompletableFuture<Void> sampling = new CompletableFuture<>();
        CompletableFuture<Long> sample = new CompletableFuture<>();
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> {
                    sampling.complete(null);
                    return sample.join();
                }, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        limiter.registerThrottleStateListener("connection", () -> { });
        controller.acquire(151);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> read = executor.submit(controller::onInboundRead);
            sampling.get(10, TimeUnit.SECONDS);
            controller.setMode(forcedMode);
            sample.complete(85L);
            read.get(10, TimeUnit.SECONDS);
            assertThat(controller.isAutomaticallyEnabled()).isFalse();
            assertThat(limiter.isThrottled()).isEqualTo(forcedMode == InProgressRequestFrameMemoryLimitMode.ENABLED);
        } finally {
            sample.complete(85L);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            controller.release(151);
        }
        assertThat(limiter.isThrottled()).isFalse();
    }

    @Test(timeOut = 30_000)
    public void shouldNotLoseLastReleaseRacingWithEnablingFromMetricsOnlyMode() throws Exception {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.DISABLED, 100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        limiter.registerThrottleStateListener("connection", () -> { });
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> enabling = executor.submit(() -> {
                for (int i = 0; i < 10_000; i++) {
                    barrier.await(10, TimeUnit.SECONDS);
                    controller.setMode(InProgressRequestFrameMemoryLimitMode.ENABLED);
                    barrier.await(10, TimeUnit.SECONDS);
                }
                return null;
            });
            for (int i = 0; i < 10_000; i++) {
                controller.setMode(InProgressRequestFrameMemoryLimitMode.DISABLED);
                controller.acquire(101);
                barrier.await(10, TimeUnit.SECONDS);
                controller.release(101);
                barrier.await(10, TimeUnit.SECONDS);
                assertThat(controller.getRetainedFrameBytes()).isZero();
                assertThat(limiter.isThrottled()).as("last release in iteration %s", i).isFalse();
            }
            enabling.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @DataProvider
    public Object[][] changedLimitsDuringSampling() {
        return new Object[][] {{0L}, {-1L}, {200L}};
    }

    @Test(dataProvider = "changedLimitsDuringSampling", timeOut = 30_000)
    public void shouldRecheckAutomaticActivationAfterConcurrentLimitChange(long newLimit) throws Exception {
        CompletableFuture<Void> sampling = new CompletableFuture<>();
        CompletableFuture<Long> sample = new CompletableFuture<>();
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> {
                    sampling.complete(null);
                    return sample.join();
                }, 100);
        controller.acquire(151);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> read = executor.submit(controller::onInboundRead);
            sampling.get(10, TimeUnit.SECONDS);
            controller.updateLimitBytes(newLimit);
            sample.complete(85L);
            read.get(10, TimeUnit.SECONDS);
            assertThat(controller.getLimiter().getLimitBytes()).isEqualTo(newLimit);
            assertThat(controller.getLimiter().isThrottled()).isFalse();
            assertThat(controller.isAutomaticallyEnabled()).isFalse();
        } finally {
            sample.complete(85L);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            controller.release(151);
        }
    }

    @Test(timeOut = 30_000)
    public void shouldNotLoseLastReleaseRacingWithLoweringTheLimit() throws Exception {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                InProgressRequestFrameMemoryLimitMode.ENABLED, 1000, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        limiter.registerThrottleStateListener("connection", () -> { });
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> updating = executor.submit(() -> {
                for (int i = 0; i < 10_000; i++) {
                    barrier.await(10, TimeUnit.SECONDS);
                    controller.updateLimitBytes(100);
                    barrier.await(10, TimeUnit.SECONDS);
                }
                return null;
            });
            for (int i = 0; i < 10_000; i++) {
                controller.updateLimitBytes(1000);
                controller.acquire(101);
                barrier.await(10, TimeUnit.SECONDS);
                controller.release(101);
                barrier.await(10, TimeUnit.SECONDS);
                assertThat(controller.getRetainedFrameBytes()).isZero();
                assertThat(limiter.getLimitBytes()).isEqualTo(100);
                assertThat(limiter.isThrottled()).as("last release in iteration %s", i).isFalse();
            }
            updating.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

}
