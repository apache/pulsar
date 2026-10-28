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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InProgressRequestFrameMemoryControllerTest {

    @DataProvider
    public Object[][] enabledModes() {
        return new Object[][] { { false }, { true } };
    }

    @Test
    public void shouldTrackUsageWhenLimitingIsDisabled() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(false,
                100, () -> 0, 100);
        controller.acquire(64);
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(64);
        controller.release(64);
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test
    public void shouldEnableAutomaticallyOnlyAboveThresholdAndThrottleOnNextRead() {
        AtomicLong directMemoryUsedBytes = new AtomicLong(84);
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(false,
                100, directMemoryUsedBytes::get, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getCurrentLimiter();
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
    public void shouldUseGlobalUsageWithEachGenerationsOwnThreshold(boolean manual) {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(manual,
                100, () -> 85, 100);
        InProgressRequestFrameMemoryLimiter oldLimiter = controller.getCurrentLimiter();
        oldLimiter.registerThrottleStateListener("old-connection", () -> { });
        if (!manual) {
            controller.acquire(151);
            controller.onInboundRead();
            controller.release(151);
            assertThat(controller.isAutomaticallyEnabled()).isTrue();
        }

        controller.updateLimitBytes(200);
        InProgressRequestFrameMemoryLimiter newLimiter = controller.getCurrentLimiter();
        newLimiter.registerThrottleStateListener("new-connection", () -> { });
        assertThat(oldLimiter.getLimitBytes()).isEqualTo(100);
        assertThat(newLimiter.getLimitBytes()).isEqualTo(200);
        assertThat(newLimiter).isNotSameAs(oldLimiter);

        controller.acquire(150);
        controller.onInboundRead();
        assertThat(oldLimiter.isThrottled()).isTrue();
        assertThat(newLimiter.isThrottled()).isFalse();

        controller.acquire(51);
        controller.onInboundRead();
        assertThat(oldLimiter.isThrottled()).isTrue();
        assertThat(newLimiter.isThrottled()).isTrue();

        controller.release(100);
        assertThat(oldLimiter.isThrottled()).isTrue();
        assertThat(newLimiter.isThrottled()).isTrue();
        controller.release(1);
        assertThat(oldLimiter.isThrottled()).isTrue();
        assertThat(newLimiter.isThrottled()).isFalse();
        controller.release(50);
        assertThat(oldLimiter.isThrottled()).isFalse();
        assertThat(newLimiter.isThrottled()).isFalse();
        controller.release(50);
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test
    public void shouldClearAutomaticStateAndSkipMemorySamplingWhenManuallyEnabled() {
        AtomicInteger samples = new AtomicInteger();
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(false,
                100, () -> {
                    samples.incrementAndGet();
                    return 85;
                }, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getCurrentLimiter();
        limiter.registerThrottleStateListener("connection", () -> { });
        controller.acquire(151);
        controller.onInboundRead();
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isTrue();
        assertThat(limiter.isThrottled()).isTrue();

        controller.release(76);
        assertThat(limiter.isThrottled()).isTrue();
        int samplesBeforeManualEnable = samples.get();
        controller.setUserEnabled(true);
        assertThat(controller.isAutomaticallyEnabled()).isFalse();
        assertThat(limiter.isThrottled()).isTrue();
        controller.onInboundRead();
        controller.release(25);
        assertThat(limiter.isThrottled()).isFalse();
        assertThat(samples).hasValue(samplesBeforeManualEnable);

        controller.setUserEnabled(false);
        controller.acquire(101);
        controller.onInboundRead();
        assertThat(controller.isAutomaticallyEnabled()).isTrue();
        controller.release(151);
    }

    @Test
    public void shouldUseNewActivationThresholdWithoutChangingOldConnectionLimits() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(false,
                100, () -> 85, 100);
        InProgressRequestFrameMemoryLimiter oldLimiter = controller.getCurrentLimiter();
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
        assertThat(oldLimiter.getLimitBytes()).isEqualTo(100);
        controller.release(301);
    }

    @Test
    public void shouldDisableAutomaticModeWhenTheCurrentLimitIsDisabled() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(false,
                100, () -> 85, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getCurrentLimiter();
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
        assertThat(limiter.getLimitBytes()).isEqualTo(100);
        controller.release(151);
    }

    @Test
    public void shouldKeepManualOldConnectionLimitWhenNewConnectionsDisableIt() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(true,
                100, () -> 85, 100);
        InProgressRequestFrameMemoryLimiter oldLimiter = controller.getCurrentLimiter();
        oldLimiter.registerThrottleStateListener("old-connection", () -> { });
        controller.updateLimitBytes(-1);
        InProgressRequestFrameMemoryLimiter newLimiter = controller.getCurrentLimiter();
        newLimiter.registerThrottleStateListener("new-connection", () -> { });
        controller.acquire(151);
        controller.onInboundRead();
        assertThat(oldLimiter.isThrottled()).isTrue();
        assertThat(newLimiter.isThrottled()).isFalse();
        controller.release(151);
    }

    @Test
    public void shouldReuseLimiterUnlessTheNumericLimitChanges() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(false,
                100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getCurrentLimiter();
        controller.setUserEnabled(true);
        controller.setUserEnabled(false);
        controller.updateLimitBytes(100);
        assertThat(controller.getCurrentLimiter()).isSameAs(limiter);
    }

    @Test
    public void shouldOnlyNotifyWhenStateChangesAndNotifyNewListenerOnce() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(true,
                100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getCurrentLimiter();
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
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(true,
                100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter oldLimiter = controller.getCurrentLimiter();
        AtomicInteger oldNotifications = new AtomicInteger();
        oldLimiter.registerThrottleStateListener("old-connection", oldNotifications::incrementAndGet);
        assertThat(controller.getRegisteredLimiterCount()).isEqualTo(1);
        controller.acquire(151);
        oldLimiter.unregisterThrottleStateListener("old-connection");
        assertThat(controller.getRegisteredLimiterCount()).isZero();
        controller.updateLimitBytes(200);

        InProgressRequestFrameMemoryLimiter newLimiter = controller.getCurrentLimiter();
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
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(true,
                100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getCurrentLimiter();
        AtomicInteger successfulNotifications = new AtomicInteger();
        limiter.registerThrottleStateListener("failing", () -> {
            throw new IllegalStateException("test listener failure");
        });
        limiter.registerThrottleStateListener("healthy", () -> {
            assertThat(controller.getRegisteredLimiterCount()).isEqualTo(1);
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
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(false,
                100, () -> -1, 100);
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
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(false,
                100, () -> {
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
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(true,
                100, () -> 0, 100);
        InProgressRequestFrameMemoryLimiter limiter = controller.getCurrentLimiter();
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

}
