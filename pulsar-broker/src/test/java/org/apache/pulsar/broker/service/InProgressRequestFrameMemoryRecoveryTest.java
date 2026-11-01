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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.InProgressRequestFrameMemoryLimitMode;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InProgressRequestFrameMemoryRecoveryTest {
    private EmbeddedChannel scheduler;
    private InProgressRequestFrameMemoryController controller;

    @BeforeMethod
    public void setup() {
        scheduler = new EmbeddedChannel();
        scheduler.freezeTime();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (controller != null) {
            controller.close();
            controller = null;
        }
        scheduler.finishAndReleaseAll();
    }

    @DataProvider
    public Object[][] limitingModes() {
        return new Object[][] {{InProgressRequestFrameMemoryLimitMode.ENABLED},
            {InProgressRequestFrameMemoryLimitMode.AUTO}};
    }

    @Test(dataProvider = "limitingModes")
    public void shouldRecoverFromOverestimatedLastReleaseWithoutAnotherInboundRead(
            InProgressRequestFrameMemoryLimitMode mode) {
        controller = spy(new InProgressRequestFrameMemoryController(scheduler.eventLoop(), mode, 100, () -> 85, 100));
        AtomicInteger notifications = new AtomicInteger();
        controller.registerThrottleStateListener(this, notifications::incrementAndGet);
        controller.acquire(151);
        if (mode == InProgressRequestFrameMemoryLimitMode.AUTO) {
            controller.onInboundRead();
            assertThat(controller.isAutomaticallyEnabled()).isTrue();
            assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        }
        controller.onInboundRead();
        assertThat(controller.getLimiter().isThrottled()).isTrue();
        assertThat(notifications).hasValue(1);

        // Simulate an overestimated concurrent sum during the final release, not a lost counter update.
        doReturn(151L).when(controller).getRetainedFrameBytes();
        controller.release(151);
        assertThat(controller.getLimiter().isThrottled()).isTrue();
        doCallRealMethod().when(controller).getRetainedFrameBytes();
        assertThat(controller.getRetainedFrameBytes()).isZero();
        scheduler.advanceTimeBy(299, TimeUnit.MILLISECONDS);
        scheduler.runScheduledPendingTasks();
        assertThat(controller.getLimiter().isThrottled()).isTrue();
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        assertThat(controller.getLimiter().isThrottled()).isFalse();
        assertThat(notifications).hasValue(2);
    }

    @Test
    public void shouldKeepOneTaskWhileThrottledAndRestartItAfterImmediateRecovery() {
        controller = spy(new InProgressRequestFrameMemoryController(scheduler.eventLoop(),
                InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100));
        controller.acquire(101);
        controller.onInboundRead();
        controller.onInboundRead();
        controller.registerThrottleStateListener("late connection", () -> { });
        clearInvocations(controller);
        scheduler.advanceTimeBy(300, TimeUnit.MILLISECONDS);
        assertThat(scheduler.runScheduledPendingTasks()).isPositive();
        verify(controller, times(1)).getRetainedFrameBytes();

        controller.release(101);
        assertThat(controller.getLimiter().isThrottled()).isFalse();
        assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        controller.acquire(101);
        controller.onInboundRead();
        clearInvocations(controller);
        scheduler.advanceTimeBy(300, TimeUnit.MILLISECONDS);
        scheduler.runScheduledPendingTasks();
        verify(controller, times(1)).getRetainedFrameBytes();
        controller.release(101);
        assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
    }

    @DataProvider
    public Object[][] throttleTriggers() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "throttleTriggers")
    public void shouldScheduleRecoveryWhenConfigurationOrRegistrationStartsThrottling(boolean configuration) {
        controller = new InProgressRequestFrameMemoryController(scheduler.eventLoop(),
                InProgressRequestFrameMemoryLimitMode.ENABLED, configuration ? 1000 : 100, () -> 0, 100);
        controller.acquire(101);
        assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        if (configuration) {
            controller.updateLimitBytes(100);
        } else {
            controller.registerThrottleStateListener(this, () -> { });
        }
        assertThat(controller.getLimiter().isThrottled()).isTrue();
        assertThat(scheduler.runScheduledPendingTasks()).isPositive();
        controller.updateLimitBytes(202);
        assertThat(controller.getLimiter().isThrottled()).isFalse();
        assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        controller.updateLimitBytes(100);
        assertThat(controller.getLimiter().isThrottled()).isTrue();
        controller.updateLimitBytes(0);
        assertThat(controller.getLimiter().isThrottled()).isFalse();
        assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        controller.release(101);
    }

    @Test
    public void shouldRetryAfterSamplingFailureAndDisableAutoWhenDirectMemoryFalls() {
        AtomicLong directMemory = new AtomicLong(85);
        AtomicBoolean failSampling = new AtomicBoolean();
        controller = new InProgressRequestFrameMemoryController(scheduler.eventLoop(),
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> {
                    if (failSampling.get()) {
                        throw new IllegalStateException("Simulated memory sampling failure");
                    }
                    return directMemory.get();
                }, 100);
        controller.acquire(151);
        controller.onInboundRead();
        controller.onInboundRead();
        failSampling.set(true);
        scheduler.advanceTimeBy(300, TimeUnit.MILLISECONDS);
        assertThat(scheduler.runScheduledPendingTasks()).isPositive();
        assertThat(controller.getLimiter().isThrottled()).isTrue();
        failSampling.set(false);
        directMemory.set(64);
        scheduler.advanceTimeBy(300, TimeUnit.MILLISECONDS);
        assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        assertThat(controller.isAutomaticallyEnabled()).isFalse();
        assertThat(controller.getLimiter().isThrottled()).isFalse();
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(151);
        controller.release(151);
    }

    @Test
    public void shouldNotScheduleChecksWhenTrackingOnly() {
        controller = new InProgressRequestFrameMemoryController(scheduler.eventLoop(),
                InProgressRequestFrameMemoryLimitMode.TRACKING, 100, () -> {
                    throw new AssertionError("TRACKING must not sample direct memory");
                }, 100);
        controller.acquire(151);
        controller.onInboundRead();
        controller.registerThrottleStateListener(this, () -> { });
        controller.updateLimitBytes(1);
        controller.release(151);
        scheduler.advanceTimeBy(1, TimeUnit.DAYS);
        assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test
    public void shouldCancelRecoveryOnCloseAndStillAccountForLateReleases() {
        controller = new InProgressRequestFrameMemoryController(scheduler.eventLoop(),
                InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        controller.acquire(101);
        controller.onInboundRead();
        assertThat(scheduler.runScheduledPendingTasks()).isPositive();
        controller.close();
        controller.onInboundRead();
        controller.updateLimitBytes(1);
        controller.registerThrottleStateListener(this, () -> { });
        scheduler.advanceTimeBy(1, TimeUnit.DAYS);
        assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        assertThat(controller.isEnabled()).isFalse();
        assertThat(controller.getLimiter().isThrottled()).isFalse();
        assertThat(controller.getRegisteredListenerCount()).isZero();
        assertThat(controller.getRetainedFrameBytes()).isEqualTo(101);
        controller.release(101);
        assertThat(controller.getRetainedFrameBytes()).isZero();
        assertThat(scheduler.eventLoop().isShuttingDown()).isFalse();
    }

    @Test
    public void shouldNotPauseWithoutARecoveryTaskWhenSchedulingIsRejected() {
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        when(executor.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenThrow(new RejectedExecutionException("Simulated scheduler shutdown"));
        controller = new InProgressRequestFrameMemoryController(executor,
                InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        Runnable listener = mock(Runnable.class);
        controller.registerThrottleStateListener(this, listener);
        controller.acquire(101);
        controller.onInboundRead();
        assertThat(controller.getLimiter().isThrottled()).isFalse();
        verify(listener).run();
        controller.release(101);
        assertThat(controller.getRetainedFrameBytes()).isZero();
    }

    @Test(timeOut = 30_000)
    public void shouldKeepTheNewRecoveryTaskWhenAnOldCheckFinishesAfterRethrottling() throws Exception {
        ScheduledExecutorService recoveryExecutor = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> firstTask = mock(ScheduledFuture.class);
        ScheduledFuture<?> secondTask = mock(ScheduledFuture.class);
        doReturn(firstTask, secondTask).when(recoveryExecutor)
                .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        CompletableFuture<Void> sampling = new CompletableFuture<>();
        CompletableFuture<Long> sample = new CompletableFuture<>();
        AtomicBoolean blockNextSample = new AtomicBoolean();
        controller = new InProgressRequestFrameMemoryController(recoveryExecutor,
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> {
                    if (blockNextSample.compareAndSet(true, false)) {
                        sampling.complete(null);
                        return sample.join();
                    }
                    return 85;
                }, 100);
        controller.acquire(151);
        controller.onInboundRead();
        controller.onInboundRead();
        ArgumentCaptor<Runnable> checks = ArgumentCaptor.forClass(Runnable.class);
        verify(recoveryExecutor).scheduleWithFixedDelay(checks.capture(), eq(300L), eq(300L),
                eq(TimeUnit.MILLISECONDS));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            blockNextSample.set(true);
            Future<?> oldCheck = executor.submit(checks.getValue());
            sampling.get(10, TimeUnit.SECONDS);
            controller.release(151);
            verify(firstTask).cancel(false);
            controller.acquire(151);
            controller.onInboundRead();
            assertThat(controller.getLimiter().isThrottled()).isTrue();
            sample.complete(85L);
            oldCheck.get(10, TimeUnit.SECONDS);
            verify(secondTask, never()).cancel(false);
            assertThat(controller.getLimiter().isThrottled()).isTrue();
            controller.close();
            verify(secondTask).cancel(false);
        } finally {
            sample.complete(85L);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(timeOut = 30_000)
    public void shouldNotRestartAfterCloseRacesWithAnInboundMemorySample() throws Exception {
        CompletableFuture<Void> sampling = new CompletableFuture<>();
        CompletableFuture<Long> sample = new CompletableFuture<>();
        controller = new InProgressRequestFrameMemoryController(scheduler.eventLoop(),
                InProgressRequestFrameMemoryLimitMode.AUTO, 100, () -> {
                    sampling.complete(null);
                    return sample.join();
                }, 100);
        controller.acquire(151);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> read = executor.submit(controller::onInboundRead);
            sampling.get(10, TimeUnit.SECONDS);
            controller.close();
            sample.complete(85L);
            read.get(10, TimeUnit.SECONDS);
            assertThat(controller.isAutomaticallyEnabled()).isFalse();
            assertThat(controller.getLimiter().isThrottled()).isFalse();
            assertThat(scheduler.runScheduledPendingTasks()).isEqualTo(-1);
        } finally {
            sample.complete(85L);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            controller.release(151);
        }
    }
}
