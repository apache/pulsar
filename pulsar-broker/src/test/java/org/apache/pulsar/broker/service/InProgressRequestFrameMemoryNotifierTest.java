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
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import io.netty.channel.DefaultEventLoop;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.InProgressRequestFrameMemoryLimitMode;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InProgressRequestFrameMemoryNotifierTest {
    // These tests drive inbound/release events explicitly; recovery-task behavior has dedicated tests.
    private static final ScheduledExecutorService RECOVERY_EXECUTOR =
            mock(ScheduledExecutorService.class, RETURNS_MOCKS);

    @Test
    public void shouldCoalesceExistingAndNewConnectionsAcrossLimitChangesOnOneEventLoop() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                RECOVERY_EXECUTOR, InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        EventExecutor executor = mock(EventExecutor.class);
        Queue<Runnable> tasks = new ArrayDeque<>();
        doAnswer(invocation -> tasks.add(invocation.getArgument(0))).when(executor).execute(any(Runnable.class));
        InProgressRequestFrameMemoryNotifier notifier = controller.getEventLoopNotifier(executor);
        InProgressRequestFrameMemoryLimiter oldLimiter = controller.getLimiter();
        AtomicInteger oldNotifications = new AtomicInteger();
        AtomicInteger newNotifications = new AtomicInteger();
        for (int i = 0; i < 100; i++) {
            notifier.add(i, () -> {
                assertThat(oldLimiter.isThrottled()).isFalse();
                oldNotifications.incrementAndGet();
            });
        }
        controller.updateLimitBytes(200);
        InProgressRequestFrameMemoryLimiter newLimiter = controller.getLimiter();
        assertThat(newLimiter).isSameAs(oldLimiter);
        for (int i = 0; i < 100; i++) {
            notifier.add(i + 100, () -> {
                assertThat(newLimiter.isThrottled()).isFalse();
                newNotifications.incrementAndGet();
            });
        }
        controller.acquire(201);
        controller.onInboundRead();
        controller.release(201);
        verify(executor, times(1)).execute(any(Runnable.class));
        assertThat(tasks).hasSize(1);
        tasks.remove().run();
        assertThat(oldNotifications).hasValue(100);
        assertThat(newNotifications).hasValue(100);
        assertThat(tasks).isEmpty();
        for (int i = 0; i < 100; i++) {
            notifier.remove(i);
            notifier.remove(i + 100);
        }
        assertThat(controller.getRegisteredListenerCount()).isZero();
        assertThat(controller.getEventLoopNotifier(executor)).isNotSameAs(notifier);
    }

    @Test
    public void shouldDeliverRecoverySignalledWhileApplyingThrottle() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                RECOVERY_EXECUTOR, InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        EventExecutor executor = mock(EventExecutor.class);
        Queue<Runnable> tasks = new ArrayDeque<>();
        doAnswer(invocation -> tasks.add(invocation.getArgument(0))).when(executor).execute(any(Runnable.class));
        InProgressRequestFrameMemoryNotifier notifier = controller.getEventLoopNotifier(executor);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        AtomicInteger notifications = new AtomicInteger();
        notifier.add("connection", () -> {
            if (notifications.getAndIncrement() == 0) {
                assertThat(limiter.isThrottled()).isTrue();
                controller.release(101);
            } else {
                assertThat(limiter.isThrottled()).isFalse();
            }
        });
        controller.acquire(101);
        controller.onInboundRead();
        tasks.remove().run();
        assertThat(notifications).hasValue(2);
        verify(executor, times(1)).execute(any(Runnable.class));
        assertThat(controller.getRetainedFrameBytes()).isZero();
        notifier.remove("connection");
    }

    @Test
    public void shouldAllowRemovalDuringNotificationAndIgnoreClosedConnections() {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                RECOVERY_EXECUTOR, InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        EventExecutor executor = mock(EventExecutor.class);
        Queue<Runnable> tasks = new ArrayDeque<>();
        doAnswer(invocation -> tasks.add(invocation.getArgument(0))).when(executor).execute(any(Runnable.class));
        InProgressRequestFrameMemoryNotifier notifier = controller.getEventLoopNotifier(executor);
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        AtomicInteger notifications = new AtomicInteger();
        notifier.add("connection", () -> {
            notifications.incrementAndGet();
            notifier.remove("connection");
        });
        controller.acquire(101);
        controller.onInboundRead();
        tasks.remove().run();
        controller.release(101);
        assertThat(tasks).isEmpty();
        assertThat(notifications).hasValue(1);
        assertThat(controller.getRegisteredListenerCount()).isZero();
    }
    @Test(timeOut = 30_000)
    public void shouldApplyWorkerThreadRecoveryOnTheOwningEventLoop() throws Exception {
        InProgressRequestFrameMemoryController controller = new InProgressRequestFrameMemoryController(
                RECOVERY_EXECUTOR, InProgressRequestFrameMemoryLimitMode.ENABLED, 100, () -> 0, 100);
        DefaultEventLoop eventLoop = new DefaultEventLoop(new DefaultThreadFactory("frame-memory-notifications"));
        InProgressRequestFrameMemoryLimiter limiter = controller.getLimiter();
        InProgressRequestFrameMemoryNotifier notifier = controller.getEventLoopNotifier(eventLoop);
        AtomicInteger throttled = new AtomicInteger();
        AtomicInteger resumed = new AtomicInteger();
        try {
            eventLoop.submit(() -> {
                for (int i = 0; i < 1000; i++) {
                    notifier.add(i, () -> {
                        assertThat(eventLoop.inEventLoop()).isTrue();
                        if (limiter.isThrottled()) {
                            throttled.incrementAndGet();
                        } else {
                            resumed.incrementAndGet();
                        }
                    });
                }
                controller.acquire(101);
                controller.onInboundRead();
            }).get(10, TimeUnit.SECONDS);
            eventLoop.submit(() -> assertThat(throttled).hasValue(1000)).get(10, TimeUnit.SECONDS);

            // This release is outside the event loop, just like a BookKeeper completion. No further inbound
            // read or periodic task is available to deliver the recovery notification.
            assertThat(eventLoop.inEventLoop()).isFalse();
            controller.release(101);
            eventLoop.submit(() -> assertThat(resumed).hasValue(1000)).get(10, TimeUnit.SECONDS);
            assertThat(controller.getRetainedFrameBytes()).isZero();
        } finally {
            try {
                eventLoop.submit(() -> {
                    for (int i = 0; i < 1000; i++) {
                        notifier.remove(i);
                    }
                }).get(10, TimeUnit.SECONDS);
            } finally {
                eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).get(10, TimeUnit.SECONDS);
            }
        }
        assertThat(controller.getRegisteredListenerCount()).isZero();
    }

}
