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
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RescheduleReadHandlerTest {
    private LongSupplier readIntervalMsSupplier;
    private ScheduledExecutorService executor;
    private Runnable cancelPendingRead;
    private Runnable rescheduleReadImmediately;
    private BooleanSupplier hasPendingReadRequestThatMightWait;
    private LongSupplier readOpCounterSupplier;
    private BooleanSupplier hasEntriesInReplayQueue;
    private RescheduleReadHandler rescheduleReadHandler;

    @BeforeMethod
    public void setUp() {
        readIntervalMsSupplier = mock(LongSupplier.class);
        executor = mock(ScheduledExecutorService.class);
        cancelPendingRead = mock(Runnable.class);
        rescheduleReadImmediately = mock(Runnable.class);
        hasPendingReadRequestThatMightWait = mock(BooleanSupplier.class);
        readOpCounterSupplier = mock(LongSupplier.class);
        hasEntriesInReplayQueue = mock(BooleanSupplier.class);
        rescheduleReadHandler = new RescheduleReadHandler(readIntervalMsSupplier, executor, cancelPendingRead,
                () -> rescheduleReadImmediately.run(), hasPendingReadRequestThatMightWait, readOpCounterSupplier, hasEntriesInReplayQueue);
    }

    @Test
    public void rescheduleReadImmediately() {
        when(readIntervalMsSupplier.getAsLong()).thenReturn(0L);

        rescheduleReadHandler.rescheduleRead();

        verify(rescheduleReadImmediately).run();
        verify(executor, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void rescheduleReadWithDelay() {
        when(readIntervalMsSupplier.getAsLong()).thenReturn(100L);

        rescheduleReadHandler.rescheduleRead();

        verify(rescheduleReadImmediately, never()).run();
        verify(executor).schedule(any(Runnable.class), eq(100L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void rescheduleReadWithDelayAndCancelPendingRead() {
        when(readIntervalMsSupplier.getAsLong()).thenReturn(100L);
        when(hasPendingReadRequestThatMightWait.getAsBoolean()).thenReturn(true);
        when(readOpCounterSupplier.getAsLong()).thenReturn(5L);
        when(hasEntriesInReplayQueue.getAsBoolean()).thenReturn(true);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        rescheduleReadHandler.rescheduleRead();

        verify(executor).schedule(any(Runnable.class), eq(100L), eq(TimeUnit.MILLISECONDS));
        verify(rescheduleReadImmediately).run();
        verify(cancelPendingRead).run();
    }

    @Test
    public void rescheduleReadWithDelayAndDontCancelPendingReadIfNoEntriesInReplayQueue() {
        when(readIntervalMsSupplier.getAsLong()).thenReturn(100L);
        when(hasPendingReadRequestThatMightWait.getAsBoolean()).thenReturn(true);
        when(readOpCounterSupplier.getAsLong()).thenReturn(5L);
        when(hasEntriesInReplayQueue.getAsBoolean()).thenReturn(false);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        rescheduleReadHandler.rescheduleRead();

        verify(executor).schedule(any(Runnable.class), eq(100L), eq(TimeUnit.MILLISECONDS));
        verify(rescheduleReadImmediately).run();
        verify(cancelPendingRead, never()).run();
    }

    @Test
    public void rescheduleReadBatching() {
        when(readOpCounterSupplier.getAsLong()).thenReturn(5L);
        when(readIntervalMsSupplier.getAsLong()).thenReturn(100L);
        AtomicReference<Runnable> scheduledRunnable = new AtomicReference<>();
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            if (!scheduledRunnable.compareAndSet(null, runnable)) {
                runnable.run();
            }
            return null;
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        // 3 rescheduleRead calls
        rescheduleReadHandler.rescheduleRead();
        rescheduleReadHandler.rescheduleRead();
        rescheduleReadHandler.rescheduleRead();
        // scheduled task runs
        scheduledRunnable.get().run();
        // verify that rescheduleReadImmediately is called only once
        verify(rescheduleReadImmediately, times(1)).run();
    }

    @Test
    public void rescheduleReadWithoutCancelPendingReadWhenReadOpCounterIncrements() {
        // given
        when(readIntervalMsSupplier.getAsLong()).thenReturn(100L);
        when(hasPendingReadRequestThatMightWait.getAsBoolean()).thenReturn(true);
        when(readOpCounterSupplier.getAsLong()).thenReturn(5L).thenReturn(6L);
        when(hasEntriesInReplayQueue.getAsBoolean()).thenReturn(true);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        // when rescheduleRead is called
        rescheduleReadHandler.rescheduleRead();
        // then verify calls
        verify(executor).schedule(any(Runnable.class), eq(100L), eq(TimeUnit.MILLISECONDS));
        verify(rescheduleReadImmediately).run();
        // verify that cancelPendingRead is not called
        verify(cancelPendingRead, never()).run();
    }
}