/**
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

public class GracefulExecutorServicesShutdownTest {

    @Test
    public void shouldShutdownExecutorsImmediately() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        GracefulExecutorServicesShutdown shutdown = GracefulExecutorServicesShutdown.initiate();
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.isTerminated()).thenReturn(true);
        when(executorService.isShutdown()).thenReturn(true);

        // when
        shutdown.shutdown(executorService);
        CompletableFuture<Void> future = shutdown.handle();

        // then
        verify(executorService, atLeastOnce()).shutdown();
        future.get(1, TimeUnit.SECONDS);
        verify(executorService, never()).shutdownNow();
        assertTrue(future.isDone());
    }

    @Test
    public void shouldTerminateExecutorOnTimeout() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        GracefulExecutorServicesShutdown shutdown = GracefulExecutorServicesShutdown.initiate();
        shutdown.timeout(Duration.ofMillis(500));
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.isShutdown()).thenReturn(true);
        AtomicBoolean terminated = new AtomicBoolean();
        when(executorService.isTerminated()).thenAnswer(invocation -> terminated.get());
        when(executorService.shutdownNow()).thenAnswer(invocation -> {
           terminated.set(true);
           return null;
        });
        when(executorService.awaitTermination(anyLong(), any())).thenAnswer(invocation  -> {
            long timeout = invocation.getArgument(0);
            TimeUnit unit = invocation.getArgument(1);
            Thread.sleep(unit.toMillis(timeout));
            return terminated.get();
        });

        // when
        shutdown.shutdown(executorService);
        CompletableFuture<Void> future = shutdown.handle();

        // then
        future.get(1, TimeUnit.SECONDS);
        verify(executorService, atLeastOnce()).shutdownNow();
        assertTrue(future.isDone());
    }

    @Test
    public void shouldWaitForExecutorToTerminate() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        GracefulExecutorServicesShutdown shutdown = GracefulExecutorServicesShutdown.initiate();
        shutdown.timeout(Duration.ofMillis(500));
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.isShutdown()).thenReturn(true);
        AtomicBoolean terminated = new AtomicBoolean();
        when(executorService.isTerminated()).thenAnswer(invocation -> terminated.get());
        when(executorService.awaitTermination(anyLong(), any())).thenAnswer(invocation  -> {
            long timeout = invocation.getArgument(0);
            // wait half the time to simulate the termination completing
            timeout = timeout / 2;
            TimeUnit unit = invocation.getArgument(1);
            Thread.sleep(unit.toMillis(timeout));
            terminated.set(true);
            return terminated.get();
        });

        // when
        shutdown.shutdown(executorService);
        CompletableFuture<Void> future = shutdown.handle();

        // then
        future.get(1, TimeUnit.SECONDS);
        verify(executorService, times(1)).awaitTermination(anyLong(), any());
        verify(executorService, never()).shutdownNow();
        assertTrue(future.isDone());
    }


    @Test
    public void shouldTerminateWhenFutureIsCancelled() throws InterruptedException, ExecutionException {
        // given
        GracefulExecutorServicesShutdown shutdown = GracefulExecutorServicesShutdown.initiate();
        shutdown.timeout(Duration.ofMillis(15000));
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.isShutdown()).thenReturn(true);
        AtomicBoolean terminated = new AtomicBoolean();
        CompletableFuture<Boolean> awaitTerminationInterrupted = new CompletableFuture<>();
        when(executorService.isTerminated()).thenAnswer(invocation -> terminated.get());
        CountDownLatch awaitingTerminationEntered = new CountDownLatch(1);
        when(executorService.awaitTermination(anyLong(), any())).thenAnswer(invocation  -> {
            long timeout = invocation.getArgument(0);
            TimeUnit unit = invocation.getArgument(1);
            awaitingTerminationEntered.countDown();
            try {
                Thread.sleep(unit.toMillis(timeout));
            } catch (InterruptedException e) {
                awaitTerminationInterrupted.complete(true);
                Thread.currentThread().interrupt();
                throw e;
            }
            awaitTerminationInterrupted.complete(false);
            throw new IllegalStateException("Thread.sleep should have been interrupted");
        });
        when(executorService.shutdownNow()).thenAnswer(invocation -> {
            terminated.set(true);
            return null;
        });

        // when
        shutdown.shutdown(executorService);
        CompletableFuture<Void> future = shutdown.handle();
        awaitingTerminationEntered.await();
        future.cancel(false);

        // then
        assertTrue(awaitTerminationInterrupted.get(), "awaitTermination should have been interrupted");
        verify(executorService, times(1)).awaitTermination(anyLong(), any());
        verify(executorService, times(1)).shutdownNow();
    }

    @Test
    public void shouldAcceptNullReferenceAndIgnoreIt() {
        ExecutorService executorService = null;
        CompletableFuture<Void> future = GracefulExecutorServicesShutdown.initiate()
                .shutdown(executorService)
                .handle();
        assertTrue(future.isDone());
    }
}
