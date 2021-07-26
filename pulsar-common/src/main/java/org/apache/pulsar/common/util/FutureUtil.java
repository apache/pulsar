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
package org.apache.pulsar.common.util;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * This class is aimed at simplifying work with {@code CompletableFuture}.
 */
public class FutureUtil {

    /**
     * Return a future that represents the completion of the futures in the provided list.
     *
     * @param futures futures to wait for
     * @return a new CompletableFuture that is completed when all of the given CompletableFutures complete
     */
    public static CompletableFuture<Void> waitForAll(List<? extends CompletableFuture<?>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Return a future that represents the completion of any future in the provided list.
     *
     * @param futures futures to wait any
     * @return a new CompletableFuture that is completed when any of the given CompletableFutures complete
     */
    public static CompletableFuture<Object> waitForAny(List<? extends CompletableFuture<?>> futures) {
        return CompletableFuture.anyOf(futures.toArray(new CompletableFuture[0]));
    }


    /**
     * Return a future that represents the completion of the futures in the provided list.
     * The future will support {@link CompletableFuture#cancel(boolean)}. It will cancel
     * all unfinished futures when the future gets cancelled.
     *
     * @param futures futures to wait for
     * @return a new CompletableFuture that is completed when all of the given CompletableFutures complete
     */
    public static CompletableFuture<Void> waitForAllAndSupportCancel(List<? extends CompletableFuture<?>> futures) {
        CompletableFuture[] futuresArray = futures.toArray(new CompletableFuture[0]);
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futuresArray);
        whenCancelledOrTimedOut(combinedFuture, () -> {
            for (CompletableFuture completableFuture : futuresArray) {
                if (!completableFuture.isDone()) {
                    completableFuture.cancel(false);
                }
            }
        });
        return combinedFuture;
    }

    /**
     * If the future is cancelled or times out, the cancel action will be
     * invoked
     *
     * The action is executed once if the future completes with
     * {@link java.util.concurrent.CancellationException} or {@link TimeoutException}
     *
     * @param future future to attach the action to
     * @param cancelAction action to invoke if the future is cancelled or times out
     */
    public static void whenCancelledOrTimedOut(CompletableFuture<?> future, Runnable cancelAction) {
        CompletableFutureCancellationHandler cancellationHandler =
                new CompletableFutureCancellationHandler();
        cancellationHandler.setCancelAction(cancelAction);
        cancellationHandler.attachToFuture(future);
    }

    public static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    public static Throwable unwrapCompletionException(Throwable t) {
        if (t instanceof CompletionException) {
            return unwrapCompletionException(t.getCause());
        } else {
            return t;
        }
    }

    /**
     * Creates a new {@link CompletableFuture} instance with timeout handling.
     *
     * @param timeout the duration of the timeout
     * @param executor the executor to use for scheduling the timeout
     * @param exceptionSupplier the supplier for creating the exception
     * @param <T> type parameter for the future
     * @return the new {@link CompletableFuture} instance
     */
    public static <T> CompletableFuture<T> createFutureWithTimeout(Duration timeout,
                                                                   ScheduledExecutorService executor,
                                                                   Supplier<Throwable> exceptionSupplier) {
        return addTimeoutHandling(new CompletableFuture<>(), timeout, executor, exceptionSupplier);
    }

    /**
     * Adds timeout handling to an existing {@link CompletableFuture}.
     *
     * @param future the target future
     * @param timeout the duration of the timeout
     * @param executor the executor to use for scheduling the timeout
     * @param exceptionSupplier the supplier for creating the exception
     * @param <T> type parameter for the future
     * @return returns the original target future
     */
    public static <T> CompletableFuture<T> addTimeoutHandling(CompletableFuture<T> future, Duration timeout,
                                               ScheduledExecutorService executor,
                                               Supplier<Throwable> exceptionSupplier) {
        ScheduledFuture<?> scheduledFuture = executor.schedule(() -> {
            if (!future.isDone()) {
                future.completeExceptionally(exceptionSupplier.get());
            }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
        future.whenComplete((res, exception) -> scheduledFuture.cancel(false));
        return future;
    }

    /**
     * Creates a low-overhead timeout exception which is performance optimized to minimize allocations
     * and cpu consumption. It sets the stacktrace of the exception to the given source class and
     * source method name. The instances of this class can be cached or stored as constants and reused
     * multiple times.
     *
     * @param message exception message
     * @param sourceClass source class for manually filled in stacktrace
     * @param sourceMethod source method name for manually filled in stacktrace
     * @return new TimeoutException instance
     */
    public static TimeoutException createTimeoutException(String message, Class<?> sourceClass, String sourceMethod) {
        return new LowOverheadTimeoutException(message, sourceClass, sourceMethod);
    }

    private static class LowOverheadTimeoutException extends TimeoutException {
        private static final long serialVersionUID = 1L;

        LowOverheadTimeoutException(String message, Class<?> sourceClass, String sourceMethod) {
            super(message);
            setStackTrace(new StackTraceElement[]{new StackTraceElement(sourceClass.getName(), sourceMethod,
                    null, -1)});
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    public static <T> Optional<Throwable> getException(CompletableFuture<T> future) {
        if (future != null && future.isCompletedExceptionally()) {
            try {
                future.get();
            } catch (InterruptedException e) {
                return Optional.ofNullable(e);
            } catch (ExecutionException e) {
                return Optional.ofNullable(e.getCause());
            }
        }
        return Optional.empty();
    }
}
