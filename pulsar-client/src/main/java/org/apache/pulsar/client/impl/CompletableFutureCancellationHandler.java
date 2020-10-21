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
package org.apache.pulsar.client.impl;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Implements cancellation and timeout support for CompletableFutures.
 * <p>
 * This class ensures that the cancel action gets called once after the future completes with
 * either {@link CancellationException} or {@link TimeoutException}.
 * The implementation handles possible race conditions that
 * might happen when the future gets cancelled before the cancel action is set to this handler.
 * <p>
 * For timeouts, CompletableFuture's "orTimeout" method introduced in JDK9
 * can be used in client code.
 * <p>
 * Cancellation and timeout support will only be active on the future where the
 * cancellation handler has been attached to. Cancellation won't happen if .cancel is called on
 * any "downstream" dependent futures. A cancellation or timeout that happens in any "upstream"
 * future will get handled.
 */
class CompletableFutureCancellationHandler {
    private volatile boolean cancelled;
    private volatile Runnable cancelAction;
    private final AtomicBoolean cancelHandled = new AtomicBoolean();

    /**
     * Creates a new {@link CompletableFuture} and attaches the cancellation handler
     * to handle cancels and timeouts.
     *
     * @param <T> the result type of the future
     * @return a new future instance
     */
    public <T> CompletableFuture<T> createFuture() {
        CompletableFuture<T> future = new CompletableFuture<>();
        attachToFuture(future);
        return future;
    }

    /**
     * Attaches the cancellation handler to handle cancels
     * and timeouts
     *
     * @param future the future to attach the handler to
     * @param <T>    the result type of the future
     */
    public <T> void attachToFuture(CompletableFuture<T> future) {
        future.whenComplete(whenCompleteFunction());
    }

    /**
     * Set the action to run when the future gets cancelled or timeouts.
     * The cancellation or timeout might be originating from any "upstream" future.
     * The implementation ensures that the cancel action gets called once.
     * Handles possible race conditions that might happen when the future gets cancelled
     * before the cancel action is set to this handler. In this case, the
     * cancel action gets called when the action is set.
     *
     * @param cancelAction the action to run when the the future gets cancelled or timeouts
     */
    public void setCancelAction(Runnable cancelAction) {
        if (this.cancelAction != null) {
            throw new IllegalStateException("cancelAction can only be set once.");
        }
        this.cancelAction = Objects.requireNonNull(cancelAction);
        // handle race condition in the case that the future was already cancelled when the handler is set
        runCancelActionOnceIfCancelled();
    }

    private <T> BiConsumer<? super T, ? super Throwable> whenCompleteFunction() {
        return (T t, Throwable throwable) -> {
            if (throwable instanceof CancellationException || throwable instanceof TimeoutException) {
                cancelled = true;
            }
            runCancelActionOnceIfCancelled();
        };
    }

    private void runCancelActionOnceIfCancelled() {
        if (cancelled && cancelAction != null && cancelHandled.compareAndSet(false, true)) {
            cancelAction.run();
        }
    }
}
