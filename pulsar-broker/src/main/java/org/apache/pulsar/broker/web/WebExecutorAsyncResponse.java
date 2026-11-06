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
package org.apache.pulsar.broker.web;

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.TimeoutHandler;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import lombok.CustomLog;

/**
 * An {@link AsyncResponse} that completes the request on the web service executor.
 *
 * <p>Jersey writes the response on the thread that calls {@code resume} or {@code cancel}, and the servlet output
 * stream blocks that thread while a slow client reads the response. Async REST endpoints usually resume in a
 * {@code CompletableFuture} continuation, which runs on the thread that completed the future, such as a metadata
 * store callback thread. This wrapper hands the completion to the web service executor, the pool meant for blocking
 * response writing, unless the calling thread already belongs to it. When the executor rejects the task, the
 * request is completed on the calling thread so that the response isn't lost.
 *
 * <p>The first {@code resume} or {@code cancel} call decides how the request completes, even while its completion
 * is still queued. Later calls fail like they do on a completed response, except that a cancellation succeeds
 * after an earlier cancellation. {@link #isSuspended()}, {@link #isCancelled()} and {@link #isDone()} report
 * Jersey's state, so they change once the queued completion has run.
 *
 * <p>Jersey's timeout handling uses its own response, not this wrapper, so a timeout could complete the request
 * while a completion is queued and the queued completion would be lost. Once a timeout or a timeout handler is
 * set, the request is therefore completed on the calling thread. Pulsar's endpoints don't use timeouts.
 */
@CustomLog
final class WebExecutorAsyncResponse implements AsyncResponse {

    private final AsyncResponse delegate;
    private final WebExecutorThreadPool executor;
    private static final int NOT_REQUESTED = 0;
    private static final int RESUME_REQUESTED = 1;
    private static final int CANCEL_REQUESTED = 2;
    private static final long REJECTION_WARNING_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

    // the responses that were completed on the calling thread since the last warning about it
    private static final AtomicLong rejectedSinceWarning = new AtomicLong();
    private static final AtomicLong nextRejectionWarningNanos = new AtomicLong(System.nanoTime());

    // Set by the first resume or cancel call, so that later calls behave like they would on a completed response
    // while the first completion is still waiting in the executor's queue.
    private final AtomicInteger requestedCompletion = new AtomicInteger(NOT_REQUESTED);
    private volatile boolean timeoutSet;

    WebExecutorAsyncResponse(AsyncResponse delegate, WebExecutorThreadPool executor) {
        this.delegate = delegate;
        this.executor = executor;
    }

    @Override
    public boolean resume(Object response) {
        return complete(RESUME_REQUESTED, () -> delegate.resume(response));
    }

    @Override
    public boolean resume(Throwable response) {
        return complete(RESUME_REQUESTED, () -> delegate.resume(response));
    }

    @Override
    public boolean cancel() {
        return complete(CANCEL_REQUESTED, delegate::cancel);
    }

    @Override
    public boolean cancel(int retryAfter) {
        return complete(CANCEL_REQUESTED, () -> delegate.cancel(retryAfter));
    }

    @Override
    public boolean cancel(Date retryAfter) {
        return complete(CANCEL_REQUESTED, () -> delegate.cancel(retryAfter));
    }

    private boolean complete(int completionType, BooleanSupplier completion) {
        int previousCompletion = requestedCompletion.compareAndExchange(NOT_REQUESTED, completionType);
        if (previousCompletion != NOT_REQUESTED) {
            // like on a completed response, cancelling again succeeds if the response was cancelled. Check isDone()
            // first, since the queued cancellation can complete between the two checks.
            return completionType == CANCEL_REQUESTED && previousCompletion == CANCEL_REQUESTED
                    && (!delegate.isDone() || delegate.isCancelled());
        }
        if (timeoutSet || delegate.isDone() || executor.isPoolThread()) {
            // a completed response doesn't write anything, so there's no need to hand it to the executor
            return completion.getAsBoolean();
        }
        try {
            executor.execute(() -> {
                try {
                    completion.getAsBoolean();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to complete the async HTTP response");
                }
            });
            return true;
        } catch (RejectedExecutionException e) {
            logRejection(e);
            return completion.getAsBoolean();
        }
    }

    private void logRejection(RejectedExecutionException e) {
        if (!executor.isRunning()) {
            // the web service is stopping
            log.debug()
                    .exceptionMessage(e)
                    .log("The web service executor rejected the response, completing it on the calling thread");
            return;
        }
        rejectedSinceWarning.incrementAndGet();
        long now = System.nanoTime();
        long nextWarningNanos = nextRejectionWarningNanos.get();
        // warn at most once per interval, since the executor rejects tasks while it is overloaded
        if (now - nextWarningNanos >= 0
                && nextRejectionWarningNanos.compareAndSet(nextWarningNanos, now + REJECTION_WARNING_INTERVAL_NANOS)) {
            log.warn()
                    .attr("rejectedResponses", rejectedSinceWarning.getAndSet(0))
                    .exceptionMessage(e)
                    .log("The web service executor's queue is full, so async HTTP responses are written on the"
                            + " threads that complete them, which can block those threads while clients read the"
                            + " responses. Consider increasing numHttpServerThreads or httpServerThreadPoolQueueSize");
        }
    }

    @Override
    public boolean isSuspended() {
        return delegate.isSuspended();
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public boolean setTimeout(long time, TimeUnit unit) {
        timeoutSet = true;
        if (requestedCompletion.get() != NOT_REQUESTED) {
            // like on a resumed response
            return false;
        }
        return delegate.setTimeout(time, unit);
    }

    @Override
    public void setTimeoutHandler(TimeoutHandler handler) {
        timeoutSet = true;
        delegate.setTimeoutHandler(handler);
    }

    @Override
    public Collection<Class<?>> register(Class<?> callback) {
        return delegate.register(callback);
    }

    @Override
    public Map<Class<?>, Collection<Class<?>>> register(Class<?> callback, Class<?>... callbacks) {
        return delegate.register(callback, callbacks);
    }

    @Override
    public Collection<Class<?>> register(Object callback) {
        return delegate.register(callback);
    }

    @Override
    public Map<Class<?>, Collection<Class<?>>> register(Object callback, Object... callbacks) {
        return delegate.register(callback, callbacks);
    }
}
