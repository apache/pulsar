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
package org.apache.pulsar.client.impl.v5;

import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClientException;

/**
 * Async-native, single-consumer receive queue shared by the v5 scalable consumers.
 *
 * <p>Mirrors the v4 {@code ConsumerBase} delivery model: a buffer of ready messages
 * plus a queue of pending receive futures. Both are confined to one pinned executor
 * (obtained from the client's external executor provider, so one thread per consumer),
 * which means a message and a waiter can never cross — no locks, and no lost wakeups.
 * Every receive future is completed on that executor, so user continuations chained on
 * the returned {@link CompletableFuture} never run on a netty IO thread.
 *
 * <p>This replaces the previous {@code LinkedTransferQueue} + {@code supplyAsync(take())}
 * approach: {@link #receiveAsync()} parks no thread, is cancelable, and honours timeouts
 * via the client timer instead of blocking a {@code ForkJoinPool.commonPool()} worker.
 *
 * <p>Backpressure: {@link #offer} returns a future that completes only when the buffer has
 * room. Producers gate re-arming on it, so the buffer is bounded by {@code receiverQueueSize}
 * plus one in-flight message per producer (each producer offers at most one message before
 * observing the pause) — modelled on v4 {@code MultiTopicsConsumerImpl}'s pause/resume of
 * sub-consumers, including its fairness rule: once any producer is paused, further offers
 * pause at the half-way mark too, so active producers can't hold the buffer above the resume
 * threshold and starve the paused ones.
 */
final class V5ReceiveQueue<T> {

    /** Shared pre-completed capacity grant for the fast path — no allocation, no executor hop. */
    private static final CompletableFuture<Void> READY = CompletableFuture.completedFuture(null);

    private final ExecutorService executor;
    private final Timer timer;
    /** Producers pause once the buffer reaches this size, and resume once it drains to half. */
    private final int highWatermark;
    private final int lowWatermark;

    // All three touched only on `executor`, so plain (non-concurrent) collections are safe.
    private final ArrayDeque<Message<T>> buffer = new ArrayDeque<>();
    private final ArrayDeque<CompletableFuture<Message<T>>> pendingReceives = new ArrayDeque<>();
    // Capacity futures handed back to producers that were paused because the buffer was full.
    private final ArrayDeque<CompletableFuture<Void>> capacityWaiters = new ArrayDeque<>();
    private boolean closed = false;

    // Snapshots of executor-confined state, readable from producer threads so the offer()
    // fast path can decide without a hop. Written only on `executor`; may lag by the offers
    // still queued, so each producer (one in-flight offer at a time) can overshoot by one.
    private volatile int approxBufferSize = 0;
    private volatile boolean producersPaused = false;

    V5ReceiveQueue(ExecutorService executor, Timer timer, int receiverQueueSize) {
        this.executor = executor;
        this.timer = timer;
        this.highWatermark = Math.max(1, receiverQueueSize);
        this.lowWatermark = highWatermark / 2;
    }

    /**
     * Deposit a freshly-arrived message. Called from the per-segment receive loops (which
     * run on a v4 client executor). Hands the message straight to a waiting receive future
     * if there is one, otherwise buffers it.
     *
     * @return a future that completes when the sink is ready for the next message — right
     *     away unless the buffer is filling up, in which case it defers until the consumer
     *     drains it below the low watermark (backpressure).
     */
    CompletableFuture<Void> offer(Message<T> msg) {
        // Fast path, decided on the caller thread: while the buffer is comfortably below the
        // watermarks and nobody is paused, grant capacity with a shared completed future so
        // the (fast-consumer) hot path pays no allocation and no serialized hop through our
        // executor before the segment loop re-arms.
        if (!producersPaused && approxBufferSize < lowWatermark) {
            executor.execute(() -> doOffer(msg, null));
            return READY;
        }
        CompletableFuture<Void> capacity = new CompletableFuture<>();
        executor.execute(() -> doOffer(msg, capacity));
        return capacity;
    }

    /** Runs on {@code executor}. {@code capacity} is null when the fast path already granted it. */
    private void doOffer(Message<T> msg, CompletableFuture<Void> capacity) {
        if (closed) {
            if (capacity != null) {
                capacity.complete(null);
            }
            return;
        }
        CompletableFuture<Message<T>> waiter = pollWaiter();
        if (waiter != null) {
            // Handed straight to a waiting receiver; the buffer didn't grow.
            waiter.complete(msg);
            if (capacity != null) {
                capacity.complete(null);
            }
            return;
        }
        buffer.add(msg);
        approxBufferSize = buffer.size();
        if (capacity == null) {
            return;
        }
        // Pause when full — or, once any producer is paused, already at the half-way mark, so
        // active producers can't keep the buffer hovering above the resume threshold while the
        // paused ones starve (v4 MultiTopicsConsumerImpl's fairness clause).
        if (buffer.size() >= highWatermark
                || (!capacityWaiters.isEmpty() && buffer.size() > lowWatermark)) {
            capacityWaiters.add(capacity);
            producersPaused = true;
        } else {
            capacity.complete(null);
        }
    }

    /** Resume paused producers once the buffer has drained to the low watermark. */
    private void maybeResumeProducers() {
        if (buffer.size() <= lowWatermark && !capacityWaiters.isEmpty()) {
            CompletableFuture<Void> capacity;
            while ((capacity = capacityWaiters.poll()) != null) {
                // Post each completion as its own task (as v4 does) instead of completing
                // inline: each grant synchronously runs a segment loop's re-arm, and a wide
                // release would otherwise stall queued user receive completions behind it.
                CompletableFuture<Void> c = capacity;
                executor.execute(() -> c.complete(null));
            }
            producersPaused = false;
        }
    }

    /** Receive a message, completing as soon as one is available. Never blocks a thread. */
    CompletableFuture<Message<T>> receiveAsync() {
        CompletableFuture<Message<T>> result = new CompletableFuture<>();
        executor.execute(() -> {
            if (closed) {
                result.completeExceptionally(alreadyClosed());
                return;
            }
            Message<T> msg = buffer.poll();
            if (msg != null) {
                approxBufferSize = buffer.size();
                result.complete(msg);
                maybeResumeProducers();
            } else {
                pendingReceives.add(result);
            }
        });
        return result;
    }

    /**
     * Receive a message, completing with {@code null} if none arrives within {@code timeout}.
     * The timeout is armed on the client timer; no thread is parked while waiting.
     */
    CompletableFuture<Message<T>> receiveAsync(Duration timeout) {
        CompletableFuture<Message<T>> result = new CompletableFuture<>();
        executor.execute(() -> {
            if (closed) {
                result.completeExceptionally(alreadyClosed());
                return;
            }
            Message<T> msg = buffer.poll();
            if (msg != null) {
                approxBufferSize = buffer.size();
                result.complete(msg);
                maybeResumeProducers();
                return;
            }
            long millis = timeout.toMillis();
            if (millis <= 0) {
                result.complete(null);
                return;
            }
            pendingReceives.add(result);
            Timeout t = timer.newTimeout(ignored -> executor.execute(() -> {
                if (!result.isDone()) {
                    pendingReceives.remove(result);
                    result.complete(null);
                }
            }), millis, TimeUnit.MILLISECONDS);
            // Cancel the timer when the message is handed off (or on close) so it doesn't linger.
            result.whenComplete((r, e) -> t.cancel());
        });
        return result;
    }

    /**
     * Receive up to {@code maxMessages}, blocking (asynchronously) up to {@code timeout} for
     * the batch. Waits for the first message, then opportunistically drains whatever else is
     * already buffered, repeating until the batch is full or the deadline passes.
     */
    CompletableFuture<List<Message<T>>> receiveMultiAsync(int maxMessages, Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        CompletableFuture<List<Message<T>>> result = new CompletableFuture<>();
        collectMulti(new ArrayList<>(), maxMessages, deadlineNanos, result);
        return result;
    }

    private void collectMulti(List<Message<T>> batch, int max, long deadlineNanos,
                              CompletableFuture<List<Message<T>>> result) {
        if (batch.size() >= max) {
            result.complete(batch);
            return;
        }
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            result.complete(batch);
            return;
        }
        receiveAsync(Duration.ofNanos(remainingNanos)).whenComplete((msg, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else if (msg == null) {
                result.complete(batch);
            } else {
                batch.add(msg);
                drainReady(batch, max).thenRun(() -> collectMulti(batch, max, deadlineNanos, result));
            }
        });
    }

    /** Move whatever is already buffered into {@code batch} (up to {@code max} total). */
    private CompletableFuture<Void> drainReady(List<Message<T>> batch, int max) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        executor.execute(() -> {
            Message<T> m;
            while (batch.size() < max && (m = buffer.poll()) != null) {
                batch.add(m);
            }
            approxBufferSize = buffer.size();
            maybeResumeProducers();
            done.complete(null);
        });
        return done;
    }

    // --- Blocking views, for the synchronous receive() API. Block only the caller's thread. ---

    Message<T> take() throws PulsarClientException {
        try {
            return receiveAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    Message<T> poll(Duration timeout) throws PulsarClientException {
        try {
            return receiveAsync(timeout).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    List<Message<T>> receiveMulti(int maxMessages, Duration timeout) throws PulsarClientException {
        try {
            return receiveMultiAsync(maxMessages, timeout).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    /** Fail any outstanding receives so blocked/awaiting callers wake instead of hanging forever. */
    void close() {
        executor.execute(() -> {
            closed = true;
            CompletableFuture<Message<T>> waiter;
            while ((waiter = pendingReceives.poll()) != null) {
                if (!waiter.isDone()) {
                    waiter.completeExceptionally(alreadyClosed());
                }
            }
            // Release any paused producers so their receive loops re-arm and observe the close.
            CompletableFuture<Void> capacity;
            while ((capacity = capacityWaiters.poll()) != null) {
                capacity.complete(null);
            }
            producersPaused = false;
            buffer.clear();
            approxBufferSize = 0;
        });
    }

    private CompletableFuture<Message<T>> pollWaiter() {
        CompletableFuture<Message<T>> waiter;
        // Skip futures already completed by cancellation or timeout.
        while ((waiter = pendingReceives.poll()) != null) {
            if (!waiter.isDone()) {
                return waiter;
            }
        }
        return null;
    }

    private static PulsarClientException alreadyClosed() {
        return new PulsarClientException.AlreadyClosedException("Consumer already closed");
    }

    private static PulsarClientException unwrap(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof PulsarClientException pce) {
            return pce;
        }
        return new PulsarClientException(cause != null ? cause : e);
    }
}
