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

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

/**
 * Async-native receive queue shared by the v5 scalable consumers.
 *
 * <p>Mirrors the v4 {@code ConsumerBase} delivery model. The buffer of ready messages is a
 * thread-safe blocking queue: producers (the per-segment receive loops, on a v4 client executor)
 * append to it directly, and the blocking {@link #take()}/{@link #poll(Duration)} pull from it on
 * the caller's own thread, so in steady state a message never has to cross to another thread just
 * to be handed over. Only the bookkeeping that needs ordering — the pending {@link #receiveAsync()}
 * futures and the paused producers — is confined to one pinned executor (obtained from the
 * client's external executor provider, so one thread per consumer), and every async receive
 * future is completed there, so user continuations chained on it never run on a netty IO thread.
 *
 * <p>Backpressure: {@link #offer} returns a future that completes only when the buffer has room.
 * Producers gate re-arming on it, so the buffer is bounded by {@code receiverQueueSize} plus one
 * in-flight message per producer (each producer offers at most one message before observing the
 * pause) — modelled on v4 {@code MultiTopicsConsumerImpl}'s pause/resume of sub-consumers,
 * including its fairness rule: once any producer is paused, further offers pause at the half-way
 * mark too, so active producers can't hold the buffer above the resume threshold and starve the
 * paused ones.
 */
final class V5ReceiveQueue<T> {

    /** Shared pre-completed capacity grant for the fast path — no allocation, no executor hop. */
    private static final CompletableFuture<Void> READY = CompletableFuture.completedFuture(null);

    private final ExecutorService executor;
    private final Timer timer;
    /** Producers pause once the buffer reaches this size, and resume once it drains to half. */
    private final int highWatermark;
    private final int lowWatermark;

    /**
     * Ready messages. Thread-safe: producers append from their own threads, blocking receives
     * poll from the caller thread, and the executor drains it for pending async receives.
     */
    private final GrowableArrayBlockingQueue<Message<T>> buffer = new GrowableArrayBlockingQueue<>();

    // Both touched only on `executor`, so plain (non-concurrent) collections are safe.
    private final ArrayDeque<CompletableFuture<Message<T>>> pendingReceives = new ArrayDeque<>();
    // Capacity futures handed back to producers that were paused because the buffer was full.
    private final ArrayDeque<CompletableFuture<Void>> capacityWaiters = new ArrayDeque<>();

    // Snapshots of the executor-confined state, readable from producer and receiver threads so
    // the hot paths can decide without a hop. Written only on `executor` (except `closed`).
    /** True while {@link #pendingReceives} may hold a future that the next message belongs to. */
    private volatile boolean hasPendingReceives = false;
    private volatile boolean producersPaused = false;
    private volatile boolean closed = false;

    /** Test seam: runs on the executor between the pause decision and its publication. */
    @VisibleForTesting
    volatile Runnable beforePausePublishedHook;

    V5ReceiveQueue(ExecutorService executor, Timer timer, int receiverQueueSize) {
        this.executor = executor;
        this.timer = timer;
        this.highWatermark = Math.max(1, receiverQueueSize);
        this.lowWatermark = highWatermark / 2;
    }

    /**
     * Deposit a freshly-arrived message. Called from the per-segment receive loops (which
     * run on a v4 client executor). Appends straight to the buffer — waking a blocked
     * {@link #take()}/{@link #poll(Duration)} if it was empty — and hands it over on the
     * executor only if an async receive is waiting.
     *
     * @return a future that completes when the sink is ready for the next message — right
     *     away unless the buffer is full, in which case it defers until the consumer drains it
     *     below the low watermark (backpressure).
     */
    CompletableFuture<Void> offer(Message<T> msg) {
        if (closed) {
            // Dropped, like a message arriving on an already-closed v4 consumer.
            return READY;
        }
        buffer.put(msg);
        if (hasPendingReceives) {
            // A receiver registering concurrently re-checks the buffer after publishing the
            // flag, and we read the flag after appending, so one side always sees the other.
            executor.execute(this::drainToPendingReceives);
        }
        // Fast path, decided on the caller thread: while the buffer is below the high watermark
        // and nobody is paused, grant capacity with a shared completed future so the hot path
        // pays no allocation and no serialized hop through our executor before the segment
        // loop re-arms.
        if (!producersPaused && buffer.size() < highWatermark) {
            return READY;
        }
        CompletableFuture<Void> capacity = new CompletableFuture<>();
        executor.execute(() -> grantCapacity(capacity));
        return capacity;
    }

    /** Runs on {@code executor}: park the producer while the buffer is full, else grant now. */
    private void grantCapacity(CompletableFuture<Void> capacity) {
        // Pause when full — or, once any producer is paused, already at the half-way mark, so
        // active producers can't keep the buffer hovering above the resume threshold while the
        // paused ones starve (v4 MultiTopicsConsumerImpl's fairness clause).
        if (!closed && (buffer.size() >= highWatermark
                || (!capacityWaiters.isEmpty() && buffer.size() > lowWatermark))) {
            Runnable hook = beforePausePublishedHook;
            if (hook != null) {
                hook.run();
            }
            capacityWaiters.add(capacity);
            producersPaused = true;
            // A direct receive that drained the buffer between the check above and this
            // publication saw producersPaused == false and posted no resume. Look again now that
            // the pause is visible: that receive decrements the size before reading the flag and
            // we publish the flag before reading the size, so one side always sees the other.
            maybeResumeProducers();
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

    /** After a receive pulled straight from the buffer: resume producers once it drained enough. */
    private void afterDirectPoll() {
        if (producersPaused && buffer.size() <= lowWatermark) {
            executor.execute(this::maybeResumeProducers);
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
            if (result.isDone()) {
                // Cancelled before we got here: it must not consume a message that has since
                // been appended straight to the buffer.
                return;
            }
            Message<T> msg = pollBehindPendingReceives(result);
            if (msg != null) {
                result.complete(msg);
                maybeResumeProducers();
            } else if (!result.isDone()) {
                addPendingReceive(result);
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
            if (result.isDone()) {
                // Cancelled before we got here: it must not consume a message that has since
                // been appended straight to the buffer.
                return;
            }
            Message<T> msg = pollBehindPendingReceives(result);
            if (msg != null) {
                result.complete(msg);
                maybeResumeProducers();
                return;
            }
            if (result.isDone()) {
                return;
            }
            long millis = timeout.toMillis();
            if (millis <= 0) {
                result.complete(null);
                return;
            }
            Timeout t = timer.newTimeout(ignored -> executor.execute(() -> {
                if (!result.isDone()) {
                    pendingReceives.remove(result);
                    hasPendingReceives = !pendingReceives.isEmpty();
                    result.complete(null);
                }
            }), millis, TimeUnit.MILLISECONDS);
            // Cancel the timer when the message is handed off (or on close) so it doesn't linger.
            result.whenComplete((r, e) -> t.cancel());
            addPendingReceive(result);
        });
        return result;
    }

    /**
     * Runs on {@code executor}: the next buffered message for a new receive — unless older
     * receives are still waiting. Messages are appended to the buffer without going through the
     * executor, so a message can sit there while an older receive is parked; that receive was
     * registered first and must be served first, so hand the older ones what is buffered and
     * report nothing for the newcomer while any of them remains. Completing an older receive runs
     * its continuations inline, and one of them may cancel the newcomer: it must then not consume
     * a message either.
     */
    private Message<T> pollBehindPendingReceives(CompletableFuture<?> newcomer) {
        if (!pendingReceives.isEmpty()) {
            drainToPendingReceives();
            if (!pendingReceives.isEmpty() || newcomer.isDone()) {
                return null;
            }
        }
        return buffer.poll();
    }

    /** Runs on {@code executor}: park an async receive until the next message arrives. */
    private void addPendingReceive(CompletableFuture<Message<T>> result) {
        pendingReceives.add(result);
        hasPendingReceives = true;
        // A producer that appended between our poll and this flag store saw the flag clear and
        // posted no drain, so look again now that the flag is published (both are volatile).
        drainToPendingReceives();
    }

    /** Runs on {@code executor}: hand buffered messages to pending async receives, in order. */
    private void drainToPendingReceives() {
        CompletableFuture<Message<T>> waiter;
        while ((waiter = pollWaiter()) != null) {
            Message<T> msg = buffer.poll();
            if (msg == null) {
                pendingReceives.addFirst(waiter);
                break;
            }
            waiter.complete(msg);
        }
        hasPendingReceives = !pendingReceives.isEmpty();
        maybeResumeProducers();
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
                drainReady(batch, max, result).thenRun(() -> collectMulti(batch, max, deadlineNanos, result));
            }
        });
    }

    /** Move whatever is already buffered into {@code batch} (up to {@code max} total). */
    private CompletableFuture<Void> drainReady(List<Message<T>> batch, int max,
                                               CompletableFuture<List<Message<T>>> result) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        executor.execute(() -> {
            Message<T> m;
            while (batch.size() < max && !result.isDone() && (m = pollBehindPendingReceives(result)) != null) {
                batch.add(m);
            }
            maybeResumeProducers();
            done.complete(null);
        });
        return done;
    }

    // --- Blocking views, for the synchronous receive() API. Pull straight from the buffer on the
    // caller's thread; it parks only while the buffer is empty. ---

    Message<T> take() throws PulsarClientException {
        if (closed) {
            throw alreadyClosed();
        }
        Message<T> msg;
        try {
            msg = buffer.take();
        } catch (InterruptedException e) {
            if (closed) {
                // close() terminates the buffer, which wakes blocked takers this way.
                throw alreadyClosed();
            }
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        }
        afterDirectPoll();
        return msg;
    }

    Message<T> poll(Duration timeout) throws PulsarClientException {
        if (closed) {
            throw alreadyClosed();
        }
        Message<T> msg;
        try {
            msg = buffer.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Receive interrupted", e);
        }
        if (msg == null) {
            if (closed) {
                throw alreadyClosed();
            }
            return null;
        }
        afterDirectPoll();
        return msg;
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
        closed = true;
        // Wake blocking receivers parked on the buffer; anything offered from now on is dropped.
        buffer.terminate(null);
        executor.execute(() -> {
            CompletableFuture<Message<T>> waiter;
            while ((waiter = pendingReceives.poll()) != null) {
                if (!waiter.isDone()) {
                    waiter.completeExceptionally(alreadyClosed());
                }
            }
            hasPendingReceives = false;
            // Release any paused producers so their receive loops re-arm and observe the close.
            CompletableFuture<Void> capacity;
            while ((capacity = capacityWaiters.poll()) != null) {
                capacity.complete(null);
            }
            producersPaused = false;
            buffer.clear();
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
