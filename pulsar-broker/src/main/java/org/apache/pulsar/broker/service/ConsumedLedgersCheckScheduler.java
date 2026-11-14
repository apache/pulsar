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

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * Submits maintenance for at most one topic every 20 milliseconds, starting after one retention check interval.
 * The interval is the minimum time between round starts; a slow round stretches it without catch-up submissions.
 * The snapshot contains only names; each check resolves the current topic instance.
 */
@CustomLog
final class ConsumedLedgersCheckScheduler implements AutoCloseable {
    private static final long TOPIC_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
    private static final long PROGRESS_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final Map<String, CompletableFuture<Optional<Topic>>> topics;
    private final LongSupplier nanoTime;
    private volatile boolean closed;
    // Scheduling and round publication are guarded by this; only the scheduler thread mutates a round.
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledTask;
    private long intervalNanos;
    private Round currentRound;

    ConsumedLedgersCheckScheduler(Map<String, CompletableFuture<Optional<Topic>>> topics) {
        this(topics, System::nanoTime);
    }

    @VisibleForTesting
    ConsumedLedgersCheckScheduler(Map<String, CompletableFuture<Optional<Topic>>> topics, LongSupplier nanoTime) {
        this.topics = topics;
        this.nanoTime = nanoTime;
    }

    synchronized void start(ScheduledExecutorService executor, Duration interval) {
        if (this.executor != null || closed) {
            return;
        }
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("Retention check interval must be positive");
        }
        this.executor = executor;
        this.intervalNanos = interval.toNanos();
        scheduleNext(intervalNanos);
    }

    private void runCheck() {
        long delayNanos = intervalNanos;
        try {
            Round round;
            synchronized (this) {
                if (closed) {
                    return;
                }
                scheduledTask = null;
                round = currentRound;
            }
            if (round == null) {
                long startedAtNanos = nanoTime.getAsLong();
                round = new Round(new ArrayDeque<>(topics.keySet()), startedAtNanos);
                synchronized (this) {
                    if (closed) {
                        return;
                    }
                    currentRound = round;
                }
                logProgress(round, "started");
            }
            String name;
            // Skip unavailable topics immediately; failed submission attempts still consume a pacing slot.
            while (!closed && (name = round.topics.poll()) != null) {
                if (submitChecks(name)) {
                    break;
                }
            }
            long now = nanoTime.getAsLong();
            delayNanos = TOPIC_DELAY_NANOS;
            if (round.topics.isEmpty()) {
                logProgress(round, "completed");
                synchronized (this) {
                    currentRound = null;
                }
                delayNanos = Math.max(TOPIC_DELAY_NANOS, intervalNanos - (now - round.startedAtNanos));
            } else if (now - round.lastProgressNanos >= PROGRESS_INTERVAL_NANOS) {
                round.lastProgressNanos = now;
                logProgress(round, "progress");
            }
        } catch (Exception e) {
            log.warn().exception(e).log("Failed to check consumed ledgers");
        } finally {
            // Wait after each attempt, including failures; never catch up with a burst after a delay.
            scheduleNext(delayNanos);
        }
    }

    private boolean submitChecks(String name) {
        try {
            CompletableFuture<Optional<Topic>> future = topics.get(name);
            if (future == null
                    || !(BrokerService.extractTopic(future).orElse(null) instanceof PersistentTopic topic)
                    || topic.isFenced() || topic.isClosingOrDeleting()) {
                return false;
            }
            ManagedLedger managedLedger = topic.getManagedLedger();
            if (managedLedger == null) {
                return false;
            }
            managedLedger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
            managedLedger.rolloverCursorsInBackground();
        } catch (Exception e) {
            log.debug().attr("topic", name).exception(e).log("Failed to submit consumed ledger checks");
        }
        return true;
    }

    private void logProgress(Round round, String phase) {
        log.debug().attr("phase", phase)
                .attr("totalTopics", round.totalTopics).attr("remainingTopics", round.topics.size())
                .attr("elapsedMs", TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - round.startedAtNanos))
                .log("Consumed ledger check submission round");
    }

    private synchronized void scheduleNext(long delayNanos) {
        if (!closed) {
            try {
                scheduledTask = executor.schedule(this::runCheck, delayNanos, TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException e) {
                closed = true;
                currentRound = null;
                log.debug().exception(e).log("Consumed ledger check scheduling was rejected");
            }
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        currentRound = null;
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
            scheduledTask = null;
        }
    }

    private static final class Round {
        private final Queue<String> topics;
        private final int totalTopics;
        private final long startedAtNanos;
        private long lastProgressNanos;

        private Round(Queue<String> topics, long startedAtNanos) {
            this.topics = topics;
            this.totalTopics = topics.size();
            this.startedAtNanos = startedAtNanos;
            this.lastProgressNanos = startedAtNanos;
        }
    }
}
