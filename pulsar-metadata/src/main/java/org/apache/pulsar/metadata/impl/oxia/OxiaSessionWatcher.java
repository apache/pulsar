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
package org.apache.pulsar.metadata.impl.oxia;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.api.options.PutOption;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

/**
 * Delivers the metadata store session events on the oxia backend by watching a private ephemeral
 * canary record.
 *
 * <p>Oxia does not expose session lifecycle events: the session machinery is deliberately kept
 * internal to the client, and the only public observation surface is the notification stream.
 * The per-record {@code KeyDeleted} notification that the server emits for every ephemeral record
 * swept by a session expiry is therefore the intended way for an embedding application to learn
 * about a session loss. This watcher applies exactly that mechanism: each store instance owns a
 * canary record, under the reserved {@link #CANARY_KEY_PREFIX} namespace, that nobody but the
 * session itself deletes. The deletion of the canary is a server-authoritative signal that the
 * session died, and maps to {@link SessionEvent#SessionLost}; a subsequently successful canary
 * write means a fresh session was established and maps to
 * {@link SessionEvent#SessionReestablished}.
 *
 * <p>Each loss moves to a fresh incarnation of the canary key, a monotonically increasing
 * generation suffixed to the instance-unique prefix. Notifications and write completions are only
 * honored for the current incarnation: a {@code KeyDeleted} of an expired incarnation delivered
 * late — the notification stream resumes from its last offset after a reconnect — cannot flip a
 * live session to lost, and the completion of a write issued before a loss cannot report a
 * recovery. Recovery is only ever reported by a write issued after the loss.
 *
 * <p>While the session is considered established, a periodic existence check of the canary backs
 * the notification stream up: a deletion whose notification was lost, or that was delivered as a
 * range delete, is detected by the next check instead of being missed forever. The same check
 * monitors connectivity: the first failed check reports {@link SessionEvent#ConnectionLost},
 * check failures spanning a full session timeout report {@link SessionEvent#SessionLost}, and a
 * successful check after a connection loss reports {@link SessionEvent#Reconnected}. A failing
 * check counts as no answer whatever its cause; the window is the session timeout because that is
 * the window after which the server reaps the session — it is the timeout the client sends in
 * {@code CreateSessionRequest} — and after which the oxia client itself expires the session. If
 * connectivity returns within the window and the session survived, the pessimistic SessionLost is
 * followed by a prompt SessionReestablished once the canary is rewritten: the same trade-off
 * {@code ZKSessionWatcher} makes at its monitor timeout.
 *
 * <p>No ordering is guaranteed between the synthesized session events and the per-key
 * notifications of the same sweep: a notification batch is a map on the wire and carries no
 * per-entry order, so consumers must not rely on SessionLost preceding the Deleted notifications
 * of the records swept with the canary.
 *
 * <p>Known limitation: the oxia client keeps one session per shard and the canary lives on a
 * single shard, so a session loss confined to a different shard — for example the clean session
 * close of a shard rebalance, which sweeps that shard's ephemeral records without an expiry — is
 * not reported by this watcher. The common loss of the session, a server restart or outage, kills
 * the sessions of all shards together with the canary and is fully covered.
 */
@CustomLog
final class OxiaSessionWatcher implements AutoCloseable {

    /**
     * Reserved namespace of the session canary records. It must not be used for anything else and
     * it must keep sorting before every {@code '/'}-rooted path, so that the canaries stay
     * outside the {@code '/'} hierarchy the store exposes to ordinary listings.
     */
    static final String CANARY_KEY_PREFIX = ".pulsar-oxia-session-canary-";

    /** Fallback session timeout for store instances built directly around a client. */
    static final long DEFAULT_SESSION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);

    private static final byte[] CANARY_VALUE = new byte[0];
    private static final long MAX_CHECK_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final long MIN_CHECK_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(1);
    private static final long WRITE_RETRY_MIN_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(1);
    private static final long WRITE_RETRY_MAX_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(8);

    private final AsyncOxiaClient client;
    private final Consumer<SessionEvent> eventSink;

    /** Instance-unique prefix of the canary keys; the generation of the moment is appended. */
    private final String canaryKeyPrefix;

    private final long sessionTimeoutMillis;
    private final long checkIntervalMillis;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory("oxia-metadata-session-watcher"));

    /**
     * Set while a canary write is scheduled or in flight, so that at most one write chain exists
     * at any time.
     */
    private final AtomicBoolean canaryWritePending = new AtomicBoolean();

    private final Object stateLock = new Object();
    private State state = State.STARTING;

    /**
     * Generation of the current canary incarnation and its key. Guarded by {@link #stateLock};
     * the key is additionally readable without the lock for notification matching, where a stale
     * read can only funnel into a loss transition that a newer generation already made a no-op.
     */
    private int generation;
    private volatile String canaryKey;

    /**
     * Set under {@link #stateLock} when the watcher closes, so that no state transition — and
     * with it no listener dispatch — can interleave with the close. Volatile for the executor
     * fast paths that do not dispatch.
     */
    private volatile boolean closed;

    /** Retry counter of the current canary write chain; only accessed on the executor. */
    private int writeFailures;

    /**
     * When the current streak of failing existence checks began, or zero outside a streak. Only
     * accessed on the executor.
     */
    private long checkFailedSinceMillis;

    private enum State {
        STARTING, ESTABLISHED, DISCONNECTED, LOST
    }

    OxiaSessionWatcher(AsyncOxiaClient client, Consumer<SessionEvent> eventSink,
            long sessionTimeoutMillis) {
        this.client = client;
        this.eventSink = eventSink;
        this.canaryKeyPrefix = CANARY_KEY_PREFIX + UUID.randomUUID() + "-";
        this.sessionTimeoutMillis = sessionTimeoutMillis;
        this.checkIntervalMillis = Math.max(MIN_CHECK_INTERVAL_MILLIS,
                Math.min(MAX_CHECK_INTERVAL_MILLIS, sessionTimeoutMillis / 3));
        this.canaryKey = canaryKeyPrefix + generation;
    }

    /**
     * Whether the notification belongs to the reserved canary namespace and must not be forwarded
     * as an ordinary metadata store notification.
     */
    static boolean isCanaryKey(Notification notification) {
        return notification.key().startsWith(CANARY_KEY_PREFIX);
    }

    void start() {
        ensureCanaryWrite();
        try {
            executor.scheduleWithFixedDelay(this::checkCanarySafely, checkIntervalMillis,
                    checkIntervalMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignore) {
            // The watcher was closed concurrently with the store construction.
        }
    }

    /**
     * Routes a notification from the canary namespace: only the deletion of the canary of the
     * current incarnation means the session was lost, every other canary notification is ignored.
     */
    void handleNotification(Notification notification) {
        if (closed) {
            return;
        }
        if (notification instanceof Notification.KeyDeleted && notification.key().equals(canaryKey)) {
            onCanaryGone("deleted");
        }
    }

    private void checkCanarySafely() {
        try {
            checkCanary();
        } catch (Throwable t) {
            log.warn().exception(t).log("Unexpected failure while checking the oxia session canary");
        }
    }

    /**
     * Existence check of the canary, backing the notification stream up while the session is
     * considered established and monitoring the connectivity to the server. Package-private so
     * that tests can drive it directly.
     */
    void checkCanary() {
        String keyAtIssue;
        int generationAtIssue;
        synchronized (stateLock) {
            if (state != State.ESTABLISHED && state != State.DISCONNECTED) {
                return;
            }
            keyAtIssue = canaryKey;
            generationAtIssue = generation;
        }
        try {
            client.get(keyAtIssue).whenComplete((result, ex) -> hop(() -> {
                if (ex != null) {
                    onCheckFailed(ex);
                } else if (result != null) {
                    onCheckSucceeded();
                } else {
                    boolean staleRead;
                    synchronized (stateLock) {
                        staleRead = closed || generation != generationAtIssue;
                    }
                    // A read of an expired incarnation finds its key missing, but that loss was
                    // already reported when the incarnation was replaced.
                    if (!staleRead) {
                        onCanaryGone("missing");
                    }
                }
            }));
        } catch (RejectedExecutionException | IllegalStateException ignore) {
            // The client was closed concurrently.
        }
    }

    private void onCheckFailed(Throwable ex) {
        long now = System.currentTimeMillis();
        boolean sessionTimedOut;
        synchronized (stateLock) {
            if (closed || state == State.LOST || state == State.STARTING) {
                return;
            }
            if (state == State.ESTABLISHED) {
                state = State.DISCONNECTED;
                checkFailedSinceMillis = now;
                log.warn().exception(ex).log("Oxia metadata store connection was lost");
                eventSink.accept(SessionEvent.ConnectionLost);
            }
            sessionTimedOut = now - checkFailedSinceMillis >= sessionTimeoutMillis;
        }
        if (sessionTimedOut) {
            // The checks could not reach the server for a full session timeout, by which point
            // the server has reaped the session. Report the loss now instead of waiting for the
            // connectivity to return and confirm it.
            onCanaryGone("unreachable");
        }
    }

    private void onCheckSucceeded() {
        synchronized (stateLock) {
            if (closed || state != State.DISCONNECTED) {
                return;
            }
            state = State.ESTABLISHED;
            checkFailedSinceMillis = 0L;
            log.info().log("Oxia metadata store connection was re-established");
            eventSink.accept(SessionEvent.Reconnected);
        }
    }

    private void onCanaryGone(String cause) {
        // The events are dispatched while holding the lock, so that the listeners observe them
        // in exactly the order of the state transitions, and so that no transition can inter
        // leave with the close. The dispatch is non-blocking.
        synchronized (stateLock) {
            if (closed || state == State.LOST) {
                return;
            }
            state = State.LOST;
            generation++;
            canaryKey = canaryKeyPrefix + generation;
            log.info().attr("canaryKey", canaryKey).attr("cause", cause)
                    .log("Oxia metadata store session was lost");
            eventSink.accept(SessionEvent.SessionLost);
        }
        ensureCanaryWrite();
    }

    /**
     * Arms the canary write if no write chain is already scheduled or in flight: the first write
     * of a chain retries with backoff until it succeeds, so re-arming is only needed when the
     * chain came to rest on a successfully written canary.
     */
    private void ensureCanaryWrite() {
        if (canaryWritePending.compareAndSet(false, true)) {
            scheduleCanaryWrite(0, true);
        }
    }

    private void scheduleCanaryWrite(long delayMillis, boolean resetFailures) {
        try {
            executor.schedule(() -> writeCanary(resetFailures), delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignore) {
            canaryWritePending.set(false);
        }
    }

    private void writeCanary(boolean resetFailures) {
        if (closed) {
            canaryWritePending.set(false);
            return;
        }
        if (resetFailures) {
            writeFailures = 0;
        }
        String keyAtIssue;
        int generationAtIssue;
        synchronized (stateLock) {
            keyAtIssue = canaryKey;
            generationAtIssue = generation;
        }
        try {
            client.put(keyAtIssue, CANARY_VALUE, Set.of(PutOption.AsEphemeralRecord))
                    .whenComplete((result, ex) -> hop(() -> onCanaryWriteCompleted(generationAtIssue, ex)));
        } catch (RuntimeException e) {
            hop(() -> onCanaryWriteCompleted(generationAtIssue, e));
        }
    }

    private void onCanaryWriteCompleted(int generationAtIssue, Throwable ex) {
        boolean appliesToCurrentIncarnation;
        synchronized (stateLock) {
            appliesToCurrentIncarnation = !closed && generation == generationAtIssue;
            if (appliesToCurrentIncarnation && ex == null) {
                boolean wasLost = state == State.LOST;
                state = State.ESTABLISHED;
                if (wasLost) {
                    log.info().attr("canaryKey", canaryKey)
                            .log("Oxia metadata store session was re-established");
                    eventSink.accept(SessionEvent.SessionReestablished);
                }
            }
        }
        if (!appliesToCurrentIncarnation) {
            // The write belongs to an incarnation that was lost in the meantime, or the watcher
            // was closed: rest the chain and, if still open, arm a fresh write for the current
            // incarnation.
            canaryWritePending.set(false);
            if (!closed) {
                ensureCanaryWrite();
            }
            return;
        }
        if (ex == null) {
            canaryWritePending.set(false);
            return;
        }

        writeFailures++;
        if (writeFailures == 1) {
            log.warn().exception(ex).log("Failed to write the oxia session canary, will retry");
        } else {
            log.debug().exception(ex).log("Failed to write the oxia session canary, will retry");
        }
        scheduleCanaryWrite(writeRetryDelay(), false);
    }

    private long writeRetryDelay() {
        long delay = WRITE_RETRY_MIN_DELAY_MILLIS << Math.min(writeFailures - 1, 3);
        return Math.min(delay, WRITE_RETRY_MAX_DELAY_MILLIS);
    }

    private void hop(Runnable task) {
        try {
            executor.execute(task);
        } catch (RejectedExecutionException ignore) {
            // The watcher was closed concurrently.
        }
    }

    @Override
    public void close() {
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            // Set under the lock: every event dispatch happens under the same lock, so after
            // this point no listener can be invoked, not even by a notification that a
            // concurrent client close is still delivering.
            closed = true;
        }
        executor.shutdownNow();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
