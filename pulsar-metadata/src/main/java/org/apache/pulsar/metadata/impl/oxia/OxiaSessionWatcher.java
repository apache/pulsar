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
 * internal to the client, and the intended way for an embedding application to learn about a
 * session loss is the per-record {@code KeyDeleted} notification that the server emits for every
 * ephemeral record swept by the session expiry. This watcher applies exactly that mechanism: each
 * store instance owns a canary record, under the reserved {@link #CANARY_KEY_PREFIX} namespace,
 * that nobody but the session itself deletes. The deletion of the canary is therefore a
 * server-authoritative signal that the session died, and maps to
 * {@link SessionEvent#SessionLost}; a subsequently successful canary write means a fresh session
 * was established and maps to {@link SessionEvent#SessionReestablished}.
 *
 * <p>The canary key sorts before every {@code '/'}-rooted path, because {@code '.'} sorts before
 * {@code '/'}, and the server currently emits the notifications of a session sweep sorted by key
 * within a single batch. Within the canary's shard, the store therefore observes the synthesized
 * SessionLost before the Deleted notifications of the swept records of the same batch. Sweeps of
 * other shards travel on their own notification streams and carry no such ordering, which is
 * benign: they are independent sessions.
 *
 * <p>While the session is considered established, a periodic existence check of the canary backs
 * the notification stream up: a deletion whose notification was lost, for example because the
 * stream was down for longer than the server-side notification retention, is detected by the next
 * check instead of being missed forever. The same check also covers a deletion delivered as a
 * range delete, which is not matched on the notification path.
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
     * it must keep sorting before every {@code '/'}-rooted path.
     */
    static final String CANARY_KEY_PREFIX = ".pulsar-oxia-session-canary-";

    private static final byte[] CANARY_VALUE = new byte[0];
    private static final long CHECK_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final long WRITE_RETRY_MIN_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(1);
    private static final long WRITE_RETRY_MAX_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(8);

    private final AsyncOxiaClient client;
    private final Consumer<SessionEvent> eventSink;
    private final String canaryKey;

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
     * Successful canary write completions so far and, for each existence check, the counter value
     * observed when the check was issued. Always accessed under {@link #stateLock}: a check whose
     * read completed while a write also completed must not report the session lost, because the
     * write proves there is a live session and the read was racing it.
     */
    private int writeCompletions;

    /** Retry counter of the current canary write chain; only accessed on the executor. */
    private int writeFailures;

    private final AtomicBoolean closed = new AtomicBoolean();

    private enum State {
        STARTING, ESTABLISHED, LOST
    }

    OxiaSessionWatcher(AsyncOxiaClient client, Consumer<SessionEvent> eventSink) {
        this.client = client;
        this.eventSink = eventSink;
        this.canaryKey = CANARY_KEY_PREFIX + UUID.randomUUID();
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
            executor.scheduleWithFixedDelay(this::checkCanarySafely, CHECK_INTERVAL_MILLIS, CHECK_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignore) {
            // The watcher was closed concurrently with the store construction.
        }
    }

    /**
     * Routes a notification from the canary namespace: only the deletion of the canary of this
     * instance means the session was lost, every other canary notification is ignored.
     */
    void handleNotification(Notification notification) {
        if (closed.get()) {
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
     * considered established. Package-private so that tests can drive it directly.
     */
    void checkCanary() {
        int writeCompletionsAtIssue;
        synchronized (stateLock) {
            if (state != State.ESTABLISHED) {
                return;
            }
            writeCompletionsAtIssue = writeCompletions;
        }
        try {
            client.get(canaryKey).whenComplete((result, ex) -> hop(() -> {
                if (closed.get() || ex != null || result != null) {
                    return;
                }
                boolean racedByWrite;
                synchronized (stateLock) {
                    racedByWrite = writeCompletions != writeCompletionsAtIssue;
                }
                if (!racedByWrite) {
                    onCanaryGone("missing");
                }
            }));
        } catch (RejectedExecutionException | IllegalStateException ignore) {
            // The client was closed concurrently.
        }
    }

    private void onCanaryGone(String cause) {
        // The event is dispatched while holding the lock, so that the listeners observe the
        // events in exactly the order of the state transitions, and never, for example, two
        // consecutive SessionReestablished. The dispatch is non-blocking.
        synchronized (stateLock) {
            if (state != State.LOST) {
                state = State.LOST;
                log.info().attr("canaryKey", canaryKey).attr("cause", cause)
                        .log("Oxia metadata store session was lost");
                eventSink.accept(SessionEvent.SessionLost);
            }
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
        if (closed.get()) {
            canaryWritePending.set(false);
            return;
        }
        if (resetFailures) {
            writeFailures = 0;
        }
        try {
            client.put(canaryKey, CANARY_VALUE, Set.of(PutOption.AsEphemeralRecord))
                    .whenComplete((result, ex) -> hop(() -> onCanaryWriteCompleted(ex)));
        } catch (RuntimeException e) {
            hop(() -> onCanaryWriteCompleted(e));
        }
    }

    private void onCanaryWriteCompleted(Throwable ex) {
        if (closed.get()) {
            canaryWritePending.set(false);
            return;
        }
        if (ex == null) {
            canaryWritePending.set(false);
            // Dispatched under the lock, like in onCanaryGone, for the same event-order
            // guarantee.
            synchronized (stateLock) {
                writeCompletions++;
                boolean wasLost = state == State.LOST;
                state = State.ESTABLISHED;
                if (wasLost) {
                    log.info().attr("canaryKey", canaryKey)
                            .log("Oxia metadata store session was re-established");
                    eventSink.accept(SessionEvent.SessionReestablished);
                }
            }
            return;
        }

        writeFailures++;
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
        if (closed.compareAndSet(false, true)) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
