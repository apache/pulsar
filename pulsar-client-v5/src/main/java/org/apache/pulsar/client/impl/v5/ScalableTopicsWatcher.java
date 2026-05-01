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

import io.github.merlimat.slog.Logger;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.ScalableTopicsWatcherSession;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.util.Backoff;

/**
 * Client-side manager for a namespace-wide scalable-topics watch session.
 *
 * <p>Opens a {@code CommandWatchScalableTopics} on attach, awaits the initial
 * {@code Snapshot}, and forwards subsequent {@code Snapshot} / {@code Diff} events
 * to a {@link Listener}. On connection drop, schedules a reconnect with backoff;
 * when the new session lands, the broker pushes a fresh snapshot and the client
 * applies it as a state replacement (no missed events).
 *
 * <p>Mirrors {@link DagWatchClient} in shape; the wire path is different — watch
 * subscribe is fire-and-forget (no request/response), with the initial snapshot
 * arriving as the first {@code WatchScalableTopicsUpdate}.
 */
final class ScalableTopicsWatcher implements ScalableTopicsWatcherSession, AutoCloseable {

    private static final Logger LOG = Logger.get(ScalableTopicsWatcher.class);
    private static final AtomicLong WATCH_ID_GENERATOR = new AtomicLong(0);

    /**
     * Listener for membership events. The watcher delivers events on the netty IO
     * thread; implementations should not block.
     */
    interface Listener {
        /** Full set; replace any local state. */
        void onSnapshot(List<String> topics);

        /** Apply removed before added (covers rapid remove-then-add of same name). */
        void onDiff(List<String> added, List<String> removed);
    }

    private final Logger log;
    private final PulsarClientImpl v4Client;
    private final NamespaceName namespace;
    private final Map<String, String> propertyFilters;
    private final long watchId;
    private final Backoff reconnectBackoff;

    private final CompletableFuture<List<String>> initialSnapshotFuture = new CompletableFuture<>();
    /**
     * Mirrors the broker's view of the matching set so we can hand a hash on
     * reconnect — when the set hasn't changed, the broker skips emitting a
     * fresh snapshot. Updated on every Snapshot replace and Diff apply.
     * Synchronised because Snapshot / Diff arrive on the netty thread but the
     * hash may be read on a reconnect callback running elsewhere.
     */
    private final Set<String> currentSet = Collections.synchronizedSet(new HashSet<>());
    private volatile Listener listener;
    private volatile ClientCnx cnx;
    private volatile boolean closed = false;

    /**
     * @param v4Client        the underlying v4 client used to open broker connections
     * @param namespace       namespace to watch
     * @param propertyFilters AND filters; empty map = match all
     */
    ScalableTopicsWatcher(PulsarClientImpl v4Client, NamespaceName namespace,
                          Map<String, String> propertyFilters) {
        this.v4Client = v4Client;
        this.namespace = namespace;
        this.propertyFilters = propertyFilters == null ? Map.of() : propertyFilters;
        this.watchId = WATCH_ID_GENERATOR.incrementAndGet();
        this.reconnectBackoff = Backoff.builder()
                .initialDelay(Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(30))
                .build();
        this.log = LOG.with()
                .attr("namespace", namespace)
                .attr("watchId", watchId)
                .attr("filters", this.propertyFilters)
                .build();
    }

    /**
     * Open the watch session on a connection to the configured service URL.
     * Resolves with the initial snapshot's topic list. After this returns, every
     * subsequent {@code Snapshot} / {@code Diff} flows through {@link #setListener}.
     */
    CompletableFuture<List<String>> start() {
        v4Client.getConnectionToServiceUrl()
                .thenAccept(this::attach)
                .exceptionally(ex -> {
                    initialSnapshotFuture.completeExceptionally(ex);
                    return null;
                });
        return initialSnapshotFuture;
    }

    private void attach(ClientCnx newCnx) {
        if (closed) {
            return;
        }
        this.cnx = newCnx;
        if (!newCnx.isSupportsScalableTopics()) {
            initialSnapshotFuture.completeExceptionally(
                    new PulsarClientException.FeatureNotSupportedException(
                            "Broker does not support scalable topics",
                            PulsarClientException.FailedFeatureCheck.SupportsScalableTopics));
            return;
        }
        newCnx.registerScalableTopicsWatcher(watchId, this);
        // First subscribe: send no hash so the broker emits the initial snapshot
        // unconditionally. snapshot is what populates initialSnapshotFuture.
        newCnx.ctx().writeAndFlush(Commands.newWatchScalableTopics(
                        watchId, namespace.toString(), propertyFilters,
                        /* currentHash= */ null))
                .addListener(writeFuture -> {
                    if (!writeFuture.isSuccess()) {
                        newCnx.removeScalableTopicsWatcher(watchId);
                        if (!initialSnapshotFuture.isDone()) {
                            initialSnapshotFuture.completeExceptionally(
                                    new PulsarClientException(writeFuture.cause()));
                        }
                    }
                });
    }

    @Override
    public void onSnapshot(List<String> topics) {
        if (closed) {
            return;
        }
        log.info().attr("topics", topics.size()).log("Snapshot received");
        // Reset backoff on every successful snapshot — that's the broker confirming
        // the session is live and our local state is consistent.
        reconnectBackoff.reset();
        // Replace local set so the next reconnect computes the right hash.
        synchronized (currentSet) {
            currentSet.clear();
            currentSet.addAll(topics);
        }
        if (!initialSnapshotFuture.isDone()) {
            initialSnapshotFuture.complete(topics);
            // The listener is set by the caller AFTER start() resolves, so the initial
            // snapshot is delivered via the future, not via onSnapshot's fan-out.
            return;
        }
        Listener l = listener;
        if (l != null) {
            try {
                l.onSnapshot(topics);
            } catch (Exception e) {
                log.error().exception(e).log("Listener threw on snapshot");
            }
        }
    }

    @Override
    public void onDiff(List<String> added, List<String> removed) {
        if (closed) {
            return;
        }
        log.info().attr("added", added.size()).attr("removed", removed.size())
                .log("Diff received");
        // Apply removed before added — covers rapid remove-then-add of the same name.
        synchronized (currentSet) {
            currentSet.removeAll(removed);
            currentSet.addAll(added);
        }
        Listener l = listener;
        if (l != null) {
            try {
                l.onDiff(added, removed);
            } catch (Exception e) {
                log.error().exception(e).log("Listener threw on diff");
            }
        }
    }

    /**
     * Snapshot the current set under lock so the hash + the watch frame agree on
     * the same view. CRC32C of sorted topic names — same function used by
     * {@code CommandGetTopicsOfNamespace} so behaviour matches the existing
     * topic-list watch on the wire.
     */
    private String currentSetHash() {
        synchronized (currentSet) {
            if (currentSet.isEmpty()) {
                return TopicList.calculateHash(java.util.List.of());
            }
            return TopicList.calculateHash(new HashSet<>(currentSet));
        }
    }

    @Override
    public void onError(ServerError error, String message) {
        log.warn().attr("error", error).attr("message", message)
                .log("WatchScalableTopics rejected");
        if (!initialSnapshotFuture.isDone()) {
            initialSnapshotFuture.completeExceptionally(
                    new PulsarClientException("WatchScalableTopics failed: " + error
                            + (message != null ? " - " + message : "")));
        }
    }

    @Override
    public void connectionClosed() {
        log.warn("Scalable-topics watcher connection closed");
        cnx = null;
        if (closed) {
            return;
        }
        if (!initialSnapshotFuture.isDone()) {
            // Initial subscribe never completed — surface the failure rather than
            // retrying silently behind the caller.
            initialSnapshotFuture.completeExceptionally(
                    new PulsarClientException(
                            "Connection closed before initial scalable-topics snapshot arrived"));
            return;
        }
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (closed) {
            return;
        }
        long delayMs = reconnectBackoff.next().toMillis();
        log.info().attr("delayMs", delayMs).log("Scheduling watcher reconnect");
        v4Client.timer().newTimeout(timeout -> reconnect(),
                delayMs, TimeUnit.MILLISECONDS);
    }

    private void reconnect() {
        if (closed) {
            return;
        }
        v4Client.getConnectionToServiceUrl()
                .thenAccept(newCnx -> {
                    if (closed) {
                        return;
                    }
                    if (!newCnx.isSupportsScalableTopics()) {
                        log.warn().log("Watcher reconnect: broker doesn't support scalable topics");
                        scheduleReconnect();
                        return;
                    }
                    this.cnx = newCnx;
                    newCnx.registerScalableTopicsWatcher(watchId, this);
                    // Reconnect: send the hash of our current set. If the broker's
                    // freshly-computed hash matches, it skips emitting a Snapshot —
                    // the watch is live and our local state is correct. Future
                    // Diffs flow as usual; if the hash differs the broker sends a
                    // Snapshot which we apply as a full-state replace.
                    String hash = currentSetHash();
                    newCnx.ctx().writeAndFlush(Commands.newWatchScalableTopics(
                                    watchId, namespace.toString(),
                                    propertyFilters, hash))
                            .addListener(writeFuture -> {
                                if (!writeFuture.isSuccess()) {
                                    newCnx.removeScalableTopicsWatcher(watchId);
                                    log.warn().exceptionMessage(writeFuture.cause())
                                            .log("Watcher reconnect write failed");
                                    scheduleReconnect();
                                    return;
                                }
                                // Write reached the broker — connection is healthy.
                                // Reset the backoff so the next disconnect starts
                                // fresh. Crucial for the hash-skip path: the broker
                                // emits no Snapshot, so onSnapshot's reset never
                                // fires; without this, a chain of short blips keeps
                                // the backoff at its peak forever.
                                reconnectBackoff.reset();
                            });
                })
                .exceptionally(ex -> {
                    log.warn().exceptionMessage(ex).log("Watcher reconnect failed");
                    scheduleReconnect();
                    return null;
                });
    }

    /**
     * Set the listener that receives {@code Snapshot} / {@code Diff} events. Should
     * be called after {@link #start()} resolves — the initial snapshot is delivered
     * via that future, not via the listener.
     */
    void setListener(Listener listener) {
        this.listener = listener;
    }

    /**
     * Visible for testing — snapshot of the current set. In production, the
     * listener is the source of truth; this method exists so tests can poke the
     * watcher's hash-tracking state directly.
     */
    Set<String> currentSetForTesting() {
        synchronized (currentSet) {
            return new HashSet<>(currentSet);
        }
    }

    long watchId() {
        return watchId;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        ClientCnx c = cnx;
        if (c != null) {
            c.removeScalableTopicsWatcher(watchId);
            c.ctx().writeAndFlush(Commands.newWatchScalableTopicsClose(watchId));
        }
    }
}
