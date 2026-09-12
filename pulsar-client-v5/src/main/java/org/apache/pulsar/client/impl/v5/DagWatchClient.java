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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.DagWatchSession;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.Backoff;

/**
 * Client-side manager for a DAG watch session on a scalable topic.
 *
 * <p>Connects to any broker, sends a ScalableTopicLookup to initiate a watch session,
 * and receives pushed updates when the segment layout changes (split/merge).
 *
 * <p>Maintains the current {@link ClientSegmentLayout} and notifies a listener
 * when it changes.
 *
 * <p>Reconnects automatically on transient broker disconnects. The initial-create
 * future ({@link #start}) surfaces failures up front so a producer / consumer
 * {@code create()} fails fast when the broker is unreachable. After the first
 * layout has arrived, subsequent disconnects schedule a reconnection with
 * exponential backoff so long-lived producers / consumers survive network blips.
 */
final class DagWatchClient implements DagWatchSession, AutoCloseable {

    private static final Logger LOG = Logger.get(DagWatchClient.class);
    private final Logger log;

    private static final AtomicLong SESSION_ID_GENERATOR = new AtomicLong(0);

    private final PulsarClientImpl v4Client;
    private final TopicName topicName;
    private final long sessionId;
    /** When false, the broker must not auto-create the scalable topic if it's missing on lookup.
     *  Namespace (multi-topic) consumers set this false so a deleted topic isn't resurrected by
     *  a reconnecting per-topic watch. */
    private final boolean createIfMissing;
    private final AtomicReference<ClientSegmentLayout> currentLayout = new AtomicReference<>();
    private final CompletableFuture<ClientSegmentLayout> initialLayoutFuture = new CompletableFuture<>();
    private final Backoff reconnectBackoff;
    private volatile LayoutChangeListener listener;
    private volatile ClientCnx cnx;
    private volatile boolean closed = false;
    private volatile boolean usingProxy = false;
    /** Canonical topic://t/n/x identity returned by the broker. Resolved on the first
     *  update; used as the parent topic when computing segment:// URIs for real DAGs. */
    private volatile TopicName resolvedTopicName;

    DagWatchClient(PulsarClientImpl v4Client, TopicName topicName) {
        this(v4Client, topicName, true);
    }

    DagWatchClient(PulsarClientImpl v4Client, TopicName topicName, boolean createIfMissing) {
        this.v4Client = v4Client;
        this.topicName = topicName;
        this.createIfMissing = createIfMissing;
        this.sessionId = SESSION_ID_GENERATOR.incrementAndGet();
        this.reconnectBackoff = Backoff.builder()
                .initialDelay(Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(30))
                .build();
        this.log = LOG.with().attr("topic", topicName).attr("sessionId", sessionId).build();
    }

    /**
     * Start the DAG watch session. Connects to any broker and sends a
     * ScalableTopicLookup to initiate the watch session.
     *
     * @return a future that completes with the initial layout
     */
    CompletableFuture<ClientSegmentLayout> start() {
        v4Client.getConnection(topicName.toString())
                .thenAccept(this::attach)
                .exceptionally(ex -> {
                    initialLayoutFuture.completeExceptionally(ex);
                    return null;
                });
        return initialLayoutFuture;
    }

    /**
     * Wire {@code newCnx} to this session and send a ScalableTopicLookup. Used by
     * both {@link #start} (first connect) and {@link #reconnect} (after disconnect).
     */
    private void attach(ClientCnx newCnx) {
        if (closed) {
            return;
        }
        if (!newCnx.isSupportsScalableTopics()) {
            PulsarClientException ex = new PulsarClientException.FeatureNotSupportedException(
                    "Broker does not support scalable topics",
                    PulsarClientException.FailedFeatureCheck.SupportsScalableTopics);
            if (!initialLayoutFuture.isDone()) {
                initialLayoutFuture.completeExceptionally(ex);
            } else {
                log.warn().exceptionMessage(ex)
                        .log("Reconnect target broker doesn't support scalable topics");
                scheduleReconnect();
            }
            return;
        }
        this.usingProxy = newCnx.isProxied();
        this.cnx = newCnx;
        newCnx.registerDagWatchSession(sessionId, this);
        newCnx.ctx().writeAndFlush(
                        Commands.newScalableTopicLookup(sessionId, topicName.toString(), createIfMissing))
                .addListener(writeFuture -> {
                    if (!writeFuture.isSuccess()) {
                        newCnx.removeDagWatchSession(sessionId);
                        if (!initialLayoutFuture.isDone()) {
                            initialLayoutFuture.completeExceptionally(
                                    new PulsarClientException(writeFuture.cause()));
                        } else {
                            log.warn().exceptionMessage(writeFuture.cause())
                                    .log("DAG watch reconnect write failed; will retry");
                            scheduleReconnect();
                        }
                    }
                });
    }

    /**
     * Called when the broker pushes a ScalableTopicUpdate for this session.
     * This is invoked from the Netty I/O thread.
     */
    @Override
    public void onUpdate(ScalableTopicDAG dag, String resolvedTopicName) {
        if (closed) {
            return;
        }

        // Cache the canonical topic://... identity returned by the broker so segment://
        // URIs are computed against the resolved parent regardless of the input domain.
        // The broker should always set this on success; fall back to the input name if
        // an older broker doesn't.
        TopicName resolvedTn;
        if (resolvedTopicName != null) {
            resolvedTn = TopicName.get(resolvedTopicName);
            this.resolvedTopicName = resolvedTn;
        } else {
            resolvedTn = this.resolvedTopicName != null ? this.resolvedTopicName : topicName;
        }

        ClientSegmentLayout newLayout = ClientSegmentLayout.fromProto(dag, resolvedTn);
        ClientSegmentLayout oldLayout = currentLayout.getAndSet(newLayout);

        log.info().attr("oldEpoch", oldLayout != null ? oldLayout.epoch() : "none")
                .attr("newEpoch", newLayout.epoch())
                .attr("activeSegmentCount", newLayout.activeSegments().size())
                .log("Layout updated");

        // Reset the reconnect backoff: the broker confirmed the session is live and
        // our local state is consistent.
        reconnectBackoff.reset();

        // Complete the initial layout future if this is the first update
        initialLayoutFuture.complete(newLayout);

        LayoutChangeListener l = listener;
        if (l != null) {
            try {
                l.onLayoutChange(newLayout, oldLayout);
            } catch (Exception e) {
                log.error().exception(e).log("Error in layout change listener");
            }
        }
    }

    @Override
    public void onError(ServerError error, String message) {
        log.error().attr("error", error).attr("message", message)
                .log("DAG watch session error");
        if (!initialLayoutFuture.isDone()) {
            String detail = "Scalable topic lookup failed: " + error + " - " + message;
            initialLayoutFuture.completeExceptionally(
                    error == ServerError.TopicNotFound
                            ? new PulsarClientException.NotFoundException(detail)
                            : new PulsarClientException(detail));
        }
        // After the initial layout has arrived, broker-side errors on this session
        // (e.g., metadata unavailable) are transient — a reconnect typically clears
        // them. The connection-closed path will pick this up; no extra work here.
    }

    @Override
    public void connectionClosed() {
        log.warn("DAG watch session connection closed");
        cnx = null;
        if (closed) {
            return;
        }
        if (!initialLayoutFuture.isDone()) {
            // Initial lookup never completed — surface the failure rather than
            // retrying silently behind the caller of producer / consumer create().
            initialLayoutFuture.completeExceptionally(
                    new PulsarClientException(
                            "Connection closed while waiting for scalable topic layout"));
            return;
        }
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (closed) {
            return;
        }
        long delayMs = reconnectBackoff.next().toMillis();
        log.info().attr("delayMs", delayMs).log("Scheduling DAG watch reconnect");
        v4Client.timer().newTimeout(timeout -> reconnect(),
                delayMs, TimeUnit.MILLISECONDS);
    }

    private void reconnect() {
        if (closed) {
            return;
        }
        v4Client.getConnection(topicName.toString())
                .thenAccept(this::attach)
                .exceptionally(ex -> {
                    log.warn().exceptionMessage(ex)
                            .log("DAG watch reconnect failed; will retry");
                    scheduleReconnect();
                    return null;
                });
    }

    /** Whether the DAG-watch connection was established through a proxy. */
    boolean isUsingProxy() {
        return usingProxy;
    }

    ClientSegmentLayout currentLayout() {
        return currentLayout.get();
    }

    void setListener(LayoutChangeListener listener) {
        this.listener = listener;
    }

    long sessionId() {
        return sessionId;
    }

    /**
     * Returns the canonical scalable-topic identity, falling back to the user's input
     * before the first response arrives. Once the broker has returned a
     * {@code resolved_topic_name}, this always reflects the resolved form so callers
     * see the canonical {@code topic://...} name regardless of how they spelled the input.
     */
    TopicName topicName() {
        TopicName resolved = resolvedTopicName;
        return resolved != null ? resolved : topicName;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        ClientCnx c = cnx;
        if (c != null) {
            c.removeDagWatchSession(sessionId);
            c.ctx().writeAndFlush(Commands.newScalableTopicClose(sessionId));
        }
    }

    /**
     * Test hook: forcibly close the underlying broker channel to simulate a network
     * drop. The cnx layer will fire {@link #connectionClosed()} which triggers the
     * automatic reconnect path. Reached via reflection from cross-module tests.
     */
    void forceCloseConnectionForTesting() {
        ClientCnx c = cnx;
        if (c != null) {
            c.ctx().channel().close();
        }
    }

    interface LayoutChangeListener {
        void onLayoutChange(ClientSegmentLayout newLayout, ClientSegmentLayout oldLayout);
    }
}
