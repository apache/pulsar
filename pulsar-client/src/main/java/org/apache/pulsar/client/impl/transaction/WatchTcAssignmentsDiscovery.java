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
package org.apache.pulsar.client.impl.transaction;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.CustomLog;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TransactionMetaStoreHandler;
import org.apache.pulsar.client.util.MathUtils;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.Backoff;

/**
 * Coordinator discovery via the metadata-store leader election. Opens a single
 * {@code CommandWatchTcAssignments} watch on a service-URL connection; the broker replies with the
 * full {@code partition -> leader} snapshot and re-pushes it on every leadership change. Each
 * coordinator gets one {@link TransactionMetaStoreHandler} pointed at its elected leader broker —
 * dialled through the proxy when the watch connection is proxied, directly otherwise (no
 * per-coordinator lookup); when a snapshot moves a coordinator's leader, the handler is retargeted.
 * Used against brokers that advertise {@code supports_tc_metadata_discovery}.
 */
@CustomLog
class WatchTcAssignmentsDiscovery implements TcDiscovery, ClientCnx.TcAssignmentsWatcherSession {

    private static final AtomicLong WATCH_ID_GENERATOR = new AtomicLong(0);

    private final PulsarClientImpl pulsarClient;
    private final long watchId = WATCH_ID_GENERATOR.incrementAndGet();
    private final Backoff reconnectBackoff;

    private final Map<Long, TransactionMetaStoreHandler> handlers = new ConcurrentHashMap<>();
    private final AtomicLong epoch = new AtomicLong(0);
    private volatile int parallelism;

    private final CompletableFuture<Void> initialSnapshotFuture = new CompletableFuture<>();
    private volatile ClientCnx cnx;
    private volatile boolean closed;
    private volatile long initialOpenDeadline;
    private volatile boolean useProxy;

    WatchTcAssignmentsDiscovery(PulsarClientImpl pulsarClient) {
        this.pulsarClient = pulsarClient;
        this.reconnectBackoff = Backoff.builder()
                .initialDelay(Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(30))
                .build();
    }

    @Override
    public CompletableFuture<Void> start() {
        // Bound initial-open retries by the client's lookup timeout (fall back to operation timeout
        // when unset, mirroring TransactionMetaStoreHandler). A transient failure on the watch broker
        // (e.g. ServiceNotReady while its TC initializes) then retries rather than hard-failing
        // transaction-client startup.
        long lookupTimeoutMs = pulsarClient.getConfiguration().getLookupTimeoutMs();
        if (lookupTimeoutMs < 0) {
            lookupTimeoutMs = pulsarClient.getConfiguration().getOperationTimeoutMs();
        }
        this.initialOpenDeadline = System.currentTimeMillis() + lookupTimeoutMs;
        openWatch();
        return initialSnapshotFuture;
    }

    private void openWatch() {
        if (closed) {
            return;
        }
        pulsarClient.getAnyBrokerProxyConnection()
                .thenAccept(this::attach)
                .exceptionally(ex -> {
                    onAttachFailure(ex);
                    return null;
                });
    }

    private void attach(ClientCnx newCnx) {
        if (closed) {
            return;
        }
        if (!newCnx.isSupportsTcMetadataDiscovery()) {
            // The broker we landed on doesn't support the watch. On the very first open this is a
            // hard failure (the caller chose this strategy on a probe that said it was supported);
            // after that, it's likely transient — config drift or we hit a different/old broker —
            // so reconnect to find a supporting broker rather than freezing on the last snapshot.
            onAttachFailure(new PulsarClientException(
                    "Broker does not support metadata-store TC discovery"));
            return;
        }
        this.cnx = newCnx;
        // Behind a proxy the leader's advertised broker address isn't directly reachable; handlers
        // must dial it through the proxy. The watch connection tells us which mode we're in.
        this.useProxy = newCnx.isProxied();
        newCnx.registerTcAssignmentsWatcher(watchId, this);
        newCnx.ctx().writeAndFlush(Commands.newWatchTcAssignments(watchId))
                .addListener(writeFuture -> {
                    if (!writeFuture.isSuccess()) {
                        newCnx.removeTcAssignmentsWatcher(watchId);
                        onAttachFailure(writeFuture.cause());
                    }
                });
    }

    private void onAttachFailure(Throwable ex) {
        if (closed) {
            return;
        }
        if (!initialSnapshotFuture.isDone()) {
            // During initial open, retry retryable failures until the lookup deadline rather than
            // failing transaction-client startup outright — the probe and the watch can land on
            // different brokers, and the watch broker may be briefly not-ready.
            Throwable cause = ex instanceof java.util.concurrent.CompletionException && ex.getCause() != null
                    ? ex.getCause() : ex;
            boolean retryable = !(cause instanceof PulsarClientException)
                    || PulsarClientException.isRetriableError((PulsarClientException) cause);
            if (retryable && System.currentTimeMillis() < initialOpenDeadline) {
                log.warn().exception(cause).log("TC-assignments watch open failed; retrying");
                scheduleReconnect();
                return;
            }
            initialSnapshotFuture.completeExceptionally(
                    PulsarClientException.wrap(ex, "Failed to open TC-assignments watch"));
            return;
        }
        scheduleReconnect();
    }

    @Override
    public void onSnapshot(int newParallelism, Map<Long, String[]> leaders) {
        if (closed) {
            return;
        }
        reconnectBackoff.reset();
        this.parallelism = newParallelism;
        // Apply the full snapshot: create handlers for newly-seen coordinators, retarget existing
        // ones whose leader moved. A coordinator absent from the snapshot is mid-election; leave its
        // handler in place to retry against its last-known leader until the next snapshot.
        boolean proxy = this.useProxy;
        for (Map.Entry<Long, String[]> e : leaders.entrySet()) {
            long tcId = e.getKey();
            URI leaderUri = selectLeaderUri(e.getValue()[0], e.getValue()[1]);
            try {
                handlers.compute(tcId, (id, existing) -> {
                    if (existing == null) {
                        TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                                id, pulsarClient, leaderUri, proxy, new CompletableFuture<>());
                        handler.start();
                        return handler;
                    }
                    existing.retargetLeader(leaderUri, proxy);
                    return existing;
                });
            } catch (RuntimeException ex) {
                // A bad/unusable leader URL for one partition (e.g. a TLS-only leader for a non-TLS
                // client) must not abort applying the rest of the snapshot or tear down the watch.
                log.warn().attr("tcId", tcId).exception(ex)
                        .log("Skipping TC assignment with unusable leader URL");
            }
        }
        if (!initialSnapshotFuture.isDone()) {
            initialSnapshotFuture.complete(null);
        }
    }

    @Override
    public void onError(ServerError error, String message) {
        log.warn().attr("error", error).attr("message", message).log("WatchTcAssignments rejected");
        if (!initialSnapshotFuture.isDone()) {
            initialSnapshotFuture.completeExceptionally(new PulsarClientException(
                    "WatchTcAssignments failed: " + error + (message != null ? " - " + message : "")));
            return;
        }
        // Post-initial error: ClientCnx has already removed this session, so no connectionClosed()
        // will fire to drive recovery. Reconnect ourselves so a transient rejection (ServiceNotReady
        // while a broker re-initializes, or transient config drift) can't freeze the watch forever.
        cnx = null;
        scheduleReconnect();
    }

    @Override
    public void connectionClosed() {
        cnx = null;
        if (closed) {
            return;
        }
        if (!initialSnapshotFuture.isDone()) {
            initialSnapshotFuture.completeExceptionally(new PulsarClientException(
                    "Connection closed before initial TC-assignments snapshot arrived"));
            return;
        }
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (closed) {
            return;
        }
        long delayMs = reconnectBackoff.next().toMillis();
        pulsarClient.timer().newTimeout(timeout -> openWatch(), delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Pick the leader URL matching the client's TLS setting and parse it. Throws if no usable URL is
     * present (e.g. a non-TLS client and a TLS-only leader) — the caller skips that partition rather
     * than tearing down the watch.
     */
    private URI selectLeaderUri(String url, String urlTls) {
        boolean tls = pulsarClient.getConfiguration().isUseTls();
        String chosen = tls && urlTls != null && !urlTls.isBlank() ? urlTls : url;
        if (chosen == null || chosen.isBlank()) {
            throw new IllegalArgumentException("No usable leader URL (useTls=" + tls
                    + ", url=" + url + ", urlTls=" + urlTls + ")");
        }
        return URI.create(chosen);
    }

    @Override
    public TransactionMetaStoreHandler handlerForCoordinator(long tcId) {
        return handlers.get(tcId);
    }

    @Override
    public TransactionMetaStoreHandler nextHandler() {
        int n = parallelism;
        if (n <= 0) {
            return null;
        }
        // Round-robin over coordinator ids 0..parallelism-1, skipping any mid-election gap.
        for (int attempt = 0; attempt < n; attempt++) {
            long tcId = MathUtils.signSafeMod(epoch.incrementAndGet(), n);
            TransactionMetaStoreHandler handler = handlers.get(tcId);
            if (handler != null) {
                return handler;
            }
        }
        return null;
    }

    @Override
    public java.util.Collection<TransactionMetaStoreHandler> handlers() {
        return new java.util.ArrayList<>(handlers.values());
    }

    @Override
    public void close() {
        closed = true;
        ClientCnx c = cnx;
        if (c != null) {
            c.removeTcAssignmentsWatcher(watchId);
            c.ctx().writeAndFlush(Commands.newWatchTcAssignmentsClose(watchId));
        }
        handlers.values().forEach(handler -> {
            try {
                handler.close();
            } catch (Exception e) {
                log.warn().exception(e).log("Close transaction meta store handler error");
            }
        });
        handlers.clear();
    }
}
