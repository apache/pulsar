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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.ReplicatedSubscriptionSnapshotCache.ClusterEntry;
import org.apache.pulsar.broker.service.persistent.ReplicatedSubscriptionSnapshotCache.SnapshotResult;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.common.api.proto.ClusterMessageId;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshotRequest;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshotResponse;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsUpdate;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;

/**
 * Encapsulate all the logic of replicated subscriptions tracking for a given topic.
 */
@Slf4j
public class ReplicatedSubscriptionsController implements AutoCloseable, Topic.PublishContext {
    private final PersistentTopic topic;
    private final String localCluster;

    // The timestamp of when the last snapshot was initiated
    private volatile long lastCompletedSnapshotStartTime = 0;

    private String lastCompletedSnapshotId;

    private volatile Position positionOfLastLocalMarker;

    private final ScheduledFuture<?> timer;

    private final ConcurrentMap<String, ReplicatedSubscriptionsSnapshotBuilder> pendingSnapshots =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<String, PendingReceivedSubscriptionUpdate> pendingReceivedSubscriptionUpdates =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PendingSubscriptionUpdate> pendingSubscriptionUpdates =
            new ConcurrentHashMap<>();
    private final Object subscriptionUpdateLifecycleLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();

    private static final class PendingReceivedSubscriptionUpdate {
        private final AtomicReference<ReceivedSubscriptionState> subscriptionState;
        private final Position position;
        private final AtomicBoolean inFlight = new AtomicBoolean();
        private final AtomicBoolean attempted = new AtomicBoolean();

        private PendingReceivedSubscriptionUpdate(ReceivedSubscriptionState subscriptionState, Position position) {
            this.subscriptionState = new AtomicReference<>(subscriptionState);
            this.position = position;
        }
    }

    private static final class ReceivedSubscriptionState {
        private final PersistentSubscription subscription;
        private final Position markDeletePosition;
        private final CompletableFuture<Void> resetFuture;
        private final boolean resetInProgress;

        private ReceivedSubscriptionState(PersistentSubscription subscription) {
            this.subscription = subscription;
            resetFuture = subscription.getLastCursorResetFuture();
            resetInProgress = resetFuture != null && !resetFuture.isDone();
            markDeletePosition = subscription.getCursor().getMarkDeletedPosition();
        }

        private boolean shouldDiscard() {
            return resetInProgress || subscription.getLastCursorResetFuture() != resetFuture
                    || subscription.getCursor().getMarkDeletedPosition().compareTo(markDeletePosition) < 0;
        }

        private CompletableFuture<Void> acknowledge(Position position) {
            return subscription.acknowledgeReplicatedSubscriptionMessage(position, markDeletePosition,
                    resetFuture, resetInProgress);
        }
    }

    private static final class PendingSubscriptionUpdate {
        private final PersistentSubscription subscription;
        private final SnapshotResult snapshot;
        private final AtomicBoolean inFlight = new AtomicBoolean();

        private PendingSubscriptionUpdate(PersistentSubscription subscription, SnapshotResult snapshot) {
            this.subscription = subscription;
            this.snapshot = new SnapshotResult(snapshot.position(), List.copyOf(snapshot.clusters()));
        }
    }

    @PulsarDeprecatedMetric(
            newMetricName = OpenTelemetryReplicatedSubscriptionStats.SNAPSHOT_OPERATION_COUNT_METRIC_NAME)
    @Deprecated
    private static final Gauge pendingSnapshotsMetric = Gauge
            .build("pulsar_replicated_subscriptions_pending_snapshots",
                    "Counter of currently pending snapshots")
            .register();

    // timeouts use SnapshotOperationResult.TIMEOUT.attributes on the same metric
    @PulsarDeprecatedMetric(
            newMetricName = OpenTelemetryReplicatedSubscriptionStats.SNAPSHOT_OPERATION_COUNT_METRIC_NAME)
    @Deprecated
    private static final Counter timedoutSnapshotsMetric = Counter
            .build().name("pulsar_replicated_subscriptions_timedout_snapshots")
            .help("Counter of timed out snapshots").register();

    private final OpenTelemetryReplicatedSubscriptionStats stats;

    public ReplicatedSubscriptionsController(PersistentTopic topic, String localCluster) {
        this(topic, localCluster, topic.getBrokerService().pulsar().getExecutor());
    }

    @VisibleForTesting
    ReplicatedSubscriptionsController(PersistentTopic topic, String localCluster, ScheduledExecutorService executor) {
        this.topic = topic;
        this.localCluster = localCluster;
        var pulsar = topic.getBrokerService().pulsar();
        stats = pulsar.getOpenTelemetryReplicatedSubscriptionStats();
        timer = executor.scheduleAtFixedRate(catchingAndLoggingThrowables(this::startNewSnapshot), 0,
                pulsar.getConfiguration().getReplicatedSubscriptionsSnapshotFrequencyMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void receivedReplicatedSubscriptionMarker(Position position, int markerType, ByteBuf payload) {
        try {
            switch (markerType) {
            case MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST_VALUE:
                receivedSnapshotRequest(Markers.parseReplicatedSubscriptionsSnapshotRequest(payload));
                break;

            case MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_RESPONSE_VALUE:
                receivedSnapshotResponse(position, Markers.parseReplicatedSubscriptionsSnapshotResponse(payload));
                break;

            case MarkerType.REPLICATED_SUBSCRIPTION_UPDATE_VALUE:
                receiveSubscriptionUpdated(Markers.parseReplicatedSubscriptionsUpdate(payload));
                break;

            default:
                // Ignore
            }
        } catch (IOException e) {
            log.warn("[{}] Failed to parse marker: {}", topic.getName(), e);
        }
    }

    public void localSubscriptionUpdated(String subscriptionName,
                                         ReplicatedSubscriptionSnapshotCache.SnapshotResult snapshot) {
        PersistentSubscription subscription = topic.getSubscription(subscriptionName);
        if (closed.get() || subscription == null || !subscription.isReplicated()
                || subscription.getCursor().getMarkDeletedPosition().compareTo(snapshot.position()) < 0) {
            return;
        }

        PendingSubscriptionUpdate update;
        synchronized (subscriptionUpdateLifecycleLock) {
            Position markDeletePosition = subscription.getCursor().getMarkDeletedPosition();
            if (closed.get() || topic.getSubscription(subscriptionName) != subscription || !subscription.isReplicated()
                    || markDeletePosition.compareTo(snapshot.position()) < 0) {
                return;
            }
            update = pendingSubscriptionUpdates.compute(subscriptionName, (name, pending) -> {
                if (pending != null && pending.subscription == subscription
                        && pending.snapshot.position().compareTo(markDeletePosition) <= 0
                        && pending.snapshot.position().compareTo(snapshot.position()) >= 0) {
                    return pending;
                }
                // Keep a coherent snapshot vector. Combining positions from different snapshots
                // could acknowledge a cut that was never covered by the local subscription.
                return new PendingSubscriptionUpdate(subscription, snapshot);
            });
        }
        tryPublishSubscriptionUpdate(subscriptionName, update);
    }

    private boolean isCurrentSubscriptionUpdate(String subscriptionName, PendingSubscriptionUpdate update) {
        if (closed.get() || pendingSubscriptionUpdates.get(subscriptionName) != update) {
            return false;
        }
        PersistentSubscription subscription = topic.getSubscription(subscriptionName);
        if (subscription != update.subscription || !subscription.isReplicated()
                || subscription.getCursor().getMarkDeletedPosition().compareTo(update.snapshot.position()) < 0) {
            // A removed, replaced, disabled or reset subscription must not retry an old update.
            pendingSubscriptionUpdates.remove(subscriptionName, update);
            return false;
        }
        return true;
    }

    private void tryPublishSubscriptionUpdate(String subscriptionName, PendingSubscriptionUpdate update) {
        if (!isCurrentSubscriptionUpdate(subscriptionName, update)
                || !update.inFlight.compareAndSet(false, true)) {
            return;
        }
        // A timer may have retained this entry while another publish completed or a newer
        // snapshot replaced it. Claiming inFlight alone does not establish entry ownership.
        if (!isCurrentSubscriptionUpdate(subscriptionName, update)) {
            update.inFlight.set(false);
            return;
        }

        try {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Updating subscription to snapshot, position: {}, clusters: {}", topic.getName(),
                        subscriptionName, update.snapshot.position(), update.snapshot.clusters().stream()
                                .map(entry -> String.format("%s -> %s", entry.cluster(), entry.position()))
                                .collect(Collectors.toList()));
            }

            Map<String, MarkersMessageIdData> clusterIds = new TreeMap<>();
            for (ClusterEntry cluster : update.snapshot.clusters()) {
                clusterIds.put(cluster.cluster(), new MarkersMessageIdData()
                        .setLedgerId(cluster.position().getLedgerId()).setEntryId(cluster.position().getEntryId()));
            }
            // Each attempt owns a fresh marker; writeMarkerAndGetPosition releases it on every path.
            ByteBuf marker = Markers.newReplicatedSubscriptionsUpdate(subscriptionName, clusterIds);
            writeMarkerAndGetPosition(marker).whenComplete((position, error) -> {
                try {
                    if (error == null) {
                        // Remove before releasing inFlight so a stale timer entry cannot claim
                        // the just-completed update. An older completion must retain newer work.
                        pendingSubscriptionUpdates.remove(subscriptionName, update);
                    } else {
                        logSubscriptionUpdateFailure(subscriptionName, update, error);
                    }
                } finally {
                    update.inFlight.set(false);
                }
            });
        } catch (Exception error) {
            update.inFlight.set(false);
            logSubscriptionUpdateFailure(subscriptionName, update, error);
        }
    }

    private void logSubscriptionUpdateFailure(String subscriptionName, PendingSubscriptionUpdate update,
                                              Throwable error) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Failed to publish replicated subscription update; retaining it for retry,"
                    + " position: {}, error: {}", topic.getName(), subscriptionName, update.snapshot.position(),
                    error.toString());
        }
    }

    private void receivedSnapshotRequest(ReplicatedSubscriptionsSnapshotRequest request) {
        Replicator replicator = topic.getReplicators().get(request.getSourceCluster());
        if (replicator == null) {
            log.warn("[{}] Received replicated subscription snapshot request {} from cluster {}, but no replicator is"
                            + " configured for that cluster. Ignoring the request.",
                    topic.getName(), request.getSnapshotId(), request.getSourceCluster());
            return;
        }

        // Send response containing the current last written message id. The response
        // marker we're publishing locally and then replicating will have a higher
        // message id.
        Position lastMsgId = topic.getLastPosition();
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Received snapshot request. Last msg id: {}",
                    topic.getBrokerService().pulsar().getBrokerId(), topic.getName(), lastMsgId);
        }

        ByteBuf marker = Markers.newReplicatedSubscriptionsSnapshotResponse(
                request.getSnapshotId(),
                request.getSourceCluster(),
                localCluster,
                lastMsgId.getLedgerId(), lastMsgId.getEntryId());
        writeMarker(marker);
    }

    private void receivedSnapshotResponse(Position position, ReplicatedSubscriptionsSnapshotResponse response) {
        String snapshotId = response.getSnapshotId();
        ReplicatedSubscriptionsSnapshotBuilder builder = pendingSnapshots.get(snapshotId);
        if (builder == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received late reply for timed-out snapshot {} from {}", topic.getName(), snapshotId,
                        response.getCluster().getCluster());
            }
            return;
        }

        builder.receivedSnapshotResponse(position, response);
    }

    private void receiveSubscriptionUpdated(ReplicatedSubscriptionsUpdate update) {
        // The parser reuses a thread-local protobuf. Retain only immutable values across futures.
        String subscriptionName = update.getSubscriptionName();
        if (closed.get()) {
            return;
        }
        if (PersistentTopic.isCompactionSubscription(subscriptionName)) {
            // The compactor owns this cursor: it may only move together with the compacted ledger.
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignoring replicated subscription update for the compaction subscription",
                        topic.getName(), subscriptionName);
            }
            return;
        }

        Position position = null;
        for (int i = 0, size = update.getClustersCount(); i < size; i++) {
            ClusterMessageId clusterMessageId = update.getClusterAt(i);
            if (localCluster.equals(clusterMessageId.getCluster())) {
                MarkersMessageIdData messageId = clusterMessageId.getMessageId();
                position = PositionFactory.create(messageId.getLedgerId(), messageId.getEntryId());
            }
        }
        if (position == null) {
            return;
        }
        Position target = position;
        PendingReceivedSubscriptionUpdate pendingUpdate;
        synchronized (subscriptionUpdateLifecycleLock) {
            if (closed.get()) {
                return;
            }
            PersistentSubscription subscription = topic.getSubscription(subscriptionName);
            ReceivedSubscriptionState currentState = subscription == null ? null
                    : new ReceivedSubscriptionState(subscription);
            pendingUpdate = pendingReceivedSubscriptionUpdates.compute(subscriptionName, (name, pending) -> {
                if (pending != null) {
                    ReceivedSubscriptionState state = pending.subscriptionState.get();
                    PersistentSubscription bound = state == null ? null : state.subscription;
                    if ((bound == null || bound == subscription) && (state == null || !state.shouldDiscard())
                            && (bound != null || currentState == null || currentState.resetFuture == null)
                            && pending.position.compareTo(target) >= 0) {
                        // An initially missing subscription may have been created while its retry
                        // was pending. Preserve the higher target only if no local reset intervened.
                        return bound == subscription ? pending
                                : new PendingReceivedSubscriptionUpdate(currentState, pending.position);
                    }
                }
                return new PendingReceivedSubscriptionUpdate(currentState, target);
            });
        }
        tryApplyReceivedSubscriptionUpdate(subscriptionName, pendingUpdate);
    }

    private boolean isCurrentReceivedSubscriptionUpdate(String subscriptionName,
                                                         PendingReceivedSubscriptionUpdate update) {
        if (closed.get() || pendingReceivedSubscriptionUpdates.get(subscriptionName) != update) {
            return false;
        }
        ReceivedSubscriptionState state = update.subscriptionState.get();
        PersistentSubscription subscription = state == null ? null : state.subscription;
        PersistentSubscription current = topic.getSubscription(subscriptionName);
        if (subscription == null && current != null) {
            if (!bindReceivedSubscriptionUpdate(subscriptionName, update, current)) {
                return false;
            }
            subscription = update.subscriptionState.get().subscription;
        }
        if (subscription != current) {
            // A retry bound to an existing subscription must not recreate or update its replacement.
            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
            return false;
        }
        return true;
    }

    private boolean bindReceivedSubscriptionUpdate(String subscriptionName, PendingReceivedSubscriptionUpdate update,
                                                    PersistentSubscription subscription) {
        ReceivedSubscriptionState state = update.subscriptionState.get();
        if (state == null) {
            ReceivedSubscriptionState newState = new ReceivedSubscriptionState(subscription);
            if (newState.resetFuture != null) {
                // This target arrived before the subscription was available. A seek already admitted
                // on that subscription takes precedence; it must not become the old target's baseline.
                pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
                return false;
            }
            update.subscriptionState.compareAndSet(null, newState);
            state = update.subscriptionState.get();
        }
        if (state.subscription != subscription) {
            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
            return false;
        }
        return true;
    }

    private void tryApplyReceivedSubscriptionUpdate(String subscriptionName, PendingReceivedSubscriptionUpdate update) {
        if (!isCurrentReceivedSubscriptionUpdate(subscriptionName, update)
                || !update.inFlight.compareAndSet(false, true)) {
            return;
        }
        if (!isCurrentReceivedSubscriptionUpdate(subscriptionName, update)) {
            update.inFlight.set(false);
            return;
        }
        boolean firstAttempt = update.attempted.compareAndSet(false, true);
        ReceivedSubscriptionState state = update.subscriptionState.get();
        PersistentSubscription subscription = state == null ? null : state.subscription;
        if (state != null && state.shouldDiscard()) {
            // A local seek takes precedence, including a reset to the same position or a rewind
            // followed by local acknowledgements back to the admission position between timer ticks.
            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
            update.inFlight.set(false);
            return;
        }
        if (!firstAttempt && subscription != null
                && subscription.getCursor().getMarkDeletedPosition().compareTo(update.position) >= 0) {
            // Preserve the initial ACK call, but do not repeatedly ACK an already-applied target.
            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
            update.inFlight.set(false);
            return;
        }
        applyReceivedSubscriptionUpdate(subscriptionName, update, firstAttempt).whenComplete((ignored, error) -> {
            try {
                if (error == null) {
                    pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
                } else {
                    handleReceivedSubscriptionUpdateFailure(subscriptionName, update, error, firstAttempt);
                }
            } finally {
                update.inFlight.set(false);
            }
        });
    }

    private void handleReceivedSubscriptionUpdateFailure(String subscriptionName,
                                                          PendingReceivedSubscriptionUpdate update,
                                                          Throwable error, boolean firstAttempt) {
        if (!isCurrentReceivedSubscriptionUpdate(subscriptionName, update)) {
            return;
        }
        ReceivedSubscriptionState state = update.subscriptionState.get();
        PersistentSubscription subscription = state == null ? null : state.subscription;
        Position markDeletePosition = subscription == null ? null : subscription.getCursor().getMarkDeletedPosition();
        Throwable cause = FutureUtil.unwrapCompletionException(error);
        if (state != null && state.shouldDiscard()) {
            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Discarding replicated subscription update superseded by a local cursor reset,"
                        + " position: {}, error: {}", topic.getName(), subscriptionName, update.position,
                        cause.toString());
            }
            return;
        }
        if (cause instanceof NotAllowedException) {
            // Retrying cannot fix policy rejection, such as exceeding the subscription limit.
            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
            if (firstAttempt) {
                log.warn("[{}][{}] Replicated subscription update rejected; not retrying, position: {}, error: {}",
                        topic.getName(), subscriptionName, update.position, cause.toString());
            }
            return;
        }
        if (markDeletePosition != null && update.position.compareTo(markDeletePosition) <= 0) {
            // A failed ACK future can still have applied, or a local consumer can have advanced farther.
            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignoring replicated subscription update behind the mark-delete position,"
                        + " position: {}, mark-delete position: {}", topic.getName(), subscriptionName,
                        update.position, markDeletePosition);
            }
        } else if (firstAttempt) {
            if (subscription == null) {
                log.warn("[{}][{}] Failed to create replicated subscription, position: {}, error: {}",
                        topic.getName(), subscriptionName, update.position, cause.toString());
            } else {
                log.warn("[{}][{}] Failed to update replicated subscription, position: {}, mark-delete position: {},"
                        + " error: {}", topic.getName(), subscriptionName, update.position, markDeletePosition,
                        cause.toString());
            }
        } else if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Failed to apply replicated subscription update; retaining it for retry,"
                            + " position: {}, mark-delete position: {}, error: {}", topic.getName(), subscriptionName,
                    update.position, markDeletePosition, cause.toString());
        }
    }

    private CompletableFuture<Void> applyReceivedSubscriptionUpdate(String subscriptionName,
                                                                    PendingReceivedSubscriptionUpdate update,
                                                                    boolean firstAttempt) {
        return FutureUtil.supplySafely(() -> {
            if (!isCurrentReceivedSubscriptionUpdate(subscriptionName, update)) {
                return CompletableFuture.completedFuture(null);
            }
            ReceivedSubscriptionState state = update.subscriptionState.get();
            PersistentSubscription subscription = state == null ? null : state.subscription;
            if (state != null && state.shouldDiscard()) {
                pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
                return CompletableFuture.completedFuture(null);
            }
            if (subscription != null) {
                // Received updates historically apply to existing non-replicated subscriptions too.
                return state.acknowledge(update.position);
            }
            if (firstAttempt) {
                log.info("[{}][{}] Creating subscription for received replicated subscription update, position: {}",
                        topic.getName(), subscriptionName, update.position);
            } else if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Creating subscription for received replicated subscription update, position: {}",
                        topic.getName(), subscriptionName, update.position);
            }
            return topic.createSubscription(subscriptionName, InitialPosition.Earliest,
                            true /* replicateSubscriptionState */, Collections.emptyMap())
                    .thenCompose(created -> {
                        if (closed.get() || pendingReceivedSubscriptionUpdates.get(subscriptionName) != update) {
                            return CompletableFuture.completedFuture(null);
                        }
                        PersistentSubscription createdSubscription = (PersistentSubscription) created;
                        if (topic.getSubscription(subscriptionName) != createdSubscription) {
                            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
                            return CompletableFuture.completedFuture(null);
                        }
                        if (!bindReceivedSubscriptionUpdate(subscriptionName, update, createdSubscription)) {
                            return CompletableFuture.completedFuture(null);
                        }
                        if (!isCurrentReceivedSubscriptionUpdate(subscriptionName, update)) {
                            return CompletableFuture.completedFuture(null);
                        }
                        if (update.subscriptionState.get().shouldDiscard()) {
                            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
                            return CompletableFuture.completedFuture(null);
                        }
                        return FutureUtil.supplySafely(() ->
                                update.subscriptionState.get().acknowledge(update.position));
                    });
        });
    }

    private void startNewSnapshot() {
        if (closed.get()) {
            return;
        }
        // The received marker may already be consumed; applying it must retry even on idle topics.
        pendingReceivedSubscriptionUpdates.forEach(this::tryApplyReceivedSubscriptionUpdate);
        // UPDATE appends need their own retry: their selected snapshot has already left
        // the subscription cache, and an idle topic need not create another snapshot.
        pendingSubscriptionUpdates.forEach(this::tryPublishSubscriptionUpdate);
        if (closed.get()) {
            return;
        }
        cleanupTimedOutSnapshots();

        if (lastCompletedSnapshotStartTime == 0 && !pendingSnapshots.isEmpty()) {
            // 1. If the remote cluster has disabled subscription replication or there's an incorrect config,
            //    it will not respond to SNAPSHOT_REQUEST. Therefore, lastCompletedSnapshotStartTime will remain 0,
            //    making it unnecessary to resend the request.
            // 2. This approach prevents sending additional SNAPSHOT_REQUEST to both local_topic and remote_topic.
            // 3. Since it's uncertain when the remote cluster will enable subscription replication,
            //    the timeout mechanism of pendingSnapshots is used to ensure retries.
            //
            // In other words, when hit this case, The frequency of sending SNAPSHOT_REQUEST
            // will use `replicatedSubscriptionsSnapshotTimeoutSeconds`.
            if (log.isDebugEnabled()) {
                log.debug("[{}] PendingSnapshot exists but has never succeeded. "
                        + "Skipping snapshot creation until pending snapshot timeout.", topic.getName());
            }
            return;
        }

        if (topic.getLastMaxReadPositionMovedForwardTimestamp() < lastCompletedSnapshotStartTime
                || topic.getLastMaxReadPositionMovedForwardTimestamp() == 0) {
            // There was no message written since the last snapshot, we can skip creating a new snapshot
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] There is no new data in topic. Skipping snapshot creation.",
                        topic.getBrokerService().pulsar().getBrokerId(), topic.getName());
            }
            return;
        }

        MutableBoolean anyReplicatorDisconnected = new MutableBoolean();
        topic.getReplicators().forEach((cluster, replicator) -> {
            if (!replicator.isConnected()) {
                anyReplicatorDisconnected.setTrue();
            }
        });

        if (anyReplicatorDisconnected.isTrue()) {
            // Do not attempt to create snapshot when some of the clusters are not reachable
            if (log.isDebugEnabled()) {
                log.debug("[{}] Do not attempt to create snapshot when some of the clusters are not reachable.",
                        topic.getName());
            }
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Starting snapshot creation.", topic.getBrokerService().pulsar().getBrokerId(),
                    topic.getName());
        }

        pendingSnapshotsMetric.inc();
        stats.recordSnapshotStarted();
        ReplicatedSubscriptionsSnapshotBuilder builder = new ReplicatedSubscriptionsSnapshotBuilder(this,
                topic.getReplicators().keySet(), topic.getBrokerService().pulsar().getConfiguration(),
                Clock.systemUTC());
        pendingSnapshots.put(builder.getSnapshotId(), builder);
        builder.start();
    }

    public Optional<String> getLastCompletedSnapshotId() {
        return Optional.ofNullable(lastCompletedSnapshotId);
    }

    private void cleanupTimedOutSnapshots() {
        Iterator<Map.Entry<String, ReplicatedSubscriptionsSnapshotBuilder>> it = pendingSnapshots.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ReplicatedSubscriptionsSnapshotBuilder> entry = it.next();
            if (entry.getValue().isTimedOut()) {
                if (!pendingSnapshots.remove(entry.getKey(), entry.getValue())) {
                    continue;
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Snapshot creation timed out for {}", topic.getName(), entry.getKey());
                }

                pendingSnapshotsMetric.dec();
                timedoutSnapshotsMetric.inc();
                var latencyMillis = entry.getValue().getDurationMillis();
                stats.recordSnapshotTimedOut(latencyMillis);
            }
        }
    }

    void snapshotCompleted(String snapshotId) {
        ReplicatedSubscriptionsSnapshotBuilder snapshot = pendingSnapshots.remove(snapshotId);
        if (snapshot == null) {
            return;
        }

        lastCompletedSnapshotId = snapshotId;
        lastCompletedSnapshotStartTime = snapshot.getStartTimeMillis();

        pendingSnapshotsMetric.dec();
        var latencyMillis = snapshot.getDurationMillis();
        ReplicatedSubscriptionsSnapshotBuilder.SNAPSHOT_METRIC.observe(latencyMillis);
        stats.recordSnapshotCompleted(latencyMillis);
    }

    void writeMarker(ByteBuf marker) {
        try {
            topic.publishMessage(marker, this);
        } finally {
            marker.release();
        }
    }

    CompletableFuture<Position> writeMarkerAndGetPosition(ByteBuf marker) {
        CompletableFuture<Position> future = new CompletableFuture<>();
        Topic.PublishContext publishContext = new Topic.PublishContext() {
            @Override
            public void completed(Exception e, long ledgerId, long entryId) {
                ReplicatedSubscriptionsController.this.completed(e, ledgerId, entryId);
                if (e != null) {
                    future.completeExceptionally(e);
                } else {
                    future.complete(PositionFactory.create(ledgerId, entryId));
                }
            }

            @Override
            public boolean isMarkerMessage() {
                return true;
            }
        };

        try {
            topic.publishMessage(marker, publishContext);
        } catch (Exception e) {
            publishContext.completed(e, -1, -1);
        } finally {
            marker.release();
        }
        return future;
    }

    /**
     * From Topic.PublishContext.
     */
    @Override
    public void completed(Exception e, long ledgerId, long entryId) {
        // Snapshot timeouts and pending subscription updates handle their respective publish retries.
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Published marker at {}:{}. Exception: {}",
                    topic.getBrokerService().pulsar().getBrokerId(), topic.getName(), ledgerId, entryId, e);
        }

        this.positionOfLastLocalMarker = PositionFactory.create(ledgerId, entryId);
    }

    PersistentTopic topic() {
        return topic;
    }

    String localCluster() {
        return localCluster;
    }

    @VisibleForTesting
    public ConcurrentMap<String, ReplicatedSubscriptionsSnapshotBuilder> pendingSnapshots() {
        return pendingSnapshots;
    }

    @Override
    public boolean isMarkerMessage() {
        // Everything published by this controller will be a marker a message
        return true;
    }

    @Override
    public void close() {
        synchronized (subscriptionUpdateLifecycleLock) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            pendingReceivedSubscriptionUpdates.clear();
            pendingSubscriptionUpdates.clear();
        }
        timer.cancel(true);
    }
}
