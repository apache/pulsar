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
import io.github.merlimat.slog.Logger;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
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
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.common.api.proto.ClusterMessageId;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
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
public class ReplicatedSubscriptionsController implements AutoCloseable, Topic.PublishContext {

    private static final Logger LOG = Logger.get(ReplicatedSubscriptionsController.class);
    final Logger log;

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
    private final Object subscriptionUpdateLifecycleLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();

    private static final class PendingReceivedSubscriptionUpdate {
        private final AtomicReference<PersistentSubscription> subscription;
        private final Position position;
        private final AtomicBoolean inFlight = new AtomicBoolean();
        private final AtomicBoolean attempted = new AtomicBoolean();

        private PendingReceivedSubscriptionUpdate(PersistentSubscription subscription, Position position) {
            this.subscription = new AtomicReference<>(subscription);
            this.position = position;
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
        this.log = LOG.with().attr("topic", topic.getName()).build();
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
            log.warn().exceptionMessage(e).log("Failed to parse marker");
        }
    }

    public void localSubscriptionUpdated(String subscriptionName,
                                         ReplicatedSubscriptionSnapshotCache.SnapshotResult snapshot) {
        log.debug()
                .attr("subscription", subscriptionName)
                .attr("clusters", snapshot.clusters().stream()
                        .map(entry -> String.format("%s -> %s", entry.cluster(), entry.position()))
                        .collect(Collectors.toList()))
                .log("Updating subscription to snapshot");

        Map<String, MarkersMessageIdData> clusterIds = new TreeMap<>();
        for (ReplicatedSubscriptionSnapshotCache.ClusterEntry cluster : snapshot.clusters()) {
            clusterIds.put(cluster.cluster(), new MarkersMessageIdData().setLedgerId(cluster.position().getLedgerId())
                    .setEntryId(cluster.position().getEntryId()));
        }

        ByteBuf subscriptionUpdate = Markers.newReplicatedSubscriptionsUpdate(subscriptionName, clusterIds);
        writeMarker(subscriptionUpdate);
    }

    private void receivedSnapshotRequest(ReplicatedSubscriptionsSnapshotRequest request) {
        Replicator replicator = topic.getReplicators().get(request.getSourceCluster());
        if (replicator == null) {
            log.warn()
                    .attr("snapshotId", request.getSnapshotId())
                    .attr("sourceCluster", request.getSourceCluster())
                    .log("Received replicated subscription snapshot request from cluster, but no replicator is"
                            + "configured for that cluster. Ignoring the request.");
            return;
        }

        // Send response containing the current last written message id. The response
        // marker we're publishing locally and then replicating will have a higher
        // message id.
        Position lastMsgId = topic.getLastPosition();
        log.debug()
                .attr("brokerId", topic.getBrokerService().pulsar().getBrokerId())
                .attr("lastMsgId", lastMsgId)
                .log("Received snapshot request. Last msg id");

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
            log.debug()
                    .attr("snapshotId", snapshotId)
                    .attr("cluster", response.getCluster().getCluster())
                    .log("Received late reply for timed-out snapshot from");
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
            log.debug()
                    .attr("subscriptionName", subscriptionName)
                    .log("Ignoring replicated subscription update for the compaction subscription");
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
            pendingUpdate = pendingReceivedSubscriptionUpdates.compute(subscriptionName, (name, pending) -> {
                if (pending != null) {
                    PersistentSubscription bound = pending.subscription.get();
                    if ((bound == null || bound == subscription) && pending.position.compareTo(target) >= 0) {
                        // An initially missing subscription may have been created while its retry
                        // was pending. Preserve the higher target, but bind its current identity.
                        return bound == subscription ? pending
                                : new PendingReceivedSubscriptionUpdate(subscription, pending.position);
                    }
                }
                return new PendingReceivedSubscriptionUpdate(subscription, target);
            });
        }
        tryApplyReceivedSubscriptionUpdate(subscriptionName, pendingUpdate);
    }

    private boolean isCurrentReceivedSubscriptionUpdate(String subscriptionName,
                                                         PendingReceivedSubscriptionUpdate update) {
        if (closed.get() || pendingReceivedSubscriptionUpdates.get(subscriptionName) != update) {
            return false;
        }
        PersistentSubscription subscription = update.subscription.get();
        PersistentSubscription current = topic.getSubscription(subscriptionName);
        if (subscription == null && current != null) {
            update.subscription.compareAndSet(null, current);
            subscription = update.subscription.get();
        }
        if (subscription != current) {
            // A retry bound to an existing subscription must not recreate or update its replacement.
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
        PersistentSubscription subscription = update.subscription.get();
        if (!firstAttempt && subscription != null
                && subscription.getCursor().getMarkDeletedPosition().compareTo(update.position) >= 0) {
            // Preserve the initial ACK call, but do not repeatedly ACK an already-applied target.
            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
            update.inFlight.set(false);
            return;
        }
        applyReceivedSubscriptionUpdate(subscriptionName, update).whenComplete((ignored, error) -> {
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
        PersistentSubscription subscription = update.subscription.get();
        Position markDeletePosition = subscription == null ? null : subscription.getCursor().getMarkDeletedPosition();
        if (markDeletePosition != null && update.position.compareTo(markDeletePosition) <= 0) {
            // A failed ACK future can still have applied, or a local consumer can have advanced farther.
            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
            log.debug()
                    .attr("subscriptionName", subscriptionName)
                    .attr("pos", update.position)
                    .attr("markDeletePosition", markDeletePosition)
                    .log("Ignoring replicated subscription update behind the mark-delete position");
        } else if (firstAttempt) {
            if (subscription == null) {
                log.warn()
                        .attr("subscriptionName", subscriptionName)
                        .attr("pos", update.position)
                        .exceptionMessage(FutureUtil.unwrapCompletionException(error))
                        .log("Failed to create replicated subscription");
            } else {
                log.warn()
                        .attr("subscriptionName", subscriptionName)
                        .attr("pos", update.position)
                        .attr("markDeletePosition", markDeletePosition)
                        .exceptionMessage(FutureUtil.unwrapCompletionException(error))
                        .log("Failed to update replicated subscription");
            }
        } else {
            log.debug()
                    .attr("subscriptionName", subscriptionName)
                    .attr("pos", update.position)
                    .attr("markDeletePosition", markDeletePosition)
                    .exceptionMessage(FutureUtil.unwrapCompletionException(error))
                    .log("Failed to apply replicated subscription update; retaining it for retry");
        }
    }

    private CompletableFuture<Void> applyReceivedSubscriptionUpdate(String subscriptionName,
                                                                    PendingReceivedSubscriptionUpdate update) {
        return FutureUtil.supplySafely(() -> {
            if (!isCurrentReceivedSubscriptionUpdate(subscriptionName, update)) {
                return CompletableFuture.completedFuture(null);
            }
            PersistentSubscription subscription = update.subscription.get();
            if (subscription != null) {
                // Received updates historically apply to existing non-replicated subscriptions too.
                return subscription.acknowledgeMessageAsync(Collections.singletonList(update.position),
                        AckType.Cumulative, Collections.emptyMap());
            }
            log.debug()
                    .attr("subscription", subscriptionName)
                    .attr("position", update.position)
                    .log("Creating subscription for received replicated subscription update");
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
                        if (!update.subscription.compareAndSet(null, createdSubscription)
                                && update.subscription.get() != createdSubscription) {
                            pendingReceivedSubscriptionUpdates.remove(subscriptionName, update);
                            return CompletableFuture.completedFuture(null);
                        }
                        if (!isCurrentReceivedSubscriptionUpdate(subscriptionName, update)) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return FutureUtil.supplySafely(() -> createdSubscription.acknowledgeMessageAsync(
                                Collections.singletonList(update.position), AckType.Cumulative,
                                Collections.emptyMap()));
                    });
        });
    }

    private void startNewSnapshot() {
        if (closed.get()) {
            return;
        }
        // The received marker may already be consumed; applying it must retry even on idle topics.
        pendingReceivedSubscriptionUpdates.forEach(this::tryApplyReceivedSubscriptionUpdate);
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
            log.debug()
                    .log("PendingSnapshot exists but has never succeeded. "
                            + "Skipping snapshot creation until pending snapshot timeout.");
            return;
        }

        if (topic.getLastMaxReadPositionMovedForwardTimestamp() < lastCompletedSnapshotStartTime
                || topic.getLastMaxReadPositionMovedForwardTimestamp() == 0) {
            // There was no message written since the last snapshot, we can skip creating a new snapshot
            log.debug()
                    .attr("brokerId", topic.getBrokerService().pulsar().getBrokerId())
                    .log("There is no new data in topic. Skipping snapshot creation.");
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
            log.debug()
                    .log("Do not attempt to create snapshot when some of the clusters are not reachable.");
            return;
        }

        log.debug()
                .attr("brokerId", topic.getBrokerService().pulsar().getBrokerId())
                .log("Starting snapshot creation.");

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

                log.debug()
                        .attr("key", entry.getKey())
                        .log("Snapshot creation timed out for");

                pendingSnapshotsMetric.dec();
                timedoutSnapshotsMetric.inc();
                var latencyMillis = entry.getValue().getDurationMillis();
                stats.recordSnapshotTimedOut(latencyMillis);
            }
        }
    }

    @SuppressWarnings("deprecation")
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
        // Nothing to do in case of publish errors since the retry logic is applied upstream after a snapshot is not
        // closed
        log.debug()
                .attr("brokerId", topic.getBrokerService().pulsar().getBrokerId())
                .attr("ledgerId", ledgerId)
                .attr("entryId", entryId)
                .exceptionMessage(e)
                .log("Published marker");

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
        }
        timer.cancel(true);
    }
}
