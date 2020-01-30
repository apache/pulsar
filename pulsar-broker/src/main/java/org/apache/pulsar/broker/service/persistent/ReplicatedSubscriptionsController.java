/**
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

import io.netty.buffer.ByteBuf;
import io.prometheus.client.Gauge;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ClusterMessageId;
import org.apache.pulsar.common.api.proto.PulsarMarkers.MarkerType;
import org.apache.pulsar.common.api.proto.PulsarMarkers.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshotRequest;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshotResponse;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsUpdate;
import org.apache.pulsar.common.protocol.Markers;

/**
 * Encapsulate all the logic of replicated subscriptions tracking for a given topic.
 */
@Slf4j
public class ReplicatedSubscriptionsController implements AutoCloseable, Topic.PublishContext {
    private final PersistentTopic topic;
    private final String localCluster;

    private final ScheduledFuture<?> timer;

    private final ConcurrentMap<String, ReplicatedSubscriptionsSnapshotBuilder> pendingSnapshots = new ConcurrentHashMap<>();

    private final static Gauge pendingSnapshotsMetric = Gauge
            .build("pulsar_replicated_subscriptions_pending_snapshots",
                    "Counter of currently pending snapshots")
            .register();

    public ReplicatedSubscriptionsController(PersistentTopic topic, String localCluster) {
        this.topic = topic;
        this.localCluster = localCluster;
        timer = topic.getBrokerService().pulsar().getExecutor()
                .scheduleAtFixedRate(this::startNewSnapshot, 0,
                        topic.getBrokerService().pulsar().getConfiguration()
                                .getReplicatedSubscriptionsSnapshotFrequencyMillis(),
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

    public void localSubscriptionUpdated(String subscriptionName, ReplicatedSubscriptionsSnapshot snapshot) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Updating subscription to snapshot {}", topic, subscriptionName,
                    snapshot.getClustersList().stream()
                            .map(cmid -> String.format("%s -> %d:%d", cmid.getCluster(),
                                    cmid.getMessageId().getLedgerId(), cmid.getMessageId().getEntryId()))
                            .collect(Collectors.toList()));
        }

        Map<String, MessageIdData> clusterIds = new TreeMap<>();
        for (int i = 0, size = snapshot.getClustersCount(); i < size; i++) {
            ClusterMessageId cmid = snapshot.getClusters(i);
            clusterIds.put(cmid.getCluster(), cmid.getMessageId());
        }

        ByteBuf subscriptionUpdate = Markers.newReplicatedSubscriptionsUpdate(subscriptionName, clusterIds);
        topic.publishMessage(subscriptionUpdate, this);
    }

    private void receivedSnapshotRequest(ReplicatedSubscriptionsSnapshotRequest request) {
        // Send response containing the current last written message id. The response
        // marker we're publishing locally and then replicating will have a higher
        // message id.
        PositionImpl lastMsgId = (PositionImpl) topic.getLastPosition();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received snapshot request. Last msg id: {}", topic.getName(), lastMsgId);
        }

        ByteBuf marker = Markers.newReplicatedSubscriptionsSnapshotResponse(
                request.getSnapshotId(),
                request.getSourceCluster(),
                localCluster,
                lastMsgId.getLedgerId(), lastMsgId.getEntryId());

        topic.publishMessage(marker, this);
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
        MessageIdData updatedMessageId = null;
        for (int i = 0, size = update.getClustersCount(); i < size; i++) {
            ClusterMessageId cmid = update.getClusters(i);
            if (localCluster.equals(cmid.getCluster())) {
                updatedMessageId = cmid.getMessageId();
            }
        }

        if (updatedMessageId == null) {
            // No updates for this cluster, ignore
            return;
        }

        Position pos = new PositionImpl(updatedMessageId.getLedgerId(), updatedMessageId.getEntryId());

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Received update for subscription to {}", topic, update.getSubscriptionName(), pos);
        }

        PersistentSubscription sub = topic.getSubscription(update.getSubscriptionName());
        if (sub != null) {
            sub.acknowledgeMessage(Collections.singletonList(pos), AckType.Cumulative, Collections.emptyMap());
        } else {
            // Subscription doesn't exist. We need to force the creation of the subscription in this cluster, because
            log.info("[{}][{}] Creating subscription at {}:{} after receiving update from replicated subcription",
                    topic, update.getSubscriptionName(), updatedMessageId.getLedgerId(), pos);
            topic.createSubscription(update.getSubscriptionName(),
                    InitialPosition.Latest, true /* replicateSubscriptionState */);
        }
    }

    private void startNewSnapshot() {
        cleanupTimedOutSnapshots();

        AtomicBoolean anyReplicatorDisconnected = new AtomicBoolean();
        topic.getReplicators().forEach((cluster, replicator) -> {
            if (!replicator.isConnected()) {
                anyReplicatorDisconnected.set(true);
            }
        });

        if (anyReplicatorDisconnected.get()) {
            // Do not attempt to create snapshot when some of the clusters are not reachable
            return;
        }

        pendingSnapshotsMetric.inc();
        ReplicatedSubscriptionsSnapshotBuilder builder = new ReplicatedSubscriptionsSnapshotBuilder(this,
                topic.getReplicators().keys(), topic.getBrokerService().pulsar().getConfiguration(), Clock.systemUTC());
        pendingSnapshots.put(builder.getSnapshotId(), builder);
        builder.start();

    }

    private void cleanupTimedOutSnapshots() {
        Iterator<Map.Entry<String, ReplicatedSubscriptionsSnapshotBuilder>> it = pendingSnapshots.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ReplicatedSubscriptionsSnapshotBuilder> entry = it.next();
            if (entry.getValue().isTimedOut()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Snapshot creation timed out for {}", topic.getName(), entry.getKey());
                }

                pendingSnapshotsMetric.dec();
                it.remove();
            }
        }
    }

    void snapshotCompleted(String snapshotId) {
        pendingSnapshots.remove(snapshotId);
        pendingSnapshotsMetric.dec();
    }

    void writeMarker(ByteBuf marker) {
        topic.publishMessage(marker, this);
    }

    /**
     * From Topic.PublishContext
     */
    @Override
    public void completed(Exception e, long ledgerId, long entryId) {
        // Nothing to do in case of publish errors since the retry logic is applied upstream after a snapshot is not
        // closed
        if (log.isDebugEnabled()) {
            log.debug("[{}] Published marker at {}:{}. Exception: {}", topic.getName(), ledgerId, entryId, e);
        }
    }

    PersistentTopic topic() {
        return topic;
    }

    String localCluster() {
        return localCluster;
    }

    @Override
    public void close() {
        timer.cancel(true);
    }
}
