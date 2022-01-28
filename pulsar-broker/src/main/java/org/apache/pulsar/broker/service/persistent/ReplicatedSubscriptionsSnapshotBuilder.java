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

import io.prometheus.client.Summary;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshotResponse;
import org.apache.pulsar.common.protocol.Markers;

@Slf4j
public class ReplicatedSubscriptionsSnapshotBuilder {

    private final String snapshotId;
    private final ReplicatedSubscriptionsController controller;

    private final Map<String, MarkersMessageIdData> responses = new TreeMap<>();
    private final List<String> remoteClusters;
    private final Set<String> missingClusters;

    private final boolean needTwoRounds;
    private boolean firstRoundComplete;

    private long startTimeMillis;
    private final long timeoutMillis;

    private final Clock clock;

    private static final Summary snapshotMetric = Summary.build("pulsar_replicated_subscriptions_snapshot_ms",
            "Time taken to create a consistent snapshot across clusters").register();

    public ReplicatedSubscriptionsSnapshotBuilder(ReplicatedSubscriptionsController controller,
            List<String> remoteClusters, ServiceConfiguration conf, Clock clock) {
        this.snapshotId = UUID.randomUUID().toString();
        this.controller = controller;
        this.remoteClusters = remoteClusters;
        this.missingClusters = new TreeSet<>(remoteClusters);
        this.clock = clock;
        this.timeoutMillis = TimeUnit.SECONDS.toMillis(conf.getReplicatedSubscriptionsSnapshotTimeoutSeconds());

        // If we have more than 2 cluster, we need to do 2 rounds of snapshots, to make sure
        // we're catching all the messages eventually exchanged between the two.
        this.needTwoRounds = remoteClusters.size() > 1;
    }

    String getSnapshotId() {
        return snapshotId;
    }

    void start() {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Starting new snapshot {} - Clusters: {}", controller.topic().getName(), snapshotId,
                    missingClusters);
        }
        startTimeMillis = clock.millis();
        controller.writeMarker(
                Markers.newReplicatedSubscriptionsSnapshotRequest(snapshotId, controller.localCluster()));
    }

    synchronized void receivedSnapshotResponse(Position position, ReplicatedSubscriptionsSnapshotResponse response) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received response from {}", controller.topic().getName(),
                    response.getCluster().getCluster());
        }
        String cluster = response.getCluster().getCluster();
        responses.putIfAbsent(cluster, new MarkersMessageIdData().copyFrom(response.getCluster().getMessageId()));
        missingClusters.remove(cluster);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Missing clusters {}", controller.topic().getName(), missingClusters);
        }

        if (!missingClusters.isEmpty()) {
            // We're still waiting for more responses to come back
            return;
        }

        // We have now received all responses

        if (needTwoRounds && !firstRoundComplete) {
            // Mark that 1st round is done and start a 2nd round
            firstRoundComplete = true;
            missingClusters.addAll(remoteClusters);

            controller.writeMarker(
                    Markers.newReplicatedSubscriptionsSnapshotRequest(snapshotId, controller.localCluster()));
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Snapshot is complete {}", controller.topic().getName(), snapshotId);
        }
        // Snapshot is now complete, store it in the local topic
        PositionImpl p = (PositionImpl) position;
        controller.writeMarker(
                Markers.newReplicatedSubscriptionsSnapshot(snapshotId, controller.localCluster(),
                        p.getLedgerId(), p.getEntryId(), responses));
        controller.snapshotCompleted(snapshotId);

        double latencyMillis = clock.millis() - startTimeMillis;
        snapshotMetric.observe(latencyMillis);
    }

    boolean isTimedOut() {
        return (startTimeMillis + timeoutMillis) < clock.millis();
    }

    long getStartTimeMillis() {
        return startTimeMillis;
    }
}
