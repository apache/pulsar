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
package org.apache.pulsar.broker.service.scalable;

import io.github.merlimat.slog.Logger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentBrokerAddress;
import org.apache.pulsar.common.api.proto.SegmentInfoProto;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;

/**
 * Broker-side handler for a client's DAG watch session.
 *
 * <p>Any broker can serve this role since metadata is in the metadata store.
 * The session watches for metadata changes (via Oxia watch) and pushes updated
 * {@link ScalableTopicLayoutResponse} to the client.
 *
 * <p>The session is tied to a connection. When the connection breaks, the session dies.
 * The client must reinitiate a new session (possibly with another broker).
 */
public class DagWatchSession {

    private static final Logger LOG = Logger.get(DagWatchSession.class);
    private final Logger log;

    @Getter
    private final long sessionId;
    private final TopicName topicName;
    private final ServerCnx cnx;
    private final ScalableTopicResources resources;
    private final BrokerService brokerService;

    private final String metadataPath;
    private final java.util.function.Consumer<Notification> notificationListener;
    private volatile boolean closed = false;

    public DagWatchSession(long sessionId,
                           TopicName topicName,
                           ServerCnx cnx,
                           ScalableTopicResources resources,
                           BrokerService brokerService) {
        this.sessionId = sessionId;
        this.topicName = topicName;
        this.cnx = cnx;
        this.resources = resources;
        this.brokerService = brokerService;
        this.metadataPath = resources.topicPath(topicName);
        this.notificationListener = this::onNotification;
        this.log = LOG.with().attr("topic", topicName).attr("sessionId", sessionId).build();
    }

    /**
     * Start the session: load current metadata, set up watch, and return
     * the initial layout response.
     */
    public CompletableFuture<ScalableTopicLayoutResponse> start() {
        // Register metadata store listener for changes to this topic's metadata
        resources.getStore().registerListener(notificationListener);

        return resources.getScalableTopicMetadataAsync(topicName, true)
                .thenCompose(optMd -> {
                    if (optMd.isEmpty()) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("Scalable topic not found: " + topicName));
                    }
                    ScalableTopicMetadata metadata = optMd.get();
                    return buildResponse(metadata);
                });
    }

    // Visible for testing — invoked by the metadata-store listener registered in start().
    void onNotification(Notification notification) {
        if (closed) {
            return;
        }
        if (!metadataPath.equals(notification.getPath())) {
            return;
        }
        if (notification.getType() == NotificationType.Deleted) {
            return;
        }
        // Metadata changed — reload and push update
        resources.getScalableTopicMetadataAsync(topicName, true)
                .thenAccept(optMd -> optMd.ifPresent(this::onMetadataChanged));
    }

    /**
     * Called when the metadata store watch fires (metadata changed).
     */
    public void onMetadataChanged(ScalableTopicMetadata newMetadata) {
        if (closed) {
            return;
        }
        buildResponse(newMetadata).thenAccept(this::pushUpdate);
    }

    /**
     * Push an update to the connected client.
     */
    public void pushUpdate(ScalableTopicLayoutResponse response) {
        if (closed) {
            return;
        }
        ScalableTopicDAG dag = buildDagProto(response);
        log.info().attr("epoch", response.epoch()).log("Pushing DAG update");
        cnx.ctx().writeAndFlush(Commands.newScalableTopicUpdate(sessionId, dag));
    }

    private ScalableTopicDAG buildDagProto(ScalableTopicLayoutResponse response) {
        ScalableTopicDAG dag = new ScalableTopicDAG();
        dag.setEpoch(response.epoch());

        for (var entry : response.segments().entrySet()) {
            SegmentInfo seg = entry.getValue();
            SegmentInfoProto segProto = dag.addSegment();
            segProto.setSegmentId(seg.segmentId());
            segProto.setHashStart(seg.hashRange().start());
            segProto.setHashEnd(seg.hashRange().end());
            segProto.setState(seg.isActive() ? SegmentState.ACTIVE : SegmentState.SEALED);
            for (int i = 0; i < seg.parentIds().size(); i++) {
                segProto.addParentId(seg.parentIds().get(i));
            }
            for (int i = 0; i < seg.childIds().size(); i++) {
                segProto.addChildId(seg.childIds().get(i));
            }
            segProto.setCreatedAtEpoch(seg.createdAtEpoch());
            if (seg.sealedAtEpoch() >= 0) {
                segProto.setSealedAtEpoch(seg.sealedAtEpoch());
            }
        }

        // Add broker addresses for active segments
        Map<Long, String> brokerAddresses = response.segmentBrokerAddresses();
        if (brokerAddresses != null) {
            for (var entry : brokerAddresses.entrySet()) {
                SegmentBrokerAddress addr = dag.addSegmentBroker();
                addr.setSegmentId(entry.getKey());
                addr.setBrokerUrl(entry.getValue());
            }
        }

        return dag;
    }

    public void close() {
        closed = true;
        // Listener is guarded by the closed flag; MetadataStore does not support unregister.
    }

    /**
     * Build a full layout response with broker addresses resolved.
     */
    private CompletableFuture<ScalableTopicLayoutResponse> buildResponse(ScalableTopicMetadata metadata) {
        SegmentLayout layout = SegmentLayout.fromMetadata(metadata);

        // Resolve broker addresses for all active segments
        CompletableFuture<Map<Long, String>> brokersFuture = resolveSegmentBrokers(layout);

        // Resolve controller broker address
        CompletableFuture<Optional<String>> controllerFuture =
                readControllerBrokerUrl();

        return brokersFuture.thenCombine(controllerFuture, (segmentBrokers, controllerUrl) ->
                new ScalableTopicLayoutResponse(
                        layout.getEpoch(),
                        layout.getAllSegments(),
                        segmentBrokers,
                        null,
                        controllerUrl.orElse(null),
                        null));
    }

    private CompletableFuture<Map<Long, String>> resolveSegmentBrokers(SegmentLayout layout) {
        Map<Long, String> result = new LinkedHashMap<>();
        CompletableFuture<?>[] futures = layout.getActiveSegments().values().stream()
                .map(segment -> {
                    // Resolve which broker owns this segment's underlying segment:// topic
                    TopicName segTn = org.apache.pulsar.common.scalable.SegmentTopicName.fromParent(
                            topicName, segment.hashRange(), segment.segmentId());
                    var lookupOptions = org.apache.pulsar.broker.namespace.LookupOptions.builder()
                            .readOnly(false).authoritative(false).build();
                    return brokerService.getPulsar().getNamespaceService()
                            .getBrokerServiceUrlAsync(segTn, lookupOptions)
                            .thenAccept(optUrl -> optUrl.ifPresent(lookupResult -> {
                                synchronized (result) {
                                    result.put(segment.segmentId(),
                                            lookupResult.getLookupData().getBrokerUrl());
                                }
                            }));
                })
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures).thenApply(__ -> result);
    }

    private CompletableFuture<Optional<String>> readControllerBrokerUrl() {
        String lockPath = resources.controllerLockPath(topicName);
        return resources.getStore().get(lockPath)
                .thenCompose(optValue -> {
                    if (optValue.isEmpty()) {
                        return CompletableFuture.completedFuture(Optional.<String>empty());
                    }
                    // The leader-election value is the brokerId of the controller leader,
                    // JSON-encoded by LeaderElection.elect(...). Decode it, then resolve
                    // to a pulsar:// service URL via NamespaceService so clients can
                    // connect to the controller broker for scalable-topic subscribe.
                    String brokerId;
                    try {
                        brokerId = org.apache.pulsar.common.util.ObjectMapperFactory.getMapper()
                                .reader().readValue(optValue.get().getValue(), String.class);
                    } catch (java.io.IOException e) {
                        log.warn().exceptionMessage(e)
                                .log("Invalid controller-leader znode value");
                        return CompletableFuture.completedFuture(Optional.<String>empty());
                    }
                    return brokerService.getPulsar().getNamespaceService()
                            .createLookupResult(brokerId, false, null)
                            .thenApply(lookupResult ->
                                    Optional.ofNullable(lookupResult.getLookupData().getBrokerUrl()))
                            .exceptionally(ex -> {
                                log.warn().attr("brokerId", brokerId).exceptionMessage(ex)
                                        .log("Failed to resolve controller broker");
                                return Optional.<String>empty();
                            });
                });
    }
}
