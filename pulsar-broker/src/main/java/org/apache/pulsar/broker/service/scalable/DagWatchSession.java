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
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
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
public class DagWatchSession implements ScalableTopicResources.MetadataPathListener {

    private static final Logger LOG = Logger.get(DagWatchSession.class);

    /** Initial segment count for an auto-created scalable topic. */
    private static final int AUTO_CREATE_INITIAL_SEGMENTS = 1;

    private final Logger log;

    @Getter
    private final long sessionId;
    private final TopicName topicName;
    private final ServerCnx cnx;
    private final ScalableTopicResources resources;
    private final BrokerService brokerService;

    private final String metadataPath;
    /** Canonical {@code topic://...} identity, regardless of the input form ({@code topic://},
     *  {@code persistent://}, or short-form). Used both as the {@code resolved_topic_name}
     *  reported to the client and as the parent when computing {@code segment://} URIs for a
     *  real DAG (those require the {@code topic://} domain). */
    private final TopicName scalableTopicName;
    private final String resolvedTopicName;
    /** When false, a {@code topic://} lookup of a non-existent scalable topic must not auto-create
     *  it (set by namespace consumers so a deleted topic isn't resurrected on a per-topic reconnect). */
    private final boolean createIfMissing;
    private volatile boolean closed = false;

    public DagWatchSession(long sessionId,
                           TopicName topicName,
                           ServerCnx cnx,
                           ScalableTopicResources resources,
                           BrokerService brokerService,
                           boolean createIfMissing) {
        this.sessionId = sessionId;
        this.topicName = topicName;
        this.cnx = cnx;
        this.resources = resources;
        this.brokerService = brokerService;
        this.createIfMissing = createIfMissing;
        this.metadataPath = resources.topicPath(topicName);
        this.scalableTopicName = topicName.toScalableTopic();
        this.resolvedTopicName = scalableTopicName.toString();
        this.log = LOG.with().attr("topic", topicName).attr("sessionId", sessionId).build();
    }

    @Override
    public String getMetadataPath() {
        return metadataPath;
    }

    /**
     * Start the session: load current metadata, set up watch, and return
     * the initial layout response.
     *
     * <p>If no scalable metadata exists at the canonical path:
     * <ul>
     *   <li>{@code topic://...} input → fail with {@code TopicNotFound} (the scalable
     *       topic doesn't exist).</li>
     *   <li>{@code persistent://...} input → build a synthetic layout that wraps the
     *       existing regular (partitioned or non-partitioned) topic as one or more
     *       legacy segments, so V5 clients can operate against the regular topic
     *       through the scalable surface until the operator migrates it.</li>
     * </ul>
     * The metadata-store watch is registered regardless, so a subsequent migration
     * that writes scalable metadata to the same path will be observed and the
     * synthetic layout will be transparently replaced with the real DAG.
     */
    public CompletableFuture<ScalableTopicLayoutResponse> start() {
        // A specific partition (persistent://t/n/x-partition-K) is not a valid
        // scalable-topic lookup target — the synthetic layout models the whole
        // partitioned topic. Reject it up front rather than producing a layout that
        // wraps nonsensical -partition-K-partition-J names.
        if (topicName.isPartitioned()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "Cannot open a scalable-topic lookup for an individual partition: " + topicName
                            + "; use the base topic name " + topicName.getPartitionedTopicName()));
        }

        // Register through the resources-level fan-out so close() can deregister us
        // and we don't accumulate stale store-level listeners over time.
        resources.registerPathListener(this);

        return resources.getScalableTopicMetadataAsync(topicName, true)
                .thenCompose(optMd -> {
                    if (optMd.isPresent()) {
                        return buildResponse(optMd.get());
                    }
                    if (topicName.getDomain() == TopicDomain.persistent) {
                        return buildSyntheticResponse();
                    }
                    if (topicName.getDomain() == TopicDomain.topic) {
                        if (!createIfMissing) {
                            // Caller (e.g. a namespace consumer) opted out of auto-creation: fail
                            // not-found rather than resurrect a topic that doesn't currently exist.
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("Scalable topic not found: " + topicName));
                        }
                        return maybeAutoCreateAndBuildResponse();
                    }
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Scalable topic not found: " + topicName));
                });
    }

    /**
     * A lookup for a {@code topic://...} scalable topic that doesn't exist yet. Auto-create it
     * with a single initial segment — gated by the same broker/namespace auto-topic-creation
     * policy as regular topics ({@link BrokerService#isAllowAutoTopicCreationAsync}) — then
     * return its layout. If the policy disallows it, fail with the same not-found error as
     * before so the client sees no behavioural change when auto-creation is off.
     */
    private CompletableFuture<ScalableTopicLayoutResponse> maybeAutoCreateAndBuildResponse() {
        return brokerService.isAllowAutoTopicCreationAsync(scalableTopicName)
                .thenCompose(allowed -> {
                    if (!allowed) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("Scalable topic not found: " + topicName));
                    }
                    return brokerService.getScalableTopicService()
                            .createScalableTopic(scalableTopicName, AUTO_CREATE_INITIAL_SEGMENTS)
                            // Tolerate a concurrent lookup that created it first; any other
                            // failure propagates.
                            .handle((v, ex) -> ex)
                            .thenCompose(ex -> {
                                if (ex != null && !(FutureUtil.unwrapCompletionException(ex)
                                        instanceof MetadataStoreException.AlreadyExistsException)) {
                                    return CompletableFuture.<ScalableTopicLayoutResponse>failedFuture(ex);
                                }
                                log.info().log("Auto-created scalable topic");
                                return resources.getScalableTopicMetadataAsync(scalableTopicName, true)
                                        .thenCompose(opt -> opt.isPresent()
                                                ? buildResponse(opt.get())
                                                : CompletableFuture.failedFuture(
                                                        new IllegalStateException(
                                                                "Scalable topic not found after "
                                                                        + "auto-create: " + topicName)));
                            });
                });
    }

    /**
     * Build a synthetic layout for a not-yet-migrated regular topic. Each partition
     * (or the whole topic, for non-partitioned) becomes an active legacy segment
     * that wraps the existing {@code persistent://...} topic. The synthetic layout
     * uses mod-N routing (signalled by all active leaves being legacy segments)
     * so V5 producers route the same way v4 producers do.
     *
     * <p>Note: {@code fetchPartitionedTopicMetadataAsync} returns {@code partitions == 0}
     * for both an existing non-partitioned topic and a topic that does not exist at all.
     * In the latter case a single-segment synthetic layout is still returned; whether the
     * underlying {@code persistent://...} topic gets auto-created is then decided by the
     * namespace's auto-creation policy when the V5 producer/consumer first attaches —
     * exactly the behaviour a v4 client would see.
     */
    private CompletableFuture<ScalableTopicLayoutResponse> buildSyntheticResponse() {
        return brokerService.fetchPartitionedTopicMetadataAsync(topicName)
                .thenApply(partitionedMd -> {
                    int partitions = partitionedMd.partitions;
                    long createdAtMs = System.currentTimeMillis();
                    Map<Long, SegmentInfo> segments = new LinkedHashMap<>();
                    if (partitions <= 0) {
                        // Non-partitioned: one legacy segment covering the full hash range.
                        // We're only called when topicName.getDomain() == persistent, so
                        // toString() is the canonical persistent://t/n/x form.
                        segments.put(0L, SegmentInfo.activeLegacy(
                                0L,
                                HashRange.of(0x0000, 0xFFFF),
                                topicName.toString(),
                                /*createdAtEpoch*/ 0L,
                                createdAtMs));
                    } else {
                        // Partitioned: N legacy segments. The hash ranges here are cosmetic —
                        // routing for synthetic layouts is mod-N over segment_id (see
                        // SegmentRouter on the SDK side), so the ranges are never consulted
                        // for routing. They're assigned for stable, human-readable layouts.
                        for (int k = 0; k < partitions; k++) {
                            HashRange range = syntheticRange(k, partitions);
                            segments.put((long) k, SegmentInfo.activeLegacy(
                                    k,
                                    range,
                                    topicName.getPartition(k).toString(),
                                    /*createdAtEpoch*/ 0L,
                                    createdAtMs));
                        }
                    }
                    return new ScalableTopicLayoutResponse(
                            /*epoch*/ 0L,
                            segments,
                            /*segmentBrokerAddresses*/ null,
                            /*segmentBrokerAddressesTls*/ null,
                            /*controllerBrokerUrl*/ null,
                            /*controllerBrokerUrlTls*/ null);
                });
    }

    /**
     * Cosmetic hash range for partition {@code k} of {@code n} in a synthetic layout.
     * When {@code n <= 0x10000} the ranges tile {@code [0x0000, 0xFFFF]} contiguously;
     * beyond that there are more partitions than hash slots, so a degenerate single-slot
     * range (clamped to the hash space) is used. Either way the value is never consulted
     * for routing — synthetic layouts route mod-N over segment_id.
     */
    private static HashRange syntheticRange(int k, int n) {
        if (n > 0x10000) {
            int slot = Math.min(k, 0xFFFF);
            return HashRange.of(slot, slot);
        }
        int width = 0x10000 / n;
        int start = k * width;
        int end = (k == n - 1) ? 0xFFFF : (start + width - 1);
        return HashRange.of(start, end);
    }

    /**
     * Invoked by the {@link ScalableTopicResources} fan-out for every metadata event
     * matching this session's topic path. The registry already path-filtered for us;
     * we re-check defensively so a registry-level bug can't cause a reload storm.
     */
    @Override
    public void onNotification(Notification notification) {
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
        // Always report the canonical topic://... identity so clients that looked up
        // via persistent://... or short-form know the resolved name.
        cnx.ctx().writeAndFlush(Commands.newScalableTopicUpdate(
                sessionId, resolvedTopicName, dag));
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
            segProto.setCreatedAtMs(seg.createdAtMs());
            if (seg.sealedAtMs() >= 0) {
                segProto.setSealedAtMs(seg.sealedAtMs());
            }
            // Legacy segments wrap an existing, externally managed persistent://... topic.
            if (seg.legacyTopicName() != null) {
                segProto.setLegacyTopicName(seg.legacyTopicName());
            }
            // PIP-486: per-segment entry-bucket split points (empty = single bucket).
            for (int i = 0; i < seg.entryBucketSplits().size(); i++) {
                segProto.addEntryBucketSplit(seg.entryBucketSplits().get(i));
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

        // Propagate the controller-broker URL so V5 clients can connect to the right broker
        // for scalable-topic subscribe. Without this the client falls back to its configured
        // service URL, which on a multi-broker cluster is rarely the controller leader.
        if (response.controllerBrokerUrl() != null) {
            dag.setControllerBrokerUrl(response.controllerBrokerUrl());
        }
        if (response.controllerBrokerUrlTls() != null) {
            dag.setControllerBrokerUrlTls(response.controllerBrokerUrlTls());
        }

        return dag;
    }

    public void close() {
        closed = true;
        // Drop ourselves from the resources' fan-out so the per-event dispatch skips
        // us — no listener leak, no per-notification dispatch tax across the broker's
        // lifetime.
        resources.deregisterPathListener(this);
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
                    // Resolve which broker owns this segment's underlying segment:// topic.
                    // SegmentTopicName.fromParent requires the topic:// domain, so use the
                    // canonical scalable name (the session's input may be persistent://).
                    TopicName segTn = org.apache.pulsar.common.scalable.SegmentTopicName.fromParent(
                            scalableTopicName, segment.hashRange(), segment.segmentId());
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
