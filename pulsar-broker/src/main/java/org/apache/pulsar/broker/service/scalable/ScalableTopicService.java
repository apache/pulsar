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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;

/**
 * Central service managing all scalable topics on this broker.
 *
 * <p>Lifecycle is tied to {@link BrokerService}. This service handles:
 * <ul>
 *   <li>Creating and deleting scalable topics</li>
 *   <li>Managing {@link ScalableTopicController} instances for topics this broker coordinates</li>
 *   <li>Admin operations: split/merge</li>
 * </ul>
 */
@Slf4j
public class ScalableTopicService {

    private final BrokerService brokerService;
    private final ScalableTopicResources resources;
    private final CoordinationService coordinationService;

    /** Active controllers for topics this broker coordinates. */
    private final ConcurrentHashMap<String, ScalableTopicController> controllers = new ConcurrentHashMap<>();

    public ScalableTopicService(BrokerService brokerService,
                                ScalableTopicResources resources,
                                CoordinationService coordinationService) {
        this.brokerService = brokerService;
        this.resources = resources;
        this.coordinationService = coordinationService;
    }

    // --- Lifecycle ---

    public void start() {
        log.info("ScalableTopicService started");
    }

    public void close() {
        log.info("Closing ScalableTopicService, releasing {} controllers", controllers.size());
        controllers.values().forEach(controller -> {
            try {
                controller.close().join();
            } catch (Exception e) {
                log.warn("Error closing controller for topic {}", controller.getTopicName(), e);
            }
        });
        controllers.clear();
    }

    // --- Controller management ---

    /**
     * Get or create a controller for a scalable topic. The controller will attempt
     * leader election; only the leader actively coordinates consumers.
     */
    public CompletableFuture<ScalableTopicController> getOrCreateController(TopicName topic) {
        String key = topic.toString();
        ScalableTopicController existing = controllers.get(key);
        if (existing != null) {
            return CompletableFuture.completedFuture(existing);
        }

        String lockPath = resources.controllerLockPath(topic);
        LeaderElection<String> election = coordinationService.getLeaderElection(
                String.class, lockPath, state -> onLeaderStateChange(topic, state));

        ScalableTopicController controller = new ScalableTopicController(
                topic, resources, brokerService, election);
        controllers.put(key, controller);

        return controller.initialize()
                .thenApply(__ -> controller)
                .exceptionally(ex -> {
                    controllers.remove(key);
                    throw new RuntimeException("Failed to initialize controller for " + topic, ex);
                });
    }

    /**
     * Release the controller for a topic (e.g., on topic unload).
     */
    public CompletableFuture<Void> releaseController(TopicName topic) {
        ScalableTopicController controller = controllers.remove(topic.toString());
        if (controller != null) {
            return controller.close();
        }
        return CompletableFuture.completedFuture(null);
    }

    // --- Admin operations ---

    /**
     * Create a new scalable topic with the given number of initial segments.
     */
    public CompletableFuture<Void> createScalableTopic(TopicName topic, int numInitialSegments) {
        return createScalableTopic(topic, numInitialSegments, Map.of());
    }

    public CompletableFuture<Void> createScalableTopic(TopicName topic, int numInitialSegments,
                                                        Map<String, String> properties) {
        if (topic.getDomain() != TopicDomain.topic) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Expected topic domain, got: " + topic.getDomain()));
        }
        if (numInitialSegments < 1) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("numInitialSegments must be >= 1"));
        }

        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(
                numInitialSegments, properties);

        return resources.createScalableTopicAsync(topic, metadata)
                .thenCompose(__ -> {
                    // Create underlying persistent topics for each initial segment
                    CompletableFuture<?>[] segmentFutures = metadata.getSegments().values().stream()
                            .map(segment -> createUnderlyingSegmentTopic(topic, segment))
                            .toArray(CompletableFuture[]::new);
                    return CompletableFuture.allOf(segmentFutures);
                });
    }

    /**
     * Delete a scalable topic and all its segment topics.
     */
    public CompletableFuture<Void> deleteScalableTopic(TopicName topic) {
        return releaseController(topic)
                .thenCompose(__ -> resources.getScalableTopicMetadataAsync(topic))
                .thenCompose(optMd -> {
                    if (optMd.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    ScalableTopicMetadata metadata = optMd.get();
                    // Delete all underlying segment topics
                    CompletableFuture<?>[] deleteFutures = metadata.getSegments().values().stream()
                            .map(segment -> deleteUnderlyingSegmentTopic(topic, segment))
                            .toArray(CompletableFuture[]::new);
                    return CompletableFuture.allOf(deleteFutures);
                })
                .thenCompose(__ -> resources.deleteScalableTopicAsync(topic));
    }

    // --- Internal helpers ---

    private void onLeaderStateChange(TopicName topic, LeaderElectionState state) {
        log.info("Leader state change for scalable topic {}: {}", topic, state);
        if (state == LeaderElectionState.NoLeader) {
            // Try to re-elect
            ScalableTopicController controller = controllers.get(topic.toString());
            if (controller != null) {
                controller.initialize().exceptionally(ex -> {
                    log.warn("Failed to re-elect for topic {}", topic, ex);
                    return null;
                });
            }
        }
    }

    private CompletableFuture<Void> createUnderlyingSegmentTopic(TopicName parentTopic, SegmentInfo segment) {
        TopicName segmentTopic = SegmentTopicName.fromParent(
                parentTopic, segment.hashRange(), segment.segmentId());
        String persistentName = toPersistentName(segmentTopic);
        return brokerService.getOrCreateTopic(persistentName)
                .thenAccept(t -> log.info("Created segment topic: {}", persistentName));
    }

    private CompletableFuture<Void> deleteUnderlyingSegmentTopic(TopicName parentTopic, SegmentInfo segment) {
        TopicName segmentTopic = SegmentTopicName.fromParent(
                parentTopic, segment.hashRange(), segment.segmentId());
        String persistentName = toPersistentName(segmentTopic);
        return brokerService.deleteTopic(persistentName, true)
                .exceptionally(ex -> {
                    log.warn("Failed to delete segment topic {}: {}", persistentName, ex.getMessage());
                    return null;
                });
    }

    /**
     * Convert a segment:// topic name to persistent:// for the underlying managed ledger topic.
     */
    private String toPersistentName(TopicName segmentTopic) {
        return "persistent://" + segmentTopic.getTenant() + "/"
                + segmentTopic.getNamespacePortion() + "/"
                + segmentTopic.getLocalName();
    }
}
