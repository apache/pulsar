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
package org.apache.pulsar.client.admin;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;

/**
 * Admin interface for scalable topic management.
 *
 * <p>Scalable topics (topic:// domain) are composed of a DAG of hash-range segments
 * that can be dynamically split and merged.
 */
public interface ScalableTopics {

    /**
     * Get the list of scalable topics under a namespace.
     *
     * @param namespace Namespace name in the format "tenant/namespace"
     * @return list of scalable topic names
     */
    List<String> listScalableTopics(String namespace) throws PulsarAdminException;

    /**
     * Get the list of scalable topics under a namespace asynchronously.
     *
     * @param namespace Namespace name in the format "tenant/namespace"
     * @return list of scalable topic names
     */
    CompletableFuture<List<String>> listScalableTopicsAsync(String namespace);

    /**
     * Create a new scalable topic.
     *
     * @param topic              Topic name in the format "tenant/namespace/topic"
     * @param numInitialSegments Number of initial segments (must be >= 1)
     */
    void createScalableTopic(String topic, int numInitialSegments) throws PulsarAdminException;

    /**
     * Create a new scalable topic asynchronously.
     *
     * @param topic              Topic name in the format "tenant/namespace/topic"
     * @param numInitialSegments Number of initial segments (must be >= 1)
     */
    CompletableFuture<Void> createScalableTopicAsync(String topic, int numInitialSegments);

    /**
     * Create a new scalable topic with properties.
     *
     * @param topic              Topic name in the format "tenant/namespace/topic"
     * @param numInitialSegments Number of initial segments (must be >= 1)
     * @param properties         Key-value properties for the topic metadata
     */
    void createScalableTopic(String topic, int numInitialSegments, Map<String, String> properties)
            throws PulsarAdminException;

    /**
     * Create a new scalable topic with properties asynchronously.
     *
     * @param topic              Topic name in the format "tenant/namespace/topic"
     * @param numInitialSegments Number of initial segments (must be >= 1)
     * @param properties         Key-value properties for the topic metadata
     */
    CompletableFuture<Void> createScalableTopicAsync(String topic, int numInitialSegments,
                                                      Map<String, String> properties);

    /**
     * Get scalable topic metadata.
     *
     * @param topic Topic name in the format "tenant/namespace/topic"
     * @return the scalable topic metadata including segment DAG
     */
    ScalableTopicMetadata getMetadata(String topic) throws PulsarAdminException;

    /**
     * Get scalable topic metadata asynchronously.
     *
     * @param topic Topic name in the format "tenant/namespace/topic"
     * @return the scalable topic metadata including segment DAG
     */
    CompletableFuture<ScalableTopicMetadata> getMetadataAsync(String topic);

    /**
     * Delete a scalable topic and all its underlying segment topics.
     *
     * @param topic Topic name in the format "tenant/namespace/topic"
     * @param force Force deletion even if topic has active subscriptions
     */
    void deleteScalableTopic(String topic, boolean force) throws PulsarAdminException;

    /**
     * Delete a scalable topic and all its underlying segment topics asynchronously.
     *
     * @param topic Topic name in the format "tenant/namespace/topic"
     * @param force Force deletion even if topic has active subscriptions
     */
    CompletableFuture<Void> deleteScalableTopicAsync(String topic, boolean force);

    /**
     * Delete a scalable topic and all its underlying segment topics.
     *
     * @param topic Topic name in the format "tenant/namespace/topic"
     */
    default void deleteScalableTopic(String topic) throws PulsarAdminException {
        deleteScalableTopic(topic, false);
    }

    /**
     * Delete a scalable topic and all its underlying segment topics asynchronously.
     *
     * @param topic Topic name in the format "tenant/namespace/topic"
     */
    default CompletableFuture<Void> deleteScalableTopicAsync(String topic) {
        return deleteScalableTopicAsync(topic, false);
    }

    /**
     * Get aggregated stats for a scalable topic.
     *
     * @param topic Topic name in the format "tenant/namespace/topic"
     * @return stats including segment counts, per-segment layout info, and per-subscription
     *         consumer counts
     */
    ScalableTopicStats getStats(String topic) throws PulsarAdminException;

    /**
     * Get aggregated stats for a scalable topic asynchronously.
     */
    CompletableFuture<ScalableTopicStats> getStatsAsync(String topic);

    /**
     * Create a subscription on a scalable topic. The controller leader propagates the
     * subscription to all active segment topics.
     *
     * @param topic        Topic name in the format "tenant/namespace/topic"
     * @param subscription Name of the subscription to create
     * @param type         Subscription type: {@link ScalableSubscriptionType#STREAM} for
     *                     controller-managed ordered subscriptions, or
     *                     {@link ScalableSubscriptionType#QUEUE} for unordered per-segment
     *                     fan-out.
     */
    void createSubscription(String topic, String subscription, ScalableSubscriptionType type)
            throws PulsarAdminException;

    /**
     * Create a subscription on a scalable topic asynchronously.
     */
    CompletableFuture<Void> createSubscriptionAsync(String topic, String subscription,
                                                     ScalableSubscriptionType type);

    /**
     * Delete a subscription from a scalable topic. Unregisters all consumers and removes
     * the subscription from every segment topic.
     *
     * @param topic        Topic name in the format "tenant/namespace/topic"
     * @param subscription Name of the subscription to delete
     */
    void deleteSubscription(String topic, String subscription) throws PulsarAdminException;

    /**
     * Delete a subscription from a scalable topic asynchronously.
     */
    CompletableFuture<Void> deleteSubscriptionAsync(String topic, String subscription);

    /**
     * Split a segment into two halves.
     *
     * @param topic     Topic name in the format "tenant/namespace/topic"
     * @param segmentId ID of the segment to split
     */
    void splitSegment(String topic, long segmentId) throws PulsarAdminException;

    /**
     * Split a segment into two halves asynchronously.
     *
     * @param topic     Topic name in the format "tenant/namespace/topic"
     * @param segmentId ID of the segment to split
     */
    CompletableFuture<Void> splitSegmentAsync(String topic, long segmentId);

    /**
     * Merge two adjacent segments into one.
     *
     * @param topic      Topic name in the format "tenant/namespace/topic"
     * @param segmentId1 First segment ID to merge
     * @param segmentId2 Second segment ID to merge
     */
    void mergeSegments(String topic, long segmentId1, long segmentId2) throws PulsarAdminException;

    /**
     * Merge two adjacent segments into one asynchronously.
     *
     * @param topic      Topic name in the format "tenant/namespace/topic"
     * @param segmentId1 First segment ID to merge
     * @param segmentId2 Second segment ID to merge
     */
    CompletableFuture<Void> mergeSegmentsAsync(String topic, long segmentId1, long segmentId2);

    // --- Segment topic operations ---

    /**
     * Create a segment topic on the broker that owns its namespace bundle.
     * Optionally creates subscriptions on the new segment.
     *
     * @param segmentTopic Full segment topic name (segment://tenant/namespace/topic/descriptor)
     * @param subscriptions Optional list of subscription names to create at earliest position
     */
    void createSegment(String segmentTopic, List<String> subscriptions) throws PulsarAdminException;

    /**
     * Create a segment topic asynchronously.
     */
    CompletableFuture<Void> createSegmentAsync(String segmentTopic, List<String> subscriptions);

    /**
     * Terminate a segment topic so that no more messages can be published to it.
     *
     * @param segmentTopic Full segment topic name (segment://tenant/namespace/topic/descriptor)
     */
    void terminateSegment(String segmentTopic) throws PulsarAdminException;

    /**
     * Terminate a segment topic asynchronously.
     */
    CompletableFuture<Void> terminateSegmentAsync(String segmentTopic);

    /**
     * Delete a segment topic.
     *
     * @param segmentTopic Full segment topic name (segment://tenant/namespace/topic/descriptor)
     * @param force Force deletion even if topic has active producers/subscriptions
     */
    void deleteSegment(String segmentTopic, boolean force) throws PulsarAdminException;

    /**
     * Delete a segment topic asynchronously.
     */
    CompletableFuture<Void> deleteSegmentAsync(String segmentTopic, boolean force);
}
