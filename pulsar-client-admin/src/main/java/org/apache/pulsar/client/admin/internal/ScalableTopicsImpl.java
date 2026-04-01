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
package org.apache.pulsar.client.admin.internal;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.ScalableTopics;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.apache.pulsar.common.policies.data.ScalableTopicStats;

public class ScalableTopicsImpl extends BaseResource implements ScalableTopics {
    private final WebTarget adminScalable;
    private final WebTarget adminSegments;

    public ScalableTopicsImpl(WebTarget web, Authentication auth, long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
        adminScalable = web.path("/admin/v2/scalable");
        adminSegments = web.path("/admin/v2/segments");
    }

    private WebTarget namespacePath(NamespaceName ns) {
        return adminScalable.path(ns.getTenant()).path(ns.getLocalName());
    }

    private WebTarget topicPath(TopicName tn) {
        return adminScalable.path(tn.getTenant()).path(tn.getNamespacePortion()).path(tn.getLocalName());
    }

    // --- List ---

    @Override
    public List<String> listScalableTopics(String namespace) throws PulsarAdminException {
        return sync(() -> listScalableTopicsAsync(namespace));
    }

    @Override
    public CompletableFuture<List<String>> listScalableTopicsAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        return asyncGetRequest(namespacePath(ns), new GenericType<List<String>>() {});
    }

    // --- Create ---

    @Override
    public void createScalableTopic(String topic, int numInitialSegments) throws PulsarAdminException {
        sync(() -> createScalableTopicAsync(topic, numInitialSegments));
    }

    @Override
    public CompletableFuture<Void> createScalableTopicAsync(String topic, int numInitialSegments) {
        return createScalableTopicAsync(topic, numInitialSegments, null);
    }

    @Override
    public void createScalableTopic(String topic, int numInitialSegments, Map<String, String> properties)
            throws PulsarAdminException {
        sync(() -> createScalableTopicAsync(topic, numInitialSegments, properties));
    }

    @Override
    public CompletableFuture<Void> createScalableTopicAsync(String topic, int numInitialSegments,
                                                              Map<String, String> properties) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn).queryParam("numInitialSegments", numInitialSegments);
        Entity<?> entity = (properties != null && !properties.isEmpty())
                ? Entity.entity(properties, MediaType.APPLICATION_JSON)
                : Entity.entity("", MediaType.APPLICATION_JSON);
        return asyncPutRequest(path, entity);
    }

    // --- Get metadata ---

    @Override
    public ScalableTopicMetadata getMetadata(String topic) throws PulsarAdminException {
        return sync(() -> getMetadataAsync(topic));
    }

    @Override
    public CompletableFuture<ScalableTopicMetadata> getMetadataAsync(String topic) {
        TopicName tn = validateTopic(topic);
        return asyncGetRequest(topicPath(tn), ScalableTopicMetadata.class);
    }

    // --- Delete ---

    @Override
    public void deleteScalableTopic(String topic, boolean force) throws PulsarAdminException {
        sync(() -> deleteScalableTopicAsync(topic, force));
    }

    @Override
    public CompletableFuture<Void> deleteScalableTopicAsync(String topic, boolean force) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn).queryParam("force", force);
        return asyncDeleteRequest(path);
    }

    // --- Stats ---

    @Override
    public ScalableTopicStats getStats(String topic) throws PulsarAdminException {
        return sync(() -> getStatsAsync(topic));
    }

    @Override
    public CompletableFuture<ScalableTopicStats> getStatsAsync(String topic) {
        TopicName tn = validateTopic(topic);
        return asyncGetRequest(topicPath(tn).path("stats"), ScalableTopicStats.class);
    }

    // --- Subscription operations ---

    @Override
    public void createSubscription(String topic, String subscription,
            org.apache.pulsar.common.policies.data.ScalableSubscriptionType type)
            throws PulsarAdminException {
        sync(() -> createSubscriptionAsync(topic, subscription, type));
    }

    @Override
    public CompletableFuture<Void> createSubscriptionAsync(String topic, String subscription,
            org.apache.pulsar.common.policies.data.ScalableSubscriptionType type) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn).path("subscriptions").path(subscription)
                .queryParam("type", type.name());
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteSubscription(String topic, String subscription) throws PulsarAdminException {
        sync(() -> deleteSubscriptionAsync(topic, subscription));
    }

    @Override
    public CompletableFuture<Void> deleteSubscriptionAsync(String topic, String subscription) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn).path("subscriptions").path(subscription);
        return asyncDeleteRequest(path);
    }

    // --- Split ---

    @Override
    public void splitSegment(String topic, long segmentId) throws PulsarAdminException {
        sync(() -> splitSegmentAsync(topic, segmentId));
    }

    @Override
    public CompletableFuture<Void> splitSegmentAsync(String topic, long segmentId) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn).path("split").path(String.valueOf(segmentId));
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    // --- Merge ---

    @Override
    public void mergeSegments(String topic, long segmentId1, long segmentId2) throws PulsarAdminException {
        sync(() -> mergeSegmentsAsync(topic, segmentId1, segmentId2));
    }

    @Override
    public CompletableFuture<Void> mergeSegmentsAsync(String topic, long segmentId1, long segmentId2) {
        TopicName tn = validateTopic(topic);
        WebTarget path = topicPath(tn).path("merge")
                .path(String.valueOf(segmentId1))
                .path(String.valueOf(segmentId2));
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    // --- Segment topic operations ---

    @Override
    public void createSegment(String segmentTopic, List<String> subscriptions) throws PulsarAdminException {
        sync(() -> createSegmentAsync(segmentTopic, subscriptions));
    }

    @Override
    public CompletableFuture<Void> createSegmentAsync(String segmentTopic, List<String> subscriptions) {
        TopicName tn = TopicName.get(segmentTopic);
        WebTarget path = adminSegments
                .path(tn.getTenant()).path(tn.getNamespacePortion())
                .path(tn.getLocalName()).path(tn.getSegmentDescriptor());
        Entity<?> entity = (subscriptions != null && !subscriptions.isEmpty())
                ? Entity.entity(subscriptions, MediaType.APPLICATION_JSON)
                : Entity.entity("", MediaType.APPLICATION_JSON);
        return asyncPutRequest(path, entity);
    }

    @Override
    public void terminateSegment(String segmentTopic) throws PulsarAdminException {
        sync(() -> terminateSegmentAsync(segmentTopic));
    }

    @Override
    public CompletableFuture<Void> terminateSegmentAsync(String segmentTopic) {
        TopicName tn = TopicName.get(segmentTopic);
        WebTarget path = adminSegments
                .path(tn.getTenant()).path(tn.getNamespacePortion())
                .path(tn.getLocalName()).path(tn.getSegmentDescriptor())
                .path("terminate");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteSegment(String segmentTopic, boolean force) throws PulsarAdminException {
        sync(() -> deleteSegmentAsync(segmentTopic, force));
    }

    @Override
    public CompletableFuture<Void> deleteSegmentAsync(String segmentTopic, boolean force) {
        TopicName tn = TopicName.get(segmentTopic);
        WebTarget path = adminSegments
                .path(tn.getTenant()).path(tn.getNamespacePortion())
                .path(tn.getLocalName()).path(tn.getSegmentDescriptor())
                .queryParam("force", force);
        return asyncDeleteRequest(path);
    }

    // --- Helpers ---

    private static TopicName validateTopic(String topic) {
        // Accept "tenant/namespace/topic" or fully qualified "topic://tenant/namespace/topic"
        if (!topic.contains("://")) {
            topic = "topic://" + topic;
        }
        return TopicName.get(topic);
    }
}
