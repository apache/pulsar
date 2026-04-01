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
package org.apache.pulsar.broker.resources;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

/**
 * Metadata store access for scalable topic metadata.
 *
 * <p>Paths:
 * <ul>
 *   <li>{@code /topics/{tenant}/{namespace}/{encodedTopicName}} — {@link ScalableTopicMetadata}
 *       (segment DAG and global topic state)</li>
 *   <li>{@code /topics/{tenant}/{namespace}/{encodedTopicName}/controller} — controller leader
 *       lock (ephemeral, broker URL as value)</li>
 *   <li>{@code /topics/{tenant}/{namespace}/{encodedTopicName}/subscriptions/{subscription}} —
 *       {@link SubscriptionMetadata}</li>
 *   <li>{@code /topics/{tenant}/{namespace}/{encodedTopicName}/subscriptions/{subscription}
 *       /consumers/{consumerName}} — {@link ConsumerRegistration} (durable session entry)</li>
 * </ul>
 */
@CustomLog
public class ScalableTopicResources extends BaseResources<ScalableTopicMetadata> {

    private static final String SCALABLE_TOPIC_PATH = "/topics";
    private static final String SUBSCRIPTIONS_SEGMENT = "subscriptions";
    private static final String CONSUMERS_SEGMENT = "consumers";

    private final MetadataCache<SubscriptionMetadata> subscriptionCache;
    private final MetadataCache<ConsumerRegistration> consumerRegistrationCache;

    public ScalableTopicResources(MetadataStore store, int operationTimeoutSec) {
        super(store, ScalableTopicMetadata.class, operationTimeoutSec);
        this.subscriptionCache = store.getMetadataCache(SubscriptionMetadata.class);
        this.consumerRegistrationCache = store.getMetadataCache(ConsumerRegistration.class);
    }

    public CompletableFuture<Void> createScalableTopicAsync(TopicName tn, ScalableTopicMetadata metadata) {
        return createAsync(topicPath(tn), metadata);
    }

    public CompletableFuture<Optional<ScalableTopicMetadata>> getScalableTopicMetadataAsync(TopicName tn) {
        return getAsync(topicPath(tn));
    }

    public CompletableFuture<Optional<ScalableTopicMetadata>> getScalableTopicMetadataAsync(TopicName tn,
                                                                                             boolean refresh) {
        if (refresh) {
            return refreshAndGetAsync(topicPath(tn));
        }
        return getAsync(topicPath(tn));
    }

    public CompletableFuture<Void> updateScalableTopicAsync(TopicName tn,
                                                             Function<ScalableTopicMetadata,
                                                                     ScalableTopicMetadata> updateFunction) {
        return setAsync(topicPath(tn), updateFunction);
    }

    public CompletableFuture<Void> deleteScalableTopicAsync(TopicName tn) {
        return deleteAsync(topicPath(tn));
    }

    public CompletableFuture<Boolean> scalableTopicExistsAsync(TopicName tn) {
        return existsAsync(topicPath(tn));
    }

    public CompletableFuture<List<String>> listScalableTopicsAsync(NamespaceName ns) {
        return getChildrenAsync(joinPath(SCALABLE_TOPIC_PATH, ns.toString()))
                .thenApply(list -> list.stream()
                        .map(encoded -> TopicName.get("topic", ns, Codec.decode(encoded)).toString())
                        .collect(Collectors.toList()));
    }

    // --- Subscriptions ---

    /**
     * Create a subscription record. Fails if it already exists.
     */
    public CompletableFuture<Void> createSubscriptionAsync(TopicName tn, String subscription,
                                                            SubscriptionType type) {
        return subscriptionCache.create(subscriptionPath(tn, subscription),
                new SubscriptionMetadata(type));
    }

    public CompletableFuture<Optional<SubscriptionMetadata>> getSubscriptionAsync(TopicName tn, String subscription) {
        return subscriptionCache.get(subscriptionPath(tn, subscription));
    }

    public CompletableFuture<Boolean> subscriptionExistsAsync(TopicName tn, String subscription) {
        return subscriptionCache.exists(subscriptionPath(tn, subscription));
    }

    /**
     * Delete a subscription and all its consumer registration children. Best-effort on
     * children — a missing child is ignored.
     */
    public CompletableFuture<Void> deleteSubscriptionAsync(TopicName tn, String subscription) {
        String subPath = subscriptionPath(tn, subscription);
        String consumersPath = joinPath(subPath, CONSUMERS_SEGMENT);
        // Delete all consumer children first, then the consumers dir, then the subscription
        return subscriptionCache.getChildren(consumersPath)
                .thenCompose(children -> {
                    if (children == null || children.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    CompletableFuture<?>[] futs = children.stream()
                            .map(c -> consumerRegistrationCache
                                    .delete(joinPath(consumersPath, c))
                                    .exceptionally(ignoreMissing()))
                            .toArray(CompletableFuture[]::new);
                    return CompletableFuture.allOf(futs);
                })
                .thenCompose(__ -> subscriptionCache.delete(subPath)
                        .exceptionally(ignoreMissing()));
    }

    public CompletableFuture<List<String>> listSubscriptionsAsync(TopicName tn) {
        return subscriptionCache.getChildren(joinPath(topicPath(tn), SUBSCRIPTIONS_SEGMENT))
                .thenApply(list -> list == null ? List.of() : list);
    }

    // --- Consumer registrations ---

    /**
     * Persist a consumer registration under a subscription. This is the durable session
     * entry — once written, the consumer's segment assignment survives controller leader
     * failover, client restarts, and TCP disconnects within the session grace period.
     *
     * <p>Idempotent: if the registration already exists, this completes successfully without
     * overwriting it. Used by the controller leader on consumer register.
     */
    public CompletableFuture<Void> registerConsumerAsync(TopicName tn, String subscription, String consumerName) {
        String path = consumerPath(tn, subscription, consumerName);
        return consumerRegistrationCache.create(path, new ConsumerRegistration())
                .exceptionally(ex -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(ex);
                    if (cause instanceof MetadataStoreException.AlreadyExistsException) {
                        // Already registered — treat as success.
                        return null;
                    }
                    throw FutureUtil.wrapToCompletionException(cause);
                });
    }

    /**
     * Remove a persisted consumer registration. Missing entries are ignored.
     */
    public CompletableFuture<Void> unregisterConsumerAsync(TopicName tn, String subscription, String consumerName) {
        return consumerRegistrationCache.delete(consumerPath(tn, subscription, consumerName))
                .exceptionally(ignoreMissing());
    }

    /**
     * List all persisted consumer names for a subscription. Used by the controller leader
     * on initialize() / failover to restore the in-memory session state.
     */
    public CompletableFuture<List<String>> listConsumersAsync(TopicName tn, String subscription) {
        return consumerRegistrationCache
                .getChildren(joinPath(subscriptionPath(tn, subscription), CONSUMERS_SEGMENT))
                .thenApply(list -> list == null ? List.of() : list);
    }

    // --- Paths ---

    /**
     * Get the metadata store path for the controller leader lock.
     */
    public String controllerLockPath(TopicName tn) {
        return joinPath(topicPath(tn), "controller");
    }

    public String topicPath(TopicName tn) {
        return joinPath(SCALABLE_TOPIC_PATH, tn.getNamespace(), tn.getEncodedLocalName());
    }

    public String subscriptionPath(TopicName tn, String subscription) {
        return joinPath(topicPath(tn), SUBSCRIPTIONS_SEGMENT, subscription);
    }

    public String consumerPath(TopicName tn, String subscription, String consumerName) {
        return joinPath(subscriptionPath(tn, subscription), CONSUMERS_SEGMENT, consumerName);
    }

    private static <T> Function<Throwable, T> ignoreMissing() {
        return ex -> {
            Throwable cause = FutureUtil.unwrapCompletionException(ex);
            if (cause instanceof MetadataStoreException.NotFoundException) {
                return null;
            }
            throw FutureUtil.wrapToCompletionException(cause);
        };
    }
}
