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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
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

    /**
     * Use the topic's {@code properties} map verbatim as the secondary-index entries.
     * Each property {@code k -> v} is registered as the index named {@code k} with
     * secondary key {@code v}; querying by that key/value pair via
     * {@link MetadataStore#findByIndex} returns the record. Index names live in a
     * per-record-type namespace, so there's no need to disambiguate them with a prefix.
     */
    private static final Function<ScalableTopicMetadata, Map<String, String>> PROPERTY_INDEX_EXTRACTOR =
            metadata -> metadata.getProperties() != null ? metadata.getProperties() : Map.of();

    private final MetadataCache<SubscriptionMetadata> subscriptionCache;
    private final MetadataCache<ConsumerRegistration> consumerRegistrationCache;

    public ScalableTopicResources(MetadataStore store, int operationTimeoutSec) {
        super(store, ScalableTopicMetadata.class, operationTimeoutSec);
        this.subscriptionCache = store.getMetadataCache(SubscriptionMetadata.class);
        this.consumerRegistrationCache = store.getMetadataCache(ConsumerRegistration.class);
    }

    public CompletableFuture<Void> createScalableTopicAsync(TopicName tn, ScalableTopicMetadata metadata) {
        return getCache().create(topicPath(tn), metadata, PROPERTY_INDEX_EXTRACTOR);
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
        // Refresh property indexes on every update — the modify function may add or remove
        // properties and the underlying store needs to see the post-modification view.
        return getCache().readModifyUpdate(topicPath(tn), updateFunction, PROPERTY_INDEX_EXTRACTOR)
                .thenApply(__ -> null);
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

    /**
     * List scalable topics in a namespace whose {@code properties} map contains every
     * key/value pair in {@code propertyFilters} (AND semantics).
     *
     * <p>Stores with native secondary-index support (Oxia) serve the most-restrictive
     * lookup via the index for one of the filters, then a record-level check rejects
     * anything that doesn't satisfy the rest. Stores without native index support fall
     * through to a children scan + the same predicate. An empty {@code propertyFilters}
     * map degenerates to {@link #listScalableTopicsAsync}.
     *
     * @param ns              the namespace to scope the query to
     * @param propertyFilters property name/value pairs that all must match (AND)
     * @return fully qualified scalable topic names matching every filter
     */
    public CompletableFuture<List<String>> findScalableTopicsByPropertiesAsync(
            NamespaceName ns, Map<String, String> propertyFilters) {
        if (propertyFilters == null || propertyFilters.isEmpty()) {
            return listScalableTopicsAsync(ns);
        }
        String scanPathPrefix = joinPath(SCALABLE_TOPIC_PATH, ns.toString());
        ObjectMapper mapper = ObjectMapperFactory.getMapper().getObjectMapper();

        // Pick any single filter to drive the index lookup (native stores will use it
        // to narrow the candidate set; iteration order is acceptable since we don't
        // know index cardinalities up front). The predicate then enforces AND across
        // every filter on the loaded record.
        Map.Entry<String, String> indexFilter = propertyFilters.entrySet().iterator().next();
        java.util.function.Predicate<org.apache.pulsar.metadata.api.GetResult> matchesAll = result -> {
            try {
                ScalableTopicMetadata md =
                        mapper.readValue(result.getValue(), ScalableTopicMetadata.class);
                Map<String, String> props = md.getProperties();
                if (props == null) {
                    return false;
                }
                for (Map.Entry<String, String> e : propertyFilters.entrySet()) {
                    if (!e.getValue().equals(props.get(e.getKey()))) {
                        return false;
                    }
                }
                return true;
            } catch (IOException e) {
                return false;
            }
        };
        return getStore().findByIndex(scanPathPrefix,
                        indexFilter.getKey(), indexFilter.getValue(), matchesAll)
                // Native-index implementations don't apply the fallback predicate, so
                // re-check here. On the fallback path this is a no-op (predicate already
                // applied) but cheap.
                .thenApply(results -> results.stream()
                        .filter(matchesAll)
                        .map(r -> {
                            String path = r.getStat().getPath();
                            String encoded = path.substring(path.lastIndexOf('/') + 1);
                            return TopicName.get("topic", ns, Codec.decode(encoded)).toString();
                        })
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
