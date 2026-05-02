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
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

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

    /**
     * Per-path listeners for scalable-topic metadata events. Each listener watches a
     * single exact path (typically a topic record); the resources-level fan-out
     * dispatches notifications whose path equals the listener's registered path.
     * Used by {@link DagWatchSession}-style subscribers that want events for one
     * specific topic.
     *
     * <p>Hosted here — rather than letting each subscriber call
     * {@code store.registerListener} directly — because {@code MetadataStore} has no
     * {@code unregisterListener}: per-subscriber direct registration would leak a
     * listener for the broker's lifetime every time a session ends, and every
     * metadata notification would fan out to all stale listeners. Mirrors
     * {@link TopicResources} for {@code TopicListener}.
     */
    private final Map<MetadataPathListener, String> pathListeners = new ConcurrentHashMap<>();

    /**
     * Per-namespace listeners for scalable-topic create / modify / delete events.
     * Used by namespace-wide watchers (e.g. multi-topic consumer wrappers); the
     * fan-out matches direct children of the listener's namespace base path. Same
     * leak-avoidance rationale as {@link #pathListeners}.
     */
    private final Map<NamespaceListener, NamespaceName> namespaceListeners =
            new ConcurrentHashMap<>();

    public ScalableTopicResources(MetadataStore store, int operationTimeoutSec) {
        super(store, ScalableTopicMetadata.class, operationTimeoutSec);
        this.subscriptionCache = store.getMetadataCache(SubscriptionMetadata.class);
        this.consumerRegistrationCache = store.getMetadataCache(ConsumerRegistration.class);
        // Single shared metadata-store listener fans out to both per-path and
        // per-namespace subscribers. Per-subscriber lifecycle goes through the
        // register / deregister methods below.
        if (store instanceof MetadataStoreExtended ext) {
            ext.registerListener(this::handleNotification);
        } else {
            store.registerListener(this::handleNotification);
        }
    }

    // --- Per-path metadata listeners ---

    /**
     * Listener for metadata events on a specific scalable-topic-related path. The
     * fan-out in {@link ScalableTopicResources} compares each notification's path
     * against {@link #getMetadataPath()} and dispatches on exact match.
     */
    public interface MetadataPathListener {
        /** Exact path this listener is interested in (no wildcard / prefix). */
        String getMetadataPath();

        /** Called for every metadata event on the listener's path. */
        void onNotification(Notification notification);
    }

    /**
     * Register a per-path metadata listener. Idempotent — re-registering the same
     * listener just refreshes its path mapping (e.g. if the listener moved its path).
     */
    public void registerPathListener(MetadataPathListener listener) {
        pathListeners.put(listener, listener.getMetadataPath());
    }

    /**
     * Deregister a previously-registered listener. Safe to call multiple times or for
     * listeners that were never registered.
     */
    public void deregisterPathListener(MetadataPathListener listener) {
        pathListeners.remove(listener);
    }

    // --- Namespace-level scalable-topics listeners ---

    /**
     * Listener for scalable-topic create / modify / delete events under a single
     * namespace. The fan-out in {@link ScalableTopicResources} filters notifications
     * to the listener's namespace and to direct topic records (skipping subtree paths
     * like {@code <topic>/subscriptions/...} or {@code <topic>/controller}).
     */
    public interface NamespaceListener {
        /** Namespace this listener is scoped to. */
        NamespaceName getNamespaceName();

        /** Called for every metadata event affecting a topic record in the namespace. */
        void onNotification(Notification notification);
    }

    /**
     * Register a per-namespace listener. The listener will receive every
     * Created / Modified / Deleted event whose path is a direct child of
     * {@code /topics/<tenant>/<ns>}. Idempotent — re-registering the same listener
     * just updates the namespace mapping.
     */
    public void registerNamespaceListener(NamespaceListener listener) {
        namespaceListeners.put(listener, listener.getNamespaceName());
    }

    /**
     * Deregister a previously-registered namespace listener. Safe to call multiple
     * times or for listeners that were never registered.
     */
    public void deregisterNamespaceListener(NamespaceListener listener) {
        namespaceListeners.remove(listener);
    }

    /**
     * Single fan-out path. For each registered subscriber:
     * <ul>
     *   <li>Path listener: dispatch when the notification's path equals the listener's
     *       registered path.</li>
     *   <li>Namespace listener: dispatch when the notification's path is a direct
     *       child of {@code /topics/<tenant>/<ns>} (skips subtree events like
     *       subscriptions / controller lock).</li>
     * </ul>
     */
    void handleNotification(Notification notification) {
        String path = notification.getPath();

        // Path listeners — exact match.
        if (!pathListeners.isEmpty()) {
            for (Map.Entry<MetadataPathListener, String> entry : pathListeners.entrySet()) {
                if (entry.getValue().equals(path)) {
                    try {
                        entry.getKey().onNotification(notification);
                    } catch (Exception e) {
                        log.warn().attr("listener", entry.getKey())
                                .attr("path", path)
                                .exceptionMessage(e)
                                .log("Failed to dispatch scalable-topic path notification");
                    }
                }
            }
        }

        // Namespace listeners — direct child of /topics/<ns>.
        if (!namespaceListeners.isEmpty() && path.startsWith(SCALABLE_TOPIC_PATH + "/")) {
            for (Map.Entry<NamespaceListener, NamespaceName> entry : namespaceListeners.entrySet()) {
                String basePath = namespacePath(entry.getValue());
                if (!path.startsWith(basePath + "/")) {
                    continue;
                }
                String rest = path.substring(basePath.length() + 1);
                if (rest.indexOf('/') >= 0) {
                    continue;
                }
                try {
                    entry.getKey().onNotification(notification);
                } catch (Exception e) {
                    log.warn().attr("listener", entry.getKey())
                            .attr("path", path)
                            .exceptionMessage(e)
                            .log("Failed to dispatch scalable-topic namespace notification");
                }
            }
        }
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

    /**
     * Path under which all scalable topic records for a namespace live as direct
     * children. Used by namespace-wide watchers as the prefix to filter metadata
     * notifications down to events that touch a topic record.
     */
    public String namespacePath(NamespaceName ns) {
        return joinPath(SCALABLE_TOPIC_PATH, ns.toString());
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
