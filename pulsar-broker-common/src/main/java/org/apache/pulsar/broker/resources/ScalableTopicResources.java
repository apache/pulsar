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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentLoadStats;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.ScanConsumer;

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
 *   <li>{@code /topic-creation-locks/{tenant}/{namespace}/{encodedTopicName}:{token}} — one
 *       ephemeral record per acquisition of the topic's creation lock, held while the topic is
 *       created or a regular topic is migrated to it (see {@link #runWithCreationLock}); outside
 *       the topic's path, so taking it doesn't create the topic's node</li>
 * </ul>
 */
@CustomLog
public class ScalableTopicResources extends BaseResources<ScalableTopicMetadata> {

    private static final String SCALABLE_TOPIC_PATH = "/topics";
    private static final String CREATION_LOCK_PATH = "/topic-creation-locks";
    /** Attempts to take a creation lock, waiting 10-20 ms, 20-40 ms, 40-80 ms... in between: about a second. */
    private static final int CREATION_LOCK_ATTEMPTS = 7;
    private static final long CREATION_LOCK_FIRST_RETRY_DELAY_MS = 10;
    private static final String SUBSCRIPTIONS_SEGMENT = "subscriptions";
    private static final String CONSUMERS_SEGMENT = "consumers";
    private static final String SEGMENTS_SEGMENT = "segments";
    private static final String LOAD_SEGMENT = "load";

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
    private final MetadataCache<SegmentLoadStats> segmentLoadCache;

    /**
     * Per-path listeners for scalable-topic metadata events. Each listener watches a
     * single exact path (typically a topic record); the resources-level fan-out
     * dispatches notifications whose path equals the listener's registered path.
     * Used by {@link DagWatchSession}-style subscribers that want events for one
     * specific topic.
     *
     * <p>Hosted here — rather than letting each subscriber register a listener with the
     * store directly — so that the store has a single listener for all the subscribers,
     * instead of every metadata notification fanning out to a listener per subscriber,
     * whose registration every session would have to cancel when it ends. Mirrors
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

    /** Broker-scoped observers sharing the existing metadata-store listener. */
    private final Set<Consumer<Notification>> metadataListeners = ConcurrentHashMap.newKeySet();

    private final Runnable cancelListener;

    public ScalableTopicResources(MetadataStore store, int operationTimeoutSec) {
        super(store, ScalableTopicMetadata.class, operationTimeoutSec);
        this.subscriptionCache = store.getMetadataCache(SubscriptionMetadata.class);
        this.consumerRegistrationCache = store.getMetadataCache(ConsumerRegistration.class);
        this.segmentLoadCache = store.getMetadataCache(SegmentLoadStats.class);
        // Single shared metadata-store listener fans out to per-path and
        // per-namespace subscribers and broker-scoped observers. Lifecycle goes through the
        // register / deregister methods below.
        cancelListener = store.registerCancellableListener(this::handleNotification);
    }

    /**
     * Unregister the shared metadata-store listener, so that the subscribers are no longer notified.
     */
    public void close() {
        cancelListener.run();
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
     * Register a broker-scoped observer of every notification below {@code /topics/}, including
     * controller and subscription paths. Registering the same observer again is idempotent.
     *
     * <p>This reuses the existing store listener; it does not register another store listener.
     * The caller must invoke {@link #deregisterMetadataListener(Consumer)} when it closes to avoid
     * retaining the observer and its broker-scoped state.
     */
    public void registerMetadataListener(Consumer<Notification> listener) {
        metadataListeners.add(listener);
    }

    /**
     * Deregister an observer on caller shutdown. Safe to call repeatedly or for an unregistered observer.
     * A notification already being dispatched may still reach the observer.
     */
    public void deregisterMetadataListener(Consumer<Notification> listener) {
        metadataListeners.remove(listener);
    }

    /**
     * Single fan-out path. For each registered subscriber:
     * <ul>
     *   <li>Path listener: dispatch when the notification's path equals the listener's
     *       registered path.</li>
     *   <li>Namespace listener: dispatch when the notification's path is a direct
     *       child of {@code /topics/<tenant>/<ns>} (skips subtree events like
     *       subscriptions / controller lock).</li>
     *   <li>Broker-scoped observer: dispatch every notification below {@code /topics/}.</li>
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

        if (!metadataListeners.isEmpty() && path.startsWith(SCALABLE_TOPIC_PATH + "/")) {
            for (Consumer<Notification> listener : metadataListeners) {
                try {
                    listener.accept(notification);
                } catch (Exception e) {
                    log.warn().attr("listener", listener)
                            .attr("path", path)
                            .exceptionMessage(e)
                            .log("Failed to dispatch scalable-topic metadata notification");
                }
            }
        }
    }

    public CompletableFuture<Void> createScalableTopicAsync(TopicName tn, ScalableTopicMetadata metadata) {
        return getCache().create(topicPath(tn), metadata, PROPERTY_INDEX_EXTRACTOR);
    }

    /** The creation lock of a scalable topic, while {@link #runWithCreationLock} holds it. */
    public interface CreationLock {
        /**
         * Complete normally if the lock is still held, or fail with {@link MetadataStoreException.LockBusyException}.
         * An action checks it before each change the lock guards: a broker whose metadata session expired lost the
         * lock, which another broker may have taken since.
         */
        CompletableFuture<Void> checkHeld();
    }

    /**
     * Run {@code action} while holding the creation lock of the scalable topic {@code tn}.
     *
     * <p>Creating a scalable topic and migrating a regular topic to it take this lock, so they exclude each other,
     * on every broker: a migration checks that the scalable topic doesn't exist, grants the regular topic's
     * permissions on it and creates it, with no creation of the same topic in between that would get those
     * permissions. A lock held by another creation or migration is retried for about a second, which covers a
     * concurrent creation, as it holds the lock only while it writes the topic's metadata. Then this fails with
     * {@link MetadataStoreException.LockBusyException}, without running {@code action}.
     *
     * <p>Each acquisition creates an ephemeral record of its own, named after the topic and a random token, and
     * holds the lock if no other record of the topic exists then. So a broker that goes away doesn't keep the lock;
     * releasing it deletes that record only, never the one of an acquisition made after this broker's metadata
     * session expired; and a create that the store retried after its response was lost finds its own record.
     */
    public <T> CompletableFuture<T> runWithCreationLock(TopicName tn,
                                                        Function<CreationLock, CompletableFuture<T>> action) {
        String lockDir = joinPath(CREATION_LOCK_PATH, tn.getNamespace());
        String prefix = tn.getEncodedLocalName() + ":";
        String name = prefix + UUID.randomUUID();
        String path = joinPath(lockDir, name);
        CreationLock lock = () -> getStore().get(path).thenAccept(record -> {
            if (record.isEmpty() || !record.get().getStat().isCreatedBySelf()) {
                throw new CompletionException(new MetadataStoreException.LockBusyException(
                        "Lost the creation lock of " + tn));
            }
        });
        return acquireCreationLock(lockDir, prefix, name, 1).thenCompose(__ -> {
            CompletableFuture<T> result = FutureUtil.supplySafely(() -> action.apply(lock));
            return result.handle((___, ____) -> null)
                    .thenCompose(___ -> releaseCreationLock(path))
                    .thenCompose(___ -> result);
        });
    }

    private CompletableFuture<Void> acquireCreationLock(String lockDir, String prefix, String name, int attempt) {
        String path = joinPath(lockDir, name);
        return getStore().put(path, new byte[0], Optional.of(-1L), Set.of(Option.Ephemeral.INSTANCE))
                .<Void>thenApply(__ -> null)
                .exceptionallyCompose(ex -> {
                    if (FutureUtil.unwrapCompletionException(ex)
                            instanceof MetadataStoreException.BadVersionException) {
                        // The record exists: the store retried a create of this acquisition whose response was
                        // lost. No other acquisition uses this name.
                        return CompletableFuture.completedFuture(null);
                    }
                    return CompletableFuture.failedFuture(ex);
                })
                // Read the store, not the cache, which may miss the record of a concurrent acquisition.
                .thenCompose(__ -> getStore().getChildrenFromStore(lockDir))
                .exceptionallyCompose(ex ->
                        // The record may have been created: delete it, or the lock would stay taken.
                        releaseCreationLock(path).thenCompose(__ -> CompletableFuture.failedFuture(ex)))
                .thenCompose(records -> {
                    if (records.stream().noneMatch(record -> record.startsWith(prefix) && !record.equals(name))) {
                        return CompletableFuture.completedFuture(null);
                    }
                    // Another acquisition holds the lock, or is taking it: of two acquisitions, the second one to
                    // create its record sees the other one, and the first one may. Back off and retry.
                    return releaseCreationLock(path).thenCompose(__ -> {
                        if (attempt == CREATION_LOCK_ATTEMPTS) {
                            return CompletableFuture.failedFuture(new MetadataStoreException.LockBusyException(
                                    "Another creation or migration of the scalable topic is in progress: " + path));
                        }
                        // With jitter, so two acquisitions that see each other don't keep retrying together.
                        long delayMs = CREATION_LOCK_FIRST_RETRY_DELAY_MS << (attempt - 1);
                        Executor delayed = CompletableFuture.delayedExecutor(
                                delayMs + ThreadLocalRandom.current().nextLong(delayMs), TimeUnit.MILLISECONDS);
                        return CompletableFuture.supplyAsync(() -> null, delayed)
                                .thenCompose(___ -> acquireCreationLock(lockDir, prefix, name, attempt + 1));
                    });
                });
    }

    /** Delete the record of an acquisition, if it's there. It's named after the acquisition, so it's no other's. */
    private CompletableFuture<Void> releaseCreationLock(String path) {
        return getStore().delete(path, Optional.empty())
                .exceptionally(ex -> {
                    if (!(FutureUtil.unwrapCompletionException(ex)
                            instanceof MetadataStoreException.NotFoundException)) {
                        // It goes away with the broker's metadata session anyway.
                        log.warn().attr("path", path).exceptionMessage(ex).log("Failed to release a creation lock");
                    }
                    return null;
                });
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
        // Recursive: the topic record has children — the controller leader lock, the
        // subscriptions (and their consumer registrations), and the per-segment load
        // records — all of which must go with the topic.
        return getStore().deleteRecursive(topicPath(tn));
    }

    /**
     * Mark the topic as being deleted, before its metadata is deleted. See
     * {@link ScalableTopicMetadata#isDeleted()}.
     */
    public CompletableFuture<Void> markScalableTopicDeletedAsync(TopicName tn) {
        return updateScalableTopicAsync(tn, md -> {
            md.setDeleted(true);
            return md;
        });
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
     * Delete the node of a namespace whose scalable topics are deleted. Not recursive, so a scalable topic still
     * in the namespace fails it.
     */
    public CompletableFuture<Void> clearNamespaceAsync(NamespaceName ns) {
        return getStore().deleteIfExists(namespacePath(ns), Optional.empty());
    }

    /** Delete the scalable topic records of a tenant whose namespaces are deleted. */
    public CompletableFuture<Void> clearTenantAsync(String tenant) {
        return getStore().deleteRecursive(joinPath(SCALABLE_TOPIC_PATH, tenant));
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
        Predicate<GetResult> matchesAll = result -> {
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
        List<GetResult> results = new ArrayList<>();
        return getStore().scanByIndex(scanPathPrefix,
                        indexFilter.getKey(), indexFilter.getValue(), indexFilter.getValue(),
                        matchesAll,
                        ScanConsumer.collectInto(results))
                .thenApply(__ ->
                        // Native-index implementations don't apply the fallback predicate, so
                        // re-check here. On the fallback path this is a no-op (predicate already
                        // applied) but cheap.
                        results.stream()
                                .filter(matchesAll)
                                // The scan returns the records in no particular order: list them in the
                                // order of the namespace's children, as listScalableTopicsAsync does
                                .sorted(Comparator.comparing(r -> r.getStat().getPath()))
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
    // --- Segment load records (PIP-483 auto split/merge) ---

    /**
     * Upsert a segment's load record. Written by the broker that owns the segment's
     * {@code segment://} topic, only when the rates have changed materially since the last
     * write (the materiality decision lives in {@code SegmentLoadReporter}).
     *
     * <p>An identical value is NOT rewritten: the record's {@code Stat} modification time is
     * what the controller uses as "cold since" for the merge window, so a no-op rewrite —
     * e.g. the first report after segment ownership moved to a broker with an empty
     * last-written cache — would spuriously reset the window and starve merges under
     * frequent rebalancing.
     */
    public CompletableFuture<Void> reportSegmentLoadAsync(TopicName tn, long segmentId,
                                                          SegmentLoadStats stats) {
        String path = segmentLoadPath(tn, segmentId);
        return segmentLoadCache.get(path).thenCompose(existing -> {
            if (existing.isPresent() && existing.get().equals(stats)) {
                return CompletableFuture.completedFuture(null);
            }
            return segmentLoadCache.readModifyUpdateOrCreate(path, __ -> stats)
                    .thenApply(__ -> null);
        });
    }

    /**
     * Read a segment's load record together with its metadata {@link Stat} — the controller's
     * auto-scaling evaluator uses {@code stat.getModificationTimestamp()} to tell how long the
     * segment has held its current load (the "cold for at least mergeWindow" check).
     *
     * @return the value and its stat, or empty if no record has been written yet
     */
    public CompletableFuture<Optional<CacheGetResult<SegmentLoadStats>>> getSegmentLoadAsync(
            TopicName tn, long segmentId) {
        return segmentLoadCache.getWithStats(segmentLoadPath(tn, segmentId));
    }

    /** Delete a segment's load record (best-effort; tolerates a missing record). */
    public CompletableFuture<Void> deleteSegmentLoadAsync(TopicName tn, long segmentId) {
        return segmentLoadCache.delete(segmentLoadPath(tn, segmentId))
                .exceptionally(ignoreMissing());
    }

    public String segmentLoadPath(TopicName tn, long segmentId) {
        return joinPath(topicPath(tn), SEGMENTS_SEGMENT, Long.toString(segmentId), LOAD_SEGMENT);
    }

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
