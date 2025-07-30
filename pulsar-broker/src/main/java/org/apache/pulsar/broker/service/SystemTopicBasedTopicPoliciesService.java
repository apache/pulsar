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
package org.apache.pulsar.broker.service;

import static java.util.Objects.requireNonNull;
import static org.apache.pulsar.broker.service.TopicPoliciesService.getEventKey;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.concurrent.ConcurrentInitializer;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.events.TopicPoliciesEvent;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.common.util.FutureUtil;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cached topic policies service will cache the system topic reader and the topic policies
 *
 * While reader cache for the namespace was removed, the topic policies will remove automatically.
 */
public class SystemTopicBasedTopicPoliciesService implements TopicPoliciesService {

    private final PulsarService pulsarService;
    private final HashSet localCluster;
    private final String clusterName;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ConcurrentInitializer<NamespaceEventsSystemTopicFactory>
            namespaceEventsSystemTopicFactoryLazyInitializer = new LazyInitializer<>() {
        @Override
        protected NamespaceEventsSystemTopicFactory initialize() {
            try {
                return new NamespaceEventsSystemTopicFactory(pulsarService.getClient());
            } catch (PulsarServerException e) {
                log.error("Create namespace event system topic factory error.", e);
                throw new RuntimeException(e);
            }
        }
    };

    @VisibleForTesting
    final Map<TopicName, TopicPolicies> policiesCache = new ConcurrentHashMap<>();

    final Map<TopicName, TopicPolicies> globalPoliciesCache = new ConcurrentHashMap<>();

    private final Map<NamespaceName, AtomicInteger> ownedBundlesCountPerNamespace = new ConcurrentHashMap<>();

    private final Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>>
            readerCaches = new ConcurrentHashMap<>();

    final Map<NamespaceName, CompletableFuture<Void>> policyCacheInitMap = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<TopicName, List<TopicPolicyListener>> listeners = new ConcurrentHashMap<>();

    private final AsyncLoadingCache<NamespaceName, SystemTopicClient.Writer<PulsarEvent>> writerCaches;

    public SystemTopicBasedTopicPoliciesService(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
        this.clusterName = pulsarService.getConfiguration().getClusterName();
        this.localCluster = Sets.newHashSet(clusterName);
        this.writerCaches = Caffeine.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .removalListener((namespaceName, writer, cause) -> {
                    try {
                        ((SystemTopicClient.Writer) writer).close();
                    } catch (Exception e) {
                        log.error("[{}] Close writer error.", namespaceName, e);
                    }
                })
                .recordStats()
                .executor(pulsarService.getExecutor())
                .buildAsync((namespaceName, executor) -> {
                    if (closed.get()) {
                        return CompletableFuture.failedFuture(
                                new BrokerServiceException(getClass().getName() + " is closed."));
                    }
                    SystemTopicClient<PulsarEvent> systemTopicClient = getNamespaceEventsSystemTopicFactory()
                            .createTopicPoliciesSystemTopicClient(namespaceName);
                    return systemTopicClient.newWriterAsync();
                });

        CacheMetricsCollector.CAFFEINE.addCache("system-topic-policies-writer-cache", writerCaches);
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
        return deleteTopicPoliciesAsync(topicName, false);
    }

    /**
     * @param keepGlobalPolicies only be used when a topic was deleted because users removes current
     *    cluster from the policy "replicatedClusters".
     *    See also https://github.com/apache/pulsar/blob/master/pip/pip-422.md
     */
    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName,
                                                            boolean keepGlobalPolicies) {
        if (NamespaceService.isHeartbeatNamespace(topicName.getNamespaceObject()) || isSelf(topicName)) {
            return CompletableFuture.completedFuture(null);
        }
        TopicName changeEvents = NamespaceEventsSystemTopicFactory.getEventsTopicName(topicName.getNamespaceObject());
        CompletableFuture<Boolean> changeEventTopicExists = pulsarService.getPulsarResources().getTopicResources()
                .persistentTopicExists(changeEvents).thenCompose(nonPartitionedExists -> {
            if (!nonPartitionedExists) {
                // To check whether partitioned __change_events exists.
                // Instead of checking partitioned metadata, we check the first partition, because there is a case
                // does not work if we choose checking partitioned metadata.
                // The case's details:
                // 1. Start 2 clusters: c1 and c2.
                // 2. Enable replication between c1 and c2 with a global ZK.
                // 3. The partitioned metadata was shared using by c1 and c2.
                // 4. Pulsar only delete partitions when the topic is deleting from c1, because c2 is still using
                //    partitioned metadata.
                return pulsarService.getPulsarResources().getTopicResources()
                        .persistentTopicExists(changeEvents.getPartition(0));
            }
            return CompletableFuture.completedFuture(true);
        });
        return changeEventTopicExists.thenCompose(exists -> {
            // If the system topic named "__change_events" has been deleted, it means all the data in the topic have
            // been deleted, so we do not need to delete the message that we want to delete again.
            if (!exists) {
                log.info("Skip delete topic-level policies because {} has been removed before", changeEvents);
                return CompletableFuture.completedFuture(null);
            }
            return sendTopicPolicyEvent(topicName, ActionType.DELETE, null,
                    keepGlobalPolicies);
        });
    }

    @Override
    public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies) {
        if (NamespaceService.isHeartbeatNamespace(topicName.getNamespaceObject())) {
            return CompletableFuture.failedFuture(new BrokerServiceException.NotAllowedException(
                    "Not allowed to update topic policy for the heartbeat topic"));
        }
        return sendTopicPolicyEvent(topicName, ActionType.UPDATE, policies, false);
    }

    private CompletableFuture<Void> sendTopicPolicyEvent(TopicName topicName, ActionType actionType,
         @Nullable TopicPolicies policies, boolean keepGlobalPoliciesAfterDeleting) {
        return pulsarService.getPulsarResources().getNamespaceResources()
                .getPoliciesAsync(topicName.getNamespaceObject())
                .thenCompose(namespacePolicies -> {
                    if (namespacePolicies.isPresent() && namespacePolicies.get().deleted) {
                        log.debug("[{}] skip sending topic policy event since the namespace is deleted", topicName);
                        return CompletableFuture.completedFuture(null);
                    }

                    try {
                        createSystemTopicFactoryIfNeeded();
                    } catch (PulsarServerException e) {
                        return CompletableFuture.failedFuture(e);
                    }
                    CompletableFuture<Void> result = new CompletableFuture<>();
                    writerCaches.get(topicName.getNamespaceObject())
                            .whenComplete((writer, cause) -> {
                                if (cause != null) {
                                    writerCaches.synchronous().invalidate(topicName.getNamespaceObject());
                                    result.completeExceptionally(cause);
                                } else {
                                    CompletableFuture<MessageId> writeFuture =
                                            sendTopicPolicyEventInternal(topicName, actionType, writer, policies,
                                                    keepGlobalPoliciesAfterDeleting);
                                    writeFuture.whenComplete((messageId, e) -> {
                                        if (e != null) {
                                            result.completeExceptionally(e);
                                        } else {
                                            if (messageId != null) {
                                                result.complete(null);
                                            } else {
                                                result.completeExceptionally(
                                                        new RuntimeException("Got message id is null."));
                                            }
                                        }
                                    });
                            }
                    });
                    return result;
                });
    }

    private CompletableFuture<MessageId> sendTopicPolicyEventInternal(TopicName topicName, ActionType actionType,
          SystemTopicClient.Writer<PulsarEvent> writer, @Nullable TopicPolicies policies,
          boolean keepGlobalPoliciesAfterDeleting) {
        PulsarEvent event = getPulsarEvent(topicName, actionType, policies);
        if (!ActionType.DELETE.equals(actionType)) {
            return writer.writeAsync(getEventKey(event, policies != null && policies.isGlobalPolicies()), event);
        }
        // When a topic is deleting, delete both non-global and global topic-level policies.
        CompletableFuture<MessageId> dealWithGlobalPolicy;
        if (keepGlobalPoliciesAfterDeleting) {
            dealWithGlobalPolicy = CompletableFuture.completedFuture(null);
        } else {
            dealWithGlobalPolicy = writer.deleteAsync(getEventKey(event, true), event);
        }
        CompletableFuture<MessageId> deletePolicies = dealWithGlobalPolicy
            .thenCompose(__ -> {
                return writer.deleteAsync(getEventKey(event, false), event);
            });
        deletePolicies.exceptionally(ex -> {
            log.error("Failed to delete topic policy [{}] error.", topicName, ex);
            return null;
        });
        return deletePolicies;
    }

    private PulsarEvent getPulsarEvent(TopicName topicName, ActionType actionType, TopicPolicies policies) {
        PulsarEvent.PulsarEventBuilder builder = PulsarEvent.builder();
        if (policies == null || !policies.isGlobalPolicies()) {
            // we don't need to replicate local policies to remote cluster, so set `replicateTo` to empty.
            builder.replicateTo(localCluster);
        }
        return builder
                .actionType(actionType)
                .eventType(EventType.TOPIC_POLICY)
                .topicPoliciesEvent(
                        TopicPoliciesEvent.builder()
                                .domain(topicName.getDomain().toString())
                                .tenant(topicName.getTenant())
                                .namespace(topicName.getNamespaceObject().getLocalName())
                                .topic(TopicName.get(topicName.getPartitionedTopicName()).getLocalName())
                                .policies(policies)
                                .build())
                .build();
    }

    private void notifyListener(Message<PulsarEvent> msg) {
        // delete policies
        if (msg.getValue() == null) {
            TopicName topicName = TopicName.get(TopicPoliciesService.unwrapEventKey(msg.getKey())
                    .getPartitionedTopicName());
            if (listeners.get(topicName) != null) {
                for (TopicPolicyListener listener : listeners.get(topicName)) {
                    try {
                        listener.onUpdate(null);
                    } catch (Throwable error) {
                        log.error("[{}] call listener error.", topicName, error);
                    }
                }
            }
            return;
        }

        if (!EventType.TOPIC_POLICY.equals(msg.getValue().getEventType())) {
            return;
        }
        TopicPoliciesEvent event = msg.getValue().getTopicPoliciesEvent();
        TopicName topicName = TopicName.get(event.getDomain(), event.getTenant(),
                event.getNamespace(), event.getTopic());
        if (listeners.get(topicName) != null) {
            TopicPolicies policies = event.getPolicies();
            for (TopicPolicyListener listener : listeners.get(topicName)) {
                try {
                    listener.onUpdate(policies);
                } catch (Throwable error) {
                    log.error("[{}] call listener error.", topicName, error);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type) {
        requireNonNull(topicName);
        final var namespace = topicName.getNamespaceObject();
        if (NamespaceService.isHeartbeatNamespace(namespace) || isSelf(topicName)) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        // When the extensible load manager initializes its channel topic, it will trigger the topic policies
        // initialization by calling this method. At the moment, the load manager does not start so the lookup
        // for "__change_events" will fail. In this case, just return an empty policies to avoid deadlock.
        final var loadManager = pulsarService.getLoadManager().get();
        if (loadManager == null || !loadManager.started() || closed.get()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        final CompletableFuture<Boolean> preparedFuture = prepareInitPoliciesCacheAsync(topicName.getNamespaceObject());
        // switch thread to avoid potential metadata thread cost and recursive deadlock
        return preparedFuture.thenComposeAsync(inserted -> {
            // initialized : policies
            final Mutable<Pair<Boolean, Optional<TopicPolicies>>> policiesFutureHolder = new MutableObject<>();
            // NOTICE: avoid using any callback with lock scope to avoid deadlock
            policyCacheInitMap.compute(namespace, (___, existingFuture) -> {
                if (!inserted || existingFuture != null) {
                    final var partitionedTopicName = TopicName.get(topicName.getPartitionedTopicName());
                    final var policies = Optional.ofNullable(switch (type) {
                        case GLOBAL_ONLY -> globalPoliciesCache.get(partitionedTopicName);
                        case LOCAL_ONLY -> policiesCache.get(partitionedTopicName);
                    });
                    policiesFutureHolder.setValue(Pair.of(true, policies));
                } else {
                    policiesFutureHolder.setValue(Pair.of(false, null));
                }
                return existingFuture;
            });
            final var p = policiesFutureHolder.getValue();
            if (!p.getLeft()) {
                log.info("The future of {} has been removed from cache, retry getTopicPolicies again", namespace);
                return getTopicPoliciesAsync(topicName, type);
            }
            return CompletableFuture.completedFuture(p.getRight());
        });
    }

    public void addOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle) {
        NamespaceName namespace = namespaceBundle.getNamespaceObject();
        if (NamespaceService.isHeartbeatNamespace(namespace)) {
            return;
        }
        synchronized (this) {
            if (readerCaches.get(namespace) != null) {
                ownedBundlesCountPerNamespace.get(namespace).incrementAndGet();
            } else {
                prepareInitPoliciesCacheAsync(namespace);
            }
        }
    }

    @VisibleForTesting
    @NonNull CompletableFuture<Boolean> prepareInitPoliciesCacheAsync(@NonNull NamespaceName namespace) {
        requireNonNull(namespace);
        if (closed.get()) {
            return CompletableFuture.completedFuture(false);
        }
        return pulsarService.getPulsarResources().getNamespaceResources().getPoliciesAsync(namespace)
                        .thenCompose(namespacePolicies -> {
                            if (namespacePolicies.isEmpty() || namespacePolicies.get().deleted) {
                                log.info("[{}] skip prepare init policies cache since the namespace is deleted",
                                        namespace);
                                return CompletableFuture.completedFuture(false);
                            }

                            return policyCacheInitMap.computeIfAbsent(namespace, (k) -> {
                                final CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerCompletableFuture =
                                        createSystemTopicClient(namespace);
                                readerCaches.put(namespace, readerCompletableFuture);
                                ownedBundlesCountPerNamespace.putIfAbsent(namespace, new AtomicInteger(1));
                                final CompletableFuture<Void> initFuture = readerCompletableFuture
                                        .thenCompose(reader -> {
                                            final CompletableFuture<Void> stageFuture = new CompletableFuture<>();
                                            initPolicesCache(reader, stageFuture);
                                            return stageFuture
                                                    // Read policies in background
                                                    .thenAccept(__ -> readMorePoliciesAsync(reader));
                                        });
                                initFuture.exceptionally(ex -> {
                                    try {
                                        if (closed.get()) {
                                            return null;
                                        }
                                        log.error("[{}] Failed to create reader on __change_events topic",
                                                namespace, ex);
                                        cleanCacheAndCloseReader(namespace, false);
                                    } catch (Throwable cleanupEx) {
                                        // Adding this catch to avoid break callback chain
                                        log.error("[{}] Failed to cleanup reader on __change_events topic",
                                                namespace, cleanupEx);
                                    }
                                    return null;
                                });
                                // let caller know we've got an exception.
                                return initFuture;
                            }).thenApply(__ -> true);
                        });
    }

    protected CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> createSystemTopicClient(
            NamespaceName namespace) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                    new BrokerServiceException(getClass().getName() + " is closed."));
        }
        try {
            createSystemTopicFactoryIfNeeded();
        } catch (PulsarServerException ex) {
            return FutureUtil.failedFuture(ex);
        }
        final SystemTopicClient<PulsarEvent> systemTopicClient = getNamespaceEventsSystemTopicFactory()
                .createTopicPoliciesSystemTopicClient(namespace);
        return systemTopicClient.newReaderAsync();
    }

    private void removeOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle) {
        NamespaceName namespace = namespaceBundle.getNamespaceObject();
        if (NamespaceService.checkHeartbeatNamespace(namespace) != null
                || NamespaceService.checkHeartbeatNamespaceV2(namespace) != null) {
            return;
        }
        AtomicInteger bundlesCount = ownedBundlesCountPerNamespace.get(namespace);
        if (bundlesCount == null || bundlesCount.decrementAndGet() <= 0) {
            cleanCacheAndCloseReader(namespace, true, true);
        }
    }

    @Override
    public void start(PulsarService pulsarService) {

        pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(
                new NamespaceBundleOwnershipListener() {

                    @Override
                    public void onLoad(NamespaceBundle bundle) {
                        addOwnedNamespaceBundleAsync(bundle);
                    }

                    @Override
                    public void unLoad(NamespaceBundle bundle) {
                        removeOwnedNamespaceBundleAsync(bundle);
                    }

                    @Override
                    public boolean test(NamespaceBundle namespaceBundle) {
                        return true;
                    }
                });
    }

    private void initPolicesCache(SystemTopicClient.Reader<PulsarEvent> reader, CompletableFuture<Void> future) {
        if (closed.get()) {
            future.completeExceptionally(new BrokerServiceException(getClass().getName() + " is closed."));
            cleanCacheAndCloseReader(reader.getSystemTopic().getTopicName().getNamespaceObject(), false);
            return;
        }
        reader.hasMoreEventsAsync().whenComplete((hasMore, ex) -> {
            if (ex != null) {
                log.error("[{}] Failed to check the move events for the system topic",
                        reader.getSystemTopic().getTopicName(), ex);
                future.completeExceptionally(ex);
                cleanCacheAndCloseReader(reader.getSystemTopic().getTopicName().getNamespaceObject(), false);
                return;
            }
            if (hasMore) {
                reader.readNextAsync().thenAccept(msg -> {
                    try {
                        refreshTopicPoliciesCache(msg);
                    } finally {
                        msg.release();
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Loop next event reading for system topic.",
                                reader.getSystemTopic().getTopicName().getNamespaceObject());
                    }
                    initPolicesCache(reader, future);
                }).exceptionally(e -> {
                    log.error("[{}] Failed to read event from the system topic.",
                            reader.getSystemTopic().getTopicName(), e);
                    future.completeExceptionally(e);
                    cleanCacheAndCloseReader(reader.getSystemTopic().getTopicName().getNamespaceObject(), false);
                    return null;
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Reach the end of the system topic.", reader.getSystemTopic().getTopicName());
                }

                // replay policy message
                policiesCache.forEach(((topicName, topicPolicies) -> {
                    if (listeners.get(topicName) != null) {
                        for (TopicPolicyListener listener : listeners.get(topicName)) {
                            try {
                                listener.onUpdate(topicPolicies);
                            } catch (Throwable error) {
                                log.error("[{}] call listener error.", topicName, error);
                            }
                        }
                    }
                }));

                future.complete(null);
            }
        });
    }

    private void cleanCacheAndCloseReader(@NonNull NamespaceName namespace, boolean cleanOwnedBundlesCount) {
        cleanCacheAndCloseReader(namespace, cleanOwnedBundlesCount, false);
    }

    private void cleanCacheAndCloseReader(@NonNull NamespaceName namespace, boolean cleanOwnedBundlesCount,
                                          boolean cleanWriterCache) {
        if (cleanWriterCache) {
            writerCaches.synchronous().invalidate(namespace);
        }
        CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerFuture = readerCaches.remove(namespace);

        if (cleanOwnedBundlesCount) {
            ownedBundlesCountPerNamespace.remove(namespace);
        }
        if (readerFuture != null && !readerFuture.isCompletedExceptionally()) {
            readerFuture.thenCompose(SystemTopicClient.Reader::closeAsync)
                    .exceptionally(ex -> {
                        log.warn("[{}] Close change_event reader fail.", namespace, ex);
                        return null;
                    });
        }

        policyCacheInitMap.compute(namespace, (k, v) -> {
            policiesCache.entrySet().removeIf(entry -> Objects.equals(entry.getKey().getNamespaceObject(), namespace));
            return null;
        });
    }

    /**
     * This is an async method for the background reader to continue syncing new messages.
     *
     * Note: You should not do any blocking call here. because it will affect
     * #{@link SystemTopicBasedTopicPoliciesService#getTopicPoliciesAsync} method to block loading topic.
     */
    private void readMorePoliciesAsync(SystemTopicClient.Reader<PulsarEvent> reader) {
        if (closed.get()) {
            cleanCacheAndCloseReader(reader.getSystemTopic().getTopicName().getNamespaceObject(), false);
            return;
        }
        reader.readNextAsync()
                .thenAccept(msg -> {
                    try {
                        refreshTopicPoliciesCache(msg);
                        notifyListener(msg);
                    } finally {
                        msg.release();
                    }
                })
                .whenComplete((__, ex) -> {
                    if (ex == null) {
                        readMorePoliciesAsync(reader);
                    } else {
                        Throwable cause = FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof PulsarClientException.AlreadyClosedException) {
                            log.info("Closing the topic policies reader for {}",
                                    reader.getSystemTopic().getTopicName());
                            cleanCacheAndCloseReader(
                                    reader.getSystemTopic().getTopicName().getNamespaceObject(), false);
                        } else {
                            log.warn("Read more topic polices exception, read again.", ex);
                            readMorePoliciesAsync(reader);
                        }
                    }
                });
    }

    private void refreshTopicPoliciesCache(Message<PulsarEvent> msg) {
        // delete policies
        if (msg.getValue() == null) {
            boolean isGlobalPolicy = TopicPoliciesService.isGlobalPolicy(msg);
            TopicName topicName = TopicName.get(TopicPoliciesService.unwrapEventKey(msg.getKey())
                    .getPartitionedTopicName());
            if (isGlobalPolicy) {
                globalPoliciesCache.remove(topicName);
            } else {
                policiesCache.remove(topicName);
            }
            return;
        }
        if (EventType.TOPIC_POLICY.equals(msg.getValue().getEventType())) {
            TopicPoliciesEvent event = msg.getValue().getTopicPoliciesEvent();
            TopicName topicName =
                    TopicName.get(event.getDomain(), event.getTenant(), event.getNamespace(), event.getTopic());
            switch (msg.getValue().getActionType()) {
                case INSERT:
                    TopicPolicies old = event.getPolicies().isGlobalPolicies()
                            ? globalPoliciesCache.putIfAbsent(topicName, event.getPolicies())
                            : policiesCache.putIfAbsent(topicName, event.getPolicies());
                    if (old != null) {
                        log.warn("Policy insert failed, the topic: {} policy already exist", topicName);
                    }
                    break;
                case UPDATE:
                    if (event.getPolicies().isGlobalPolicies()) {
                        globalPoliciesCache.put(topicName, event.getPolicies());
                    } else {
                        policiesCache.put(topicName, event.getPolicies());
                    }
                    break;
                case DELETE:
                    // Since PR #11928, this branch is no longer needed.
                    // However, due to compatibility, it is temporarily retained here
                    // and can be deleted in the future.
                    policiesCache.remove(topicName);
                    try {
                        createSystemTopicFactoryIfNeeded();
                    } catch (PulsarServerException e) {
                        log.error("Failed to create system topic factory");
                        break;
                    }
                    SystemTopicClient<PulsarEvent> systemTopicClient = getNamespaceEventsSystemTopicFactory()
                            .createTopicPoliciesSystemTopicClient(topicName.getNamespaceObject());
                    systemTopicClient.newWriterAsync().thenAccept(writer -> {
                        sendTopicPolicyEventInternal(topicName, ActionType.DELETE, writer, event.getPolicies(), false)
                            .whenComplete((result, e) -> writer.closeAsync()
                            .whenComplete((res, ex) -> {
                                if (ex != null) {
                                    log.error("close writer failed ", ex);
                                }
                            }));
                    });
                    break;
                case NONE:
                    break;
                default:
                    log.warn("Unknown event action type: {}", msg.getValue().getActionType());
                    break;
            }
        }
    }

    private boolean hasReplicateTo(Message<?> message) {
        if (message instanceof MessageImpl) {
            return ((MessageImpl<?>) message).hasReplicateTo()
                    ? (((MessageImpl<?>) message).getReplicateTo().size() == 1
                    ? !((MessageImpl<?>) message).getReplicateTo().contains(clusterName) : true)
                    : false;
        }
        if (message instanceof TopicMessageImpl) {
            return hasReplicateTo(((TopicMessageImpl<?>) message).getMessage());
        }
        return false;
    }

    private void createSystemTopicFactoryIfNeeded() throws PulsarServerException {
        try {
            getNamespaceEventsSystemTopicFactory();
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    @VisibleForTesting
    NamespaceEventsSystemTopicFactory getNamespaceEventsSystemTopicFactory() {
        try {
            return namespaceEventsSystemTopicFactoryLazyInitializer.get();
        } catch (Exception e) {
            log.error("Create namespace event system topic factory error.", e);
            throw new RuntimeException(e);
        }
    }



    @VisibleForTesting
    long getPoliciesCacheSize() {
        return policiesCache.size();
    }

    @VisibleForTesting
    boolean checkReaderIsCached(NamespaceName namespaceName) {
        return readerCaches.get(namespaceName) != null;
    }

    @VisibleForTesting
    public CompletableFuture<Void> getPoliciesCacheInit(NamespaceName namespaceName) {
        return policyCacheInitMap.get(namespaceName);
    }

    @Override
    public boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
        listeners.compute(topicName, (k, topicListeners) -> {
            if (topicListeners == null) {
                topicListeners = new CopyOnWriteArrayList<>();
            }
            topicListeners.add(listener);
            return topicListeners;
        });
        return true;
    }

    @Override
    public void unregisterListener(TopicName topicName, TopicPolicyListener listener) {
        listeners.compute(topicName, (k, topicListeners) -> {
            if (topicListeners != null){
                topicListeners.remove(listener);
                if (topicListeners.isEmpty()) {
                    topicListeners = null;
                }
            }
            return topicListeners;
        });
    }

    @VisibleForTesting
    protected Map<TopicName, TopicPolicies> getPoliciesCache() {
        return policiesCache;
    }

    @VisibleForTesting
    protected Map<TopicName, List<TopicPolicyListener>> getListeners() {
        return listeners;
    }

    @VisibleForTesting
    protected AsyncLoadingCache<NamespaceName, SystemTopicClient.Writer<PulsarEvent>> getWriterCaches() {
        return writerCaches;
    }

    private static final Logger log = LoggerFactory.getLogger(SystemTopicBasedTopicPoliciesService.class);

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            writerCaches.synchronous().invalidateAll();
            readerCaches.values().forEach(future -> {
                try {
                    final var reader = future.getNow(null);
                    if (reader != null) {
                        reader.close();
                        log.info("Closed the reader for topic policies");
                    } else {
                        // Avoid blocking the thread that the reader is created
                        future.thenAccept(SystemTopicClient.Reader::closeAsync).whenComplete((__, e) -> {
                            if (e == null) {
                                log.info("Closed the reader for topic policies");
                            } else {
                                log.error("Failed to close the reader for topic policies", e);
                            }
                        });
                    }
                } catch (Throwable ignored) {
                }
            });
            readerCaches.clear();
        }
    }

    private static boolean isSelf(TopicName topicName) {
        final var localName = topicName.getLocalName();
        if (!topicName.isPartitioned()) {
            return localName.equals(SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
        }
        final var index = localName.lastIndexOf(TopicName.PARTITIONED_TOPIC_SUFFIX);
        return localName.substring(0, index).equals(SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
    }
}
