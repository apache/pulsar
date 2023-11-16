/**
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicPoliciesCacheNotInitException;
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
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.FutureUtil;
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
    private volatile NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    @VisibleForTesting
    final Map<TopicName, TopicPolicies> policiesCache = new ConcurrentHashMap<>();

    final Map<TopicName, TopicPolicies> globalPoliciesCache = new ConcurrentHashMap<>();

    private final Map<NamespaceName, AtomicInteger> ownedBundlesCountPerNamespace = new ConcurrentHashMap<>();

    private final Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>>
            readerCaches = new ConcurrentHashMap<>();

    final Map<NamespaceName, CompletableFuture<Void>> policyCacheInitMap = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listeners = new ConcurrentHashMap<>();

    public SystemTopicBasedTopicPoliciesService(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
        this.clusterName = pulsarService.getConfiguration().getClusterName();
        this.localCluster = Sets.newHashSet(clusterName);
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
        return sendTopicPolicyEvent(topicName, ActionType.DELETE, null);
    }

    @Override
    public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies) {
        return sendTopicPolicyEvent(topicName, ActionType.UPDATE, policies);
    }

    private CompletableFuture<Void> sendTopicPolicyEvent(TopicName topicName, ActionType actionType,
                                                         TopicPolicies policies) {
        if (NamespaceService.isHeartbeatNamespace(topicName.getNamespaceObject())) {
            return FutureUtil.failedFuture(
                    new BrokerServiceException.NotAllowedException("Not allowed to send event to health check topic"));
        }
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
                        return FutureUtil.failedFuture(e);
                    }

                    SystemTopicClient<PulsarEvent> systemTopicClient = namespaceEventsSystemTopicFactory
                                    .createTopicPoliciesSystemTopicClient(topicName.getNamespaceObject());

                    return systemTopicClient.newWriterAsync()
                            .thenCompose(writer -> {
                            PulsarEvent event = getPulsarEvent(topicName, actionType, policies);
                            CompletableFuture<MessageId> writeFuture =
                                    ActionType.DELETE.equals(actionType) ? writer.deleteAsync(event)
                                            : writer.writeAsync(event);
                            return writeFuture.handle((messageId, e) -> {
                                if (e != null) {
                                    return FutureUtil.failedFuture(e);
                                } else {
                                    if (messageId != null) {
                                        return CompletableFuture.completedFuture(null);
                                    } else {
                                        return FutureUtil.failedFuture(
                                                new RuntimeException("Got message id is null."));
                                    }
                                }
                            }).thenRun(() ->
                                        writer.closeAsync().whenComplete((v, cause) -> {
                                            if (cause != null) {
                                                log.error("[{}] Close writer error.", topicName, cause);
                                            } else {
                                                if (log.isDebugEnabled()) {
                                                    log.debug("[{}] Close writer success.", topicName);
                                                }
                                            }
                                        })
                            );
                    });
                });
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
            TopicName topicName =  TopicName.get(TopicName.get(msg.getKey()).getPartitionedTopicName());
            if (listeners.get(topicName) != null) {
                for (TopicPolicyListener<TopicPolicies> listener : listeners.get(topicName)) {
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
            for (TopicPolicyListener<TopicPolicies> listener : listeners.get(topicName)) {
                try {
                    listener.onUpdate(policies);
                } catch (Throwable error) {
                    log.error("[{}] call listener error.", topicName, error);
                }
            }
        }
    }

    @Override
    public TopicPolicies getTopicPolicies(TopicName topicName) throws TopicPoliciesCacheNotInitException {
        return getTopicPolicies(topicName, false);
    }

    @Override
    public TopicPolicies getTopicPolicies(TopicName topicName,
                                          boolean isGlobal) throws TopicPoliciesCacheNotInitException {
        if (!policyCacheInitMap.containsKey(topicName.getNamespaceObject())) {
            NamespaceName namespace = topicName.getNamespaceObject();
            prepareInitPoliciesCacheAsync(namespace);
        }

        MutablePair<TopicPoliciesCacheNotInitException, TopicPolicies> result = new MutablePair<>();
        policyCacheInitMap.compute(topicName.getNamespaceObject(), (k, initialized) -> {
            if (initialized == null || !initialized.isDone()) {
                result.setLeft(new TopicPoliciesCacheNotInitException());
            } else {
                TopicPolicies topicPolicies =
                        isGlobal ? globalPoliciesCache.get(TopicName.get(topicName.getPartitionedTopicName()))
                                : policiesCache.get(TopicName.get(topicName.getPartitionedTopicName()));
                result.setRight(topicPolicies);
            }
            return initialized;
        });

        if (result.getLeft() != null) {
            throw result.getLeft();
        } else {
            return result.getRight();
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName,
                                                                            boolean isGlobal) {
        requireNonNull(topicName);
        final CompletableFuture<Void> preparedFuture = prepareInitPoliciesCacheAsync(topicName.getNamespaceObject());
        return preparedFuture.thenApply(__ -> {
            final TopicPolicies candidatePolicies = isGlobal
                    ? globalPoliciesCache.get(TopicName.get(topicName.getPartitionedTopicName()))
                    : policiesCache.get(TopicName.get(topicName.getPartitionedTopicName()));
            return Optional.ofNullable(candidatePolicies);
        });
    }

    @Nonnull
    @Override
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName) {
        requireNonNull(topicName);
        final CompletableFuture<Void> preparedFuture = prepareInitPoliciesCacheAsync(topicName.getNamespaceObject());
        return preparedFuture.thenApply(__ -> {
            final TopicPolicies localPolicies = policiesCache.get(TopicName.get(topicName.getPartitionedTopicName()));
            if (localPolicies != null) {
                return Optional.of(localPolicies);
            }
            return Optional.ofNullable(globalPoliciesCache.get(TopicName.get(topicName.getPartitionedTopicName())));
        });
    }

    @Override
    public TopicPolicies getTopicPoliciesIfExists(TopicName topicName) {
        return policiesCache.get(TopicName.get(topicName.getPartitionedTopicName()));
    }

    @Override
    public CompletableFuture<TopicPolicies> getTopicPoliciesBypassCacheAsync(TopicName topicName) {
        CompletableFuture<TopicPolicies> result = new CompletableFuture<>();
        try {
            createSystemTopicFactoryIfNeeded();
        } catch (PulsarServerException e) {
            result.complete(null);
            return result;
        }
        SystemTopicClient<PulsarEvent> systemTopicClient = namespaceEventsSystemTopicFactory
                .createTopicPoliciesSystemTopicClient(topicName.getNamespaceObject());
        systemTopicClient.newReaderAsync().thenAccept(r ->
                fetchTopicPoliciesAsyncAndCloseReader(r, topicName, null, result));
        return result;
    }

    @Override
    public CompletableFuture<Void> addOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle) {
        NamespaceName namespace = namespaceBundle.getNamespaceObject();
        if (NamespaceService.checkHeartbeatNamespace(namespace) != null
                || NamespaceService.checkHeartbeatNamespaceV2(namespace) != null) {
            return CompletableFuture.completedFuture(null);
        }
        synchronized (this) {
            if (readerCaches.get(namespace) != null) {
                ownedBundlesCountPerNamespace.get(namespace).incrementAndGet();
                return CompletableFuture.completedFuture(null);
            } else {
                return prepareInitPoliciesCacheAsync(namespace);
            }
        }
    }

    private @Nonnull CompletableFuture<Void> prepareInitPoliciesCacheAsync(@Nonnull NamespaceName namespace) {
        requireNonNull(namespace);
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
                    log.error("[{}] Failed to create reader on __change_events topic", namespace, ex);
                    cleanCacheAndCloseReader(namespace, false);
                } catch (Throwable cleanupEx) {
                    // Adding this catch to avoid break callback chain
                    log.error("[{}] Failed to cleanup reader on __change_events topic", namespace, cleanupEx);
                }
                return null;
            });
            // let caller know we've got an exception.
            return initFuture;
        });
    }

    protected CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> createSystemTopicClient(
            NamespaceName namespace) {
        try {
            createSystemTopicFactoryIfNeeded();
        } catch (PulsarServerException ex) {
            return FutureUtil.failedFuture(ex);
        }
        final SystemTopicClient<PulsarEvent> systemTopicClient = namespaceEventsSystemTopicFactory
                .createTopicPoliciesSystemTopicClient(namespace);
        return systemTopicClient.newReaderAsync();
    }

    @Override
    public CompletableFuture<Void> removeOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle) {
        NamespaceName namespace = namespaceBundle.getNamespaceObject();
        if (NamespaceService.checkHeartbeatNamespace(namespace) != null
                || NamespaceService.checkHeartbeatNamespaceV2(namespace) != null) {
            return CompletableFuture.completedFuture(null);
        }
        AtomicInteger bundlesCount = ownedBundlesCountPerNamespace.get(namespace);
        if (bundlesCount == null || bundlesCount.decrementAndGet() <= 0) {
            cleanCacheAndCloseReader(namespace, true);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void start() {

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
                    refreshTopicPoliciesCache(msg);
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
                        for (TopicPolicyListener<TopicPolicies> listener : listeners.get(topicName)) {
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

    private void cleanCacheAndCloseReader(@Nonnull NamespaceName namespace, boolean cleanOwnedBundlesCount) {
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
     * #{@link SystemTopicBasedTopicPoliciesService#getTopicPoliciesAsync(TopicName)} method to block loading topic.
     */
    private void readMorePoliciesAsync(SystemTopicClient.Reader<PulsarEvent> reader) {
        reader.readNextAsync()
                .thenAccept(msg -> {
                    refreshTopicPoliciesCache(msg);
                    notifyListener(msg);
                })
                .whenComplete((__, ex) -> {
                    if (ex == null) {
                        readMorePoliciesAsync(reader);
                    } else {
                        Throwable cause = FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof PulsarClientException.AlreadyClosedException) {
                            log.warn("Read more topic policies exception, close the read now!", ex);
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
            TopicName topicName = TopicName.get(TopicName.get(msg.getKey()).getPartitionedTopicName());
            if (hasReplicateTo(msg)) {
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
                        log.warn("Policy insert failed, the topic: {}' policy already exist", topicName);
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
                    SystemTopicClient<PulsarEvent> systemTopicClient = namespaceEventsSystemTopicFactory
                            .createTopicPoliciesSystemTopicClient(topicName.getNamespaceObject());
                    systemTopicClient.newWriterAsync().thenAccept(writer
                            -> writer.deleteAsync(getPulsarEvent(topicName, ActionType.DELETE, null))
                            .whenComplete((result, e) -> writer.closeAsync().whenComplete((res, ex) -> {
                                if (ex != null) {
                                    log.error("close writer failed ", ex);
                                }
                            })));
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
        if (namespaceEventsSystemTopicFactory == null) {
            synchronized (this) {
                if (namespaceEventsSystemTopicFactory == null) {
                    try {
                        namespaceEventsSystemTopicFactory =
                                new NamespaceEventsSystemTopicFactory(pulsarService.getClient());
                    } catch (PulsarServerException e) {
                        log.error("Create namespace event system topic factory error.", e);
                        throw e;
                    }
                }
            }
        }
    }

    private void fetchTopicPoliciesAsyncAndCloseReader(SystemTopicClient.Reader<PulsarEvent> reader,
                                                       TopicName topicName, TopicPolicies policies,
                                                       CompletableFuture<TopicPolicies> future) {
        reader.hasMoreEventsAsync().whenComplete((hasMore, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
            }
            if (hasMore) {
                reader.readNextAsync().whenComplete((msg, e) -> {
                    if (e != null) {
                        future.completeExceptionally(e);
                    }
                    if (msg.getValue() != null
                            && EventType.TOPIC_POLICY.equals(msg.getValue().getEventType())) {
                        TopicPoliciesEvent topicPoliciesEvent = msg.getValue().getTopicPoliciesEvent();
                        if (topicName.equals(TopicName.get(
                                topicPoliciesEvent.getDomain(),
                                topicPoliciesEvent.getTenant(),
                                topicPoliciesEvent.getNamespace(),
                                topicPoliciesEvent.getTopic()))
                        ) {
                            fetchTopicPoliciesAsyncAndCloseReader(reader, topicName,
                                    topicPoliciesEvent.getPolicies(), future);
                        } else {
                            fetchTopicPoliciesAsyncAndCloseReader(reader, topicName, policies, future);
                        }
                    } else {
                        future.complete(null);
                    }
                });
            } else {
                future.complete(policies);
                reader.closeAsync().whenComplete((v, e) -> {
                    if (e != null) {
                        log.error("[{}] Close reader error.", topicName, e);
                    }
                });
            }
        });
    }

    @VisibleForTesting
    long getPoliciesCacheSize() {
        return policiesCache.size();
    }

    @VisibleForTesting
    long getReaderCacheCount() {
        return readerCaches.size();
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
    public void registerListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
        listeners.compute(topicName, (k, topicListeners) -> {
            if (topicListeners == null) {
                topicListeners = Lists.newCopyOnWriteArrayList();
            }
            topicListeners.add(listener);
            return topicListeners;
        });
    }

    @Override
    public void unregisterListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
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
    protected Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> getListeners() {
        return listeners;
    }

    private static final Logger log = LoggerFactory.getLogger(SystemTopicBasedTopicPoliciesService.class);
}
