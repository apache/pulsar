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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.util.RetryUtil;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.events.TopicPoliciesEvent;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cached topic policies service will cache the system topic reader and the topic policies
 *
 * While reader cache for the namespace was removed, the topic policies will remove automatically.
 */
public class SystemTopicBasedTopicPoliciesService implements TopicPoliciesService {

    private final PulsarService pulsarService;
    private volatile NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    @VisibleForTesting
    final Map<TopicName, TopicPolicies> policiesCache = new ConcurrentHashMap<>();

    final Map<TopicName, TopicPolicies> globalPoliciesCache = new ConcurrentHashMap<>();

    private final Map<NamespaceName, AtomicInteger> ownedBundlesCountPerNamespace = new ConcurrentHashMap<>();

    private final Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>>
            readerCaches = new ConcurrentHashMap<>();
    @VisibleForTesting
    final Map<NamespaceName, Boolean> policyCacheInitMap = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listeners = new ConcurrentHashMap<>();

    public SystemTopicBasedTopicPoliciesService(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
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
        createSystemTopicFactoryIfNeeded();

        CompletableFuture<Void> result = new CompletableFuture<>();

        SystemTopicClient<PulsarEvent> systemTopicClient =
                namespaceEventsSystemTopicFactory.createTopicPoliciesSystemTopicClient(topicName.getNamespaceObject());

        CompletableFuture<SystemTopicClient.Writer<PulsarEvent>> writerFuture = systemTopicClient.newWriterAsync();
        writerFuture.whenComplete((writer, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else {
                PulsarEvent event = getPulsarEvent(topicName, actionType, policies);
                CompletableFuture<MessageId> actionFuture =
                        ActionType.DELETE.equals(actionType) ? writer.deleteAsync(event) : writer.writeAsync(event);
                actionFuture.whenComplete(((messageId, e) -> {
                            if (e != null) {
                                result.completeExceptionally(e);
                            } else {
                                if (messageId != null) {
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(new RuntimeException("Got message id is null."));
                                }
                            }
                            writer.closeAsync().whenComplete((v, cause) -> {
                                if (cause != null) {
                                    log.error("[{}] Close writer error.", topicName, cause);
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Close writer success.", topicName);
                                    }
                                }
                            });
                        })
                );
            }
        });
        return result;
    }

    private PulsarEvent getPulsarEvent(TopicName topicName, ActionType actionType, TopicPolicies policies) {
        PulsarEvent.PulsarEventBuilder builder = PulsarEvent.builder();
        if (policies == null || !policies.isGlobalPolicies()) {
            // we don't need to replicate local policies to remote cluster, so set `replicateTo` to empty.
            builder.replicateTo(new HashSet<>());
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
                    listener.onUpdate(null);
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
                listener.onUpdate(policies);
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
            prepareInitPoliciesCache(namespace, new CompletableFuture<>());
        }
        if (policyCacheInitMap.containsKey(topicName.getNamespaceObject())
                && !policyCacheInitMap.get(topicName.getNamespaceObject())) {
            throw new TopicPoliciesCacheNotInitException();
        }
        return isGlobal ? globalPoliciesCache.get(TopicName.get(topicName.getPartitionedTopicName()))
                : policiesCache.get(TopicName.get(topicName.getPartitionedTopicName()));
    }

    @Override
    public TopicPolicies getTopicPoliciesIfExists(TopicName topicName) {
        return policiesCache.get(TopicName.get(topicName.getPartitionedTopicName()));
    }

    @Override
    public CompletableFuture<TopicPolicies> getTopicPoliciesBypassCacheAsync(TopicName topicName) {
        CompletableFuture<TopicPolicies> result = new CompletableFuture<>();
        createSystemTopicFactoryIfNeeded();
        if (namespaceEventsSystemTopicFactory == null) {
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
        CompletableFuture<Void> result = new CompletableFuture<>();
        NamespaceName namespace = namespaceBundle.getNamespaceObject();
        if (NamespaceService.checkHeartbeatNamespace(namespace) != null
                || NamespaceService.checkHeartbeatNamespaceV2(namespace) != null) {
            result.complete(null);
            return result;
        }
        createSystemTopicFactoryIfNeeded();
        synchronized (this) {
            if (readerCaches.get(namespace) != null) {
                ownedBundlesCountPerNamespace.get(namespace).incrementAndGet();
                result.complete(null);
            } else {
                ownedBundlesCountPerNamespace.putIfAbsent(namespace, new AtomicInteger(1));
                prepareInitPoliciesCache(namespace, result);
            }
        }
        return result;
    }

    private void prepareInitPoliciesCache(NamespaceName namespace, CompletableFuture<Void> result) {
        if (policyCacheInitMap.putIfAbsent(namespace, false) == null) {
            CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerCompletableFuture =
                    creatSystemTopicClientWithRetry(namespace);
            readerCaches.put(namespace, readerCompletableFuture);
            readerCompletableFuture.whenComplete((reader, ex) -> {
                if (ex != null) {
                    log.error("[{}] Failed to create reader on __change_events topic", namespace, ex);
                    result.completeExceptionally(ex);
                    readerCaches.remove(namespace);
                    reader.closeAsync();
                } else {
                    initPolicesCache(reader, result);
                    result.thenRun(() -> readMorePolicies(reader));
                }
            });
        }
    }

    protected CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> creatSystemTopicClientWithRetry(
            NamespaceName namespace) {
        SystemTopicClient<PulsarEvent> systemTopicClient = namespaceEventsSystemTopicFactory
                .createTopicPoliciesSystemTopicClient(namespace);
        CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> result = new CompletableFuture<>();
        Backoff backoff = new Backoff(1, TimeUnit.SECONDS, 3, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);
        RetryUtil.retryAsynchronously(() -> {
            try {
                return systemTopicClient.newReader();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }, backoff, pulsarService.getExecutor(), result);
        return result;
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
            CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerCompletableFuture =
                    readerCaches.remove(namespace);
            if (readerCompletableFuture != null) {
                readerCompletableFuture.thenAccept(SystemTopicClient.Reader::closeAsync);
                ownedBundlesCountPerNamespace.remove(namespace);
                policyCacheInitMap.remove(namespace);
                policiesCache.entrySet().removeIf(entry -> entry.getKey().getNamespaceObject().equals(namespace));
            }
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
                readerCaches.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                policyCacheInitMap.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                reader.closeAsync();
                return;
            }
            if (hasMore) {
                reader.readNextAsync().whenComplete((msg, e) -> {
                    if (e != null) {
                        log.error("[{}] Failed to read event from the system topic.",
                                reader.getSystemTopic().getTopicName(), ex);
                        future.completeExceptionally(e);
                        readerCaches.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                        policyCacheInitMap.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                        reader.closeAsync();
                        return;
                    }
                    refreshTopicPoliciesCache(msg);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Loop next event reading for system topic.",
                                reader.getSystemTopic().getTopicName().getNamespaceObject());
                    }
                    initPolicesCache(reader, future);
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Reach the end of the system topic.", reader.getSystemTopic().getTopicName());
                }
                policyCacheInitMap.computeIfPresent(
                        reader.getSystemTopic().getTopicName().getNamespaceObject(), (k, v) -> true);
                // replay policy message
                policiesCache.forEach(((topicName, topicPolicies) -> {
                    if (listeners.get(topicName) != null) {
                        for (TopicPolicyListener<TopicPolicies> listener : listeners.get(topicName)) {
                            listener.onUpdate(topicPolicies);
                        }
                    }
                }));
                future.complete(null);
            }
        });
    }

    private void readMorePolicies(SystemTopicClient.Reader<PulsarEvent> reader) {
        reader.readNextAsync().whenComplete((msg, ex) -> {
            if (ex == null) {
                refreshTopicPoliciesCache(msg);
                notifyListener(msg);
                readMorePolicies(reader);
            } else {
                if (ex instanceof PulsarClientException.AlreadyClosedException) {
                    log.error("Read more topic policies exception, close the read now!", ex);
                    NamespaceName namespace = reader.getSystemTopic().getTopicName().getNamespaceObject();
                    ownedBundlesCountPerNamespace.remove(namespace);
                    readerCaches.remove(namespace);
                } else {
                    readMorePolicies(reader);
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

    private static boolean hasReplicateTo(Message<?> message) {
        if (message instanceof MessageImpl) {
            return ((MessageImpl<?>) message).hasReplicateTo();
        }
        if (message instanceof TopicMessageImpl) {
            return hasReplicateTo(((TopicMessageImpl<?>) message).getMessage());
        }
        return false;
    }

    private void createSystemTopicFactoryIfNeeded() {
        if (namespaceEventsSystemTopicFactory == null) {
            synchronized (this) {
                if (namespaceEventsSystemTopicFactory == null) {
                    try {
                        namespaceEventsSystemTopicFactory =
                                new NamespaceEventsSystemTopicFactory(pulsarService.getClient());
                    } catch (PulsarServerException e) {
                        log.error("Create namespace event system topic factory error.", e);
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
    public Boolean getPoliciesCacheInit(NamespaceName namespaceName) {
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
