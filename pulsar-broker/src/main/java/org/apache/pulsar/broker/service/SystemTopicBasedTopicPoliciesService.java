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
import org.apache.pulsar.broker.service.BrokerServiceException.TopicPoliciesCacheNotInitException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.events.TopicPoliciesEvent;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cached topic policies service will cache the system topic reader and the topic policies
 *
 * While reader cache for the namespace was removed, the topic policies will remove automatically.
 */
public class SystemTopicBasedTopicPoliciesService implements TopicPoliciesService {

    private final PulsarService pulsarService;
    private NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    private final Map<TopicName, TopicPolicies> policiesCache = new ConcurrentHashMap<>();

    private final Map<NamespaceName, AtomicInteger> ownedBundlesCountPerNamespace = new ConcurrentHashMap<>();

    private final Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader>> readerCaches = new ConcurrentHashMap<>();

    private final Map<NamespaceName, Boolean> policyCacheInitMap = new ConcurrentHashMap<>();

    public SystemTopicBasedTopicPoliciesService(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
    }

    @Override
    public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        createSystemTopicFactoryIfNeeded();
        SystemTopicClient systemTopicClient = namespaceEventsSystemTopicFactory.createSystemTopic(topicName.getNamespaceObject(),
                EventType.TOPIC_POLICY);

        CompletableFuture<SystemTopicClient.Writer> writerFuture = systemTopicClient.newWriterAsync();
        writerFuture.whenComplete((writer, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else {
                writer.writeAsync(
                    PulsarEvent.builder()
                        .actionType(ActionType.UPDATE)
                        .eventType(EventType.TOPIC_POLICY)
                        .topicPoliciesEvent(
                            TopicPoliciesEvent.builder()
                                .domain(topicName.getDomain().toString())
                                .tenant(topicName.getTenant())
                                .namespace(topicName.getNamespaceObject().getLocalName())
                                .topic(topicName.getLocalName())
                                .policies(policies)
                                .build())
                        .build()).whenComplete(((messageId, e) -> {
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
                if (listeners.get(topicName) != null) {
                    for (TopicPolicyListener<TopicPolicies> listener : listeners.get(topicName)) {
                        listener.onUpdate(policies);
                    }
                }
            }
        });


        return result;
    }

    @Override
    public TopicPolicies getTopicPolicies(TopicName topicName) throws TopicPoliciesCacheNotInitException {
        if (policyCacheInitMap.containsKey(topicName.getNamespaceObject())
                && !policyCacheInitMap.get(topicName.getNamespaceObject())) {
            throw new TopicPoliciesCacheNotInitException();
        }
        return policiesCache.get(topicName);
    }

    @Override
    public CompletableFuture<TopicPolicies> getTopicPoliciesBypassCacheAsync(TopicName topicName) {
        CompletableFuture<TopicPolicies> result = new CompletableFuture<>();
        createSystemTopicFactoryIfNeeded();
        if (namespaceEventsSystemTopicFactory == null) {
            result.complete(null);
            return result;
        }
        SystemTopicClient systemTopicClient = namespaceEventsSystemTopicFactory.createSystemTopic(topicName.getNamespaceObject()
                , EventType.TOPIC_POLICY);
        systemTopicClient.newReaderAsync().thenAccept(r ->
                fetchTopicPoliciesAsyncAndCloseReader(r, topicName, null, result));
        return result;
    }

    @Override
    public CompletableFuture<Void> addOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        NamespaceName namespace = namespaceBundle.getNamespaceObject();
        createSystemTopicFactoryIfNeeded();
        synchronized (this) {
            if (readerCaches.get(namespace) != null) {
                ownedBundlesCountPerNamespace.get(namespace).incrementAndGet();
                result.complete(null);
            } else {
                SystemTopicClient systemTopicClient = namespaceEventsSystemTopicFactory.createSystemTopic(namespace
                        , EventType.TOPIC_POLICY);
                ownedBundlesCountPerNamespace.putIfAbsent(namespace, new AtomicInteger(1));
                policyCacheInitMap.put(namespace, false);
                CompletableFuture<SystemTopicClient.Reader> readerCompletableFuture = systemTopicClient.newReaderAsync();
                readerCaches.put(namespace, readerCompletableFuture);
                readerCompletableFuture.whenComplete((reader, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    } else {
                        initPolicesCache(reader, result);
                        readMorePolicies(reader);
                    }
                });
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> removeOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle) {
        NamespaceName namespace = namespaceBundle.getNamespaceObject();
        AtomicInteger bundlesCount = ownedBundlesCountPerNamespace.get(namespace);
        if (bundlesCount == null || bundlesCount.decrementAndGet() <= 0) {
            CompletableFuture<SystemTopicClient.Reader> readerCompletableFuture = readerCaches.remove(namespace);
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

        pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(new NamespaceBundleOwnershipListener() {

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

    private void initPolicesCache(SystemTopicClient.Reader reader, CompletableFuture<Void> future) {
        reader.hasMoreEventsAsync().whenComplete((hasMore, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
                readerCaches.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
            }
            if (hasMore) {
                reader.readNextAsync().whenComplete((msg, e) -> {
                    if (e != null) {
                        future.completeExceptionally(e);
                        readerCaches.remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                    }
                    refreshTopicPoliciesCache(msg);
                    initPolicesCache(reader, future);
                });
            } else {
                future.complete(null);
                policyCacheInitMap.computeIfPresent(reader.getSystemTopic().getTopicName().getNamespaceObject(), (k, v) -> true);
            }
        });
    }

    private void readMorePolicies(SystemTopicClient.Reader reader) {
        reader.readNextAsync().whenComplete((msg, ex) -> {
            if (ex == null) {
                refreshTopicPoliciesCache(msg);
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
        if (EventType.TOPIC_POLICY.equals(msg.getValue().getEventType())) {
            TopicPoliciesEvent event = msg.getValue().getTopicPoliciesEvent();
            policiesCache.put(
                    TopicName.get(event.getDomain(), event.getTenant(), event.getNamespace(), event.getTopic()),
                    event.getPolicies()
            );
        }
    }

    private void createSystemTopicFactoryIfNeeded() {
        if (namespaceEventsSystemTopicFactory == null) {
            synchronized (this) {
                if (namespaceEventsSystemTopicFactory == null) {
                    try {
                        namespaceEventsSystemTopicFactory = new NamespaceEventsSystemTopicFactory(pulsarService.getClient());
                    } catch (PulsarServerException e) {
                        log.error("Create namespace event system topic factory error.", e);
                    }
                }
            }
        }
    }

    private void fetchTopicPoliciesAsyncAndCloseReader(SystemTopicClient.Reader reader, TopicName topicName, TopicPolicies policies,
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
                    if (EventType.TOPIC_POLICY.equals(msg.getValue().getEventType())) {
                        TopicPoliciesEvent topicPoliciesEvent = msg.getValue().getTopicPoliciesEvent();
                        if (topicName.equals(TopicName.get(
                                topicPoliciesEvent.getDomain(),
                                topicPoliciesEvent.getTenant(),
                                topicPoliciesEvent.getNamespace(),
                                topicPoliciesEvent.getTopic()))
                        ) {
                            fetchTopicPoliciesAsyncAndCloseReader(reader, topicName, topicPoliciesEvent.getPolicies(), future);
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
    Boolean getPoliciesCacheInit(NamespaceName namespaceName) {
        return policyCacheInitMap.get(namespaceName);
    }

    @Override
    public void registerListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
        listeners.computeIfAbsent(topicName, k -> Lists.newCopyOnWriteArrayList()).add(listener);
    }

    @Override
    public void unregisterListener(TopicName topicName, TopicPolicyListener<TopicPolicies> listener) {
        listeners.computeIfAbsent(topicName, k -> Lists.newCopyOnWriteArrayList()).remove(listener);
    }

    private static final Logger log = LoggerFactory.getLogger(SystemTopicBasedTopicPoliciesService.class);
}
