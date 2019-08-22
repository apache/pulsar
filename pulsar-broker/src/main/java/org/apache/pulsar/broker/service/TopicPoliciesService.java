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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopic;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.events.TopicPoliciesEvent;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Topic policies service
 */
public class TopicPoliciesService {

    private final PulsarService pulsarService;
    private NamespaceEventsSystemTopicFactory namespaceEventsSystemTopicFactory;

    private final Map<TopicName, TopicPolicies> policiesCache = new ConcurrentHashMap<>();

    private final LoadingCache<NamespaceName, CompletableFuture<SystemTopic.Reader>> readerCache;

    public TopicPoliciesService(PulsarService pulsarService) {
        this(pulsarService, 1000, 10, TimeUnit.MINUTES);
    }

    public TopicPoliciesService(PulsarService pulsarService, long cacheSize, long cacheExpireDuration, TimeUnit cacheExpireUnit) {
        this.pulsarService = pulsarService;
        this.readerCache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize)
            .expireAfterAccess(cacheExpireDuration, cacheExpireUnit)
            .removalListener((RemovalListener<NamespaceName, CompletableFuture<SystemTopic.Reader>>) notification -> {
                NamespaceName namespaceName = notification.getKey();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Reader cache was evicted, current reader cache size is {} ", namespaceName,
                            TopicPoliciesService.this.readerCache.asMap().size());
                }
                policiesCache.entrySet().removeIf(entry -> entry.getKey().getNamespaceObject().equals(namespaceName));
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Topic policies cache deleted success, current policies cache size is {} ",
                            namespaceName, policiesCache.size());
                }
                notification.getValue().whenComplete((reader, ex) -> {
                    if (ex == null && reader != null) {
                        reader.closeAsync().whenComplete((v, e) -> {
                            if (e != null) {
                                log.error("[{}] Close reader error for reader cache expire", namespaceName, e);
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Reader is closed for reader cache expire.",
                                            reader.getSystemTopic().getTopicName());
                                }
                            }
                        });
                    } else {
                        TopicPoliciesService.this.readerCache.asMap().remove(namespaceName, notification.getValue());
                    }
                });
            })
            .build(new CacheLoader<NamespaceName, CompletableFuture<SystemTopic.Reader>>() {
                @Override
                public CompletableFuture<SystemTopic.Reader> load(NamespaceName namespaceName) {
                    CompletableFuture<SystemTopic.Reader> readerFuture = loadSystemTopicReader(namespaceName);
                    readerFuture.whenComplete((r, cause) -> {
                        if (null != cause || r == null) {
                            readerCache.asMap().remove(namespaceName, readerFuture);
                        }
                    });
                    return readerFuture;
                }
            });
    }

    public CompletableFuture<MessageId> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies) {
        createSystemTopicFactoryIfNeeded();
        SystemTopic systemTopic = namespaceEventsSystemTopicFactory.createSystemTopic(topicName.getNamespaceObject(),
                EventType.TOPIC_POLICY);
        CompletableFuture<MessageId> result = new CompletableFuture<>();
        CompletableFuture<SystemTopic.Writer> writerFuture = systemTopic.newWriterAsync();
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
                                result.complete(messageId);
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

    public CompletableFuture<TopicPolicies> getTopicPoliciesAsync(TopicName topicName) {
        CompletableFuture<SystemTopic.Reader> readerFuture = null;
        try {
            readerFuture = readerCache.get(topicName.getNamespaceObject());
        } catch (ExecutionException e) {
            log.error("[{}] Load reader error.", topicName, e);
        }
        if (readerFuture == null) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<TopicPolicies> result = new CompletableFuture<>();
        CompletableFuture<Void> refreshFuture = new CompletableFuture<>();
        refreshFuture.whenComplete((v, ex) -> result.complete(policiesCache.get(topicName)));
        readerFuture.thenAccept(reader -> refreshCacheIfNeeded(reader, refreshFuture));
        return result;
    }

    public CompletableFuture<TopicPolicies> getTopicPoliciesWithoutCacheAsync(TopicName topicName) {
        CompletableFuture<TopicPolicies> result = new CompletableFuture<>();
        createSystemTopicFactoryIfNeeded();
        if (namespaceEventsSystemTopicFactory == null) {
            result.complete(null);
            return result;
        }
        SystemTopic systemTopic = namespaceEventsSystemTopicFactory.createSystemTopic(topicName.getNamespaceObject()
                , EventType.TOPIC_POLICY);
        systemTopic.newReaderAsync().thenAccept(r ->
                fetchTopicPoliciesAsyncAndCloseReader(r, topicName, null, result));
        return result;
    }

    private CompletableFuture<SystemTopic.Reader> loadSystemTopicReader(NamespaceName namespaceName) {
        createSystemTopicFactoryIfNeeded();
        SystemTopic systemTopic = namespaceEventsSystemTopicFactory.createSystemTopic(namespaceName
                , EventType.TOPIC_POLICY);
        return systemTopic.newReaderAsync();
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

    private void refreshCacheIfNeeded(SystemTopic.Reader reader, CompletableFuture<Void> refreshFuture) {
        reader.hasMoreEventsAsync().whenComplete((hasMore, ex) -> {
            if (ex != null) {
                refreshFuture.completeExceptionally(ex);
                readerCache.asMap().remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
            }
            if (hasMore) {
                reader.readNextAsync().whenComplete((msg, e) -> {
                    if (e != null) {
                        refreshFuture.completeExceptionally(e);
                        readerCache.asMap().remove(reader.getSystemTopic().getTopicName().getNamespaceObject());
                    }
                    if (EventType.TOPIC_POLICY.equals(msg.getValue().getEventType())) {
                        TopicPoliciesEvent event = msg.getValue().getTopicPoliciesEvent();
                        policiesCache.put(
                            TopicName.get(event.getDomain(), event.getTenant(), event.getNamespace(), event.getTopic()),
                            event.getPolicies()
                        );
                    }
                    refreshCacheIfNeeded(reader, refreshFuture);
                });
            } else {
                refreshFuture.complete(null);
            }
        });
    }

    private void fetchTopicPoliciesAsyncAndCloseReader(SystemTopic.Reader reader, TopicName topicName, TopicPolicies policies,
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
        return readerCache.size();
    }

    @VisibleForTesting
    boolean checkReaderIsCached(NamespaceName namespaceName) {
        return readerCache.getIfPresent(namespaceName) != null;
    }

    private static final Logger log = LoggerFactory.getLogger(TopicPoliciesService.class);
}
