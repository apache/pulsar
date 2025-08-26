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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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
import org.apache.pulsar.client.api.MessageIdAdv;
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
    private final HashSet<String> localCluster;
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

    private final Map<NamespaceName, TopicPolicyMessageHandlerTracker> topicPolicyMessageHandlerTrackers =
            new ConcurrentHashMap<>();

    private final AsyncLoadingCache<NamespaceName, SystemTopicClient.Writer<PulsarEvent>> writerCaches;

    // Sequencer for policy updates per topic and per policy type (global/local)
    // Key: Pair<TopicName, Boolean (isGlobal)>, Value: CompletableFuture<Void> representing the last update in sequence
    private final ConcurrentHashMap<Pair<TopicName, Boolean>, CompletableFuture<Void>> topicPolicyUpdateSequencer =
            new ConcurrentHashMap<>();

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
            // delete local policy
            return updateTopicPoliciesAsync(topicName, null, false, ActionType.DELETE, true).thenCompose(__ -> {
                if (keepGlobalPolicies) {
                    // skip deleting global policy due to PIP-422
                    return CompletableFuture.completedFuture(null);
                }
                // delete global policy
                return updateTopicPoliciesAsync(topicName, null, true, ActionType.DELETE, true);
            });
        });
    }

    @Override
    public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName,
                                                            boolean isGlobalPolicy,
                                                            boolean skipUpdateWhenTopicPolicyDoesntExist,
                                                            Consumer<TopicPolicies> policyUpdater) {
        if (NamespaceService.isHeartbeatNamespace(topicName.getNamespaceObject())) {
            return CompletableFuture.failedFuture(new BrokerServiceException.NotAllowedException(
                    "Not allowed to update topic policy for the heartbeat topic"));
        }
        return updateTopicPoliciesAsync(topicName, policyUpdater, isGlobalPolicy, ActionType.UPDATE,
                skipUpdateWhenTopicPolicyDoesntExist);
    }

    private CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName,
                                                             Consumer<TopicPolicies> policyUpdater,
                                                             boolean isGlobalPolicy,
                                                             ActionType actionType,
                                                             boolean skipUpdateWhenTopicPolicyDoesntExist) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new BrokerServiceException(getClass().getName() + " is closed."));
        }
        TopicName partitionedTopicName = TopicName.get(topicName.getPartitionedTopicName());
        Pair<TopicName, Boolean> sequencerKey = Pair.of(partitionedTopicName, isGlobalPolicy);

        CompletableFuture<Void> operationFuture = new CompletableFuture<>();

        // Chain the operation on the sequencer for the specific topic and policy type
        topicPolicyUpdateSequencer.compute(sequencerKey, (key, existingFuture) -> {
            CompletableFuture<Void> chain = (existingFuture == null || existingFuture.isDone())
                    ? CompletableFuture.completedFuture(null)
                    : existingFuture;

            return chain.thenCompose(v ->
                    pulsarService.getPulsarResources().getNamespaceResources()
                            .getPoliciesAsync(topicName.getNamespaceObject())
                            .thenCompose(namespacePolicies -> {
                                if (namespacePolicies.isPresent() && namespacePolicies.get().deleted) {
                                    log.debug("[{}] skip sending topic policy event since the namespace is deleted",
                                            topicName);
                                    return CompletableFuture.completedFuture(null);
                                }
                                return getTopicPoliciesAsync(partitionedTopicName,
                                        isGlobalPolicy ? GetType.GLOBAL_ONLY : GetType.LOCAL_ONLY)
                                        .thenCompose(currentPolicies -> {
                                            if (currentPolicies.isEmpty() && skipUpdateWhenTopicPolicyDoesntExist) {
                                                log.debug("[{}] No existing policies, skipping sending event as "
                                                        + "requested", topicName);
                                                return CompletableFuture.completedFuture(null);
                                            }
                                            TopicPolicies policiesToUpdate;
                                            if (actionType == ActionType.DELETE) {
                                                policiesToUpdate = null; // For delete, policies object is null
                                            } else {
                                                policiesToUpdate = currentPolicies.isEmpty()
                                                        ? createTopicPolicies(isGlobalPolicy)
                                                        : currentPolicies.get().clone();
                                                policyUpdater.accept(policiesToUpdate);
                                            }
                                            return sendTopicPolicyEventInternal(topicName, actionType, policiesToUpdate,
                                                    isGlobalPolicy);
                                        })
                                        .thenCompose(messageId -> {
                                            if (messageId == null) {
                                                return CompletableFuture.completedFuture(null);
                                            } else {
                                                // asynchronously wait until the message ID is read by the reader
                                                return untilMessageIdHasBeenRead(topicName.getNamespaceObject(),
                                                        messageId);
                                            }
                                        });
                            }));
        }).whenComplete((res, ex) -> {
            // remove the current future from the sequencer map, if it is done
            // this would remove the future from the sequencer map when the last operation completes in the chained
            // future
            topicPolicyUpdateSequencer.compute(sequencerKey, (key, chainedFuture) -> {
                if (chainedFuture != null && chainedFuture.isDone()) {
                    // Remove the completed future from the sequencer map
                    return null;
                }
                return chainedFuture;
            });
            if (ex != null) {
                operationFuture.completeExceptionally(FutureUtil.unwrapCompletionException(ex));
            } else {
                operationFuture.complete(res);
            }
        });
        return operationFuture;
    }

    /**
     * Asynchronously waits until the message ID has been read by the reader.
     * This ensures that the write operation has been fully processed and the changes are effective.
     * @param namespaceObject the namespace object for which the message ID is being tracked
     * @param messageId the message ID to wait for being handled
     * @return a CompletableFuture that completes when the message ID has been read by the reader
     */
    private CompletableFuture<Void> untilMessageIdHasBeenRead(NamespaceName namespaceObject, MessageId messageId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        getMessageHandlerTracker(namespaceObject).addPendingFuture((MessageIdAdv) messageId, future);
        return future;
    }

    private TopicPolicyMessageHandlerTracker getMessageHandlerTracker(NamespaceName namespaceObject) {
        return topicPolicyMessageHandlerTrackers.computeIfAbsent(namespaceObject,
                ns -> new TopicPolicyMessageHandlerTracker());
    }

    private record PendingMessageFuture(MessageId messageId, CompletableFuture<Void> future)
            implements Comparable<PendingMessageFuture> {
        @Override
        public int compareTo(PendingMessageFuture o) {
            return messageId.compareTo(o.messageId);
        }
    }

    /**
     * This tracks the last handled message IDs for each partition of the topic policies topic and
     * pending futures for topic policy messages. Each namespace has its own tracker instance since
     * this is tracking the per-namespace __change_events topic.
     * The purpose for this tracker is to ensure that write operations on topic policies don't complete before the topic
     * policies message has been read by the reader and effective.
     */
    private static class TopicPolicyMessageHandlerTracker implements AutoCloseable {
        private final List<MessageIdAdv> lastHandledMessageIds = new ArrayList<>();
        private final List<PriorityQueue<PendingMessageFuture>> pendingFutures = new ArrayList<>();
        private boolean closed = false;

        /**
         * Called after a message ID has been handled by the reader.
         * This will update the last handled message ID for the partition and complete any pending futures that are
         * registered to the handled message ID or before it.
         * @param messageId the message ID that has been handled
         */
        public synchronized void handleMessageId(MessageIdAdv messageId) {
            if (closed) {
                return;
            }
            int partitionIndex = messageId.getPartitionIndex();
            if (partitionIndex < 0) {
                partitionIndex = 0;
            }
            while (lastHandledMessageIds.size() <= partitionIndex) {
                lastHandledMessageIds.add(null);
            }
            lastHandledMessageIds.set(partitionIndex, messageId);
            if (pendingFutures.size() > partitionIndex) {
                PriorityQueue<PendingMessageFuture> pq = pendingFutures.get(partitionIndex);
                while (!pq.isEmpty() && pq.peek().messageId.compareTo(messageId) <= 0) {
                    PendingMessageFuture pendingFuture = pq.poll();
                    completeFuture(pendingFuture.messageId(), pendingFuture.future());
                }
            }
        }

        /**
         * Adds a pending future for a message ID. If the message ID is already handled, the future will be completed
         * immediately.
         * @param messageId the message ID to add the future for
         * @param future the future to complete when the message ID is handled
         */
        public synchronized void addPendingFuture(MessageIdAdv messageId, CompletableFuture<Void> future) {
            if (closed) {
                completeFuture(messageId, future);
                return;
            }
            int partitionIndex = messageId.getPartitionIndex();
            if (partitionIndex < 0) {
                partitionIndex = 0;
            }
            while (pendingFutures.size() <= partitionIndex) {
                pendingFutures.add(new PriorityQueue<>());
            }
            MessageIdAdv lastHandledMessageId =
                    lastHandledMessageIds.size() > partitionIndex ? lastHandledMessageIds.get(partitionIndex) : null;
            if (lastHandledMessageId != null && lastHandledMessageId.compareTo(messageId) >= 0) {
                // If the messageId is already handled, complete the future immediately
                completeFuture(messageId, future);
                return;
            }
            pendingFutures.get(partitionIndex).add(new PendingMessageFuture(messageId, future));
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                for (PriorityQueue<PendingMessageFuture> pq : pendingFutures) {
                    while (!pq.isEmpty()) {
                        PendingMessageFuture pendingFuture = pq.poll();
                        completeFuture(pendingFuture.messageId(), pendingFuture.future());
                    }
                }
                pendingFutures.clear();
                lastHandledMessageIds.clear();
            }
        }

        private void completeFuture(MessageId messageId, CompletableFuture<Void> future) {
            try {
                future.complete(null);
            } catch (Exception ex) {
                log.error("Failed to complete pending future for message id {}.", messageId, ex);
            }
        }
    }


    private static TopicPolicies createTopicPolicies(boolean isGlobalPolicy) {
        TopicPolicies topicPolicies = new TopicPolicies();
        topicPolicies.setIsGlobal(isGlobalPolicy);
        return topicPolicies;
    }

    private CompletableFuture<MessageId> sendTopicPolicyEventInternal(TopicName topicName, ActionType actionType,
                                                                      @Nullable TopicPolicies policies,
                                                                      boolean isGlobalPolicy) {
        return writerCaches.get(topicName.getNamespaceObject())
                .thenCompose(writer -> {
                    PulsarEvent event = getPulsarEvent(topicName, actionType, policies, isGlobalPolicy);
                    String eventKey = getEventKey(event, isGlobalPolicy);

                    if (actionType == ActionType.DELETE) {
                        var future = writer.deleteAsync(eventKey, event);
                        future.exceptionally(ex -> {
                            log.error("Failed to delete {} topic policy [{}] error.",
                                    isGlobalPolicy ? "global" : "local", topicName, ex);
                            return null;
                        });
                        return future;
                    } else {
                        return writer.writeAsync(eventKey, event);
                    }
                }).exceptionally(t -> {
                    // The cached writer will be closed when an exception happens
                    // This is potentially not a great idea since we should be able to rely on the Pulsar client's
                    // behavior for restoring a Producer after a failure.
                    writerCaches.synchronous().invalidate(topicName.getNamespaceObject());
                    throw FutureUtil.wrapToCompletionException(t);
                });
    }

    private PulsarEvent getPulsarEvent(TopicName topicName, ActionType actionType, @Nullable TopicPolicies policies,
                                       boolean isGlobalPolicy) {
        PulsarEvent.PulsarEventBuilder builder = PulsarEvent.builder();
        if (!isGlobalPolicy) {
            // we don't need to replicate local policies to remote cluster, so set `replicateTo` to localCluster.
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
            List<TopicPolicyListener> listeners = this.listeners.get(topicName);
            if (listeners != null) {
                for (TopicPolicyListener listener : listeners) {
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
        List<TopicPolicyListener> listeners = this.listeners.get(topicName);
        if (listeners != null) {
            TopicPolicies policies = event.getPolicies();
            for (TopicPolicyListener listener : listeners) {
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
        AtomicInteger bundlesCount =
                ownedBundlesCountPerNamespace.computeIfAbsent(namespace, k -> new AtomicInteger(0));
        int previousCount = bundlesCount.getAndIncrement();
        if (previousCount == 0) {
            // initialize policies cache asynchronously on the first bundle load
            prepareInitPoliciesCacheAsync(namespace).exceptionally(t -> {
                log.warn("Failed to prepare policies cache for namespace {} due to previously logged error ({}).",
                        namespace, t.getMessage());
                return null;
            });
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
        if (NamespaceService.isHeartbeatNamespace(namespace)) {
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
                        pulsarService.getOrderedExecutor().executeOrdered(bundle.getNamespaceObject(),
                                () -> addOwnedNamespaceBundleAsync(bundle));
                    }

                    @Override
                    public void unLoad(NamespaceBundle bundle) {
                        pulsarService.getOrderedExecutor().executeOrdered(bundle.getNamespaceObject(),
                                () -> removeOwnedNamespaceBundleAsync(bundle));
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

        TopicPolicyMessageHandlerTracker topicPolicyMessageHandlerTracker =
                topicPolicyMessageHandlerTrackers.remove(namespace);
        if (topicPolicyMessageHandlerTracker != null) {
            topicPolicyMessageHandlerTracker.close();
        }

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
            globalPoliciesCache.entrySet()
                    .removeIf(entry -> Objects.equals(entry.getKey().getNamespaceObject(), namespace));
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
        NamespaceName namespaceObject = reader.getSystemTopic().getTopicName().getNamespaceObject();
        if (closed.get()) {
            cleanCacheAndCloseReader(namespaceObject, false);
            return;
        }
        reader.readNextAsync()
                .thenAccept(msg -> {
                    try {
                        refreshTopicPoliciesCache(msg);
                        try {
                            getMessageHandlerTracker(namespaceObject).handleMessageId(
                                    (MessageIdAdv) msg.getMessageId());
                        } finally {
                            notifyListener(msg);
                        }
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
                            cleanCacheAndCloseReader(namespaceObject, false);
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
                    sendTopicPolicyEventInternal(topicName, ActionType.DELETE, null,
                            event.getPolicies().isGlobalPolicies())
                            .whenComplete((__, ex) -> {
                                if (ex != null) {
                                    log.error("Failed to send delete topic policy event for {}", topicName, ex);
                                }
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

    @VisibleForTesting
    public int getTopicPolicyUpdateSequencerSize() {
        return topicPolicyUpdateSequencer.size();
    }
}
