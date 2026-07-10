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
import io.opentelemetry.api.metrics.LongCounter;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.CustomLog;
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

/**
 * Cached topic policies service will cache the system topic reader and the topic policies
 *
 * While reader cache for the namespace was removed, the topic policies will remove automatically.
 */
@CustomLog
public class SystemTopicBasedTopicPoliciesService implements TopicPoliciesService {

    public static final String TOPIC_POLICIES_CACHE_INIT_TIMEOUT_METRIC_NAME =
            "pulsar.broker.topic.policies.cache.init.timeout.count";

    private final PulsarService pulsarService;
    private final HashSet<String> localCluster;
    private final String clusterName;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final LongCounter policyCacheInitTimeoutCounter;

    private final ConcurrentInitializer<NamespaceEventsSystemTopicFactory>
            namespaceEventsSystemTopicFactoryLazyInitializer = new LazyInitializer<>() {
        @Override
        protected NamespaceEventsSystemTopicFactory initialize() {
            try {
                return new NamespaceEventsSystemTopicFactory(pulsarService.getClient());
            } catch (PulsarServerException e) {
                log.error().exception(e).log("Create namespace event system topic factory error.");
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
        this.policyCacheInitTimeoutCounter = pulsarService.getOpenTelemetry().getMeter()
                .counterBuilder(TOPIC_POLICIES_CACHE_INIT_TIMEOUT_METRIC_NAME)
                .setDescription("The number of times initializing a namespace's topic policies cache timed out "
                        + "because the __change_events system-topic reader was stuck. Each occurrence closes the "
                        + "stuck reader and clears the cached state so topic loading can be retried.")
                .setUnit("{timeout}")
                .build();
        this.writerCaches = Caffeine.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .removalListener((namespaceName, writer, cause) -> {
                    try {
                        ((SystemTopicClient.Writer) writer).close();
                    } catch (Exception e) {
                        log.error().attr("namespace", namespaceName).exception(e).log("Close writer error.");
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
                log.info()
                        .attr("changeEvents", changeEvents)
                        .log("Skip delete topic-level policies because has been removed before");
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
                                    log.debug()
                                            .attr("topic", topicName)
                                            .log("skip sending topic policy event since the namespace is deleted");
                                    return CompletableFuture.completedFuture(null);
                                }
                                return getTopicPoliciesAsync(partitionedTopicName,
                                        isGlobalPolicy ? GetType.GLOBAL_ONLY : GetType.LOCAL_ONLY)
                                        .thenCompose(currentPolicies -> {
                                            if (currentPolicies.isEmpty() && skipUpdateWhenTopicPolicyDoesntExist) {
                                                log.debug()
                                                        .attr("topic", topicName)
                                                        .log("No existing policies, skipping sending event as "
                                                                + "requested");
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
                log.error()
                        .attr("messageId", messageId)
                        .exception(ex)
                        .log("Failed to complete pending future for message id.");
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
                            log.error()
                                    .attr("policyType", isGlobalPolicy ? "global" : "local")
                                    .attr("topic", topicName)
                                    .exception(ex)
                                    .log("Failed to delete topic policy");
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
                    cleanWriterCache(topicName.getNamespaceObject());
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
            notifyListenersForTopic(topicName, null);
            return;
        }

        if (!EventType.TOPIC_POLICY.equals(msg.getValue().getEventType())) {
            return;
        }
        TopicPoliciesEvent event = msg.getValue().getTopicPoliciesEvent();
        TopicName topicName = TopicName.get(event.getDomain(), event.getTenant(),
                event.getNamespace(), event.getTopic());
        notifyListenersForTopic(topicName, event.getPolicies());
    }

    /**
     * Notifies the topic-policy listeners registered for {@code topicName} of a policy update.
     *
     * <p>The {@link TopicPolicyListener#onUpdate} calls are dispatched to the per-topic ordered executor
     * rather than run inline. The reader callbacks that drive notifications (the {@link #initPolicesCache}
     * replay loop and {@link #readMorePoliciesAsync}) run on the single, process-wide shared
     * {@code broker-client-shared-internal-executor} thread. A listener (e.g.
     * {@code PersistentTopic.onUpdate} -> {@code applyUpdatedTopicPolicies}) can perform non-trivial and
     * even blocking work, so running it inline serializes and can stall topic-policy loading for every
     * namespace (issue #26037). Keying {@code executeOrdered} by {@code topicName} preserves per-topic
     * notification ordering.
     */
    private void notifyListenersForTopic(TopicName topicName, @Nullable TopicPolicies policies) {
        // The per-topic value is a CopyOnWriteArrayList, so iterating it later on the executor thread stays
        // safe even if a listener is registered/unregistered between dispatch and execution.
        List<TopicPolicyListener> topicListeners = listeners.get(topicName);
        if (topicListeners == null || topicListeners.isEmpty()) {
            return;
        }
        pulsarService.getBrokerService().getTopicPoliciesNotifyThread(topicName).execute(() -> {
            internalNotifyTopicListeners(topicName, policies, topicListeners);
        });
    }

    // this method should only be called from the topic ordered executor thread
    // use notifyListenersForTopic/notifyListenersForTopicAsync instead
    private static void internalNotifyTopicListeners(TopicName topicName, @Nullable TopicPolicies policies,
                                  List<TopicPolicyListener> topicListeners) {
        for (TopicPolicyListener listener : topicListeners) {
            try {
                listener.onUpdate(policies);
            } catch (Throwable error) {
                log.error().attr("topic", topicName).attr("listener", listener).exception(error)
                        .log("Error in notifying listener on topic policy update.");
            }
        }
    }

    private CompletableFuture<Void> notifyListenersForTopicAsync(TopicName topicName,
                                                                 @Nullable TopicPolicies policies) {
        // The per-topic value is a CopyOnWriteArrayList, so iterating it later on the executor thread stays
        // safe even if a listener is registered/unregistered between dispatch and execution.
        List<TopicPolicyListener> topicListeners = listeners.get(topicName);
        if (topicListeners == null || topicListeners.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        ExecutorService pinnedTopicOrderedExecutor =
                pulsarService.getBrokerService().getTopicPoliciesNotifyThread(topicName);
        return CompletableFuture.runAsync(() -> {
            internalNotifyTopicListeners(topicName, policies, topicListeners);
        }, pinnedTopicOrderedExecutor);
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
            final var p = policiesFutureHolder.get();
            if (!p.getLeft()) {
                log.info()
                        .attr("namespace", namespace)
                        .log("The future of has been removed from cache, retry getTopicPolicies again");
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
                log.warn()
                        .attr("namespace", namespace)
                        .exceptionMessage(t)
                        .log("Failed to prepare policies cache for namespace due to previously logged error");
                return null;
            });
        }
    }

    @VisibleForTesting
    @NonNull CompletableFuture<Boolean> prepareInitPoliciesCacheAsync(@NonNull NamespaceName namespace) {
        if (namespace == null) {
            return FutureUtil.failedFuture(new NullPointerException("Expected NamespaceName should not be null"));
        }
        if (closed.get()) {
            return CompletableFuture.completedFuture(false);
        }
        return pulsarService.getPulsarResources().getNamespaceResources().getPoliciesAsync(namespace)
                .thenCompose(namespacePolicies -> {
                    if (namespacePolicies.isEmpty() || namespacePolicies.get().deleted) {
                        log.info()
                                .attr("namespace", namespace)
                                .log("skip prepare init policies cache since the namespace is deleted");
                        return CompletableFuture.completedFuture(false);
                    }

                    CompletableFuture<Void> initNamespacePolicyFuture = new CompletableFuture<>();
                    CompletableFuture<Void> existingFuture =
                            policyCacheInitMap.putIfAbsent(namespace, initNamespacePolicyFuture);
                    if (existingFuture == null) {
                        // Topic loading waits on this future, so a system-topic reader that gets stuck (e.g. after
                        // __change_events is unloaded and the reconnected reader stops making progress) would pin the
                        // policy cache for the whole namespace until the broker restarts (issue #25294). Bound the
                        // initialization so it fails fast and cleans up, letting topic loading retry with a fresh
                        // reader.
                        scheduleInitPolicesCacheTimeout(namespace, initNamespacePolicyFuture);
                        final CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerCompletableFuture =
                                newReader(namespace);
                        readerCompletableFuture
                                .thenCompose(reader -> {
                                    final CompletableFuture<Void> stageFuture = new CompletableFuture<>();
                                    initPolicesCache(reader, stageFuture);
                                    return stageFuture
                                            // Read policies in background
                                            .thenAccept(__ -> readMorePoliciesAsync(reader, initNamespacePolicyFuture));
                                }).thenApply(__ -> {
                                    initNamespacePolicyFuture.complete(null);
                                    return null;
                                }).exceptionally(ex -> {
                                    try {
                                        // Identity-guarded cleanup: a concurrent timeout cleanup or a namespace unload
                                        // may already have dropped this future and let a retry install a fresh
                                        // future/reader, so clean up by namespace key here would clobber that newer
                                        // attempt. Tear down state only while this future still owns the namespace.
                                        if (readerCompletableFuture.isCompletedExceptionally()) {
                                            log.error()
                                                    .attr("namespace", namespace)
                                                    .exception(ex)
                                                    .log("Failed to create reader on __change_events topic");
                                            initNamespacePolicyFuture.completeExceptionally(ex);
                                            cleanupFailedPolicyCacheInit(namespace, initNamespacePolicyFuture, true);
                                        } else {
                                            initNamespacePolicyFuture.completeExceptionally(ex);
                                            cleanupFailedPolicyCacheInit(namespace, initNamespacePolicyFuture,
                                                    isAlreadyClosedException(ex));
                                        }
                                    } catch (Throwable cleanupEx) {
                                        // Adding this catch to avoid break callback chain
                                        log.error()
                                                .attr("namespace", namespace)
                                                .exception(cleanupEx)
                                                .log("Failed to cleanup reader on __change_events topic");
                                    }
                                    return null;
                                });

                        return initNamespacePolicyFuture.thenApply(__ -> true);
                    } else {
                        return existingFuture.thenApply(__ -> true);
                    }
                });
    }

    private CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> newReader(NamespaceName ns) {
        return readerCaches.computeIfAbsent(ns, __ -> createSystemTopicClient(ns));
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

    void removeOwnedNamespaceBundleAsync(NamespaceBundle namespaceBundle) {
        NamespaceName namespace = namespaceBundle.getNamespaceObject();
        if (NamespaceService.isHeartbeatNamespace(namespace)) {
            return;
        }
        AtomicInteger bundlesCount = ownedBundlesCountPerNamespace.get(namespace);
        if (bundlesCount == null || bundlesCount.decrementAndGet() <= 0) {
            cleanPoliciesCacheInitMap(namespace);
            cleanWriterCache(namespace);
            cleanOwnedBundlesCount(namespace);
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

    /**
     * Bound the topic-policies cache initialization for {@code namespace} so a stuck {@code __change_events} reader
     * cannot pin the cache (and therefore topic loading) for the whole namespace indefinitely (issue #25294). If the
     * timeout wins the race to complete {@code initNamespacePolicyFuture}, the cached state is cleared and the stuck
     * reader is closed so a subsequent load retries from scratch rather than waiting until the broker restarts.
     */
    private void scheduleInitPolicesCacheTimeout(@NonNull NamespaceName namespace,
                                                 @NonNull CompletableFuture<Void> initNamespacePolicyFuture) {
        long timeoutSeconds = pulsarService.getConfiguration().getTopicPoliciesCacheInitTimeoutSeconds();
        if (timeoutSeconds <= 0) {
            return;
        }
        final ScheduledFuture<?> timeoutTask = pulsarService.getExecutor().schedule(() -> {
            TimeoutException timeoutException = new TimeoutException(String.format(
                    "Timed out after %d seconds initializing the topic policies cache for namespace %s; the "
                            + "__change_events reader did not reach the end of the topic", timeoutSeconds, namespace));
            if (initNamespacePolicyFuture.completeExceptionally(timeoutException)) {
                policyCacheInitTimeoutCounter.add(1);
                log.error()
                        .attr("namespace", namespace)
                        .attr("timeoutSeconds", timeoutSeconds)
                        .log("Timed out initializing the topic policies cache; closing the stuck __change_events "
                                + "reader so the namespace can be loaded again");
                try {
                    cleanupFailedPolicyCacheInit(namespace, initNamespacePolicyFuture, true);
                } catch (Throwable cleanupEx) {
                    log.error()
                            .attr("namespace", namespace)
                            .exception(cleanupEx)
                            .log("Failed to clean up the topic policies cache after init timeout");
                }
            }
        }, timeoutSeconds, TimeUnit.SECONDS);
        // Cancel the timeout once initialization finishes (successfully or not) so we don't leak scheduled tasks.
        initNamespacePolicyFuture.whenComplete((__, ex) -> timeoutTask.cancel(false));
    }

    /**
     * Identity-guarded cleanup for an initialization that failed (it timed out, the {@code __change_events} reader
     * could not be created, or reading the topic threw), or whose background reader was later closed (an
     * {@code AlreadyClosedException} surfaced in {@link #readMorePoliciesAsync} after a namespace unload closed the
     * reader). Unlike {@link #cleanPoliciesCacheInitMap}, which
     * removes/closes by namespace key unconditionally, this only tears down state that still belongs to
     * {@code initFuture}. By the time the failure is observed, a concurrent retry — or a namespace-bundle unload that
     * left the init future orphaned — may already own the namespace with a fresh future and reader; removing by key
     * would drop that newer future and close its reader, pinning the namespace again. Guarding on identity ensures a
     * late failure never clobbers a newer initialization.
     *
     * @param closeReader when {@code true}, also closes the reader and message-handler tracker that belong to this
     *                    initialization; when {@code false}, only the init future is dropped, leaving the reader
     *                    cached for the retry to reuse. The cached policies are intentionally left in place; they
     *                    are cleared only when the whole namespace is unloaded, so this cleanup cannot race a
     *                    concurrent re-initialization.
     */
    @VisibleForTesting
    void cleanupFailedPolicyCacheInit(@NonNull NamespaceName namespace,
                                      @NonNull CompletableFuture<Void> initFuture, boolean closeReader) {
        // Capture the reader before dropping the init future so we only close the reader that belongs to this
        // initialization, never one a concurrent retry creates immediately afterwards.
        CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerFuture =
                closeReader ? readerCaches.get(namespace) : null;
        TopicPolicyMessageHandlerTracker tracker = topicPolicyMessageHandlerTrackers.get(namespace);

        // Identity guard: only proceed while this initialization still owns the namespace's init future.
        if (!policyCacheInitMap.remove(namespace, initFuture)) {
            // Superseded by a retry or an unload; that owner is responsible for its own reader/state.
            return;
        }

        // Complete the dropped future (a no-op if the caller already completed it) outside any map remapping function,
        // so awaiting topic loads fail fast and retry instead of hanging until the broker restarts (issue #25294).
        failPendingPolicyCacheInit(namespace, initFuture);
        if (!closeReader) {
            return;
        }

        // Close the tracker captured above only if it is still the one installed for this namespace, so a
        // concurrent re-initialization that installed a newer tracker is left untouched.
        if (tracker != null && topicPolicyMessageHandlerTrackers.remove(namespace, tracker)) {
            tracker.close();
        }

        // Remove and close the reader captured above only if it is still the current one, so a reader
        // created by a later initialization is never closed by this stale cleanup.
        if (readerFuture != null && readerCaches.remove(namespace, readerFuture)
                && !readerFuture.isCompletedExceptionally()) {
            readerFuture.thenCompose(SystemTopicClient.Reader::closeAsync)
                    .exceptionally(ex -> {
                        log.warn().attr("namespace", namespace).exception(ex).log("Close change_event reader fail.");
                        return null;
                    });
        }
    }

    private void initPolicesCache(SystemTopicClient.Reader<PulsarEvent> reader, CompletableFuture<Void> future) {
        if (closed.get()) {
            future.completeExceptionally(new BrokerServiceException(getClass().getName() + " is closed."));
            cleanPoliciesCacheInitMap(reader.getSystemTopic().getTopicName().getNamespaceObject());
            return;
        }
        reader.hasMoreEventsAsync().whenComplete((hasMore, ex) -> {
            if (ex != null) {
                log.error()
                        .attr("topic", reader.getSystemTopic().getTopicName())
                        .exception(ex)
                        .log("Failed to check the move events for the system topic");
                future.completeExceptionally(ex);
                return;
            }
            if (hasMore) {
                reader.readNextAsync().thenAccept(msg -> {
                    try {
                        refreshTopicPoliciesCache(msg);
                    } finally {
                        msg.release();
                    }
                    log.debug()
                            .attr("namespaceObject", reader.getSystemTopic().getTopicName().getNamespaceObject())
                            .log("Loop next event reading for system topic.");
                    initPolicesCache(reader, future);
                }).exceptionally(e -> {
                    log.error()
                            .attr("topic", reader.getSystemTopic().getTopicName())
                            .exception(e)
                            .log("Failed to read event from the system topic.");
                    future.completeExceptionally(e);
                    return null;
                });
            } else {
                log.debug()
                        .attr("topic", reader.getSystemTopic().getTopicName())
                        .log("Reach the end of the system topic.");

                // Optionally re-notify the topic-policy listeners for this namespace with the cached policies.
                // Topics load their own policies on load (AbstractTopic#initTopicPolicy), so this replay is only
                // needed for custom plugins that register TopicPolicyListeners and rely on the broadcast; it is
                // off by default (topicPolicyListenerReplayEnabled).
                if (pulsarService.getConfiguration().isTopicPolicyListenerReplayEnabled()) {
                    NamespaceName namespaceObject = reader.getSystemTopic().getTopicName().getNamespaceObject();
                    FutureUtil.completeAfter(future, replayTopicPolicyListeners(namespaceObject));
                } else {
                    future.complete(null);
                }
            }
        });
    }

    /**
     * Re-notifies the registered topic-policy listeners for every topic in {@code namespace} with the currently
     * cached policies (both local and global). Topics apply their own policies on load, so this is only needed for
     * custom plugins that register {@link TopicPolicyListener}s and rely on the broadcast when a namespace's policy
     * cache finishes loading (see {@code topicPolicyListenerReplayEnabled}).
     */
    @VisibleForTesting
    CompletableFuture<Void> replayTopicPolicyListeners(NamespaceName namespace) {
        List<CompletableFuture<Void>> notifyFutures = new ArrayList<>();
        addNamespacePolicyNotifications(policiesCache, namespace, notifyFutures);
        addNamespacePolicyNotifications(globalPoliciesCache, namespace, notifyFutures);
        return FutureUtil.waitForAll(notifyFutures);
    }

    private void addNamespacePolicyNotifications(Map<TopicName, TopicPolicies> cache, NamespaceName namespace,
                                                 List<CompletableFuture<Void>> notifyFutures) {
        for (Map.Entry<TopicName, TopicPolicies> entry : cache.entrySet()) {
            if (Objects.equals(entry.getKey().getNamespaceObject(), namespace)) {
                notifyFutures.add(notifyListenersForTopicAsync(entry.getKey(), entry.getValue()));
            }
        }
    }

    // Full teardown of a namespace's topic-policies state: removes and closes the reader, the message-handler
    // tracker, the cached policies and the init future. Used when the whole namespace is unloaded.
    @VisibleForTesting
    void cleanPoliciesCacheInitMap(@NonNull NamespaceName namespace) {
        TopicPolicyMessageHandlerTracker topicPolicyMessageHandlerTracker =
                topicPolicyMessageHandlerTrackers.remove(namespace);
        if (topicPolicyMessageHandlerTracker != null) {
            topicPolicyMessageHandlerTracker.close();
        }

        CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> readerFuture = readerCaches.remove(namespace);
        MutableObject<CompletableFuture<Void>> removedInitFuture = new MutableObject<>();
        policyCacheInitMap.compute(namespace, (k, v) -> {
            removedInitFuture.setValue(v);
            policiesCache.entrySet().removeIf(entry -> Objects.equals(entry.getKey().getNamespaceObject(), namespace));
            globalPoliciesCache.entrySet()
                    .removeIf(entry -> Objects.equals(entry.getKey().getNamespaceObject(), namespace));
            return null;
        });
        // Complete the removed init future outside the compute() remapping function: completing it can run the
        // awaiting topic-load callbacks synchronously, and doing that while holding the ConcurrentHashMap bin lock
        // risks a recursive map update / deadlock (see #24977).
        failPendingPolicyCacheInit(namespace, removedInitFuture.getValue());
        if (readerFuture != null && !readerFuture.isCompletedExceptionally()) {
            readerFuture
                    .thenCompose(SystemTopicClient.Reader::closeAsync)
                    .exceptionally(ex -> {
                        log.warn().attr("namespace", namespace).exception(ex).log("Close change_event reader fail.");
                        return null;
                    });
        }
    }

    /**
     * Complete an init future that is being dropped from {@link #policyCacheInitMap} but never completed, so the
     * topic loads awaiting it fail fast and retry instead of hanging until the broker restarts (issue #25294).
     * No-op if the future was already completed by its own initialization chain.
     */
    private void failPendingPolicyCacheInit(@NonNull NamespaceName namespace,
                                            @Nullable CompletableFuture<Void> initFuture) {
        if (initFuture != null && !initFuture.isDone()) {
            initFuture.completeExceptionally(new BrokerServiceException(
                    "Topic policies cache initialization for namespace " + namespace
                            + " was aborted because the cached state was cleared"));
        }
    }

    private void cleanWriterCache(@NonNull NamespaceName namespace) {
        writerCaches.synchronous().invalidate(namespace);
    }

    private void cleanOwnedBundlesCount(@NonNull NamespaceName namespace) {
        ownedBundlesCountPerNamespace.remove(namespace);
    }

    /**
     * This is an async method for the background reader to continue syncing new messages.
     *
     * Note: You should not do any blocking call here. because it will affect
     * #{@link SystemTopicBasedTopicPoliciesService#getTopicPoliciesAsync} method to block loading topic.
     */
    private void readMorePoliciesAsync(SystemTopicClient.Reader<PulsarEvent> reader,
                                       CompletableFuture<Void> initFuture) {
        NamespaceName namespaceObject = reader.getSystemTopic().getTopicName().getNamespaceObject();
        if (closed.get()) {
            cleanupFailedPolicyCacheInit(namespaceObject, initFuture, true);
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
                        readMorePoliciesAsync(reader, initFuture);
                    } else {
                        if (isAlreadyClosedException(ex)) {
                            log.info()
                                    .attr("topic", reader.getSystemTopic().getTopicName())
                                    .log("Closing the topic policies reader for");
                            // Tear down by init-future identity, not by namespace key: this reader may have been
                            // closed by a namespace unload while a concurrent reload already installed a fresh
                            // reader and init future for the same namespace (the close only surfaces here, on the
                            // client executor, afterwards). A namespace-keyed cleanup would clobber that newer
                            // generation and abort its init with "...aborted because the cached state was cleared",
                            // failing the reloading topic. cleanupFailedPolicyCacheInit only tears down state that
                            // still belongs to this initialization, so a superseded reader's late close is a no-op.
                            cleanupFailedPolicyCacheInit(namespaceObject, initFuture, true);
                        } else {
                            log.warn().exception(ex).log("Read more topic polices exception, read again.");
                            readMorePoliciesAsync(reader, initFuture);
                        }
                    }
                });
    }

    private boolean isAlreadyClosedException(Throwable ex) {
        Throwable cause = FutureUtil.unwrapCompletionException(ex);
        return cause instanceof PulsarClientException.AlreadyClosedException;
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
                        log.warn()
                                .attr("topic", topicName)
                                .log("Policy insert failed, the topic: policy already exist");
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
                                    log.error()
                                            .attr("topic", topicName)
                                            .exception(ex)
                                            .log("Failed to send delete topic policy event for");
                                }
                            });
                    break;
                case NONE:
                    break;
                default:
                    log.warn().attr("actionType", msg.getValue().getActionType()).log("Unknown event action type");
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
            log.error().exception(e).log("Create namespace event system topic factory error.");
            throw new RuntimeException(e);
        }
    }


    @VisibleForTesting
    public Map<NamespaceName, CompletableFuture<SystemTopicClient.Reader<PulsarEvent>>> getReaderCaches() {
        return readerCaches;
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
                                log.error().exception(e).log("Failed to close the reader for topic policies");
                            }
                        });
                    }
                } catch (Throwable ignored) {
                }
            });
            readerCaches.clear();
            // Release any topic loads still waiting on an in-progress policy cache initialization so they fail
            // fast instead of hanging until the awaited future is completed indirectly (issue #25294).
            policyCacheInitMap.forEach(this::failPendingPolicyCacheInit);
            policyCacheInitMap.clear();
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
