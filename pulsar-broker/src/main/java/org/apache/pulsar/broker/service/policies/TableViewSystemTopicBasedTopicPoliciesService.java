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
package org.apache.pulsar.broker.service.policies;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.*;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Sets;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.TopicPolicyListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.events.TopicPoliciesEvent;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class TableViewSystemTopicBasedTopicPoliciesService implements TopicPoliciesService {
    private final ScheduledExecutorService asyncExecutor;
    private final Supplier<PulsarClient> internalClient;
    private final NamespaceService namespaceService;
    // todo support key filter view to reduce memory cost
    private final Map<NamespaceName, CompletableFuture<TableView<PulsarEvent>>> views;
    private final Map<NamespaceName, CompletableFuture<Producer<PulsarEvent>>> writers;
    private final Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listeners;
    private final String localCluster;

    public TableViewSystemTopicBasedTopicPoliciesService(@Nonnull PulsarService pulsarService) {
        requireNonNull(pulsarService);
        this.internalClient = () -> {
            try {
                return pulsarService.getClient();
            } catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
        };
        this.namespaceService = pulsarService.getNamespaceService();
        this.views = new ConcurrentHashMap<>();
        this.writers = new ConcurrentHashMap<>();
        this.listeners = new ConcurrentHashMap<>();
        this.localCluster = pulsarService.getConfiguration().getClusterName();
        this.asyncExecutor = pulsarService.getExecutor();
    }

    @Override
    public @Nonnull CompletableFuture<Void> deleteTopicPoliciesAsync(@Nonnull TopicName topicName) {
        return updateTopicPoliciesAsync(topicName, null);
    }

    @Override
    public @Nonnull CompletableFuture<Void> updateTopicPoliciesAsync(@Nonnull TopicName topicName,
                                                                     @Nullable TopicPolicies policies) {
        if (topicName == null) {
            return failedFuture(new NullPointerException());
        }
        final var ns = topicName.getNamespaceObject();
        if (NamespaceService.isHeartbeatNamespace(ns)) {
            return completedFuture(null);
        }
        final CompletableFuture<Void> updateFuture = getOrInitWriterAsync(ns)
                .thenCompose(writer -> {
                    final var key = topicName.getPartitionedTopicName();
                    if (policies == null) {
                        return writer.newMessage().key(key).value(null).sendAsync();
                    }
                    final var builder = PulsarEvent.builder();
                    if (!policies.isGlobalPolicies()) {
                        // we don't need to replicate local policies to remote cluster, so set `replicateTo` to empty.
                        builder.replicateTo(Sets.newHashSet(localCluster));
                    }
                    final var event = builder
                            .actionType(ActionType.UPDATE)
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
                    return writer.newMessage().key(key).value(event).sendAsync();
                }).thenApply(__ -> null);
        updateFuture.exceptionally(ex -> {
            // auto-recovery
            cleanupWriter(ns);
            return null;
        });
        return updateFuture;
    }

    @Override
    public @Nullable TopicPolicies getTopicPolicies(@Nonnull TopicName topicName)
            throws BrokerServiceException.TopicPoliciesCacheNotInitException {
        requireNonNull(topicName);
        final var policiesFuture = getTopicPoliciesAsync(topicName);
        // using retry to implement async-like logic
        if (!policiesFuture.isDone() || policiesFuture.isCompletedExceptionally()) {
            throw new BrokerServiceException.TopicPoliciesCacheNotInitException();
        }
        return policiesFuture.join().orElse(null);
    }

    @Override
    public @Nullable TopicPolicies getTopicPoliciesIfExists(@Nonnull TopicName topicName) {
        requireNonNull(topicName);
        var policiesFuture = getTopicPoliciesAsync(topicName);
        if (!policiesFuture.isDone() || policiesFuture.isCompletedExceptionally()) {
            return null;
        }
        return policiesFuture.join().orElse(null);
    }

    @Override
    public @Nullable TopicPolicies getTopicPolicies(@Nonnull TopicName topicName, boolean isGlobal)
            throws BrokerServiceException.TopicPoliciesCacheNotInitException {
        var topicPolicies = getTopicPolicies(topicName);
        if (topicPolicies == null) {
            return null;
        }
        if (topicPolicies.isGlobalPolicies() != isGlobal) {
            return null;
        }
        return topicPolicies;
    }

    @Override
    public @Nonnull CompletableFuture<TopicPolicies> getTopicPoliciesBypassCacheAsync(@Nonnull TopicName topicName) {
        if (topicName == null) {
            return failedFuture(new NullPointerException());
        }
        return getTopicPoliciesAsync(topicName)
                .thenApply(topicPolicies -> topicPolicies.orElse(null));
    }


    private @Nonnull CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName) {
        var ns = topicName.getNamespaceObject();
        if (NamespaceService.isHeartbeatNamespace(ns)) {
            return completedFuture(Optional.empty());
        }
        var viewFuture = getOrInitViewAsync(ns);
        final CompletableFuture<Optional<TopicPolicies>> policiesFuture = viewFuture.thenApply(view -> {
            var event = view.get(topicName.getPartitionedTopicName());
            if (event == null || event.getEventType() != EventType.TOPIC_POLICY) {
                return Optional.empty();
            }
            var topicPoliciesEvent = event.getTopicPoliciesEvent();
            if (topicPoliciesEvent == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(topicPoliciesEvent.getPolicies());
        });
        policiesFuture.exceptionally(ex -> {
            cleanupView(ns);
            return Optional.empty();
        });
        return policiesFuture;
    }

    @Override
    public @Nonnull CompletableFuture<Void> addOwnedNamespaceBundleAsync(@Nonnull NamespaceBundle bundle) {
        if (bundle == null) {
            return failedFuture(new NullPointerException());
        }
        final var ns = bundle.getNamespaceObject();

        if (NamespaceService.isHeartbeatNamespace(ns)) {
            return completedFuture(null);
        }
        return getOrInitViewAsync(ns).thenApply(__ -> null);
    }

    @Override

    public @Nonnull CompletableFuture<Void> removeOwnedNamespaceBundleAsync(@Nonnull NamespaceBundle bundle) {
        final var ns = bundle.getNamespaceObject();
        return namespaceService.getOwnedBundles(ns)
                .thenCompose(ownedBundles -> {
                    if (ownedBundles.size() != 0) {
                        // don't need to clean up view
                        return completedFuture(null);
                    }
                    final List<CompletableFuture<Void>> container = new ArrayList<>(2);
                    container.add(cleanupView(ns));
                    container.add(cleanupWriter(ns));
                    return FutureUtil.waitForAll(container);
                });
    }

    @Override
    public void start() {
        namespaceService.addNamespaceBundleOwnershipListener(
                new NamespaceBundleOwnershipListener() {
                    @Override
                    public void onLoad(NamespaceBundle bundle) {
                        // switch thread to avoid deadlock causing topic load timeout
                        supplyAsync(() -> null)
                                .thenComposeAsync(__ -> addOwnedNamespaceBundleAsync(bundle), asyncExecutor)
                                .exceptionally(ex -> {
                                    log.warn("Exception occur while trying init "
                                                    + "topic policies by namespace onload event. namespace: {}",
                                            bundle.getNamespaceObject(), ex);
                                    return null;
                                });

                    }

                    @Override
                    public void unLoad(NamespaceBundle bundle) {
                        // switch thread to avoid deadlock causing topic load timeout
                        supplyAsync(() -> null)
                                .thenComposeAsync(__ -> removeOwnedNamespaceBundleAsync(bundle), asyncExecutor)
                                .exceptionally(ex -> {
                                    log.warn("Exception occur while trying cleanup "
                                                    + "topic policies by namespace unloading event. namespace: {}",
                                            bundle.getNamespaceObject(), ex);
                                    return null;
                                });
                    }

                    @Override
                    public boolean test(NamespaceBundle namespaceBundle) {
                        return true;
                    }
                });
    }

    @Override
    public void registerListener(@Nonnull TopicName topicName, @Nonnull TopicPolicyListener<TopicPolicies> listener) {
        requireNonNull(topicName);
        requireNonNull(listener);
        var listenerList = listeners.computeIfAbsent(topicName, (tp) -> new CopyOnWriteArrayList<>());
        listenerList.add(listener);
        //  synchronize listener list by topic name to avoid ABA problems with the existing callback.
        var viewFuture = views.get(topicName.getNamespaceObject());
        if (viewFuture == null || !viewFuture.isDone() || viewFuture.isCompletedExceptionally()) {
            // we don't need compensation measures
            return;
        }
        var view = viewFuture.join();
        synchronized (listenerList) {
            var event = view.get(topicName.getPartitionedTopicName());
            if (event == null || event.getTopicPoliciesEvent() == null) {
                return;
            }
            try {
                listener.onUpdate(event.getTopicPoliciesEvent().getPolicies());
            } catch (Throwable ex) {
                // avoid listener affect topic creation
                log.warn("Error occur while trying callback listener. event_key: {},"
                        + " event: {} listener :{}", topicName.getPartitionedTopicName(), event, listener);
            }
        }
    }

    @Override
    public void unregisterListener(@Nonnull TopicName topicName, @Nonnull TopicPolicyListener<TopicPolicies> listener) {
        requireNonNull(topicName);
        requireNonNull(listener);
        listeners.computeIfPresent(topicName, (tp, listeners) -> {
            listeners.remove(listener);
            if (listeners.size() == 0) { // cleanup the listeners
                return null;
            }
            return listeners;
        });
    }

    private @Nonnull CompletableFuture<Void> cleanupView(@Nonnull NamespaceName ns) {
        final var viewFuture = views.remove(ns);
        if (viewFuture == null) {
            return completedFuture(null);
        }
        if (!viewFuture.isDone()) {
            viewFuture.cancel(true);
            return completedFuture(null);
        }
        if (viewFuture.isCompletedExceptionally()) {
            // don't need to close the resource of view
            return completedFuture(null);
        }
        final var view = viewFuture.join();
        return view.closeAsync();
    }

    private @Nonnull CompletableFuture<Void> cleanupWriter(@Nonnull NamespaceName ns) {
        final var writerFuture = writers.remove(ns);
        if (writerFuture == null) {
            return completedFuture(null);
        }
        if (!writerFuture.isDone()) {
            writerFuture.cancel(true);
            return completedFuture(null);
        }
        if (writerFuture.isCompletedExceptionally()) {
            // don't need to close the resource of writer
            return completedFuture(null);
        }
        final var writer = writerFuture.join();
        return writer.closeAsync();
    }

    private @Nonnull CompletableFuture<TableView<PulsarEvent>> getOrInitViewAsync(@Nonnull NamespaceName ns) {
        // Don't move listenerProcessor into lambada. because we rely on map lock to avoid race condition
        // with registerListener method. :)
        // See  if (viewFuture == null || !viewFuture.isDone() || viewFuture.isCompletedExceptionally()) {
        final MutableBoolean updatedView = new MutableBoolean(false);
        var viewFuture = views.computeIfAbsent(ns, (namespace) -> {
            updatedView.setTrue();
            return internalClient.get().newTableView(Schema.AVRO(PulsarEvent.class))
                    .topic(getEventTopic(namespace))
                    .createAsync();
        });
        if (updatedView.isFalse()) {
            return viewFuture;
        }
        return viewFuture.thenApply(view -> {
            view.forEachAndListen(this::listenerProcessor);
            return view;
        });
    }

    private @Nonnull CompletableFuture<Producer<PulsarEvent>> getOrInitWriterAsync(@Nonnull NamespaceName ns) {
        return writers.computeIfAbsent(ns, (namespace) -> internalClient.get().newProducer(Schema.AVRO(PulsarEvent.class))
                .topic(getEventTopic(namespace))
                .enableBatching(false)
                .createAsync());
    }

    private void listenerProcessor(@Nonnull String key, @Nullable PulsarEvent event) {
        // Note: Table view will call this method internally, If there are performance problem, we can try
        //       moving the listener out of table view then use multi thread to invoke callback.
        try {
            if (event == null) {
                return;
            }
            if (event.getEventType() != EventType.TOPIC_POLICY) {
                return;
            }
            final var partitionParentTopicName = TopicName.get(key);
            var listenerList = listeners.get(partitionParentTopicName);
            if (listenerList == null || listenerList.isEmpty()) {
                return;
            }
            synchronized (listenerList) {
                listenerList.forEach(listener -> {
                    try {
                        listener.onUpdate(event.getTopicPoliciesEvent().getPolicies());
                    } catch (Throwable ex) {
                        // avoid listener affect all of listeners
                        log.warn("Error occur while trying callback listener. event_key: {},"
                                + " event: {} listener :{}", key, event, listener.toString());
                    }
                });
            }
        } catch (Throwable ex) {
            // avoid listener affect broker
            log.warn("Error occur while trying callback listener. event_key: {}, event: {}",
                    key, event);
        }
    }

    private static @Nonnull String getEventTopic(@Nonnull NamespaceName namespace) {
        return "persistent://" + namespace + "/" + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
    }
}
