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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;

/**
 * Routes topic policy operations to the legacy system-topic backend when a namespace already has
 * a topic-policy {@code __change_events} system topic, and otherwise to the configured backend.
 */
@CustomLog
public class LegacyAwareTopicPoliciesService implements TopicPoliciesService {

    private final PulsarService pulsar;
    private final TopicPoliciesService systemTopicService;
    private final TopicPoliciesService configuredService;
    private final Function<NamespaceName, CompletableFuture<Boolean>> legacyNamespaceChecker;
    private final Consumer<NamespaceBundle> systemTopicBundleLoad;
    private final Consumer<NamespaceBundle> systemTopicBundleUnload;
    private final Map<NamespaceName, Set<NamespaceBundle>> ownedBundles = new ConcurrentHashMap<>();
    private final Map<NamespaceName, Set<NamespaceBundle>> legacyOwnedBundles = new ConcurrentHashMap<>();

    public LegacyAwareTopicPoliciesService(PulsarService pulsar,
                                           SystemTopicBasedTopicPoliciesService systemTopicService,
                                           TopicPoliciesService configuredService) {
        this(pulsar, systemTopicService, configuredService,
                namespace -> NamespaceEventsSystemTopicFactory.checkSystemTopicExists(namespace,
                        EventType.TOPIC_POLICY, pulsar),
                systemTopicService::addOwnedNamespaceBundleAsync,
                systemTopicService::removeOwnedNamespaceBundleAsync);
    }

    LegacyAwareTopicPoliciesService(PulsarService pulsar,
                                    TopicPoliciesService systemTopicService,
                                    TopicPoliciesService configuredService,
                                    Function<NamespaceName, CompletableFuture<Boolean>> legacyNamespaceChecker,
                                    Consumer<NamespaceBundle> systemTopicBundleLoad,
                                    Consumer<NamespaceBundle> systemTopicBundleUnload) {
        this.pulsar = pulsar;
        this.systemTopicService = systemTopicService;
        this.configuredService = configuredService;
        this.legacyNamespaceChecker = legacyNamespaceChecker;
        this.systemTopicBundleLoad = systemTopicBundleLoad;
        this.systemTopicBundleUnload = systemTopicBundleUnload;
    }

    @Override
    public void start(PulsarService pulsarService) {
        configuredService.start(pulsarService);
        pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(
                new NamespaceBundleOwnershipListener() {
                    @Override
                    public void onLoad(NamespaceBundle bundle) {
                        pulsarService.getOrderedExecutor().executeOrdered(bundle.getNamespaceObject(),
                                () -> onBundleLoaded(bundle));
                    }

                    @Override
                    public void unLoad(NamespaceBundle bundle) {
                        pulsarService.getOrderedExecutor().executeOrdered(bundle.getNamespaceObject(),
                                () -> onBundleUnloaded(bundle));
                    }

                    @Override
                    public boolean test(NamespaceBundle namespaceBundle) {
                        return true;
                    }
                });
    }

    @Override
    public void close() throws Exception {
        try {
            configuredService.close();
        } finally {
            systemTopicService.close();
        }
    }

    @Override
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type) {
        return resolveService(topicName.getNamespaceObject())
                .thenCompose(service -> service.getTopicPoliciesAsync(topicName, type));
    }

    @Override
    public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, boolean isGlobalPolicy,
                                                            boolean skipUpdateWhenTopicPolicyDoesntExist,
                                                            Consumer<TopicPolicies> policyUpdater) {
        return resolveService(topicName.getNamespaceObject())
                .thenCompose(service -> service.updateTopicPoliciesAsync(topicName, isGlobalPolicy,
                        skipUpdateWhenTopicPolicyDoesntExist, policyUpdater));
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
        return resolveService(topicName.getNamespaceObject())
                .thenCompose(service -> service.deleteTopicPoliciesAsync(topicName));
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName,
                                                            boolean keepGlobalPoliciesAfterDeleting) {
        return resolveService(topicName.getNamespaceObject())
                .thenCompose(service -> service.deleteTopicPoliciesAsync(topicName,
                        keepGlobalPoliciesAfterDeleting));
    }

    @Override
    public boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
        boolean configuredRegistered = configuredService.registerListener(topicName, listener);
        boolean systemTopicRegistered = systemTopicService.registerListener(topicName, listener);
        return configuredRegistered || systemTopicRegistered;
    }

    @Override
    public void unregisterListener(TopicName topicName, TopicPolicyListener listener) {
        configuredService.unregisterListener(topicName, listener);
        systemTopicService.unregisterListener(topicName, listener);
    }

    CompletableFuture<TopicPoliciesService> resolveService(NamespaceName namespace) {
        return legacyNamespaceChecker.apply(namespace)
                .thenCompose(isLegacy -> reconcileLegacyOwnership(namespace, Boolean.TRUE.equals(isLegacy))
                        .thenApply(__ -> Boolean.TRUE.equals(isLegacy) ? systemTopicService : configuredService));
    }

    void onBundleLoaded(NamespaceBundle bundle) {
        NamespaceName namespace = bundle.getNamespaceObject();
        addBundle(ownedBundles, bundle);
        legacyNamespaceChecker.apply(namespace).whenComplete((isLegacy, error) -> {
            if (error != null) {
                log.warn()
                        .attr("namespace", namespace)
                        .exception(error)
                        .log("Failed to check topic-policy system topic for namespace");
                return;
            }
            reconcileLegacyOwnership(namespace, Boolean.TRUE.equals(isLegacy)).exceptionally(reconcileError -> {
                log.warn()
                        .attr("namespace", namespace)
                        .exception(reconcileError)
                        .log("Failed to reconcile legacy topic-policy ownership for namespace");
                return null;
            });
        });
    }

    void onBundleUnloaded(NamespaceBundle bundle) {
        removeBundle(ownedBundles, bundle);
        if (removeBundle(legacyOwnedBundles, bundle)) {
            systemTopicBundleUnload.accept(bundle);
        }
    }

    private CompletableFuture<Void> reconcileLegacyOwnership(NamespaceName namespace, boolean isLegacy) {
        return executeOrdered(namespace, () -> applyLegacyOwnership(namespace, isLegacy));
    }

    private CompletableFuture<Void> executeOrdered(NamespaceName namespace, Runnable action) {
        if (pulsar == null) {
            action.run();
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            pulsar.getOrderedExecutor().executeOrdered(namespace, () -> {
                try {
                    action.run();
                    future.complete(null);
                } catch (Throwable error) {
                    future.completeExceptionally(error);
                }
            });
        } catch (Throwable error) {
            future.completeExceptionally(error);
        }
        return future;
    }

    private void applyLegacyOwnership(NamespaceName namespace, boolean isLegacy) {
        if (isLegacy) {
            Set<NamespaceBundle> namespaceOwnedBundles = ownedBundles.get(namespace);
            if (namespaceOwnedBundles == null || namespaceOwnedBundles.isEmpty()) {
                return;
            }
            Set<NamespaceBundle> namespaceLegacyBundles =
                    legacyOwnedBundles.computeIfAbsent(namespace, __ -> ConcurrentHashMap.newKeySet());
            for (NamespaceBundle bundle : namespaceOwnedBundles) {
                if (namespaceLegacyBundles.add(bundle)) {
                    systemTopicBundleLoad.accept(bundle);
                }
            }
            return;
        }
        Set<NamespaceBundle> namespaceLegacyBundles = legacyOwnedBundles.remove(namespace);
        if (namespaceLegacyBundles == null) {
            return;
        }
        for (NamespaceBundle bundle : namespaceLegacyBundles) {
            systemTopicBundleUnload.accept(bundle);
        }
    }

    private static void addBundle(Map<NamespaceName, Set<NamespaceBundle>> bundlesByNamespace, NamespaceBundle bundle) {
        bundlesByNamespace.computeIfAbsent(bundle.getNamespaceObject(), __ -> ConcurrentHashMap.newKeySet()).add(bundle);
    }

    private static boolean removeBundle(Map<NamespaceName, Set<NamespaceBundle>> bundlesByNamespace,
                                        NamespaceBundle bundle) {
        NamespaceName namespace = bundle.getNamespaceObject();
        Set<NamespaceBundle> namespaceBundles = bundlesByNamespace.get(namespace);
        if (namespaceBundles == null) {
            return false;
        }
        boolean removed = namespaceBundles.remove(bundle);
        if (namespaceBundles.isEmpty()) {
            bundlesByNamespace.remove(namespace, namespaceBundles);
        }
        return removed;
    }
}
