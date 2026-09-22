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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.jspecify.annotations.Nullable;

/**
 * Topic policies service backed by Pulsar metadata stores.
 */
@CustomLog
public class MetadataStoreTopicPoliciesService implements TopicPoliciesService {

    public static final String GLOBAL_POLICIES_ROOT = "/admin/topic-policies/global";
    public static final String LOCAL_POLICIES_ROOT = "/admin/topic-policies/local";

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<TopicName, List<TopicPolicyListener>> listeners = new ConcurrentHashMap<>();
    private MetadataCache<TopicPolicies> localPoliciesCache;
    private MetadataCache<TopicPolicies> globalPoliciesCache;

    @Override
    public void start(PulsarService pulsar) {
        MetadataStore localStore = pulsar.getLocalMetadataStore();
        MetadataStore configurationStore = pulsar.getConfigurationMetadataStore();
        this.localPoliciesCache = localStore.getMetadataCache(TopicPolicies.class);
        this.globalPoliciesCache = configurationStore.getMetadataCache(TopicPolicies.class);
        localStore.registerListener(notification -> handleNotification(notification, false));
        configurationStore.registerListener(notification -> handleNotification(notification, true));
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
        return deleteTopicPoliciesAsync(topicName, false);
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName,
                                                            boolean keepGlobalPoliciesAfterDeleting) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        if (NamespaceService.isHeartbeatNamespace(partitionedTopicName.getNamespaceObject())) {
            return CompletableFuture.completedFuture(null);
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new BrokerServiceException(getClass().getName() + " is closed."));
        }
        CompletableFuture<Void> deleteLocal =
                deleteIfExists(localPoliciesCache, pathFor(partitionedTopicName, false));
        if (keepGlobalPoliciesAfterDeleting) {
            return deleteLocal;
        }
        CompletableFuture<Void> deleteGlobal =
                deleteIfExists(globalPoliciesCache, pathFor(partitionedTopicName, true));
        return CompletableFuture.allOf(deleteLocal, deleteGlobal);
    }

    @Override
    public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, boolean isGlobalPolicy,
                                                            boolean skipUpdateWhenTopicPolicyDoesntExist,
                                                            Consumer<TopicPolicies> policyUpdater) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        if (NamespaceService.isHeartbeatNamespace(partitionedTopicName.getNamespaceObject())) {
            return CompletableFuture.failedFuture(new BrokerServiceException.NotAllowedException(
                    "Not allowed to update topic policy for the heartbeat topic"));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new BrokerServiceException(getClass().getName() + " is closed."));
        }
        MetadataCache<TopicPolicies> cache = cache(isGlobalPolicy);
        String path = pathFor(partitionedTopicName, isGlobalPolicy);
        CompletableFuture<TopicPolicies> updateFuture;
        if (skipUpdateWhenTopicPolicyDoesntExist) {
            updateFuture = cache.readModifyUpdate(path,
                    current -> updatePolicies(Optional.of(current), isGlobalPolicy, policyUpdater));
        } else {
            updateFuture = cache.readModifyUpdateOrCreate(path,
                    current -> updatePolicies(current, isGlobalPolicy, policyUpdater));
        }
        return updateFuture.thenAccept(__ -> { }).exceptionally(error -> {
            if (skipUpdateWhenTopicPolicyDoesntExist
                    && FutureUtil.unwrapCompletionException(error) instanceof NotFoundException) {
                return null;
            }
            throw FutureUtil.wrapToCompletionException(error);
        });
    }

    @Override
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        if (NamespaceService.isHeartbeatNamespace(partitionedTopicName.getNamespaceObject())) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (closed.get()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        boolean global = type == GetType.GLOBAL_ONLY;
        return cache(global).get(pathFor(partitionedTopicName, global))
                .thenApply(policies -> policies.map(policy -> cloneWithScope(policy, global)));
    }

    @Override
    public boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
        listeners.compute(normalizeTopicName(topicName), (__, topicListeners) -> {
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
        listeners.computeIfPresent(normalizeTopicName(topicName), (__, topicListeners) -> {
            topicListeners.remove(listener);
            return topicListeners.isEmpty() ? null : topicListeners;
        });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            listeners.clear();
            if (localPoliciesCache != null) {
                localPoliciesCache.invalidateAll();
            }
            if (globalPoliciesCache != null) {
                globalPoliciesCache.invalidateAll();
            }
        }
    }

    private MetadataCache<TopicPolicies> cache(boolean isGlobalPolicy) {
        return isGlobalPolicy ? globalPoliciesCache : localPoliciesCache;
    }

    private CompletableFuture<Void> deleteIfExists(MetadataCache<TopicPolicies> cache, String path) {
        return cache.delete(path).handle((__, error) -> {
            cache.invalidate(path);
            if (error == null || FutureUtil.unwrapCompletionException(error) instanceof NotFoundException) {
                return null;
            }
            throw FutureUtil.wrapToCompletionException(error);
        });
    }

    private static TopicPolicies updatePolicies(Optional<TopicPolicies> currentPolicies,
                                                boolean isGlobalPolicy,
                                                Consumer<TopicPolicies> policyUpdater) {
        TopicPolicies policies = currentPolicies.map(TopicPolicies::clone).orElseGet(TopicPolicies::new);
        policies.setIsGlobal(isGlobalPolicy);
        policyUpdater.accept(policies);
        return policies;
    }

    private void handleNotification(Notification notification, boolean isGlobalPolicy) {
        if (closed.get()
                || (notification.getType() != NotificationType.Created
                && notification.getType() != NotificationType.Modified
                && notification.getType() != NotificationType.Deleted)) {
            return;
        }
        String path = notification.getPath();
        String root = isGlobalPolicy ? GLOBAL_POLICIES_ROOT : LOCAL_POLICIES_ROOT;
        Optional<TopicName> topicName = topicNameFromPath(root, path);
        if (topicName.isEmpty()) {
            return;
        }
        MetadataCache<TopicPolicies> cache = cache(isGlobalPolicy);
        cache.invalidate(path);
        if (notification.getType() == NotificationType.Deleted) {
            notifyListeners(topicName.get(), null);
            return;
        }
        cache.get(path).whenComplete((policies, error) -> {
            if (error != null) {
                log.warn()
                        .attr("path", path)
                        .exception(error)
                        .log("Failed to refresh topic policies after metadata notification");
                return;
            }
            notifyListeners(topicName.get(),
                    policies.map(policy -> cloneWithScope(policy, isGlobalPolicy)).orElse(null));
        });
    }

    private void notifyListeners(TopicName topicName, @Nullable TopicPolicies policies) {
        List<TopicPolicyListener> topicListeners = listeners.get(topicName);
        if (topicListeners == null) {
            return;
        }
        for (TopicPolicyListener listener : topicListeners) {
            try {
                listener.onUpdate(policies == null ? null : policies.clone());
            } catch (Throwable error) {
                log.error().attr("topic", topicName).exception(error).log("Call topic policy listener error");
            }
        }
    }

    private static TopicName normalizeTopicName(TopicName topicName) {
        return TopicName.get(topicName.getPartitionedTopicName());
    }

    private static TopicPolicies cloneWithScope(TopicPolicies policies, boolean isGlobalPolicy) {
        TopicPolicies cloned = policies.clone();
        cloned.setIsGlobal(isGlobalPolicy);
        return cloned;
    }

    @VisibleForTesting
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesDirectFromStore(TopicName topicName,
                                                                                      boolean isGlobal) {
        String path = pathFor(topicName, isGlobal);
        MetadataCache<TopicPolicies> c = cache(isGlobal);
        c.invalidate(path);
        return c.get(path).thenApply(opt -> opt.map(p -> cloneWithScope(p, isGlobal)));
    }

    @VisibleForTesting
    static String pathFor(TopicName topicName, boolean isGlobalPolicy) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        return (isGlobalPolicy ? GLOBAL_POLICIES_ROOT : LOCAL_POLICIES_ROOT)
                + "/" + partitionedTopicName.getTenant()
                + "/" + partitionedTopicName.getNamespacePortion()
                + "/" + partitionedTopicName.getDomain()
                + "/" + partitionedTopicName.getEncodedLocalName();
    }

    @VisibleForTesting
    private static Optional<TopicName> topicNameFromPath(String root, String path) {
        if (!path.startsWith(root + "/")) {
            return Optional.empty();
        }
        String[] parts = path.substring(root.length() + 1).split("/", 4);
        if (parts.length != 4) {
            return Optional.empty();
        }
        return Optional.of(TopicName.get(parts[2], parts[0], parts[1], Codec.decode(parts[3])));
    }
}
