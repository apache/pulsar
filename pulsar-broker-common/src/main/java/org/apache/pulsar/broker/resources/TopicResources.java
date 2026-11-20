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
package org.apache.pulsar.broker.resources;

import static org.apache.pulsar.common.util.Codec.decode;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

@CustomLog
public class TopicResources {
    private static final String MANAGED_LEDGER_PATH = "/managed-ledgers";
    private static final String MANAGED_LEDGER_TOPIC_PREFIX = MANAGED_LEDGER_PATH + "/";
    private static final String PERSISTENT_TOPIC_PATH = "/" + TopicDomain.persistent + "/";

    private final MetadataStoreExtended store;

    // Keep the global registry for listener equality and session events, and index topic notifications
    // by namespace. Mutations to both maps are serialized; callbacks do not hold this lock.
    private final Map<TopicListener, ListenerRegistration> topicListeners;
    private final Map<String, Set<TopicListener>> topicListenersByNamespace;
    private final Object listenerRegistrationLock = new Object();
    private final Runnable cancelListener;
    private final Runnable cancelSessionListener;

    private record ListenerRegistration(TopicListener listener, String namespace) {
    }

    public TopicResources(MetadataStoreExtended store) {
        this.store = store;
        topicListeners = new ConcurrentHashMap<>();
        topicListenersByNamespace = new ConcurrentHashMap<>();
        cancelListener = store.registerCancellableListener(this::handleNotification);
        cancelSessionListener = store.registerCancellableSessionListener(this::handleSessionEvent);
    }

    /**
     * Unregister the listeners of the metadata store, so that the topic listeners are no longer notified.
     */
    public void close() {
        cancelListener.run();
        cancelSessionListener.run();
    }

    /***
     * List persistent topics names under a namespace, the topic name contains the partition suffix.
     */
    public CompletableFuture<List<String>> listPersistentTopicsAsync(NamespaceName ns) {
        String path = MANAGED_LEDGER_PATH + "/" + ns + "/persistent";

        return store.getChildren(path).thenApply(children ->
                children.stream().map(c -> TopicName.get(TopicDomain.persistent.toString(), ns, decode(c)).toString())
                        .collect(Collectors.toList())
        );
    }

    public CompletableFuture<List<String>> getExistingPartitions(TopicName topic) {
        return getExistingPartitions(topic.getNamespaceObject(), topic.getDomain());
    }

    public CompletableFuture<List<String>> getExistingPartitions(NamespaceName ns, TopicDomain domain) {
        String topicPartitionPath = MANAGED_LEDGER_PATH + "/" + ns + "/" + domain;
        return store.getChildren(topicPartitionPath).thenApply(topics ->
                topics.stream()
                        .map(s -> String.format("%s://%s/%s", domain.value(), ns, decode(s)))
                        .collect(Collectors.toList())
        );
    }

    public CompletableFuture<Void> createPersistentTopicAsync(TopicName topic) {
        String path = MANAGED_LEDGER_PATH + "/" + topic.getPersistenceNamingEncoding();
        return store.put(path, new byte[0], Optional.of(-1L))
                .thenApply(__ -> null);
    }

    public CompletableFuture<Boolean> persistentTopicExists(TopicName topic) {
        String path = MANAGED_LEDGER_PATH + "/" + topic.getPersistenceNamingEncoding();
        return store.exists(path);
    }

    public CompletableFuture<Void> clearNamespacePersistence(NamespaceName ns) {
        String path = MANAGED_LEDGER_PATH + "/" + ns;
        log.info().attr("namespace", ns).attr("path", path).log("Clearing namespace persistence for namespace: , path");
        return store.deleteIfExists(path, Optional.empty());
    }

    public CompletableFuture<Void> clearDomainPersistence(NamespaceName ns) {
        String path = MANAGED_LEDGER_PATH + "/" + ns + "/persistent";
        log.info().attr("namespace", ns).attr("path", path).log("Clearing domain persistence for namespace: , path");
        return store.deleteIfExists(path, Optional.empty());
    }

    /**
     * List the segment topics of the scalable topics in a namespace that have a managed ledger, stored at
     * {@code /managed-ledgers/<namespace>/segment/<topic>/<descriptor>}.
     */
    public CompletableFuture<List<String>> listSegmentTopicsAsync(NamespaceName ns) {
        String path = MANAGED_LEDGER_PATH + "/" + ns + "/" + TopicDomain.segment;
        return store.getChildren(path).thenCompose(encodedTopics -> {
            List<CompletableFuture<List<String>>> futures = encodedTopics.stream()
                    .map(encodedTopic -> store.getChildren(path + "/" + encodedTopic)
                            .thenApply(descriptors -> descriptors.stream()
                                    .map(descriptor -> TopicDomain.segment + "://" + ns + "/"
                                            + decode(encodedTopic) + "/" + descriptor)
                                    .toList()))
                    .toList();
            return FutureUtil.waitForAll(futures).thenApply(__ -> futures.stream()
                    .flatMap(future -> future.join().stream())
                    .toList());
        });
    }

    /**
     * Delete the segment domain of a namespace: the node of each scalable topic whose segment topics had a
     * managed ledger, then the domain node. Not recursive, so a segment topic that still has a managed ledger
     * fails it.
     */
    public CompletableFuture<Void> clearSegmentDomainPersistence(NamespaceName ns) {
        String path = MANAGED_LEDGER_PATH + "/" + ns + "/" + TopicDomain.segment;
        log.debug().attr("namespace", ns).attr("path", path).log("Clearing segment domain persistence");
        return store.getChildren(path)
                .thenCompose(encodedTopics -> FutureUtil.waitForAll(encodedTopics.stream()
                        .map(encodedTopic -> store.deleteIfExists(path + "/" + encodedTopic, Optional.empty()))
                        .toList()))
                .thenCompose(__ -> store.deleteIfExists(path, Optional.empty()));
    }

    public CompletableFuture<Void> clearTenantPersistence(String tenant) {
        String path = MANAGED_LEDGER_PATH + "/" + tenant;
        log.info().attr("tenant", tenant).attr("path", path).log("Clearing tenant persistence for tenant: , path");
        return store.deleteRecursive(path);
    }

    void handleNotification(Notification notification) {
        if (topicListeners.isEmpty()
                || (notification.getType() != NotificationType.Created
                && notification.getType() != NotificationType.Deleted)) {
            return;
        }

        String path = notification.getPath();
        if (!path.startsWith(MANAGED_LEDGER_TOPIC_PREFIX)) {
            return;
        }

        int domainStart = path.lastIndexOf(PERSISTENT_TOPIC_PATH);
        if (domainStart <= MANAGED_LEDGER_TOPIC_PREFIX.length()) {
            return;
        }

        int topicStart = domainStart + PERSISTENT_TOPIC_PATH.length();
        if (topicStart == path.length() || path.indexOf('/', topicStart) != -1) {
            return;
        }

        String namespace = path.substring(MANAGED_LEDGER_TOPIC_PREFIX.length(), domainStart);
        Set<TopicListener> listeners = topicListenersByNamespace.get(namespace);
        if (listeners == null || listeners.isEmpty()) {
            return;
        }

        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get(namespace), decode(path.substring(topicStart)));
        if (!SystemTopicNames.isSystemTopic(topicName)) {
            String topic = topicName.toString();
            listeners.forEach(listener -> {
                ListenerRegistration registration = topicListeners.get(listener);
                if (registration != null && namespace.equals(registration.namespace())) {
                    listener.onTopicEvent(topic, notification.getType());
                }
            });
        }
    }

    public void registerPersistentTopicListener(TopicListener listener) {
        String namespace = listener.getNamespaceName().toString();
        synchronized (listenerRegistrationLock) {
            ListenerRegistration previous = topicListeners.get(listener);
            TopicListener registeredListener = previous == null ? listener : previous.listener();
            // Publish the new index entry before switching the authoritative registration.
            // Dispatch validates the registration, so an overlapping entry cannot deliver
            // notifications for a namespace that is no longer registered.
            topicListenersByNamespace.computeIfAbsent(namespace, __ -> ConcurrentHashMap.newKeySet())
                    .add(registeredListener);
            topicListeners.put(registeredListener, new ListenerRegistration(registeredListener, namespace));
            if (previous != null && !previous.namespace().equals(namespace)) {
                removeFromNamespace(previous);
            }
        }
    }

    public void deregisterPersistentTopicListener(TopicListener listener) {
        synchronized (listenerRegistrationLock) {
            ListenerRegistration registration = topicListeners.remove(listener);
            if (registration != null) {
                removeFromNamespace(registration);
            }
        }
    }

    private void removeFromNamespace(ListenerRegistration registration) {
        Set<TopicListener> listeners = topicListenersByNamespace.get(registration.namespace());
        listeners.remove(registration.listener());
        if (listeners.isEmpty()) {
            topicListenersByNamespace.remove(registration.namespace());
        }
    }

    @VisibleForTesting
    int namespaceListenerBucketCount() {
        return topicListenersByNamespace.size();
    }

    private void handleSessionEvent(SessionEvent sessionEvent) {
        topicListeners.keySet().forEach(listener -> {
            try {
                listener.onSessionEvent(sessionEvent);
            } catch (Exception e) {
                log.warn()
                        .attr("event", sessionEvent)
                        .attr("listener", listener)
                        .exception(e)
                        .log("Failed to handle session event for listener");
            }
        });
    }

    @Deprecated
    public void registerPersistentTopicListener(
            NamespaceName namespaceName, BiConsumer<String, NotificationType> listener) {
        registerPersistentTopicListener(new BiConsumerTopicListener(listener, namespaceName));
    }

    @Deprecated
    public void deregisterPersistentTopicListener(BiConsumer<String, NotificationType> listener) {
        deregisterPersistentTopicListener(new BiConsumerTopicListener(listener, null));
    }

    // for backwards compatibility with broker plugins that could be using the BiConsumer based methods
    @Deprecated
    static class BiConsumerTopicListener implements TopicListener {
        private final BiConsumer<String, NotificationType> listener;
        private final NamespaceName namespaceName;

        BiConsumerTopicListener(BiConsumer<String, NotificationType> listener, NamespaceName namespaceName) {
            this.listener = listener;
            this.namespaceName = namespaceName;
        }

        @Override
        public NamespaceName getNamespaceName() {
            return namespaceName;
        }

        @Override
        public void onTopicEvent(String topicName, NotificationType notificationType) {
            listener.accept(topicName, notificationType);
        }

        @Override
        public void onSessionEvent(SessionEvent sessionEvent) {
            // ignore
        }

        @Override
        public String toString() {
            return "BiConsumerTopicListener [listener=" + listener + ", namespaceName=" + namespaceName + "]";
        }

        @Override
        public int hashCode() {
            return listener.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof BiConsumerTopicListener && listener.equals(((BiConsumerTopicListener) obj).listener);
        }
    }
}
