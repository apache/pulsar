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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

@Slf4j
public class TopicResources {
    private static final String MANAGED_LEDGER_PATH = "/managed-ledgers";

    private final MetadataStoreExtended store;

    private final Map<TopicListener, Pattern> topicListeners;

    public TopicResources(MetadataStoreExtended store) {
        this.store = store;
        topicListeners = new ConcurrentHashMap<>();
        store.registerListener(this::handleNotification);
        store.registerSessionListener(this::handleSessionEvent);
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
        log.info("Clearing namespace persistence for namespace: {}, path {}", ns, path);
        return store.deleteIfExists(path, Optional.empty());
    }

    public CompletableFuture<Void> clearDomainPersistence(NamespaceName ns) {
        String path = MANAGED_LEDGER_PATH + "/" + ns + "/persistent";
        log.info("Clearing domain persistence for namespace: {}, path {}", ns, path);
        return store.deleteIfExists(path, Optional.empty());
    }

    public CompletableFuture<Void> clearTenantPersistence(String tenant) {
        String path = MANAGED_LEDGER_PATH + "/" + tenant;
        log.info("Clearing tenant persistence for tenant: {}, path {}", tenant, path);
        return store.deleteRecursive(path);
    }

    void handleNotification(Notification notification) {
        if (topicListeners.isEmpty()) {
            return;
        }
        if (notification.getPath().startsWith(MANAGED_LEDGER_PATH)
                && (notification.getType() == NotificationType.Created
                || notification.getType() == NotificationType.Deleted)) {
            for (Map.Entry<TopicListener, Pattern> entry :
                    topicListeners.entrySet()) {
                Matcher matcher = entry.getValue().matcher(notification.getPath());
                if (matcher.matches()) {
                    TopicName topicName = TopicName.get(
                            matcher.group(2), NamespaceName.get(matcher.group(1)), decode(matcher.group(3)));
                    if (!SystemTopicNames.isSystemTopic(topicName)) {
                        entry.getKey().onTopicEvent(topicName.toString(), notification.getType());
                    }
                }
            }
        }
    }

    Pattern namespaceNameToTopicNamePattern(NamespaceName namespaceName) {
        return Pattern.compile(MANAGED_LEDGER_PATH + "/(" + Pattern.quote(namespaceName.toString()) + ")/("
                        + TopicDomain.persistent + ")/(" + "[^/]+)");
    }

    public void registerPersistentTopicListener(TopicListener listener) {
        topicListeners.put(listener, namespaceNameToTopicNamePattern(listener.getNamespaceName()));
    }

    public void deregisterPersistentTopicListener(TopicListener listener) {
        topicListeners.remove(listener);
    }

    private void handleSessionEvent(SessionEvent sessionEvent) {
        topicListeners.keySet().forEach(listener -> {
            try {
                listener.onSessionEvent(sessionEvent);
            } catch (Exception e) {
                log.warn("Failed to handle session event {} for listener {}", sessionEvent, listener, e);
            }
        });
    }
}
