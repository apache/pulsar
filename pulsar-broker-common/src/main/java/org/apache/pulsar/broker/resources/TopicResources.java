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
package org.apache.pulsar.broker.resources;

import static org.apache.pulsar.common.util.Codec.decode;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStore;

public class TopicResources {
    private static final String MANAGED_LEDGER_PATH = "/managed-ledgers";

    private final MetadataStore store;

    public TopicResources(MetadataStore store) {
        this.store = store;
    }

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

    public CompletableFuture<Void> deletePersistentTopicAsync(TopicName topic) {
        String path = MANAGED_LEDGER_PATH + "/" + topic.getPersistenceNamingEncoding();
        return store.delete(path, Optional.of(-1L));
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
        return store.exists(path)
                .thenCompose(exists -> {
                    if (exists) {
                        return store.delete(path, Optional.empty());
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<Void> clearDomainPersistence(NamespaceName ns) {
        String path = MANAGED_LEDGER_PATH + "/" + ns + "/persistent";
        return store.exists(path)
                .thenCompose(exists -> {
                    if (exists) {
                        return store.delete(path, Optional.empty());
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<Void> clearTenantPersistence(String tenant) {
        String path = MANAGED_LEDGER_PATH + "/" + tenant;
        return store.exists(path)
                .thenCompose(exists -> {
                    if (exists) {
                        return store.delete(path, Optional.empty());
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }
}
