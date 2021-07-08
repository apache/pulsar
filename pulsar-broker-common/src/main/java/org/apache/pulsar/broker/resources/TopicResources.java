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
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStore;

public class TopicResources {
    private static final String MANAGED_LEDGER_PATH = "/managed-ledgers";

    private final MetadataStore store;

    TopicResources(MetadataStore store) {
        this.store = store;
    }

    public CompletableFuture<List<String>> getExistingPartitions(TopicName topic) {
        String topicPartitionPath = MANAGED_LEDGER_PATH + "/" + topic.getNamespace() + "/"
                + topic.getDomain();
        return store.getChildren(topicPartitionPath).thenApply(topics ->
                topics.stream()
                        .map(s -> String.format("%s://%s/%s",
                                topic.getDomain().value(), topic.getNamespace(), decode(s)))
                        .collect(Collectors.toList())
        );
    }

    public CompletableFuture<Boolean> persistentTopicExists(TopicName topic) {
        String path = MANAGED_LEDGER_PATH + "/" + topic.getPersistenceNamingEncoding();;
        return store.exists(path);
    }

    public CompletableFuture<Void> clearNamespacePersistence(NamespaceName ns) {
        String path = MANAGED_LEDGER_PATH + "/" + ns;
        return store.delete(path, Optional.empty());
    }

    public CompletableFuture<Void> clearDomainPersistence(NamespaceName ns) {
        String path = MANAGED_LEDGER_PATH + "/" + ns + "/persistent";
        return store.delete(path, Optional.empty());
    }

    public CompletableFuture<Void> clearTennantPersistence(String tenant) {
        String path = MANAGED_LEDGER_PATH + "/" + tenant;
        return store.delete(path, Optional.empty());
    }
}
