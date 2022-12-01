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

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Getter
@Slf4j
public class NamespaceResources extends BaseResources<Policies> {
    private final IsolationPolicyResources isolationPolicies;
    private final PartitionedTopicResources partitionedTopicResources;
    private final MetadataStoreExtended configurationStore;
    private static final String NAMESPACE_BASE_PATH = "/namespace";

    public NamespaceResources(MetadataStoreExtended configurationStore, int operationTimeoutSec) {
        super(configurationStore, Policies.class, operationTimeoutSec);
        this.configurationStore = configurationStore;
        isolationPolicies = new IsolationPolicyResources(configurationStore, operationTimeoutSec);
        partitionedTopicResources = new PartitionedTopicResources(configurationStore, operationTimeoutSec);
    }

    public CompletableFuture<Void> deletePoliciesAsync(NamespaceName ns){
        return deleteIfExistsAsync(joinPath(BASE_POLICIES_PATH, ns.toString()));
    }
    public CompletableFuture<Optional<Policies>> getPoliciesAsync(NamespaceName ns) {
        return getCache().get(joinPath(BASE_POLICIES_PATH, ns.toString()));
    }

    // clear resource of `/namespace/{namespaceName}` for zk-node
    public CompletableFuture<Void> deleteNamespaceAsync(NamespaceName ns) {
        final String namespacePath = joinPath(NAMESPACE_BASE_PATH, ns.toString());
        return deleteIfExistsAsync(namespacePath);
    }

    // clear resource of `/namespace/{tenant}` for zk-node
    public CompletableFuture<Void> deleteTenantAsync(String tenant) {
        final String tenantPath = joinPath(NAMESPACE_BASE_PATH, tenant);
        return deleteIfExistsAsync(tenantPath);
    }

    public static class IsolationPolicyResources extends BaseResources<Map<String, NamespaceIsolationDataImpl>> {
        public IsolationPolicyResources(MetadataStoreExtended store, int operationTimeoutSec) {
            super(store, new TypeReference<Map<String, NamespaceIsolationDataImpl>>() {
            }, operationTimeoutSec);
        }

        public Optional<NamespaceIsolationPolicies> getPolicies(String path) throws MetadataStoreException {
            Optional<Map<String, NamespaceIsolationDataImpl>> data = super.get(path);
            return data.isPresent() ? Optional.of(new NamespaceIsolationPolicies(data.get())) : Optional.empty();
        }
    }

    public static class PartitionedTopicResources extends BaseResources<PartitionedTopicMetadata> {
        private static final String PARTITIONED_TOPIC_PATH = "/admin/partitioned-topics";

        public PartitionedTopicResources(MetadataStoreExtended configurationStore, int operationTimeoutSec) {
            super(configurationStore, PartitionedTopicMetadata.class, operationTimeoutSec);
        }

        public CompletableFuture<Void> createPartitionedTopicAsync(TopicName tn, PartitionedTopicMetadata tm) {
            return createAsync(joinPath(PARTITIONED_TOPIC_PATH, tn.getNamespace(), tn.getDomain().value(),
                    tn.getEncodedLocalName()), tm);
        }

        public CompletableFuture<Void> clearPartitionedTopicMetadataAsync(NamespaceName namespaceName) {
            final String globalPartitionedPath = joinPath(PARTITIONED_TOPIC_PATH, namespaceName.toString());

            CompletableFuture<Void> completableFuture = new CompletableFuture<>();

            deleteRecursiveAsync(globalPartitionedPath)
                    .thenAccept(ignore -> {
                        log.info("Clear partitioned topic metadata [{}] success.", namespaceName);
                        completableFuture.complete(null);
                    }).exceptionally(ex -> {
                        log.error("Clear partitioned topic metadata failed.");
                        completableFuture.completeExceptionally(ex.getCause());
                        return null;
                    });

            return completableFuture;
        }
    }
}
