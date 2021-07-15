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

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.policies.path.PolicyPath;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

@Getter
public class NamespaceResources extends BaseResources<Policies> {
    private final IsolationPolicyResources isolationPolicies;
    private final PartitionedTopicResources partitionedTopicResources;
    private final MetadataStore configurationStore;

    private final MetadataCache<LocalPolicies> localPoliciesCache;

    private static final String LOCAL_POLICIES_ROOT = "/admin/local-policies";

    public NamespaceResources(MetadataStore localStore, MetadataStore configurationStore, int operationTimeoutSec) {
        super(configurationStore, Policies.class, operationTimeoutSec);
        this.configurationStore = configurationStore;
        isolationPolicies = new IsolationPolicyResources(configurationStore, operationTimeoutSec);
        partitionedTopicResources = new PartitionedTopicResources(configurationStore, operationTimeoutSec);

        if (localStore != null) {
            localPoliciesCache = localStore.getMetadataCache(LocalPolicies.class);
        } else {
            localPoliciesCache = null;
        }
    }

    public CompletableFuture<Optional<Policies>> getPolicies(NamespaceName ns) {
        return getCache().get(PolicyPath.path(POLICIES, ns.toString()));
    }

    public CompletableFuture<Optional<LocalPolicies>> getLocalPolicies(NamespaceName ns) {
        if (localPoliciesCache == null) {
            return FutureUtil.failedFuture(new IllegalStateException("Local metadata store not setup"));
        } else {
            return localPoliciesCache.get(PolicyPath.joinPath(LOCAL_POLICIES_ROOT, ns.toString()));
        }
    }

    public static class IsolationPolicyResources extends BaseResources<Map<String, NamespaceIsolationDataImpl>> {
        public IsolationPolicyResources(MetadataStore store, int operationTimeoutSec) {
            super(store, new TypeReference<Map<String, NamespaceIsolationDataImpl>>() {
            }, operationTimeoutSec);
        }

        public Optional<NamespaceIsolationPolicies> getPolicies(String path) throws MetadataStoreException {
            Optional<Map<String, NamespaceIsolationDataImpl>> data = super.get(path);
            return data.isPresent() ? Optional.of(new NamespaceIsolationPolicies(data.get())) : Optional.empty();
        }
    }

    public static class PartitionedTopicResources extends BaseResources<PartitionedTopicMetadata> {
        public PartitionedTopicResources(MetadataStore configurationStore, int operationTimeoutSec) {
            super(configurationStore, PartitionedTopicMetadata.class, operationTimeoutSec);
        }
    }
}