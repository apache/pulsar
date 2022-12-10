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

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class NamespaceResources extends BaseResources<Policies> {
    private static final Logger log = LoggerFactory.getLogger(NamespaceResources.class);

    private final IsolationPolicyResources isolationPolicies;
    private final PartitionedTopicResources partitionedTopicResources;
    private final MetadataStore configurationStore;

    private static final String POLICIES_READONLY_FLAG_PATH = "/admin/flags/policies-readonly";
    private static final String NAMESPACE_BASE_PATH = "/namespace";
    private static final String BUNDLE_DATA_BASE_PATH = "/loadbalance/bundle-data";

    public NamespaceResources(MetadataStore configurationStore, int operationTimeoutSec) {
        super(configurationStore, Policies.class, operationTimeoutSec);
        this.configurationStore = configurationStore;
        isolationPolicies = new IsolationPolicyResources(configurationStore, operationTimeoutSec);
        partitionedTopicResources = new PartitionedTopicResources(configurationStore, operationTimeoutSec);
    }

    public CompletableFuture<List<String>> listNamespacesAsync(String tenant) {
        return getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenant));
    }

    public CompletableFuture<Boolean> getPoliciesReadOnlyAsync() {
        return super.existsAsync(POLICIES_READONLY_FLAG_PATH);
    }

    public boolean getPoliciesReadOnly() throws MetadataStoreException {
        try {
            return getPoliciesReadOnlyAsync().get(getOperationTimeoutSec(), TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to check exist " + POLICIES_READONLY_FLAG_PATH, e);
        }
    }

    public void createPolicies(NamespaceName ns, Policies policies) throws MetadataStoreException{
        create(joinPath(BASE_POLICIES_PATH, ns.toString()), policies);
    }

    public CompletableFuture<Void> createPoliciesAsync(NamespaceName ns, Policies policies) {
        return createAsync(joinPath(BASE_POLICIES_PATH, ns.toString()), policies);
    }

    public boolean namespaceExists(NamespaceName ns) throws MetadataStoreException {
        String path = joinPath(BASE_POLICIES_PATH, ns.toString());
        return super.exists(path) && super.getChildren(path).isEmpty();
    }

    public CompletableFuture<Boolean> namespaceExistsAsync(NamespaceName ns) {
        String path = joinPath(BASE_POLICIES_PATH, ns.toString());
        return getCache().exists(path)
                .thenCompose(exists -> {
                    if (!exists) {
                        return CompletableFuture.completedFuture(false);
                    } else {
                        return getChildrenAsync(path).thenApply(children -> children.isEmpty());
                    }
                });
    }

    public void deletePolicies(NamespaceName ns) throws MetadataStoreException{
        delete(joinPath(BASE_POLICIES_PATH, ns.toString()));
    }

    public CompletableFuture<Void> deletePoliciesAsync(NamespaceName ns){
        return deleteAsync(joinPath(BASE_POLICIES_PATH, ns.toString()));
    }

    public Optional<Policies> getPolicies(NamespaceName ns) throws MetadataStoreException{
        return get(joinPath(BASE_POLICIES_PATH, ns.toString()));
    }

    public Optional<Policies> getPoliciesIfCached(NamespaceName ns) {
        return getCache().getIfCached(joinPath(BASE_POLICIES_PATH, ns.toString()));
    }

    public CompletableFuture<Optional<Policies>> getPoliciesAsync(NamespaceName ns) {
        return getCache().get(joinPath(BASE_POLICIES_PATH, ns.toString()));
    }

    public void setPolicies(NamespaceName ns, Function<Policies, Policies> function) throws MetadataStoreException {
        set(joinPath(BASE_POLICIES_PATH, ns.toString()), function);
    }

    public CompletableFuture<Void> setPoliciesAsync(NamespaceName ns, Function<Policies, Policies> function) {
        return setAsync(joinPath(BASE_POLICIES_PATH, ns.toString()), function);
    }

    public static boolean pathIsFromNamespace(String path) {
        return path.startsWith(BASE_POLICIES_PATH + "/")
                && path.substring(BASE_POLICIES_PATH.length() + 1).contains("/");
    }

    public static boolean pathIsNamespaceLocalPolicies(String path) {
        return path.startsWith(LOCAL_POLICIES_ROOT + "/")
                && path.substring(LOCAL_POLICIES_ROOT.length() + 1).contains("/");
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

    public static NamespaceName namespaceFromPath(String path) {
        return NamespaceName.get(path.substring(BASE_POLICIES_PATH.length() + 1));
    }

    public static NamespaceName namespaceFromLocalPoliciesPath(String path) {
        return NamespaceName.get(path.substring(LOCAL_POLICIES_ROOT.length() + 1));
    }

    public static class IsolationPolicyResources extends BaseResources<Map<String, NamespaceIsolationDataImpl>> {
        private static final String NAMESPACE_ISOLATION_POLICIES = "namespaceIsolationPolicies";

        public IsolationPolicyResources(MetadataStore store, int operationTimeoutSec) {
            super(store, new TypeReference<Map<String, NamespaceIsolationDataImpl>>() {
            }, operationTimeoutSec);
        }

        public Optional<NamespaceIsolationPolicies> getIsolationDataPolicies(String cluster)
                throws MetadataStoreException {
            Optional<Map<String, NamespaceIsolationDataImpl>> data =
                    super.get(joinPath(BASE_CLUSTERS_PATH, cluster, NAMESPACE_ISOLATION_POLICIES));
            return data.isPresent() ? Optional.of(new NamespaceIsolationPolicies(data.get())) : Optional.empty();
        }

        public CompletableFuture<Optional<NamespaceIsolationPolicies>> getIsolationDataPoliciesAsync(String cluster) {
            return getAsync(joinPath(BASE_CLUSTERS_PATH, cluster, NAMESPACE_ISOLATION_POLICIES))
                    .thenApply(data -> data.map(NamespaceIsolationPolicies::new));
        }

        public void deleteIsolationData(String cluster) throws MetadataStoreException {
            delete(joinPath(BASE_CLUSTERS_PATH, cluster, NAMESPACE_ISOLATION_POLICIES));
        }

        public CompletableFuture<Void> deleteIsolationDataAsync(String cluster) {
            return deleteAsync(joinPath(BASE_CLUSTERS_PATH, cluster, NAMESPACE_ISOLATION_POLICIES));
        }

        public void createIsolationData(String cluster, Map<String, NamespaceIsolationDataImpl> id)
                throws MetadataStoreException {
            create(joinPath(BASE_CLUSTERS_PATH, cluster, NAMESPACE_ISOLATION_POLICIES), id);
        }

        public void setIsolationData(String cluster,
                                     Function<Map<String, NamespaceIsolationDataImpl>, Map<String,
                                             NamespaceIsolationDataImpl>> modifyFunction)
                throws MetadataStoreException {
            set(joinPath(BASE_CLUSTERS_PATH, cluster, NAMESPACE_ISOLATION_POLICIES), modifyFunction);
        }

        public CompletableFuture<Void> setIsolationDataAsync(String cluster,
                                                             Function<Map<String, NamespaceIsolationDataImpl>,
                                                             Map<String, NamespaceIsolationDataImpl>> modifyFunction) {
            return setAsync(joinPath(BASE_CLUSTERS_PATH, cluster, NAMESPACE_ISOLATION_POLICIES), modifyFunction);
        }

        public CompletableFuture<Void> setIsolationDataWithCreateAsync(String cluster,
                                                                       Function<Optional<Map<String,
                                                                       NamespaceIsolationDataImpl>>,
                                                                       Map<String, NamespaceIsolationDataImpl>>
                                                                               createFunction) {
            return setWithCreateAsync(joinPath(BASE_CLUSTERS_PATH, cluster, NAMESPACE_ISOLATION_POLICIES),
                    createFunction);
        }

        public void setIsolationDataWithCreate(String cluster,
                                     Function<Optional<Map<String, NamespaceIsolationDataImpl>>, Map<String,
                                             NamespaceIsolationDataImpl>> createFunction)
                throws MetadataStoreException {
            setWithCreate(joinPath(BASE_CLUSTERS_PATH, cluster, NAMESPACE_ISOLATION_POLICIES), createFunction);
        }
    }

    public static class PartitionedTopicResources extends BaseResources<PartitionedTopicMetadata> {
        private static final String PARTITIONED_TOPIC_PATH = "/admin/partitioned-topics";

        public PartitionedTopicResources(MetadataStore configurationStore, int operationTimeoutSec) {
            super(configurationStore, PartitionedTopicMetadata.class, operationTimeoutSec);
        }

        public CompletableFuture<Void> updatePartitionedTopicAsync(TopicName tn, Function<PartitionedTopicMetadata,
                PartitionedTopicMetadata> f) {
            return setAsync(joinPath(PARTITIONED_TOPIC_PATH, tn.getNamespace(), tn.getDomain().value(),
                    tn.getEncodedLocalName()), f);
        }

        public void createPartitionedTopic(TopicName tn, PartitionedTopicMetadata tm) throws MetadataStoreException {
            create(joinPath(PARTITIONED_TOPIC_PATH, tn.getNamespace(), tn.getDomain().value(),
                    tn.getEncodedLocalName()), tm);
        }

        public CompletableFuture<Void> createPartitionedTopicAsync(TopicName tn, PartitionedTopicMetadata tm) {
            return createAsync(joinPath(PARTITIONED_TOPIC_PATH, tn.getNamespace(), tn.getDomain().value(),
                    tn.getEncodedLocalName()), tm);
        }

        public CompletableFuture<List<String>> listPartitionedTopicsAsync(NamespaceName ns, TopicDomain domain) {
            return getChildrenAsync(joinPath(PARTITIONED_TOPIC_PATH, ns.toString(), domain.value()))
                    .thenApply(list ->
                            list.stream().map(x -> TopicName.get(domain.value(), ns, Codec.decode(x)).toString())
                                    .collect(Collectors.toList())
                    );
        }

        public CompletableFuture<Optional<PartitionedTopicMetadata>> getPartitionedTopicMetadataAsync(TopicName tn) {
            return getAsync(joinPath(PARTITIONED_TOPIC_PATH, tn.getNamespace(), tn.getDomain().value(),
                    tn.getEncodedLocalName()));
        }

        public boolean partitionedTopicExists(TopicName tn) throws MetadataStoreException {
            return exists(joinPath(PARTITIONED_TOPIC_PATH, tn.getNamespace(), tn.getDomain().value(),
                    tn.getEncodedLocalName()));
        }

        public CompletableFuture<Boolean> partitionedTopicExistsAsync(TopicName tn) {
            return existsAsync(joinPath(PARTITIONED_TOPIC_PATH, tn.getNamespace(), tn.getDomain().value(),
                    tn.getEncodedLocalName()));
        }

        public CompletableFuture<Void> deletePartitionedTopicAsync(TopicName tn) {
            return deleteAsync(joinPath(PARTITIONED_TOPIC_PATH, tn.getNamespace(), tn.getDomain().value(),
                    tn.getEncodedLocalName()));
        }

        public CompletableFuture<Void> clearPartitionedTopicMetadataAsync(NamespaceName namespaceName) {
            final String globalPartitionedPath = joinPath(PARTITIONED_TOPIC_PATH, namespaceName.toString());
            return getStore().deleteRecursive(globalPartitionedPath);
        }

        public CompletableFuture<Void> clearPartitionedTopicTenantAsync(String tenant) {
            final String partitionedTopicPath = joinPath(PARTITIONED_TOPIC_PATH, tenant);
            return deleteIfExistsAsync(partitionedTopicPath);
        }

        public CompletableFuture<Void> markPartitionedTopicDeletedAsync(TopicName tn) {
            if (tn.isPartitioned()) {
                return CompletableFuture.completedFuture(null);
            }
            if (log.isDebugEnabled()) {
                log.debug("markPartitionedTopicDeletedAsync {}", tn);
            }
            return updatePartitionedTopicAsync(tn, md -> {
                md.deleted = true;
                return md;
            });
        }

        public CompletableFuture<Void> unmarkPartitionedTopicDeletedAsync(TopicName tn) {
            if (tn.isPartitioned()) {
                return CompletableFuture.completedFuture(null);
            }
            if (log.isDebugEnabled()) {
                log.debug("unmarkPartitionedTopicDeletedAsync {}", tn);
            }
            return updatePartitionedTopicAsync(tn, md -> {
                md.deleted = false;
                return md;
            });
        }

        public CompletableFuture<Boolean> isPartitionedTopicBeingDeletedAsync(TopicName tn) {
            if (tn.isPartitioned()) {
                tn = TopicName.get(tn.getPartitionedTopicName());
            }
            return getPartitionedTopicMetadataAsync(tn)
                    .thenApply(mdOpt -> mdOpt.map(partitionedTopicMetadata -> partitionedTopicMetadata.deleted)
                            .orElse(false));
        }

        public CompletableFuture<Void> runWithMarkDeleteAsync(TopicName topic,
                                                              Supplier<CompletableFuture<Void>> supplier) {
            CompletableFuture<Void> future = new CompletableFuture<>();

            markPartitionedTopicDeletedAsync(topic).whenCompleteAsync((markResult, markExc) -> {
                final boolean mdFound;
                if (markExc != null) {
                    if (markExc.getCause() instanceof MetadataStoreException.NotFoundException) {
                        mdFound = false;
                    } else {
                        log.error("Failed to mark the topic {} as deleted", topic, markExc);
                        future.completeExceptionally(markExc);
                        return;
                    }
                } else {
                    mdFound = true;
                }

                supplier.get().whenComplete((deleteResult, deleteExc) -> {
                    if (deleteExc != null && mdFound) {
                        unmarkPartitionedTopicDeletedAsync(topic)
                                .thenRun(() -> future.completeExceptionally(deleteExc))
                                .exceptionally(ex -> {
                                    log.warn("Failed to unmark the topic {} as deleted", topic, ex);
                                    future.completeExceptionally(deleteExc);
                                    return null;
                                });
                    } else if (deleteExc != null) {
                        future.completeExceptionally(deleteExc);
                    } else {
                        future.complete(deleteResult);
                    }
                });
            });

            return future;
        }
    }

    // clear resource of `/loadbalance/bundle-data/{tenant}/{namespace}/` in metadata-store
    public CompletableFuture<Void> deleteBundleDataAsync(NamespaceName ns) {
        final String namespaceBundlePath = joinPath(BUNDLE_DATA_BASE_PATH, ns.toString());
        return getStore().deleteRecursive(namespaceBundlePath);
    }

    // clear resource of `/loadbalance/bundle-data/{tenant}/` in metadata-store
    public CompletableFuture<Void> deleteBundleDataTenantAsync(String tenant) {
        final String tenantBundlePath = joinPath(BUNDLE_DATA_BASE_PATH, tenant);
        return getStore().deleteRecursive(tenantBundlePath);
    }

}