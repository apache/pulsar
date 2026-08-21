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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterPoliciesImpl;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;

public class ClusterResources extends BaseResources<ClusterData> {

    @Getter
    private FailureDomainResources failureDomainResources;
    @Getter
    private ClusterPoliciesResources clusterPoliciesResources;

    public ClusterResources(MetadataStore localStore, MetadataStore configurationStore, int operationTimeoutSec) {
        super(configurationStore, ClusterData.class, operationTimeoutSec);
        this.failureDomainResources = new FailureDomainResources(configurationStore, FailureDomainImpl.class,
                operationTimeoutSec);
        if (localStore != null) {
            this.clusterPoliciesResources = new ClusterPoliciesResources(localStore, ClusterPoliciesImpl.class,
                    operationTimeoutSec);
        } else {
            this.clusterPoliciesResources = null;
        }
    }

    public CompletableFuture<Set<String>> listAsync() {
        return getChildrenAsync(BASE_CLUSTERS_PATH).thenApply(HashSet::new);
    }

    public Set<String> list() throws MetadataStoreException {
        return new HashSet<>(super.getChildren(BASE_CLUSTERS_PATH));
    }

    public Optional<ClusterData> getCluster(String clusterName) throws MetadataStoreException {
        return get(joinPath(BASE_CLUSTERS_PATH, clusterName));
    }

    public CompletableFuture<Optional<ClusterData>> getClusterAsync(String clusterName) {
        return getAsync(joinPath(BASE_CLUSTERS_PATH, clusterName));
    }

    public CompletableFuture<List<String>> getNamespacesForClusterAsync(String tenant, String clusterName) {
        return getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenant, clusterName));
    }

    public List<String> getNamespacesForCluster(String tenant, String clusterName) throws MetadataStoreException {
        try {
            return getNamespacesForClusterAsync(tenant, clusterName)
                    .get(getOperationTimeoutSec(), TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException(
                    "Failed to get namespaces for tenant " + tenant + " in cluster " + clusterName, e);
        }
    }

    public void createCluster(String clusterName, ClusterData clusterData) throws MetadataStoreException {
        create(joinPath(BASE_CLUSTERS_PATH, clusterName), clusterData);
    }

    public CompletableFuture<Void> createClusterAsync(String clusterName, ClusterData clusterData) {
        return createAsync(joinPath(BASE_CLUSTERS_PATH, clusterName), clusterData);
    }

    public CompletableFuture<Void> updateClusterAsync(String clusterName,
                                                      Function<ClusterData, ClusterData> modifyFunction) {
        return setAsync(joinPath(BASE_CLUSTERS_PATH, clusterName), modifyFunction);
    }

    public void updateCluster(String clusterName, Function<ClusterData, ClusterData> modifyFunction)
            throws MetadataStoreException {
        set(joinPath(BASE_CLUSTERS_PATH, clusterName), modifyFunction);
    }

    public void deleteCluster(String clusterName) throws MetadataStoreException {
        delete(joinPath(BASE_CLUSTERS_PATH, clusterName));
    }

    public CompletableFuture<Void> deleteClusterAsync(String clusterName) {
        return deleteAsync(joinPath(BASE_CLUSTERS_PATH, clusterName));
    }

    public CompletableFuture<Boolean> isClusterUsedAsync(String clusterName) {
        return getCache().getChildren(BASE_POLICIES_PATH)
                .thenCompose(tenants -> {
                    List<CompletableFuture<List<String>>> futures = tenants.stream()
                            .map(tenant -> getCache().getChildren(joinPath(BASE_POLICIES_PATH, tenant, clusterName)))
                            .collect(Collectors.toList());
                    return FutureUtil.waitForAll(futures)
                            .thenApply(__ -> {
                                // Futures are already complete after waitForAll; use getNow to avoid
                                // accidental blocking if this is ever refactored.
                                return futures.stream()
                                        .map(f -> f.getNow(List.of()))
                                        .anyMatch(CollectionUtils::isNotEmpty);
                            });
                });
    }

    /**
     * Synchronous wrapper around {@link #isClusterUsedAsync(String)}.
     * Prefer the async method so callers do not block metadata store threads.
     */
    public boolean isClusterUsed(String clusterName) throws MetadataStoreException {
        try {
            return isClusterUsedAsync(clusterName).get(getOperationTimeoutSec(), TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to check if cluster is used: " + clusterName, e);
        }
    }

    public boolean clusterExists(String clusterName) throws MetadataStoreException {
        return exists(joinPath(BASE_CLUSTERS_PATH, clusterName));
    }

    public CompletableFuture<Boolean> clusterExistsAsync(String clusterName) {
        return getCache().exists(joinPath(BASE_CLUSTERS_PATH, clusterName));
    }

    public static boolean pathRepresentsClusterName(String path) {
        return path.startsWith(BASE_CLUSTERS_PATH);
    }

    public static String clusterNameFromPath(String path) {
        return path.substring(BASE_CLUSTERS_PATH.length() + 1);
    }

    public static class FailureDomainResources extends BaseResources<FailureDomainImpl> {
        public static final String FAILURE_DOMAIN = "failureDomain";

        public FailureDomainResources(MetadataStore store, Class<FailureDomainImpl> clazz,
                int operationTimeoutSec) {
            super(store, clazz, operationTimeoutSec);
        }

        public CompletableFuture<List<String>> listFailureDomainsAsync(String clusterName) {
            return getChildrenAsync(joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN));
        }
        public List<String> listFailureDomains(String clusterName) throws MetadataStoreException {
            return getChildren(joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN));
        }

        public CompletableFuture<Optional<FailureDomainImpl>> getFailureDomainAsync(String clusterName,
                                                                                    String domainName) {
            return getAsync(joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName));
        }
        public Optional<FailureDomainImpl> getFailureDomain(String clusterName, String domainName)
                throws MetadataStoreException {
            return get(joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName));
        }

        public void deleteFailureDomain(String clusterName, String domainName) throws MetadataStoreException {
            String path = joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName);
            delete(path);
        }

        public CompletableFuture<Void> deleteFailureDomainAsync(String clusterName, String domainName) {
            String path = joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName);
            return deleteAsync(path);
        }

        public CompletableFuture<Void> deleteFailureDomainsAsync(String clusterName) {
            String failureDomainPath = joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN);
            return existsAsync(failureDomainPath)
                    .thenCompose(exists -> {
                        if (!exists) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return getChildrenAsync(failureDomainPath)
                                .thenCompose(children -> FutureUtil.waitForAll(children.stream()
                                        .map(domain -> deleteAsync(joinPath(failureDomainPath, domain)))
                                        .collect(Collectors.toList())))
                                .thenCompose(__ -> deleteAsync(failureDomainPath));
                    });
        }

        public void deleteFailureDomains(String clusterName) throws MetadataStoreException {
            String failureDomainPath = joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN);
            if (!exists(failureDomainPath)) {
                return;
            }

            for (String domain : getChildren(failureDomainPath)) {
                delete(joinPath(failureDomainPath, domain));
            }

            delete(failureDomainPath);
        }

        public CompletableFuture<Void> setFailureDomainWithCreateAsync(String clusterName, String domainName,
                                           Function<Optional<FailureDomainImpl>, FailureDomainImpl> createFunction) {
            return setWithCreateAsync(
                    joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName), createFunction);
        }
        public void setFailureDomainWithCreate(String clusterName, String domainName,
                                               Function<Optional<FailureDomainImpl>, FailureDomainImpl> createFunction)
                throws MetadataStoreException {
            setWithCreate(joinPath(BASE_CLUSTERS_PATH, clusterName, FAILURE_DOMAIN, domainName), createFunction);
        }

        public void registerListener(Consumer<Notification> listener) {
            getStore().registerListener(n -> {
                // Prefilter the notification just for failure domains
                if (n.getPath().startsWith(BASE_CLUSTERS_PATH)
                        && n.getPath().contains("/" + FAILURE_DOMAIN)) {
                    listener.accept(n);
                }
            });
        }
    }

    public static class ClusterPoliciesResources extends BaseResources<ClusterPoliciesImpl> {
        public static final String LOCAL_POLICIES_PATH = "policies";

        public ClusterPoliciesResources(MetadataStore store, Class<ClusterPoliciesImpl> clazz,
                int operationTimeoutSec) {
            super(store, clazz, operationTimeoutSec);
        }

        public Optional<ClusterPoliciesImpl> getClusterPolicies(String clusterName) throws MetadataStoreException {
            return get(joinPath(BASE_CLUSTERS_PATH, clusterName, LOCAL_POLICIES_PATH));
        }

        public CompletableFuture<Optional<ClusterPoliciesImpl>> getClusterPoliciesAsync(String clusterName) {
            return getAsync(joinPath(BASE_CLUSTERS_PATH, clusterName, LOCAL_POLICIES_PATH));
        }

        public CompletableFuture<Void> setPoliciesWithCreateAsync(String clusterName,
                Function<Optional<ClusterPoliciesImpl>, ClusterPoliciesImpl> createFunction) {
            return setWithCreateAsync(joinPath(BASE_CLUSTERS_PATH, clusterName, LOCAL_POLICIES_PATH), createFunction);
        }
    }
}
