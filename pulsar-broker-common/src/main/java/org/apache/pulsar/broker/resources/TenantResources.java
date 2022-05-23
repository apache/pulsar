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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

@Slf4j
public class TenantResources extends BaseResources<TenantInfo> {
    public TenantResources(MetadataStore store, int operationTimeoutSec) {
        super(store, TenantInfo.class, operationTimeoutSec);
    }

    public List<String> listTenants() throws MetadataStoreException {
        return getChildren(BASE_POLICIES_PATH);
    }

    public CompletableFuture<List<String>> listTenantsAsync() {
        return getChildrenAsync(BASE_POLICIES_PATH);
    }

    public CompletableFuture<Void> deleteTenantAsync(String tenantName) {
        return getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenantName))
                .thenCompose(clusters -> FutureUtil.waitForAll(clusters.stream()
                        .map(cluster -> getCache().delete(joinPath(BASE_POLICIES_PATH, tenantName, cluster)))
                        .collect(Collectors.toList()))
                ).thenCompose(__ -> deleteAsync(joinPath(BASE_POLICIES_PATH, tenantName)));
    }

    public boolean tenantExists(String tenantName) throws MetadataStoreException {
        return exists(joinPath(BASE_POLICIES_PATH, tenantName));
    }

    public Optional<TenantInfo> getTenant(String tenantName) throws MetadataStoreException {
        return get(joinPath(BASE_POLICIES_PATH, tenantName));
    }

    public CompletableFuture<Optional<TenantInfo>> getTenantAsync(String tenantName) {
        return getAsync(joinPath(BASE_POLICIES_PATH, tenantName));
    }

    public void createTenant(String tenantName, TenantInfo ti) throws MetadataStoreException {
        create(joinPath(BASE_POLICIES_PATH, tenantName), ti);
    }

    public CompletableFuture<Void> createTenantAsync(String tenantName, TenantInfo ti) {
        return createAsync(joinPath(BASE_POLICIES_PATH, tenantName), ti);
    }

    public CompletableFuture<Void> updateTenantAsync(String tenantName, Function<TenantInfo, TenantInfo> f) {
        return setAsync(joinPath(BASE_POLICIES_PATH, tenantName), f);
    }

    public CompletableFuture<Boolean> tenantExistsAsync(String tenantName) {
        return existsAsync(joinPath(BASE_POLICIES_PATH, tenantName));
    }

    public List<String> getListOfNamespaces(String tenant) throws MetadataStoreException {
        List<String> namespaces = new ArrayList<>();

        // this will return a cluster in v1 and a namespace in v2
        for (String clusterOrNamespace : getChildren(joinPath(BASE_POLICIES_PATH, tenant))) {
            // Then get the list of namespaces
            final List<String> children = getChildren(joinPath(BASE_POLICIES_PATH, tenant, clusterOrNamespace));
            if (children == null || children.isEmpty()) {
                String namespace = NamespaceName.get(tenant, clusterOrNamespace).toString();
                // if the length is 0 then this is probably a leftover cluster from namespace created
                // with the v1 admin format (prop/cluster/ns) and then deleted, so no need to add it to the list
                try {
                    if (get(joinPath(BASE_POLICIES_PATH, namespace)).isPresent()) {
                        namespaces.add(namespace);
                    }
                } catch (MetadataStoreException.ContentDeserializationException e) {
                    // not a namespace node
                }

            } else {
                children.forEach(ns -> {
                    namespaces.add(NamespaceName.get(tenant, clusterOrNamespace, ns).toString());
                });
            }
        }

        return namespaces;
    }

    public CompletableFuture<List<String>> getListOfNamespacesAsync(String tenant) {
        // this will return a cluster in v1 and a namespace in v2
        return getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenant))
                .thenCompose(clusterOrNamespaces -> clusterOrNamespaces.stream().map(key ->
                        getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenant, key))
                                .thenCompose(children -> {
                                    if (children == null || children.isEmpty()) {
                                        String namespace = NamespaceName.get(tenant, key).toString();
                                        // if the length is 0 then this is probably a leftover cluster from namespace
                                        // created with the v1 admin format (prop/cluster/ns) and then deleted, so no
                                        // need to add it to the list
                                        return getAsync(joinPath(BASE_POLICIES_PATH, namespace))
                                           .thenApply(opt -> opt.isPresent() ? Collections.singletonList(namespace)
                                                   : new ArrayList<String>())
                                           .exceptionally(ex -> {
                                                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                                                if (cause instanceof MetadataStoreException
                                                        .ContentDeserializationException) {
                                                    return new ArrayList<>();
                                                }
                                                throw FutureUtil.wrapToCompletionException(ex);
                                            });
                                    } else {
                                        CompletableFuture<List<String>> ret = new CompletableFuture();
                                        ret.complete(children.stream().map(ns -> NamespaceName.get(tenant, key, ns)
                                                .toString()).collect(Collectors.toList()));
                                        return ret;
                                    }
                                })).reduce(CompletableFuture.completedFuture(new ArrayList<>()),
                                        (accumulator, n) -> accumulator.thenCompose(namespaces -> n.thenApply(m -> {
                                            namespaces.addAll(m);
                                            return namespaces;
                                    }))));
    }

    public CompletableFuture<List<String>> getActiveNamespaces(String tenant, String cluster) {
        return getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenant, cluster));
    }

    public CompletableFuture<Void> hasActiveNamespace(String tenant) {
        CompletableFuture<Void> activeNamespaceFuture = new CompletableFuture<>();
        getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenant)).thenAccept(clusterOrNamespaceList -> {
            if (clusterOrNamespaceList == null || clusterOrNamespaceList.isEmpty()) {
                activeNamespaceFuture.complete(null);
                return;
            }
            List<CompletableFuture<Void>> activeNamespaceListFuture = new ArrayList<>();
            clusterOrNamespaceList.forEach(clusterOrNamespace -> {
                // get list of active V1 namespace
                CompletableFuture<Void> checkNs = new CompletableFuture<>();
                activeNamespaceListFuture.add(checkNs);
                getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenant, clusterOrNamespace))
                        .whenComplete((children, ex) -> {
                            if (ex != null) {
                                checkNs.completeExceptionally(ex);
                                return;
                            }
                            if (children != null && !children.isEmpty()) {
                                checkNs.completeExceptionally(
                                        new IllegalStateException("The tenant still has active namespaces"));
                                return;
                            }
                            String namespace = NamespaceName.get(tenant, clusterOrNamespace).toString();
                            // if the length is 0 then this is probably a leftover cluster from namespace
                            // created
                            // with the v1 admin format (prop/cluster/ns) and then deleted, so no need to
                            // add it to the list
                            getAsync(joinPath(BASE_POLICIES_PATH, namespace)).thenApply(data -> {
                                if (data.isPresent()) {
                                    checkNs.completeExceptionally(new IllegalStateException(
                                            "The tenant still has active namespaces"));
                                } else {
                                    checkNs.complete(null);
                                }
                                return null;
                            }).exceptionally(ex2 -> {
                                if (ex2.getCause() instanceof MetadataStoreException.ContentDeserializationException) {
                                    // it's not a valid namespace-node
                                    checkNs.complete(null);
                                } else {
                                    checkNs.completeExceptionally(ex2);
                                }
                                return null;
                            });
                        });
                FutureUtil.waitForAll(activeNamespaceListFuture).thenAccept(r -> {
                    activeNamespaceFuture.complete(null);
                }).exceptionally(ex -> {
                    activeNamespaceFuture.completeExceptionally(ex.getCause());
                    return null;
                });
            });
        }).exceptionally(ex -> {
            activeNamespaceFuture.completeExceptionally(ex.getCause());
            return null;
        });
        return activeNamespaceFuture;
    }
}
