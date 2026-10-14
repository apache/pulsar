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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

@CustomLog
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
                .thenCompose(namespaces -> FutureUtil.waitForAll(namespaces.stream()
                        .map(ns -> getCache().delete(joinPath(BASE_POLICIES_PATH, tenantName, ns)))
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

        for (String ns : getChildren(joinPath(BASE_POLICIES_PATH, tenant))) {
            String namespace = NamespaceName.get(tenant, ns).toString();
            try {
                if (get(joinPath(BASE_POLICIES_PATH, namespace)).isPresent()) {
                    namespaces.add(namespace);
                }
            } catch (MetadataStoreException.ContentDeserializationException e) {
                // not a namespace node
            }
        }

        return namespaces;
    }

    public CompletableFuture<List<String>> getListOfNamespacesAsync(String tenant) {
        return getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenant))
                .thenCompose(nsList -> nsList.stream().map(key -> {
                    String namespace = NamespaceName.get(tenant, key).toString();
                    return getAsync(joinPath(BASE_POLICIES_PATH, namespace))
                        .thenApply(opt -> opt.isPresent()
                                ? List.of(namespace)
                                : new ArrayList<String>())
                        .exceptionally(ex -> {
                            Throwable cause = FutureUtil.unwrapCompletionException(ex);
                            if (cause instanceof MetadataStoreException
                                    .ContentDeserializationException) {
                                return new ArrayList<>();
                            }
                            throw FutureUtil.wrapToCompletionException(ex);
                        });
                }).reduce(CompletableFuture.completedFuture(new ArrayList<>()),
                        (accumulator, n) -> accumulator.thenCompose(namespaces -> n.thenApply(m -> {
                            namespaces.addAll(m);
                            return namespaces;
                        }))));
    }

    public CompletableFuture<Void> hasActiveNamespace(String tenant) {
        CompletableFuture<Void> activeNamespaceFuture = new CompletableFuture<>();
        getChildrenAsync(joinPath(BASE_POLICIES_PATH, tenant)).thenAccept(nsList -> {
            if (nsList == null || nsList.isEmpty()) {
                activeNamespaceFuture.complete(null);
                return;
            }
            List<CompletableFuture<Void>> activeNamespaceListFuture = new ArrayList<>();
            nsList.forEach(ns -> {
                CompletableFuture<Void> checkNs = new CompletableFuture<>();
                activeNamespaceListFuture.add(checkNs);
                String namespace = NamespaceName.get(tenant, ns).toString();
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
        }).exceptionally(ex -> {
            activeNamespaceFuture.completeExceptionally(ex.getCause());
            return null;
        });
        return activeNamespaceFuture;
    }
}
