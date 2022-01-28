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
package org.apache.pulsar.client.admin.internal;

import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.ResourceQuotas;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ResourceQuota;

public class ResourceQuotasImpl extends BaseResource implements ResourceQuotas {

    private final WebTarget adminQuotas;
    private final WebTarget adminV2Quotas;

    public ResourceQuotasImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminQuotas = web.path("/admin/resource-quotas");
        adminV2Quotas = web.path("/admin/v2/resource-quotas");
    }

    @Override
    public ResourceQuota getDefaultResourceQuota() throws PulsarAdminException {
        return sync(() -> getDefaultResourceQuotaAsync());
    }

    @Override
    public CompletableFuture<ResourceQuota> getDefaultResourceQuotaAsync() {
        final CompletableFuture<ResourceQuota> future = new CompletableFuture<>();
        asyncGetRequest(adminV2Quotas,
                new InvocationCallback<ResourceQuota>() {
                    @Override
                    public void completed(ResourceQuota resourceQuota) {
                        future.complete(resourceQuota);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setDefaultResourceQuota(ResourceQuota quota) throws PulsarAdminException {
        sync(() -> setDefaultResourceQuotaAsync(quota));
    }

    @Override
    public CompletableFuture<Void> setDefaultResourceQuotaAsync(ResourceQuota quota) {
        return asyncPostRequest(adminV2Quotas, Entity.entity(quota, MediaType.APPLICATION_JSON));
    }

    @Override
    public ResourceQuota getNamespaceBundleResourceQuota(String namespace, String bundle) throws PulsarAdminException {
        return sync(() -> getNamespaceBundleResourceQuotaAsync(namespace, bundle));
    }

    @Override
    public CompletableFuture<ResourceQuota> getNamespaceBundleResourceQuotaAsync(String namespace, String bundle) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle);
        final CompletableFuture<ResourceQuota> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<ResourceQuota>() {
                    @Override
                    public void completed(ResourceQuota resourceQuota) {
                        future.complete(resourceQuota);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setNamespaceBundleResourceQuota(String namespace, String bundle, ResourceQuota quota)
            throws PulsarAdminException {
        sync(() -> setNamespaceBundleResourceQuotaAsync(namespace, bundle, quota));
    }

    @Override
    public CompletableFuture<Void> setNamespaceBundleResourceQuotaAsync(
            String namespace, String bundle, ResourceQuota quota) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle);
        return asyncPostRequest(path, Entity.entity(quota, MediaType.APPLICATION_JSON));
    }

    @Override
    public void resetNamespaceBundleResourceQuota(String namespace, String bundle) throws PulsarAdminException {
        sync(() -> resetNamespaceBundleResourceQuotaAsync(namespace, bundle));
    }

    @Override
    public CompletableFuture<Void> resetNamespaceBundleResourceQuotaAsync(String namespace, String bundle) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle);
        return asyncDeleteRequest(path);
    }

    private WebTarget namespacePath(NamespaceName namespace, String... parts) {
        final WebTarget base = namespace.isV2() ? adminV2Quotas : adminQuotas;
        WebTarget namespacePath = base.path(namespace.toString());
        namespacePath = WebTargets.addParts(namespacePath, parts);
        return namespacePath;
    }
}

