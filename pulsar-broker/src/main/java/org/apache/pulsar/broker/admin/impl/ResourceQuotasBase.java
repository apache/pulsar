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
package org.apache.pulsar.broker.admin.impl;

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.policies.data.ResourceQuota;

@Slf4j
public abstract class ResourceQuotasBase extends NamespacesBase {

    public ResourceQuota getDefaultResourceQuota() {
        return sync(() -> getDefaultResourceQuotaAsync());
    }

    public CompletableFuture<ResourceQuota> getDefaultResourceQuotaAsync() {
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> pulsar().getBrokerService().getBundlesQuotas().getDefaultResourceQuota());
    }

    public CompletableFuture<Void> setDefaultResourceQuotaAsync(ResourceQuota quota) {
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> pulsar().getBrokerService().getBundlesQuotas().setDefaultResourceQuota(quota));
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<ResourceQuota> internalGetNamespaceBundleResourceQuota(String bundleRange) {
        return getNamespaceBundleRangeAsync(bundleRange, false)
                .thenCompose(nsBundle -> pulsar().getBrokerService().getBundlesQuotas().getResourceQuota(nsBundle));
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalSetNamespaceBundleResourceQuota(String bundleRange, ResourceQuota quota) {
        return getNamespaceBundleRangeAsync(bundleRange, true)
                .thenCompose(nsBundle ->
                        pulsar().getBrokerService().getBundlesQuotas().setResourceQuota(nsBundle, quota));
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalRemoveNamespaceBundleResourceQuota(String bundleRange) {
        return getNamespaceBundleRangeAsync(bundleRange, true)
                .thenCompose(nsBundle ->
                        pulsar().getBrokerService().getBundlesQuotas().resetResourceQuota(nsBundle));
    }

    private CompletableFuture<NamespaceBundle> getNamespaceBundleRangeAsync(String bundleRange, boolean checkReadOnly) {
        CompletableFuture<Void> ret =
                validateSuperUserAccessAsync().thenCompose(__ -> {
                    if (checkReadOnly) {
                        return validatePoliciesReadOnlyAccessAsync();
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        if (!namespaceName.isGlobal()) {
            ret = ret.thenCompose(__ -> validateClusterOwnershipAsync(namespaceName.getCluster()))
                    .thenCompose(__ -> validateClusterForTenantAsync(namespaceName.getTenant(),
                            namespaceName.getCluster()));
        }
        return ret
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> validateNamespaceBundleRange(namespaceName, policies.bundles, bundleRange));
    }
}
