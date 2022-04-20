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

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.ResourceQuota;

@Slf4j
public abstract class ResourceQuotasBase extends NamespacesBase {

    public ResourceQuota getDefaultResourceQuota() throws Exception {
        validateSuperUserAccess();
        try {
            return pulsar().getBrokerService().getBundlesQuotas().getDefaultResourceQuota()
                    .get(config().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("[{}] Failed to get default resource quota", clientAppId());
            throw new RestException(e);
        }
    }

    public void setDefaultResourceQuota(ResourceQuota quota) throws Exception {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();
        try {
            pulsar().getBrokerService().getBundlesQuotas().setDefaultResourceQuota(quota)
                    .get(config().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("[{}] Failed to get default resource quota", clientAppId());
            throw new RestException(e);
        }
    }

    @SuppressWarnings("deprecation")
    protected ResourceQuota internalGetNamespaceBundleResourceQuota(String bundleRange) {
        validateSuperUserAccess();

        Policies policies = getNamespacePolicies(namespaceName);

        if (!namespaceName.isGlobal()) {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        NamespaceBundle nsBundle = validateNamespaceBundleRange(namespaceName, policies.bundles, bundleRange);

        try {
            return pulsar().getBrokerService().getBundlesQuotas().getResourceQuota(nsBundle)
                    .get(config().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("[{}] Failed to get resource quota for namespace bundle {}", clientAppId(), nsBundle.toString());
            throw new RestException(e);
        }
    }

    @SuppressWarnings("deprecation")
    protected void internalSetNamespaceBundleResourceQuota(String bundleRange, ResourceQuota quota) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        Policies policies = getNamespacePolicies(namespaceName);

        if (!namespaceName.isGlobal()) {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        NamespaceBundle nsBundle = validateNamespaceBundleRange(namespaceName, policies.bundles, bundleRange);

        try {
            pulsar().getBrokerService().getBundlesQuotas().setResourceQuota(nsBundle, quota)
                    .get(config().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
            log.info("[{}] Successfully set resource quota for namespace bundle {}", clientAppId(),
                    nsBundle.toString());
        } catch (Exception e) {
            log.error("[{}] Failed to set resource quota for namespace bundle {}", clientAppId(), nsBundle.toString());
            throw new RestException(e);
        }

    }

    @SuppressWarnings("deprecation")
    protected void internalRemoveNamespaceBundleResourceQuota(String bundleRange) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        Policies policies = getNamespacePolicies(namespaceName);

        if (!namespaceName.isGlobal()) {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        NamespaceBundle nsBundle = validateNamespaceBundleRange(namespaceName, policies.bundles, bundleRange);

        try {
            pulsar().getBrokerService().getBundlesQuotas().resetResourceQuota(nsBundle)
                    .get(config().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
            log.info("[{}] Successfully unset resource quota for namespace bundle {}", clientAppId(),
                    nsBundle.toString());
        } catch (Exception e) {
            log.error("[{}] Failed to unset resource quota for namespace bundle {}", clientAppId(),
                    nsBundle.toString());
            throw new RestException(e);
        }
    }
}
