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

import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ResourceQuotasBase extends NamespacesBase {

    public ResourceQuota getDefaultResourceQuota() throws Exception {
        validateSuperUserAccess();
        try {
            return pulsar().getLocalZkCacheService().getResourceQuotaCache().getDefaultQuota();
        } catch (Exception e) {
            log.error("[{}] Failed to get default resource quota", clientAppId());
            throw new RestException(e);
        }

    }

    public void setDefaultResourceQuota(ResourceQuota quota) throws Exception {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();
        try {
            pulsar().getLocalZkCacheService().getResourceQuotaCache().setDefaultQuota(quota);
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
            return pulsar().getLocalZkCacheService().getResourceQuotaCache().getQuota(nsBundle);
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
            pulsar().getLocalZkCacheService().getResourceQuotaCache().setQuota(nsBundle, quota);
            log.info("[{}] Successfully set resource quota for namespace bundle {}", clientAppId(),
                    nsBundle.toString());
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to set resource quota for namespace bundle {}: concurrent modification",
                    clientAppId(), nsBundle.toString());
            throw new RestException(Status.CONFLICT, "Concurrent modification on namespace bundle quota");
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
            pulsar().getLocalZkCacheService().getResourceQuotaCache().unsetQuota(nsBundle);
            log.info("[{}] Successfully unset resource quota for namespace bundle {}", clientAppId(),
                    nsBundle.toString());
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to unset resource quota for namespace bundle {}: concurrent modification",
                    clientAppId(), nsBundle.toString());
            throw new RestException(Status.CONFLICT, "Concurrent modification on namespace bundle quota");
        } catch (Exception e) {
            log.error("[{}] Failed to unset resource quota for namespace bundle {}", clientAppId(),
                    nsBundle.toString());
            throw new RestException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ResourceQuotasBase.class);
}
