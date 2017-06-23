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
package org.apache.pulsar.broker.admin;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/resource-quotas")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/resource-quotas", description = "Quota admin APIs", tags = "resource-quotas")
public class ResourceQuotas extends AdminResource {

    private static final Logger log = LoggerFactory.getLogger(ResourceQuotas.class);

    @GET
    @ApiOperation(value = "Get the default quota", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public ResourceQuota getDefaultResourceQuota() throws Exception {
        validateSuperUserAccess();
        try {
            return pulsar().getLocalZkCacheService().getResourceQuotaCache().getDefaultQuota();
        } catch (Exception e) {
            log.error("[{}] Failed to get default resource quota", clientAppId());
            throw new RestException(e);
        }

    }

    @POST
    @ApiOperation(value = "Set the default quota", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
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

    @GET
    @Path("/{property}/{cluster}/{namespace}/{bundle}")
    @ApiOperation(value = "Get resource quota of a namespace bundle.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace does not exist") })
    public ResourceQuota getNamespaceBundleResourceQuota(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange) {
        validateSuperUserAccess();

        Policies policies = getNamespacePolicies(property, cluster, namespace);

        if (!cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForProperty(property, cluster);
        }

        NamespaceName fqnn = new NamespaceName(property, cluster, namespace);
        NamespaceBundle nsBundle = validateNamespaceBundleRange(fqnn, policies.bundles, bundleRange);

        try {
            return pulsar().getLocalZkCacheService().getResourceQuotaCache().getQuota(nsBundle);
        } catch (Exception e) {
            log.error("[{}] Failed to get resource quota for namespace bundle {}", clientAppId(), nsBundle.toString());
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{property}/{cluster}/{namespace}/{bundle}")
    @ApiOperation(value = "Set resource quota on a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void setNamespaceBundleResourceQuota(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange, ResourceQuota quota) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        Policies policies = getNamespacePolicies(property, cluster, namespace);

        if (!cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForProperty(property, cluster);
        }

        NamespaceName fqnn = new NamespaceName(property, cluster, namespace);
        NamespaceBundle nsBundle = validateNamespaceBundleRange(fqnn, policies.bundles, bundleRange);

        try {
            pulsar().getLocalZkCacheService().getResourceQuotaCache().setQuota(nsBundle, quota);
            log.info("[{}] Successfully set resource quota for namespace bundle {}", clientAppId(),
                    nsBundle.toString());
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to set resource quota for namespace bundle {}: concurrent modification",
                    clientAppId(), nsBundle.toString());
            throw new RestException(Status.CONFLICT, "Cuncurrent modification on namespace bundle quota");
        } catch (Exception e) {
            log.error("[{}] Failed to set resource quota for namespace bundle {}", clientAppId(), nsBundle.toString());
            throw new RestException(e);
        }

    }

    @DELETE
    @Path("/{property}/{cluster}/{namespace}/{bundle}")
    @ApiOperation(value = "Remove resource quota for a namespace.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeNamespaceBundleResourceQuota(@PathParam("property") String property,
            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
            @PathParam("bundle") String bundleRange) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        Policies policies = getNamespacePolicies(property, cluster, namespace);

        if (!cluster.equals(Namespaces.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForProperty(property, cluster);
        }

        NamespaceName fqnn = new NamespaceName(property, cluster, namespace);
        NamespaceBundle nsBundle = validateNamespaceBundleRange(fqnn, policies.bundles, bundleRange);

        try {
            pulsar().getLocalZkCacheService().getResourceQuotaCache().unsetQuota(nsBundle);
            log.info("[{}] Successfully unset resource quota for namespace bundle {}", clientAppId(),
                    nsBundle.toString());
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to unset resource quota for namespace bundle {}: concurrent modification",
                    clientAppId(), nsBundle.toString());
            throw new RestException(Status.CONFLICT, "Cuncurrent modification on namespace bundle quota");
        } catch (Exception e) {
            log.error("[{}] Failed to unset resource quota for namespace bundle {}", clientAppId(),
                    nsBundle.toString());
            throw new RestException(e);
        }
    }
}
