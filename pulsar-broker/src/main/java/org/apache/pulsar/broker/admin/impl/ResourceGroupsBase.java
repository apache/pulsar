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
package org.apache.pulsar.broker.admin.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ResourceGroupsBase extends AdminResource {

    protected CompletableFuture<Void> internalSetReplicatorDispatchRate(String rgName, String remoteCluster,
                                                                 DispatchRate dispatchRate) {
        if (remoteCluster == null) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Remote cluster name is not provided"));
        }
        return validateSuperUserAccessAsync()
                .thenCompose((__) -> resourceGroupResources()
                        .updateResourceGroupAsync(rgName, rg -> {
                            String key = getReplicatorDispatchRateKey(remoteCluster);
                            if (rg.getReplicatorDispatchRate() == null) {
                                rg.setReplicatorDispatchRate(new ConcurrentHashMap<>());
                            }
                            if (dispatchRate == null) {
                                rg.getReplicatorDispatchRate().remove(key);
                            } else {
                                rg.getReplicatorDispatchRate().put(key, dispatchRate);
                            }
                            return rg;
                        })
                );
    }

    protected CompletableFuture<DispatchRate> internalGetReplicatorDispatchRate(String rgName, String remoteCluster) {
        if (remoteCluster == null) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Remote cluster name is not provided"));
        }
        return validateSuperUserAccessAsync()
                .thenCompose((__) -> resourceGroupResources()
                        .getResourceGroupAsync(rgName))
                .thenCompose((resourceGroupOptional) -> resourceGroupOptional
                        .map((rg) -> {
                            Map<String, DispatchRate> replicatorDispatchRate = rg.getReplicatorDispatchRate();
                            DispatchRate dispatchRate = null;
                            if (replicatorDispatchRate != null) {
                                dispatchRate = rg.getReplicatorDispatchRate()
                                        .get(getReplicatorDispatchRateKey(remoteCluster));
                            }
                            return CompletableFuture.completedFuture(dispatchRate);
                        })
                        .orElseGet(() -> FutureUtil.failedFuture(
                                new RestException(Status.NOT_FOUND, "ResourceGroup does not exist")))
                );
    }

    protected List<String> internalGetResourceGroups() {
        try {
            validateSuperUserAccess();
            return resourceGroupResources().listResourceGroups();
        } catch (Exception e) {
            log.error("[{}] Failed to get ResourceGroups list: {}", clientAppId(), e);
            throw new RestException(e);
        }
    }

    protected ResourceGroup internalGetResourceGroup(String rgName) {
        try {
            validateSuperUserAccess();
            ResourceGroup resourceGroup = resourceGroupResources().getResourceGroup(rgName)
                    .orElseThrow(() -> new RestException(Response.Status.NOT_FOUND, "ResourceGroup does not exist"));
            return resourceGroup;
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get ResourceGroup  {}", clientAppId(), rgName, e);
            throw new RestException(e);
        }
    }

    protected void internalUpdateResourceGroup(String rgName, ResourceGroup rgConfig) {

        try {
            ResourceGroup resourceGroup = resourceGroupResources().getResourceGroup(rgName).orElseThrow(() ->
                    new RestException(Response.Status.NOT_FOUND, "ResourceGroup does not exist"));

            /*
             * assuming read-modify-write
             */
            if (rgConfig.getPublishRateInMsgs() != null) {
                resourceGroup.setPublishRateInMsgs(rgConfig.getPublishRateInMsgs());
            }
            if (rgConfig.getPublishRateInBytes() != null) {
                resourceGroup.setPublishRateInBytes(rgConfig.getPublishRateInBytes());
            }
            if (rgConfig.getDispatchRateInMsgs() != null) {
                resourceGroup.setDispatchRateInMsgs(rgConfig.getDispatchRateInMsgs());
            }
            if (rgConfig.getDispatchRateInBytes() != null) {
                resourceGroup.setDispatchRateInBytes(rgConfig.getDispatchRateInBytes());
            }
            if (rgConfig.getReplicationDispatchRateInBytes() != null) {
                resourceGroup.setReplicationDispatchRateInBytes(rgConfig.getReplicationDispatchRateInBytes());
            }
            if (rgConfig.getReplicationDispatchRateInMsgs() != null) {
                resourceGroup.setReplicationDispatchRateInMsgs(rgConfig.getReplicationDispatchRateInMsgs());
            }
            // write back the new ResourceGroup config.
            resourceGroupResources().updateResourceGroup(rgName, r -> resourceGroup);
            log.info("[{}] Successfully updated the ResourceGroup {}", clientAppId(), rgName);
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update configuration for ResourceGroup {}", clientAppId(), rgName, e);
            throw new RestException(e);
        }
    }

    protected void internalCreateResourceGroup(String rgName, ResourceGroup rgConfig) {
        rgConfig.setPublishRateInMsgs(rgConfig.getPublishRateInMsgs() == null
                ? -1 : rgConfig.getPublishRateInMsgs());
        rgConfig.setPublishRateInBytes(rgConfig.getPublishRateInBytes() == null
                ? -1 : rgConfig.getPublishRateInBytes());
        rgConfig.setDispatchRateInMsgs(rgConfig.getDispatchRateInMsgs() == null
                ? -1 : rgConfig.getDispatchRateInMsgs());
        rgConfig.setDispatchRateInBytes(rgConfig.getDispatchRateInBytes() == null
                ? -1 : rgConfig.getDispatchRateInBytes());
        rgConfig.setReplicationDispatchRateInBytes(rgConfig.getReplicationDispatchRateInBytes() == null
                ? -1 : rgConfig.getReplicationDispatchRateInBytes());
        rgConfig.setReplicationDispatchRateInMsgs(rgConfig.getReplicationDispatchRateInMsgs() == null
                ? -1 : rgConfig.getReplicationDispatchRateInMsgs());
        try {
            resourceGroupResources().createResourceGroup(rgName, rgConfig);
            log.info("[{}] Created ResourceGroup {}", clientAppId(), rgName);
        } catch (MetadataStoreException.AlreadyExistsException e) {
            log.warn("[{}] Failed to create ResourceGroup {} - already exists", clientAppId(), rgName);
            throw new RestException(Response.Status.CONFLICT, "ResourceGroup already exists");
        } catch (Exception e) {
            log.error("[{}] Failed to create ResourceGroup {}", clientAppId(), rgName, e);
            throw new RestException(e);
        }

    }
    protected void internalCreateOrUpdateResourceGroup(String rgName, ResourceGroup rgConfig) {
        try {
            validateSuperUserAccess();
            checkNotNull(rgConfig);
            /*
             * see if ResourceGroup exists and treat the request as a update if it does.
             */
            boolean rgExists = false;
            try {
                rgExists = resourceGroupResources().resourceGroupExists(rgName);
            } catch (Exception e) {
                log.error("[{}] Failed to create/update ResourceGroup {}: {}", clientAppId(), rgName, e);
            }

            try {
                if (rgExists) {
                    internalUpdateResourceGroup(rgName, rgConfig);
                } else {
                    internalCreateResourceGroup(rgName, rgConfig);
                }
            } catch (Exception e) {
                log.error("[{}] Failed to create/update ResourceGroup {}: {}", clientAppId(), rgName, e);
                throw new RestException(e);
            }
        } catch (Exception e) {
            log.error("[{}] Failed to create/update ResourceGroup {}: {}", clientAppId(), rgName, e);
            throw new RestException(e);
        }
    }

    protected void internalDeleteResourceGroup(String rgName) {
        /*
         * need to walk the namespaces and make sure it is not in use
         */
        try {
            validateSuperUserAccess();
            /*
             * walk the namespaces and make sure it is not in use.
             */
            try {
                pulsar().getResourceGroupServiceManager().checkResourceGroupInUse(rgName);
            } catch (PulsarAdminException e) {
                log.error("[{}] Check if ResourceGroup {} is in use: {}", clientAppId(), rgName, e);
                throw new RestException(Response.Status.PRECONDITION_FAILED,
                        "ResourceGroup is in use");
            }
            resourceGroupResources().deleteResourceGroup(rgName);
            log.info("[{}] Deleted ResourceGroup {}", clientAppId(), rgName);
        } catch (Exception e) {
            log.error("[{}] Failed to delete ResourceGroup {}.", clientAppId(), rgName, e);
            throw new RestException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ResourceGroupsBase.class);
}
