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
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.metadata.api.MetadataStoreException;

public abstract class ResourceGroupsBase extends AdminResource {
    protected List<String> internalGetResourceGroups() {
        try {
            validateSuperUserAccess();
            return resourceGroupResources().listResourceGroups();
        } catch (Exception e) {
            log.error().exception(e).log("Failed to get ResourceGroups list");
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
            log.error()
                    .attr("resourceGroup", rgName)
                    .exception(e)
                    .log("Failed to get ResourceGroup");
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

            // write back the new ResourceGroup config.
            resourceGroupResources().updateResourceGroup(rgName, r -> resourceGroup);
            log.info()
                    .attr("resourceGroup", rgName)
                    .log("Successfully updated the ResourceGroup");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error()
                    .attr("resourceGroup", rgName)
                    .exception(e)
                    .log("Failed to update configuration for ResourceGroup");
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
        try {
            resourceGroupResources().createResourceGroup(rgName, rgConfig);
            log.info().attr("resourceGroup", rgName).log("Created ResourceGroup");
        } catch (MetadataStoreException.AlreadyExistsException e) {
            log.warn()
                    .attr("resourceGroup", rgName)
                    .log("Failed to create ResourceGroup - already exists");
            throw new RestException(Response.Status.CONFLICT, "ResourceGroup already exists");
        } catch (Exception e) {
            log.error()
                    .attr("resourceGroup", rgName)
                    .exception(e)
                    .log("Failed to create ResourceGroup");
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
                log.error()
                        .attr("resourceGroup", rgName)
                        .exceptionMessage(e)
                        .log("Failed to create/update ResourceGroup");
            }

            try {
                if (rgExists) {
                    internalUpdateResourceGroup(rgName, rgConfig);
                } else {
                    internalCreateResourceGroup(rgName, rgConfig);
                }
            } catch (Exception e) {
                log.error()
                        .attr("resourceGroup", rgName)
                        .exceptionMessage(e)
                        .log("Failed to create/update ResourceGroup");
                throw new RestException(e);
            }
        } catch (Exception e) {
            log.error()
                    .attr("resourceGroup", rgName)
                    .exceptionMessage(e)
                    .log("Failed to create/update ResourceGroup");
            throw new RestException(e);
        }
    }

    @SuppressWarnings("deprecation")
    protected boolean internalCheckRgInUse(String rgName) {
        try {
            for (String tenant : tenantResources().listTenants()) {
                for (String namespace : tenantResources().getListOfNamespaces(tenant)) {
                    Policies policies = getNamespacePolicies(NamespaceName.get(namespace));
                    if (null != policies && rgName.equals(policies.resource_group_name)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            log.error()
                    .attr("resourceGroup", rgName)
                    .exceptionMessage(e)
                    .log("Failed to get tenant/namespace list");
            throw new RestException(e);
        }
        return false;
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
            if (internalCheckRgInUse(rgName)) {
                throw new RestException(Response.Status.PRECONDITION_FAILED, "ResourceGroup is in use");
            }
            resourceGroupResources().deleteResourceGroup(rgName);
            log.info().attr("resourceGroup", rgName).log("Deleted ResourceGroup");
        } catch (Exception e) {
            log.error()
                    .attr("resourceGroup", rgName)
                    .exception(e)
                    .log("Failed to delete ResourceGroup .");
            throw new RestException(e);
        }
    }
}
