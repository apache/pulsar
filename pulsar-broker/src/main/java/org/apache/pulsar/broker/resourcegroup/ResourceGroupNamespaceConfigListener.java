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
package org.apache.pulsar.broker.resourcegroup;

import java.util.function.Consumer;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource Group Namespace Config Listener
 *
 * <P>Meta data store listener of updates to namespace attachment to resource groups.
 * <P>Listens to namespace(policy) config changes and updates internal data structures.
 *
 * @see <a href="https://github.com/apache/pulsar/wiki/PIP-82%3A-Tenant-and-namespace-level-rate-limiting">Global-quotas</a>
 *
 */
public class ResourceGroupNamespaceConfigListener implements Consumer<Notification> {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceGroupNamespaceConfigListener.class);
    private final ResourceGroupService rgService;
    private final PulsarService pulsarService;
    private final NamespaceResources namespaceResources;
    private final TenantResources tenantResources;
    private final ResourceGroupConfigListener rgConfigListener;

    public ResourceGroupNamespaceConfigListener(ResourceGroupService rgService, PulsarService pulsarService,
            ResourceGroupConfigListener rgConfigListener) {
        this.rgService = rgService;
        this.pulsarService = pulsarService;
        this.namespaceResources = pulsarService.getPulsarResources().getNamespaceResources();
        this.tenantResources = pulsarService.getPulsarResources().getTenantResources();
        this.rgConfigListener = rgConfigListener;
        loadAllNamespaceResourceGroups();
        this.namespaceResources.getStore().registerListener(this);
    }

    private void updateNamespaceResourceGroup(NamespaceName nsName) {
        namespaceResources.getPoliciesAsync(nsName).whenCompleteAsync((optionalPolicies, ex) -> {
            if (ex != null) {
                LOG.error("Exception when getting namespace {}", nsName, ex);
                return;
            }
            Policies policy = optionalPolicies.get();
            reconcileNamespaceResourceGroup(nsName, policy);
        });
    }

    private void loadAllNamespaceResourceGroups() {
        tenantResources.listTenantsAsync().whenComplete((tenantList, ex) -> {
            if (ex != null) {
                LOG.error("Exception when fetching tenants", ex);
                return;
            }
            for (String ts: tenantList) {
                namespaceResources.listNamespacesAsync(ts).whenComplete((nsList, ex1) -> {
                    if (ex1 != null) {
                        LOG.error("Exception when fetching namespaces", ex1);
                    } else {
                        for (String ns : nsList) {
                            NamespaceName nsn = NamespaceName.get(ts, ns);
                            namespaceResources.namespaceExistsAsync(nsn)
                                    .thenAccept(exists -> {
                                        if (exists) {
                                            updateNamespaceResourceGroup(NamespaceName.get(ts, ns));
                                        }
                                    });
                        }
                    }
                });
            }
        });
    }

    public void reloadAllNamespaceResourceGroups() {
        loadAllNamespaceResourceGroups();
    }

    public void reconcileNamespaceResourceGroup(NamespaceName ns, Policies policy) {
        boolean delete = false, add = false;
        org.apache.pulsar.broker.resourcegroup.ResourceGroup current = rgService
                .getNamespaceResourceGroup(ns);

        if (policy == null || policy.resource_group_name == null) {
            if (current != null) {
                delete = true;
            }
        } else {
            if (current == null) {
                add = true;
            }
            if (current != null && !policy.resource_group_name.equals(current.resourceGroupName)) {
                delete = true;
            }
        }
        try {
            if (delete) {
                LOG.info("Unregistering namespace {} from resource group {}", ns, current.resourceGroupName);
                rgService.unRegisterNameSpace(current.resourceGroupName, ns);
            }
            if (add) {
                LOG.info("Registering namespace {} with resource group {}", ns, policy.resource_group_name);
                rgService.registerNameSpace(policy.resource_group_name, ns);
            }
        } catch (PulsarAdminException e) {
            LOG.error("Failed to {} namespace {} with resource group {}",
                    delete ? "unregister" : "register", ns, policy.resource_group_name, e);
        }
    }

    @Override
    public void accept(Notification notification) {
        String notifyPath = notification.getPath();

        if (!NamespaceResources.pathIsFromNamespace(notifyPath)) {
            return;
        }
        String[] parts = notifyPath.split("/");
        if (parts.length < 4) {
            // We are only interested in the tenant and namespace level notifications
            return;
        }
        LOG.info("Metadata store notification: Path {}, Type {}", notifyPath, notification.getType());

        if (parts.length == 4 && notification.getType() == NotificationType.ChildrenChanged) {
            // Namespace add/delete
            reloadAllNamespaceResourceGroups();
        } else if (parts.length == 5) {
            switch (notification.getType()) {
                case Modified: {
                    NamespaceName nsName = NamespaceResources.namespaceFromPath(notifyPath);
                    updateNamespaceResourceGroup(nsName);
                    break;
                }
                case Deleted: {
                    NamespaceName nsName = NamespaceResources.namespaceFromPath(notifyPath);
                    ResourceGroup rg = rgService
                            .getNamespaceResourceGroup(nsName);
                    if (rg != null) {
                        try {
                            rgService.unRegisterNameSpace(rg.resourceGroupName, nsName);
                        } catch (PulsarAdminException e) {
                            LOG.error("Failed to unregister namespace", e);
                        }
                    }
                    break;
                }
            default:
                break;
            }
        }
    }
}