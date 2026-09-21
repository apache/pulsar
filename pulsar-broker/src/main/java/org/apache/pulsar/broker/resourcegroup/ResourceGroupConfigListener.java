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
package org.apache.pulsar.broker.resourcegroup;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.ResourceGroupResources;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;

/**
 * Resource Group Config Listener
 *
 * <P>Meta data store listener of updates to resource group config.
 * <P>Listens to resource group configuration changes and updates internal datastructures.
 *
 * @see <a href="https://github.com/apache/pulsar/wiki/PIP-82%3A-Tenant-and-namespace-level-rate-limiting">Global-quotas</a>
 *
 */
@CustomLog
public class ResourceGroupConfigListener implements Consumer<Notification> {
    private final ResourceGroupService rgService;
    private final PulsarService pulsarService;
    private final ResourceGroupResources rgResources;
    private volatile ResourceGroupNamespaceConfigListener rgNamespaceConfigListener;

    public ResourceGroupConfigListener(ResourceGroupService rgService, PulsarService pulsarService) {
        this.rgService = rgService;
        this.pulsarService = pulsarService;
        this.rgResources = pulsarService.getPulsarResources().getResourcegroupResources();
        this.rgResources.getStore().registerListener(this);
        execute(() -> loadAllResourceGroupsWithRetryAsync(0));
    }

    private void loadAllResourceGroupsWithRetryAsync(long retry) {
        loadAllResourceGroupsAsync().thenAccept(__ -> {
            if (rgNamespaceConfigListener == null) {
                rgNamespaceConfigListener = new ResourceGroupNamespaceConfigListener(rgService, pulsarService, this);
            }
        }).exceptionally(e -> {
            long nextRetry = retry + 1;
            long delay = 500 * nextRetry;
            log.error()
                    .attr("afterMs", delay)
                    .exception(e)
                    .log("Failed to load all resource groups during initialization, retrying later");
            schedule(() -> loadAllResourceGroupsWithRetryAsync(nextRetry), delay);
            return null;
        });
    }

    private CompletableFuture<Void> loadAllResourceGroupsAsync() {
        return rgResources.listResourceGroupsAsync().thenCompose(rgList -> {
            final Set<String> existingSet = rgService.resourceGroupGetAll();
            HashSet<String> newSet = new HashSet<>();

            newSet.addAll(rgList);

            final Sets.SetView<String> deleteList = Sets.difference(existingSet, newSet);

            for (String rgName : deleteList) {
                deleteResourceGroup(rgName);
            }

            final Sets.SetView<String> addList = Sets.difference(newSet, existingSet);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String rgName : addList) {
                futures.add(pulsarService.getPulsarResources()
                        .getResourcegroupResources()
                        .getResourceGroupAsync(rgName)
                        .thenAccept(optionalRg -> {
                            if (optionalRg.isPresent()) {
                                ResourceGroup rg = optionalRg.get();
                                createResourceGroup(rgName, rg);
                            }
                        })
                );
            }

            return FutureUtil.waitForAll(futures);
        });
    }

    public synchronized void deleteResourceGroup(String rgName) {
        try {
            if (rgService.resourceGroupGet(rgName) != null) {
                log.info().attr("resourceGroup", rgName).log("Deleting resource group");
                rgService.resourceGroupDelete(rgName);
            }
        } catch (PulsarAdminException e) {
            log.error().attr("resourceGroup", rgName).exceptionMessage(e)
                    .log("Got exception while deleting resource group");
        }
    }

    public synchronized void createResourceGroup(String rgName, ResourceGroup rg) {
        if (rgService.resourceGroupGet(rgName) == null) {
            log.info().attr("resourceGroup", rgName).attr("value", rg).log("Creating resource group");
            try {
                rgService.resourceGroupCreate(rgName, rg);
            } catch (PulsarAdminException ex1) {
                log.error().attr("resourceGroup", rgName).exception(ex1).log("Got an exception while creating RG");
            }
        }
    }

    private void updateResourceGroup(String rgName) {
        rgResources.getResourceGroupAsync(rgName).whenComplete((optionalRg, ex) -> {
            if (ex != null) {
                log.error().attr("resourceGroup", rgName).exception(ex).log("Exception when getting resource group");
                return;
            }
            ResourceGroup rg = optionalRg.get();
            try {
                log.info().attr("resourceGroup", rgName).attr("value", rg).log("Updating resource group");
                rgService.resourceGroupUpdate(rgName, rg);
            } catch (PulsarAdminException ex1) {
                log.error().attr("resourceGroup", rgName).exception(ex1)
                        .log("Got an exception while creating resource group");
            }
        });
    }

    @Override
    public void accept(Notification notification) {
        String notifyPath = notification.getPath();

        if (!ResourceGroupResources.isResourceGroupPath(notifyPath)) {
            return;
        }
        log.info()
                .attr("path", notifyPath)
                .attr("type", notification.getType())
                .log("Metadata store notification: Path , Type");

        Optional<String> rgName = ResourceGroupResources.resourceGroupNameFromPath(notifyPath);
        if ((notification.getType() == NotificationType.ChildrenChanged)
            || (notification.getType() == NotificationType.Created)) {
            loadAllResourceGroupsAsync().exceptionally((ex) -> {
                log.error().exception(ex).log("Exception when fetching resource groups");
                return null;
            });
        } else if (rgName.isPresent()) {
            switch (notification.getType()) {
            case Modified:
                updateResourceGroup(rgName.get());
                break;
            default:
                break;
            }
        }
    }

    protected void execute(Runnable runnable) {
        pulsarService.getExecutor().execute(catchingAndLoggingThrowables(runnable));
    }

    protected void schedule(Runnable runnable, long delayMs) {
        pulsarService.getExecutor().schedule(catchingAndLoggingThrowables(runnable), delayMs, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    ResourceGroupNamespaceConfigListener getRgNamespaceConfigListener() {
        return rgNamespaceConfigListener;
    }
}