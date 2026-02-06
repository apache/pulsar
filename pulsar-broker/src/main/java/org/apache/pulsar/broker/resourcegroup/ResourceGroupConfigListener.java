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
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.ResourceGroupResources;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource Group Config Listener
 *
 * <P>Meta data store listener of updates to resource group config.
 * <P>Listens to resource group configuration changes and updates internal datastructures.
 *
 * @see <a href="https://github.com/apache/pulsar/wiki/PIP-82%3A-Tenant-and-namespace-level-rate-limiting">Global-quotas</a>
 *
 */
public class ResourceGroupConfigListener implements Consumer<Notification> {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceGroupConfigListener.class);
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
            LOG.error("Failed to load all resource groups during initialization, retrying after {}ms: ", delay, e);
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
                LOG.info("Deleting resource group {}", rgName);
                rgService.resourceGroupDelete(rgName);
            }
        } catch (PulsarAdminException e) {
            LOG.error("Got exception while deleting resource group {}, {}", rgName, e);
        }
    }

    public synchronized void createResourceGroup(String rgName, ResourceGroup rg) {
        if (rgService.resourceGroupGet(rgName) == null) {
            LOG.info("Creating resource group {}, {}", rgName, rg.toString());
            try {
                rgService.resourceGroupCreate(rgName, rg);
            } catch (PulsarAdminException ex1) {
                LOG.error("Got an exception while creating RG {}", rgName, ex1);
            }
        }
    }

    private void updateResourceGroup(String rgName) {
        rgResources.getResourceGroupAsync(rgName).whenComplete((optionalRg, ex) -> {
            if (ex != null) {
                LOG.error("Exception when getting resource group {}", rgName, ex);
                return;
            }
            ResourceGroup rg = optionalRg.get();
            try {
                LOG.info("Updating resource group {}, {}", rgName, rg);
                rgService.resourceGroupUpdate(rgName, rg);
            } catch (PulsarAdminException ex1) {
                LOG.error("Got an exception while creating resource group {}", rgName, ex1);
            }
        });
    }

    @Override
    public void accept(Notification notification) {
        String notifyPath = notification.getPath();

        if (!ResourceGroupResources.isResourceGroupPath(notifyPath)) {
            return;
        }
        LOG.info("Metadata store notification: Path {}, Type {}", notifyPath, notification.getType());

        Optional<String> rgName = ResourceGroupResources.resourceGroupNameFromPath(notifyPath);
        if ((notification.getType() == NotificationType.ChildrenChanged)
            || (notification.getType() == NotificationType.Created)) {
            loadAllResourceGroupsAsync().exceptionally((ex) -> {
                LOG.error("Exception when fetching resource groups", ex);
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