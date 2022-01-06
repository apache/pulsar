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
package org.apache.pulsar.broker.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.service.MetadataChangeEvent.EventType;
import org.apache.pulsar.broker.service.MetadataChangeEvent.ResourceType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataStoreSynchronizer extends AbstractMetadataStoreSynchronizer {

    private static final Logger log = LoggerFactory.getLogger(MetadataStoreSynchronizer.class);

    public MetadataStoreSynchronizer(PulsarService pulsar) throws PulsarServerException {
        super(pulsar);
    }

    @Override
    public CompletableFuture<Void> updateNamespaceMetadata(MetadataChangeEvent event) {
        CompletableFuture<Void> updateResult = new CompletableFuture<>();
        NamespaceName namespaceName;
        Policies policies;
        try {
            namespaceName = NamespaceName.get(event.getResourceName());
            policies = ObjectMapperFactory.getThreadLocal().readValue(event.getData(), Policies.class);
        } catch (Exception e) {
            log.error("Failed to deserialize namespace-metadata event {}", topicName, e);
            updateResult.complete(null);
            return updateResult;
        }

        pulsar.getPulsarResources().getNamespaceResources().getPoliciesAsync(namespaceName).thenAccept(old -> {
            Policies existingNamespace = old.isPresent() ? old.get() : null;
            CompletableFuture<Void> result = null;
            EventType type = event.getType() != EventType.Synchronize ? event.getType()
                    : (old.isPresent() ? EventType.Modified : EventType.Created);
            switch (type) {
            case Created:
                if (existingNamespace == null) {
                    result = pulsar.getPulsarResources().getNamespaceResources().createPoliciesAsync(namespaceName,
                            policies);
                } else {
                    log.info("skip change-event, namespace {} already exist", namespaceName);
                }
                break;
            case Modified:
                if (existingNamespace != null
                // latest remote update or lexicographical-sorting on clustername if timestamp is same
                        && ((existingNamespace.lastUpdatedTimestamp < policies.lastUpdatedTimestamp)
                                || (event.getSourceCluster() != null
                                        && existingNamespace.lastUpdatedTimestamp == policies.lastUpdatedTimestamp
                                        && pulsar.getConfig().getClusterName()
                                                .compareTo(event.getSourceCluster()) < 0))) {
                    result = pulsar.getPulsarResources().getNamespaceResources().setPoliciesAsync(namespaceName, __ -> {
                        return policies;
                    });
                } else {
                    log.info("skip change-event with {} for namespace {} updated at {}", policies.lastUpdatedTimestamp,
                            namespaceName, existingNamespace != null ? existingNamespace.lastUpdatedTimestamp : null);
                }
                break;
            case Deleted:
                if (existingNamespace != null && existingNamespace.lastUpdatedTimestamp < event.getEventTime()) {
                    result = pulsar.getPulsarResources().getNamespaceResources().deletePoliciesAsync(namespaceName);
                }
                break;
            default:
                log.info("Skipped unknown namespace update with {}", event);
            }
            if (result != null) {
                result.thenAccept(__ -> {
                    log.info("successfully updated namespace-event {}", event);
                    updateResult.complete(null);
                }).exceptionally(ue -> {
                    log.warn("Failed while updating namespace metadata {}", event, ue);
                    updateResult.completeExceptionally(ue);
                    return null;
                });
            } else {
                updateResult.complete(null);
            }
        }).exceptionally(ex -> {
            log.warn("Failed to update namespace metadata {}", event, ex);
            updateResult.completeExceptionally(ex);
            return null;
        });
        return updateResult;
    }

    @Override
    public CompletableFuture<Void> updateTenantMetadata(MetadataChangeEvent event) {
        CompletableFuture<Void> updateResult = new CompletableFuture<>();
        String tenantName = event.getResourceName();
        TenantInfoImpl tenant;
        try {
            tenant = ObjectMapperFactory.getThreadLocal().readValue(event.getData(), TenantInfoImpl.class);
        } catch (Exception e) {
            log.error("Failed to deserialize metadata event {}", topicName, e);
            updateResult.complete(null);
            return updateResult;
        }

        pulsar.getPulsarResources().getTenantResources().getTenantAsync(tenantName).thenAccept(old -> {
            TenantInfoImpl existingTenant = old.isPresent() ? (TenantInfoImpl) old.get() : null;
            CompletableFuture<Void> result = null;
            EventType type = event.getType() != EventType.Synchronize ? event.getType()
                    : (old.isPresent() ? EventType.Modified : EventType.Created);
            switch (type) {
            case Created:
                if (existingTenant == null) {
                    result = pulsar.getPulsarResources().getTenantResources().createTenantAsync(tenantName, tenant);
                }
                break;
            case Modified:
                if (existingTenant != null
                     // latest remote update or lexicographical-sorting on clustername if timestamp is same
                        && ((existingTenant.getLastUpdatedTimestamp() < tenant.getLastUpdatedTimestamp())
                                || (event.getSourceCluster() != null
                                        && (existingTenant.getLastUpdatedTimestamp() == tenant.getLastUpdatedTimestamp()
                                                && pulsar.getConfig().getClusterName()
                                                        .compareTo(event.getSourceCluster()) < 0)))) {
                    result = pulsar.getPulsarResources().getTenantResources().updateTenantAsync(tenantName, __ -> {
                        return tenant;
                    });
                } else {
                    log.info("skip change-event with {} for tenant {} updated at {}", tenant.getLastUpdatedTimestamp(),
                            tenantName, existingTenant != null ? existingTenant.getLastUpdatedTimestamp() : null);
                }

                break;
            case Deleted:
                if (existingTenant != null && existingTenant.getLastUpdatedTimestamp() < event.getEventTime()) {
                    result = pulsar.getPulsarResources().getTenantResources().deleteTenantAsync(tenantName);
                }
                break;
            default:
                log.info("Skipped tenant update with {}", event);
            }
            if (result != null) {
                result.thenAccept(__ -> {
                    log.info("successfully updated tenant-event {}", event);
                    updateResult.complete(null);
                }).exceptionally(ue -> {
                    log.warn("Failed while updating tenant metadata {}", event, ue);
                    updateResult.completeExceptionally(ue);
                    return null;
                });
            } else {
                updateResult.complete(null);
            }
        }).exceptionally(ex -> {
            log.warn("Failed to update tenant metadata {}", event, ex);
            updateResult.completeExceptionally(ex);
            return null;
        });
        return updateResult;
    }

    @Override
    public CompletableFuture<Void> updatePartitionMetadata(MetadataChangeEvent event) {
        CompletableFuture<Void> updateResult = new CompletableFuture<>();
        String topic = event.getResourceName();
        PartitionedTopicMetadata partitions;
        TopicName topicName;
        try {
            topicName = TopicName.get(topic);
            partitions = ObjectMapperFactory.getThreadLocal().readValue(event.getData(),
                    PartitionedTopicMetadata.class);
        } catch (Exception e) {
            log.error("Failed to deserialize metadata event {}", event, e);
            updateResult.complete(null);
            return updateResult;
        }

        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(topicName).thenAccept(old -> {
                    PartitionedTopicMetadata existingPartitions = old.isPresent() ? old.get() : null;
                    CompletableFuture<Void> result = null;
                    try {
                        switch (event.getType()) {
                        case Created:
                            if (existingPartitions == null) {
                                result = pulsar.getAdminClient().topics().createPartitionedTopicAsync(topic,
                                        partitions.partitions);
                            }
                            break;
                        case Modified:
                            if (existingPartitions != null
                                 // latest remote update or lexicographical-sorting on clustername if timestamp is same
                                    && ((existingPartitions.lastUpdatedTimestamp < partitions.lastUpdatedTimestamp)
                                            || (event.getSourceCluster() != null
                                                    && existingPartitions.lastUpdatedTimestamp
                                                    == partitions.lastUpdatedTimestamp
                                                    && pulsar.getConfig().getClusterName()
                                                            .compareTo(event.getSourceCluster()) < 0))) {
                                result = pulsar.getAdminClient().topics().updatePartitionedTopicAsync(topic,
                                        partitions.partitions);
                            } else {
                                log.info("skip change-event with {} for partitions {} updated at {}",
                                        partitions.lastUpdatedTimestamp, topic,
                                        existingPartitions != null ? existingPartitions.lastUpdatedTimestamp : null);
                            }
                            break;
                        case Deleted:
                            if (existingPartitions != null
                                    && existingPartitions.lastUpdatedTimestamp < event.getEventTime()) {
                                result = pulsar.getAdminClient().topics().deletePartitionedTopicAsync(topic);
                            }
                            break;
                        default:
                            log.info("Skipped partitioned update with {}", event);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to get admin-client while updating partitioned metadata {}", event, e);
                        updateResult.completeExceptionally(e);
                        return;
                    }
                    if (result != null) {
                        result.thenAccept(__ -> {
                            log.info("successfully updated partitioned-event {}", event);
                            updateResult.complete(null);
                        }).exceptionally(ue -> {
                            log.warn("Failed while updating partitioned metadata {}", event, ue);
                            updateResult.completeExceptionally(ue);
                            return null;
                        });
                    } else {
                        updateResult.complete(null);
                    }
                }).exceptionally(ex -> {
                    log.warn("Failed to update partitioned metadata {}", event, ex);
                    updateResult.completeExceptionally(ex);
                    return null;
                });

        return updateResult;
    }

    @Override
    public void triggerSyncSnapshot() {
        pulsar.getPulsarResources().getTenantResources().listTenantsAsync().thenAccept(tenants -> {
            tenants.forEach(tenant -> {
                if (log.isDebugEnabled()) {
                    log.info("triggering tenant {} metadata-event", tenant);
                }
                publishAsync(TenantResources.getPath(tenant), ResourceType.Tenants, tenant, EventType.Synchronize);
                brokerService.executor().execute(() -> {
                    try {
                        List<String> namespaces = pulsar.getPulsarResources().getTenantResources()
                                .getListOfNamespaces(tenant);
                        namespaces.forEach(ns -> {
                            if (log.isDebugEnabled()) {
                                log.info("triggering namespace {} metadata-event", ns);
                            }
                            publishAsync(NamespaceResources.getPath(ns), ResourceType.Namespaces, ns,
                                    EventType.Synchronize);
                        });
                    } catch (Exception e) {
                        log.warn("Failed to get list of namesapces for {}, {}", tenant, e.getMessage());
                    }
                });
            });
        });
    }

}
