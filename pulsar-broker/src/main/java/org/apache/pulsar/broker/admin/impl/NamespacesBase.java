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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.policies.data.PoliciesUtil.defaultBundle;
import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.Policies.BundleType;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicHashPositions;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.ValidateResult;
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.zookeeper.KeeperException;

@Slf4j
public abstract class NamespacesBase extends AdminResource {

    protected CompletableFuture<List<String>> internalGetTenantNamespaces(String tenant) {
        if (tenant == null) {
            return FutureUtil.failedFuture(new RestException(Status.BAD_REQUEST, "Tenant should not be null"));
        }
        try {
            NamedEntity.checkName(tenant);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Tenant name is invalid {}", clientAppId(), tenant, e);
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED, "Tenant name is not valid"));
        }
        return validateTenantOperationAsync(tenant, TenantOperation.LIST_NAMESPACES)
                .thenCompose(__ -> tenantResources().tenantExistsAsync(tenant))
                .thenCompose(existed -> {
                    if (!existed) {
                        throw new RestException(Status.NOT_FOUND, "Tenant not found");
                    }
                    return tenantResources().getListOfNamespacesAsync(tenant);
                });
    }

    protected CompletableFuture<Void> internalCreateNamespace(Policies policies) {
        return validateTenantOperationAsync(namespaceName.getTenant(), TenantOperation.CREATE_NAMESPACE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> validatePoliciesAsync(namespaceName, policies))
                .thenCompose(__ -> {
                    int maxNamespacesPerTenant = pulsar().getConfiguration().getMaxNamespacesPerTenant();
                    // no distributed locks are added here.In a concurrent scenario, the threshold will be exceeded.
                    if (maxNamespacesPerTenant > 0) {
                        return tenantResources().getListOfNamespacesAsync(namespaceName.getTenant())
                                .thenAccept(namespaces -> {
                                    if (namespaces != null && namespaces.size() > maxNamespacesPerTenant) {
                                        throw new RestException(Status.PRECONDITION_FAILED,
                                                "Exceed the maximum number of namespace in tenant :"
                                                        + namespaceName.getTenant());
                                    }
                                });
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .thenCompose(__ -> namespaceResources().createPoliciesAsync(namespaceName, policies))
                .thenAccept(__ -> log.info("[{}] Created namespace {}", clientAppId(), namespaceName));
    }

    protected CompletableFuture<List<String>> internalGetListOfTopics(Policies policies,
                                                                      CommandGetTopicsOfNamespace.Mode mode) {
        switch (mode) {
            case ALL:
                return pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName)
                        .thenCombine(internalGetNonPersistentTopics(policies),
                                (persistentTopics, nonPersistentTopics) ->
                                        ListUtils.union(persistentTopics, nonPersistentTopics));
            case NON_PERSISTENT:
                return internalGetNonPersistentTopics(policies);
            case PERSISTENT:
            default:
                return pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName);
        }
    }

    protected CompletableFuture<List<String>> internalGetNonPersistentTopics(Policies policies) {
        final List<CompletableFuture<List<String>>> futures = new ArrayList<>();
        final List<String> boundaries = policies.bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            final String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            try {
                futures.add(pulsar().getAdminClient().topics()
                        .getListInBundleAsync(namespaceName.toString(), bundle));
            } catch (PulsarServerException e) {
                throw new RestException(e);
            }
        }
        return FutureUtil.waitForAll(futures)
                .thenApply(__ -> {
                    final List<String> topics = new ArrayList<>();
                    for (int i = 0; i < futures.size(); i++) {
                        List<String> topicList = futures.get(i).join();
                        if (topicList != null) {
                            topics.addAll(topicList);
                        }
                    }
                    return topics.stream().filter(name -> !TopicName.get(name).isPersistent())
                            .collect(Collectors.toList());
                });
    }

    /**
     * Delete the namespace and retry to resolve some topics that were not created successfully(in metadata)
     * during the deletion.
     */
    protected @Nonnull CompletableFuture<Void> internalDeleteNamespaceAsync(boolean force) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        internalRetryableDeleteNamespaceAsync0(force, 5, future);
        return future;
    }
    private void internalRetryableDeleteNamespaceAsync0(boolean force, int retryTimes,
                                                        @Nonnull CompletableFuture<Void> callback) {
        precheckWhenDeleteNamespace(namespaceName, force)
                .thenCompose(policies -> {
                    final CompletableFuture<List<String>> topicsFuture;
                    if (policies == null || CollectionUtils.isEmpty(policies.replication_clusters)){
                        topicsFuture = pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName);
                    } else {
                        topicsFuture = pulsar().getNamespaceService().getFullListOfTopics(namespaceName);
                    }
                    return topicsFuture.thenCompose(allTopics ->
                            pulsar().getNamespaceService().getFullListOfPartitionedTopic(namespaceName)
                                    .thenCompose(allPartitionedTopics -> {
                                        List<List<String>> topicsSum = new ArrayList<>(2);
                                        topicsSum.add(allTopics);
                                        topicsSum.add(allPartitionedTopics);
                                        return CompletableFuture.completedFuture(topicsSum);
                                    }))
                            .thenCompose(topics -> {
                                List<String> allTopics = topics.get(0);
                                Set<String> allUserCreatedTopics = new HashSet<>();
                                List<String> allPartitionedTopics = topics.get(1);
                                Set<String> allUserCreatedPartitionTopics = new HashSet<>();
                                boolean hasNonSystemTopic = false;
                                Set<String> allSystemTopics = new HashSet<>();
                                Set<String> allPartitionedSystemTopics = new HashSet<>();
                                Set<String> topicPolicy = new HashSet<>();
                                Set<String> partitionedTopicPolicy = new HashSet<>();
                                for (String topic : allTopics) {
                                    if (!pulsar().getBrokerService().isSystemTopic(TopicName.get(topic))) {
                                        hasNonSystemTopic = true;
                                        allUserCreatedTopics.add(topic);
                                    } else {
                                        if (SystemTopicNames.isTopicPoliciesSystemTopic(topic)) {
                                            topicPolicy.add(topic);
                                        } else if (!isDeletedAlongWithUserCreatedTopic(topic)) {
                                            allSystemTopics.add(topic);
                                        }
                                    }
                                }
                                for (String topic : allPartitionedTopics) {
                                    if (!pulsar().getBrokerService().isSystemTopic(TopicName.get(topic))) {
                                        hasNonSystemTopic = true;
                                        allUserCreatedPartitionTopics.add(topic);
                                    } else {
                                        if (SystemTopicNames.isTopicPoliciesSystemTopic(topic)) {
                                            partitionedTopicPolicy.add(topic);
                                        } else {
                                            allPartitionedSystemTopics.add(topic);
                                        }
                                    }
                                }
                                if (!force) {
                                    if (hasNonSystemTopic) {
                                        throw new RestException(Status.CONFLICT, "Cannot delete non empty namespace");
                                    }
                                }
                                final CompletableFuture<Void> markDeleteFuture;
                                if (policies != null && policies.deleted) {
                                    markDeleteFuture = CompletableFuture.completedFuture(null);
                                } else {
                                    markDeleteFuture = namespaceResources().setPoliciesAsync(namespaceName, old -> {
                                        old.deleted = true;
                                        return old;
                                    });
                                }
                                return markDeleteFuture.thenCompose(__ ->
                                                internalDeleteTopicsAsync(allUserCreatedTopics))
                                        .thenCompose(ignore ->
                                                internalDeletePartitionedTopicsAsync(allUserCreatedPartitionTopics))
                                        .thenCompose(ignore ->
                                                internalDeleteTopicsAsync(allSystemTopics))
                                        .thenCompose(ignore ->
                                                internalDeletePartitionedTopicsAsync(allPartitionedSystemTopics))
                                        .thenCompose(ignore ->
                                                internalDeleteTopicsAsync(topicPolicy))
                                        .thenCompose(ignore ->
                                                internalDeletePartitionedTopicsAsync(partitionedTopicPolicy));
                            });
                })
                .thenCompose(ignore -> pulsar().getNamespaceService()
                        .getNamespaceBundleFactory().getBundlesAsync(namespaceName))
                .thenCompose(bundles -> FutureUtil.waitForAll(bundles.getBundles().stream()
                        .map(bundle -> pulsar().getNamespaceService().checkOwnershipPresentAsync(bundle)
                                .thenCompose(present -> {
                                    // check if the bundle is owned by any broker,
                                    // if not then we do not need to delete the bundle
                                    if (present) {
                                        PulsarAdmin admin;
                                        try {
                                            admin = pulsar().getAdminClient();
                                        } catch (PulsarServerException ex) {
                                            log.error("[{}] Get admin client error when preparing to delete topics.",
                                                    clientAppId(), ex);
                                            return FutureUtil.failedFuture(ex);
                                        }
                                        log.info("[{}] Deleting namespace bundle {}/{}", clientAppId(),
                                                namespaceName, bundle.getBundleRange());
                                        return admin.namespaces().deleteNamespaceBundleAsync(namespaceName.toString(),
                                                bundle.getBundleRange(), force);
                                    } else {
                                        log.warn("[{}] Skipping deleting namespace bundle {}/{} "
                                                        + "as it's not owned by any broker",
                                                clientAppId(), namespaceName, bundle.getBundleRange());
                                    }
                                    return CompletableFuture.completedFuture(null);
                                })
                        ).collect(Collectors.toList())))
                .thenCompose(ignore -> internalClearZkSources())
                .whenComplete((result, error) -> {
                    if (error != null) {
                        final Throwable rc = FutureUtil.unwrapCompletionException(error);
                        if (rc instanceof MetadataStoreException) {
                            if (rc.getCause() != null && rc.getCause() instanceof KeeperException.NotEmptyException) {
                                KeeperException.NotEmptyException ne =
                                        (KeeperException.NotEmptyException) rc.getCause();
                                log.info("[{}] There are in-flight topics created during the namespace deletion, "
                                        + "retry to delete the namespace again. (path {} is not empty on metadata)",
                                        namespaceName, ne.getPath());
                                final int next = retryTimes - 1;
                                if (next > 0) {
                                    // async recursive
                                    internalRetryableDeleteNamespaceAsync0(force, next, callback);
                                } else {
                                    callback.completeExceptionally(
                                            new RestException(Status.CONFLICT, "The broker still have in-flight topics"
                                                    + " created during namespace deletion (path " + ne.getPath() + ") "
                                                    + "is not empty on metadata store, please try again."));
                                    // drop out recursive
                                }
                                return;
                            }
                        }
                        callback.completeExceptionally(error);
                        return;
                    }
                    callback.complete(result);
                });
    }

    private boolean isDeletedAlongWithUserCreatedTopic(String topic) {
        // The transaction pending ack topic will be deleted while topic unsubscribe corresponding subscription.
        return topic.endsWith(SystemTopicNames.PENDING_ACK_STORE_SUFFIX);
    }

    private CompletableFuture<Void> internalDeletePartitionedTopicsAsync(Set<String> topicNames) {
        if (CollectionUtils.isEmpty(topicNames)) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String topicName : topicNames) {
            TopicName tn = TopicName.get(topicName);
            futures.add(pulsar().getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                    .runWithMarkDeleteAsync(tn,
                            () -> namespaceResources().getPartitionedTopicResources().deletePartitionedTopicAsync(tn)));
        }
        return FutureUtil.waitForAll(futures);
    }

    private CompletableFuture<Void> internalDeleteTopicsAsync(Set<String> topicNames) {
        if (CollectionUtils.isEmpty(topicNames)) {
            return CompletableFuture.completedFuture(null);
        }
        PulsarAdmin admin;
        try {
            admin = pulsar().getAdminClient();
        } catch (Exception ex) {
            log.error("[{}] Get admin client error when preparing to delete topics.", clientAppId(), ex);
            return FutureUtil.failedFuture(ex);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String topicName : topicNames) {
            futures.add(admin.topics().deleteAsync(topicName, true));
        }
        return FutureUtil.waitForAll(futures);
    }

    private CompletableFuture<Policies> precheckWhenDeleteNamespace(NamespaceName nsName, boolean force) {
        CompletableFuture<Policies> preconditionCheck =
                validateTenantOperationAsync(nsName.getTenant(), TenantOperation.DELETE_NAMESPACE)
                        .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                        .thenCompose(__ -> {
                            if (force && !pulsar().getConfiguration().isForceDeleteNamespaceAllowed()) {
                                throw new RestException(Status.METHOD_NOT_ALLOWED,
                                        "Broker doesn't allow forced deletion of namespaces");
                            }
                            // ensure that non-global namespace is directed to the correct cluster
                            if (!nsName.isGlobal()) {
                                return validateClusterOwnershipAsync(nsName.getCluster());
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        })
                        .thenCompose(__ -> namespaceResources().getPoliciesAsync(nsName))
                        .thenCompose(policiesOpt -> {
                            if (policiesOpt.isEmpty()) {
                                throw new RestException(Status.NOT_FOUND, "Namespace " + nsName + " does not exist.");
                            }
                            if (!nsName.isGlobal()) {
                                return CompletableFuture.completedFuture(null);
                            }
                            Policies policies = policiesOpt.get();
                            Set<String> replicationClusters = policies.replication_clusters;
                            if (replicationClusters.size() > 1) {
                                // There are still more than one clusters configured for the global namespace
                                throw new RestException(Status.PRECONDITION_FAILED,
                                        "Cannot delete the global namespace " + nsName + ". There are still more than "
                                        + "one replication clusters configured.");
                            }
                            if (replicationClusters.size() == 1
                                    && !policies.replication_clusters.contains(config().getClusterName())) {
                                // the only replication cluster is other cluster, redirect
                                String replCluster = new ArrayList<>(policies.replication_clusters).get(0);
                                return clusterResources().getClusterAsync(replCluster)
                                        .thenCompose(replClusterDataOpt -> {
                                            ClusterData replClusterData = replClusterDataOpt
                                                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                                            "Cluster " + replCluster + " does not exist"));
                                            URL replClusterUrl;
                                            try {
                                                if (!config().isTlsEnabled() || !isRequestHttps()) {
                                                    replClusterUrl = new URL(replClusterData.getServiceUrl());
                                                } else if (StringUtils.isNotBlank(replClusterData.getServiceUrlTls())) {
                                                    replClusterUrl = new URL(replClusterData.getServiceUrlTls());
                                                } else {
                                                    throw new RestException(Status.PRECONDITION_FAILED,
                                                    "The replication cluster does not provide TLS encrypted service");
                                                }
                                            } catch (MalformedURLException checkedEx) {
                                                throw new RestException(checkedEx);
                                            }
                                            URI redirect = UriBuilder.fromUri(uri.getRequestUri())
                                                    .host(replClusterUrl.getHost())
                                                    .port(replClusterUrl.getPort())
                                                    .replaceQueryParam("authoritative", false).build();
                                            if (log.isDebugEnabled()) {
                                                log.debug("[{}] Redirecting the rest call to {}: cluster={}",
                                                        clientAppId(), redirect, replCluster);
                                            }
                                            throw new WebApplicationException(
                                                    Response.temporaryRedirect(redirect).build());
                                        });
                            }
                            return CompletableFuture.completedFuture(policies);
                        });
        return preconditionCheck;
    }

    // clear zk-node resources for deleting namespace
    protected CompletableFuture<Void> internalClearZkSources() {
        // clear resource of `/namespace/{namespaceName}` for zk-node
        return namespaceResources().deleteNamespaceAsync(namespaceName)
                .thenCompose(ignore -> namespaceResources().getPartitionedTopicResources()
                        .clearPartitionedTopicMetadataAsync(namespaceName))
                // clear resource for manager-ledger z-node
                .thenCompose(ignore -> pulsar().getPulsarResources().getTopicResources()
                        .clearDomainPersistence(namespaceName))
                .thenCompose(ignore -> pulsar().getPulsarResources().getTopicResources()
                        .clearNamespacePersistence(namespaceName))
                // we have successfully removed all the ownership for the namespace, the policies
                // z-node can be deleted now
                .thenCompose(ignore -> namespaceResources().deletePoliciesAsync(namespaceName))
                // clear z-node of local policies
                .thenCompose(ignore -> getLocalPolicies().deleteLocalPoliciesAsync(namespaceName))
                // clear /loadbalance/bundle-data
                .thenCompose(ignore -> namespaceResources().deleteBundleDataAsync(namespaceName));

    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalDeleteNamespaceBundleAsync(String bundleRange, boolean authoritative,
                                                                         boolean force) {
        log.info("[{}] Deleting namespace bundle {}/{} authoritative:{} force:{}",
                clientAppId(), namespaceName, bundleRange, authoritative, force);
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.DELETE_BUNDLE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    if (!namespaceName.isGlobal()) {
                        return validateClusterOwnershipAsync(namespaceName.getCluster());
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies -> {
                    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
                    if (namespaceName.isGlobal()) {

                        if (policies.replication_clusters.size() > 1) {
                            // There are still more than one clusters configured for the global namespace
                            throw new RestException(Status.PRECONDITION_FAILED, "Cannot delete the global namespace "
                                    + namespaceName
                                    + ". There are still more than one replication clusters configured.");
                        }
                        if (policies.replication_clusters.size() == 1
                                && !policies.replication_clusters.contains(config().getClusterName())) {
                            // the only replication cluster is other cluster, redirect
                            String replCluster = new ArrayList<>(policies.replication_clusters).get(0);
                            future = clusterResources().getClusterAsync(replCluster)
                                    .thenCompose(clusterData -> {
                                        if (clusterData.isEmpty()) {
                                            throw new RestException(Status.NOT_FOUND,
                                                    "Cluster " + replCluster + " does not exist");
                                        }
                                        ClusterData replClusterData = clusterData.get();
                                        URL replClusterUrl;
                                        try {
                                            if (!config().isTlsEnabled() || !isRequestHttps()) {
                                                replClusterUrl = new URL(replClusterData.getServiceUrl());
                                            } else if (StringUtils.isNotBlank(replClusterData.getServiceUrlTls())) {
                                                replClusterUrl = new URL(replClusterData.getServiceUrlTls());
                                            } else {
                                                throw new RestException(Status.PRECONDITION_FAILED,
                                                        "The replication cluster does not provide TLS encrypted "
                                                                + "service");
                                            }
                                        } catch (MalformedURLException malformedURLException) {
                                            throw new RestException(malformedURLException);
                                        }

                                        URI redirect =
                                                UriBuilder.fromUri(uri.getRequestUri()).host(replClusterUrl.getHost())
                                                        .port(replClusterUrl.getPort())
                                                        .replaceQueryParam("authoritative", false).build();
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Redirecting the rest call to {}: cluster={}",
                                                    clientAppId(), redirect, replCluster);
                                        }
                                        throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                                    });
                        }
                    }
                    return future
                            .thenCompose(__ ->
                                    validateNamespaceBundleOwnershipAsync(namespaceName, policies.bundles,
                                            bundleRange,
                                            authoritative, true))
                            .thenCompose(bundle -> {
                                return pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName)
                                        .thenCompose(topics -> {
                                            CompletableFuture<Void> deleteTopicsFuture =
                                                    CompletableFuture.completedFuture(null);
                                            if (!force) {
                                                List<CompletableFuture<NamespaceBundle>> futures = new ArrayList<>();
                                                for (String topic : topics) {
                                                    futures.add(pulsar().getNamespaceService()
                                                            .getBundleAsync(TopicName.get(topic))
                                                            .thenCompose(topicBundle -> {
                                                                if (bundle.equals(topicBundle)) {
                                                                    throw new RestException(Status.CONFLICT,
                                                                            "Cannot delete non empty bundle");
                                                                }
                                                                return CompletableFuture.completedFuture(null);
                                                            }));

                                                }
                                                deleteTopicsFuture = FutureUtil.waitForAll(futures);
                                            }
                                            return deleteTopicsFuture.thenCompose(
                                                            ___ -> pulsar().getNamespaceService()
                                                                    .removeOwnedServiceUnitAsync(bundle))
                                                    .thenRun(() -> pulsar().getBrokerService().getBundleStats()
                                                            .remove(bundle.toString()));
                                        });
                            });
                });
    }

    protected CompletableFuture<Void> internalGrantPermissionOnNamespaceAsync(String role, Set<AuthAction> actions) {
        AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
        if (null != authService) {
            return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.GRANT_PERMISSION)
                    .thenAccept(__ -> {
                        checkNotNull(role, "Role should not be null");
                        checkNotNull(actions, "Actions should not be null");
                    }).thenCompose(__ ->
                            authService.grantPermissionAsync(namespaceName, actions, role, null))
                    .thenAccept(unused ->
                            log.info("[{}] Successfully granted access for role {}: {} - namespaceName {}",
                                    clientAppId(), role, actions, namespaceName))
                    .exceptionally(ex -> {
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        //The IllegalArgumentException and the IllegalStateException were historically thrown by the
                        // grantPermissionAsync method, so we catch them here to ensure backwards compatibility.
                        if (realCause instanceof MetadataStoreException.NotFoundException
                                || realCause instanceof IllegalArgumentException) {
                            log.warn("[{}] Failed to set permissions for namespace {}: does not exist", clientAppId(),
                                    namespaceName, ex);
                            throw new RestException(Status.NOT_FOUND, "Topic's namespace does not exist");
                        } else if (realCause instanceof MetadataStoreException.BadVersionException
                                || realCause instanceof IllegalStateException) {
                            log.warn("[{}] Failed to set permissions for namespace {}: {}",
                                    clientAppId(), namespaceName, ex.getCause().getMessage(), ex);
                            throw new RestException(Status.CONFLICT, "Concurrent modification");
                        } else {
                            log.error("[{}] Failed to get permissions for namespace {}",
                                    clientAppId(), namespaceName, ex);
                            throw new RestException(realCause);
                        }
                    });
        } else {
            String msg = "Authorization is not enabled";
            return FutureUtil.failedFuture(new RestException(Status.NOT_IMPLEMENTED, msg));
        }
    }


    protected CompletableFuture<Void> internalGrantPermissionOnSubscriptionAsync(String subscription,
                                                                                Set<String> roles) {
        AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
        if (null != authService) {
            return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.GRANT_PERMISSION)
                    .thenAccept(__ -> {
                        checkNotNull(subscription, "Subscription should not be null");
                        checkNotNull(roles, "Roles should not be null");
                    })
                    .thenCompose(__ -> authService.grantSubscriptionPermissionAsync(namespaceName, subscription,
                            roles, null))
                    .thenAccept(unused -> {
                        log.info("[{}] Successfully granted permission on subscription for role {}:{} - "
                                + "namespaceName {}", clientAppId(), roles, subscription, namespaceName);
                    })
                    .exceptionally(ex -> {
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        //The IllegalArgumentException and the IllegalStateException were historically thrown by the
                        // grantPermissionAsync method, so we catch them here to ensure backwards compatibility.
                        if (realCause.getCause() instanceof IllegalArgumentException) {
                            log.warn("[{}] Failed to set permissions for namespace {}: does not exist", clientAppId(),
                                    namespaceName);
                            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
                        } else if (realCause.getCause() instanceof IllegalStateException) {
                            log.warn("[{}] Failed to set permissions for namespace {}: concurrent modification",
                                    clientAppId(), namespaceName);
                            throw new RestException(Status.CONFLICT, "Concurrent modification");
                        } else {
                            log.error("[{}] Failed to get permissions for namespace {}",
                                    clientAppId(), namespaceName, realCause);
                            throw new RestException(realCause);
                        }
                    });
        } else {
            String msg = "Authorization is not enabled";
            return FutureUtil.failedFuture(new RestException(Status.NOT_IMPLEMENTED, msg));
        }
    }

    protected CompletableFuture<Void> internalRevokePermissionsOnNamespaceAsync(String role) {
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.REVOKE_PERMISSION)
                .thenAccept(__ -> checkNotNull(role, "Role should not be null"))
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.auth_policies.getNamespaceAuthentication().remove(role);
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalRevokePermissionsOnSubscriptionAsync(String subscriptionName,
                                                                                  String role) {
        AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
        if (null != authService) {
            return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.REVOKE_PERMISSION)
                    .thenAccept(__ -> {
                        checkNotNull(subscriptionName, "SubscriptionName should not be null");
                        checkNotNull(role, "Role should not be null");
                    })
                    .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                    .thenCompose(__ -> authService.revokeSubscriptionPermissionAsync(namespaceName,
                            subscriptionName, role, null/* additional auth-data json */));
        } else {
            String msg = "Authorization is not enabled";
            return FutureUtil.failedFuture(new RestException(Status.NOT_IMPLEMENTED, msg));
        }
    }

    protected CompletableFuture<Set<String>> internalGetNamespaceReplicationClustersAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.REPLICATION, PolicyOperation.READ)
                .thenAccept(__ -> {
                    if (!namespaceName.isGlobal()) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Cannot get the replication clusters for a non-global namespace");
                    }
                }).thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.replication_clusters);
    }

    @SuppressWarnings("checkstyle:WhitespaceAfter")
    protected CompletableFuture<Void> internalSetNamespaceReplicationClusters(List<String> clusterIds) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.REPLICATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenApply(__ -> {
                    if (CollectionUtils.isEmpty(clusterIds)) {
                        throw new RestException(Status.PRECONDITION_FAILED, "ClusterIds should not be null or empty");
                    }
                    if (!namespaceName.isGlobal() && !(clusterIds.size() == 1
                            && clusterIds.get(0).equals(pulsar().getConfiguration().getClusterName()))) {
                            throw new RestException(Status.PRECONDITION_FAILED,
                                    "Cannot set replication on a non-global namespace");
                    }
                    Set<String> replicationClusterSet = Sets.newHashSet(clusterIds);
                    if (replicationClusterSet.contains("global")) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Cannot specify global in the list of replication clusters");
                    }
                    return replicationClusterSet;
                }).thenCompose(replicationClusterSet -> clustersAsync()
                        .thenCompose(clusters -> {
                            List<CompletableFuture<Void>> futures =
                                    replicationClusterSet.stream().map(clusterId -> {
                                        if (!clusters.contains(clusterId)) {
                                            throw new RestException(Status.FORBIDDEN,
                                                    "Invalid cluster id: " + clusterId);
                                        }
                                        return validatePeerClusterConflictAsync(clusterId, replicationClusterSet)
                                                .thenCompose(__ -> getNamespacePoliciesAsync(this.namespaceName)
                                                        .thenCompose(nsPolicies -> {
                                                            if (nsPolicies.allowed_clusters.isEmpty()) {
                                                                return validateClusterForTenantAsync(
                                                                        namespaceName.getTenant(), clusterId);
                                                            }
                                                            if (!nsPolicies.allowed_clusters.contains(clusterId)) {
                                                                String msg = String.format("Cluster [%s] is not in the "
                                                                        + "list of allowed clusters list for namespace "
                                                                        + "[%s]", clusterId, namespaceName.toString());
                                                                log.info(msg);
                                                                throw new RestException(Status.FORBIDDEN, msg);
                                                            }
                                                            return CompletableFuture.completedFuture(null);
                                                        }));
                                    }).collect(Collectors.toList());
                            return FutureUtil.waitForAll(futures).thenApply(__ -> replicationClusterSet);
                        }))
                .thenCompose(replicationClusterSet -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.replication_clusters = replicationClusterSet;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetNamespaceMessageTTLAsync(Integer messageTTL) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.TTL, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> {
                    if (messageTTL != null && messageTTL < 0) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Invalid value for message TTL, message TTL must >= 0");
                    }
                }).thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.message_ttl_in_seconds = messageTTL;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetSubscriptionExpirationTimeAsync(Integer expirationTime) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SUBSCRIPTION_EXPIRATION_TIME,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> {
                    if (expirationTime != null && expirationTime < 0) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Invalid value for subscription expiration time");
                    }
                }).thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.subscription_expiration_time_minutes = expirationTime;
                    return policies;
                }));
    }

    protected CompletableFuture<AutoTopicCreationOverride> internalGetAutoTopicCreationAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.AUTO_TOPIC_CREATION,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.autoTopicCreationOverride);
    }

    protected CompletableFuture<Void> internalSetAutoTopicCreationAsync(
            AutoTopicCreationOverride autoTopicCreationOverride) {
        return validateNamespacePolicyOperationAsync(namespaceName,
                PolicyName.AUTO_TOPIC_CREATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> {
                    int maxPartitions = pulsar().getConfig().getMaxNumPartitionsPerPartitionedTopic();
                    if (autoTopicCreationOverride != null) {
                        ValidateResult validateResult =
                                AutoTopicCreationOverrideImpl.validateOverride(autoTopicCreationOverride);
                        if (!validateResult.isSuccess()) {
                            throw new RestException(Status.PRECONDITION_FAILED,
                                    "Invalid configuration for autoTopicCreationOverride. the detail is "
                                            + validateResult.getErrorInfo());
                        }
                        if (Objects.equals(autoTopicCreationOverride.getTopicType(),
                                                                  TopicType.PARTITIONED.toString())){
                            if (maxPartitions > 0
                                    && autoTopicCreationOverride.getDefaultNumPartitions() > maxPartitions) {
                                throw new RestException(Status.NOT_ACCEPTABLE,
                                        "Number of partitions should be less than or equal to " + maxPartitions);
                            }

                        }
                    }
                })
                .thenCompose(__ -> namespaceResources().setPoliciesAsync(namespaceName, policies -> {
                    policies.autoTopicCreationOverride = autoTopicCreationOverride;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetAutoSubscriptionCreationAsync(AutoSubscriptionCreationOverride
                                                               autoSubscriptionCreationOverride) {
        // Force to read the data s.t. the watch to the cache content is setup.
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.AUTO_SUBSCRIPTION_CREATION,
                PolicyOperation.WRITE)
                .thenCompose(__ ->  validatePoliciesReadOnlyAccessAsync())
                        .thenCompose(unused -> namespaceResources().setPoliciesAsync(namespaceName, policies -> {
                            policies.autoSubscriptionCreationOverride = autoSubscriptionCreationOverride;
                            return policies;
                        }))
                .thenAccept(r -> {
                    if (autoSubscriptionCreationOverride != null) {
                        String autoOverride = autoSubscriptionCreationOverride.isAllowAutoSubscriptionCreation()
                                ? "enabled" : "disabled";
                        log.info("[{}] Successfully {} autoSubscriptionCreation on namespace {}", clientAppId(),
                                autoOverride, namespaceName);
                    } else {
                        log.info("[{}] Successfully remove autoSubscriptionCreation on namespace {}",
                                clientAppId(), namespaceName);
                    }
                });
    }

    protected CompletableFuture<AutoSubscriptionCreationOverride> internalGetAutoSubscriptionCreationAsync() {

        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.AUTO_SUBSCRIPTION_CREATION,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.autoSubscriptionCreationOverride);
    }

    protected CompletableFuture<Void> internalModifyDeduplicationAsync(Boolean enableDeduplication) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DEDUPLICATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.deduplicationEnabled = enableDeduplication;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalUnloadNamespaceAsync() {
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> {
                    log.info("[{}] Unloading namespace {}", clientAppId(), namespaceName);
                    if (namespaceName.isGlobal()) {
                        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                        return validateGlobalNamespaceOwnershipAsync(namespaceName);
                    } else {
                        return validateClusterOwnershipAsync(namespaceName.getCluster())
                                .thenCompose(ignore -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                        namespaceName.getCluster()));
                    }
                })
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies -> {
                    final List<CompletableFuture<Void>> futures = new ArrayList<>();
                    List<String> boundaries = policies.bundles.getBoundaries();
                    for (int i = 0; i < boundaries.size() - 1; i++) {
                        String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
                        try {
                            futures.add(pulsar().getAdminClient().namespaces().unloadNamespaceBundleAsync(
                                    namespaceName.toString(), bundle));
                        } catch (PulsarServerException e) {
                            log.error("[{}] Failed to unload namespace {}", clientAppId(), namespaceName, e);
                            throw new RestException(e);
                        }
                    }
                    return FutureUtil.waitForAll(futures);
                });
    }


    protected void internalSetBookieAffinityGroup(BookieAffinityGroupData bookieAffinityGroup) {
        validateSuperUserAccess();
        log.info("[{}] Setting bookie-affinity-group {} for namespace {}", clientAppId(), bookieAffinityGroup,
                this.namespaceName);

        if (namespaceName.isGlobal()) {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } else {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        try {
            getLocalPolicies().setLocalPoliciesWithCreate(namespaceName, oldPolicies -> {
                LocalPolicies localPolicies = oldPolicies.map(
                        policies -> new LocalPolicies(policies.bundles,
                                bookieAffinityGroup,
                                policies.namespaceAntiAffinityGroup))
                        .orElseGet(() -> new LocalPolicies(getBundles(config().getDefaultNumberOfNamespaceBundles()),
                                bookieAffinityGroup,
                                null));
                log.info("[{}] Successfully updated local-policies configuration: namespace={}, map={}", clientAppId(),
                        namespaceName, localPolicies);
                return localPolicies;
            });
        } catch (NotFoundException e) {
            log.warn("[{}] Failed to update local-policy configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update local-policy configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected void internalDeleteBookieAffinityGroup() {
        internalSetBookieAffinityGroup(null);
    }

    protected BookieAffinityGroupData internalGetBookieAffinityGroup() {
        validateSuperUserAccess();

        if (namespaceName.isGlobal()) {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } else {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }
        try {
            final BookieAffinityGroupData bookkeeperAffinityGroup = getLocalPolicies().getLocalPolicies(namespaceName)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                            "Namespace local-policies does not exist")).bookieAffinityGroup;
            return bookkeeperAffinityGroup;
        } catch (NotFoundException e) {
            log.warn("[{}] Failed to get local-policy configuration for namespace {}: does not exist",
                    clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace policies does not exist");
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get local-policy configuration for namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    private CompletableFuture<Void> validateLeaderBrokerAsync() {
        if (this.isLeaderBroker()) {
            return CompletableFuture.completedFuture(null);
        }
        Optional<LeaderBroker> currentLeaderOpt = pulsar().getLeaderElectionService().getCurrentLeader();
        if (currentLeaderOpt.isEmpty()) {
            String errorStr = "The current leader is empty.";
            log.error(errorStr);
            return FutureUtil.failedFuture(new RestException(Response.Status.PRECONDITION_FAILED, errorStr));
        }
        LeaderBroker leaderBroker = pulsar().getLeaderElectionService().getCurrentLeader().get();
        String leaderBrokerId = leaderBroker.getBrokerId();
        return pulsar().getNamespaceService()
                .createLookupResult(leaderBrokerId, false, null)
                .thenCompose(lookupResult -> {
                    String redirectUrl = isRequestHttps() ? lookupResult.getLookupData().getHttpUrlTls()
                            : lookupResult.getLookupData().getHttpUrl();
                    if (redirectUrl == null) {
                        log.error("Redirected broker's service url is not configured");
                        return FutureUtil.failedFuture(new RestException(Response.Status.PRECONDITION_FAILED,
                                "Redirected broker's service url is not configured."));
                    }

                    try {
                        URL url = new URL(redirectUrl);
                        URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(url.getHost())
                                .port(url.getPort())
                                .replaceQueryParam("authoritative",
                                        false).build();
                        // Redirect
                        if (log.isDebugEnabled()) {
                            log.debug("Redirecting the request call to leader - {}", redirect);
                        }
                        return FutureUtil.failedFuture((
                                new WebApplicationException(Response.temporaryRedirect(redirect).build())));
                    } catch (MalformedURLException exception) {
                        log.error("The redirect url is malformed - {}", redirectUrl);
                        return FutureUtil.failedFuture(new RestException(exception));
                    }
                });
    }

    public CompletableFuture<Void> setNamespaceBundleAffinityAsync(String bundleRange, String destinationBroker) {
        if (StringUtils.isBlank(destinationBroker)) {
            return CompletableFuture.completedFuture(null);
        }
        return pulsar().getLoadManager().get().getAvailableBrokersAsync()
                .thenCompose(brokers -> {
                    if (!brokers.contains(destinationBroker)) {
                        log.warn("[{}] Failed to unload namespace bundle {}/{} to inactive broker {}.",
                                clientAppId(), namespaceName, bundleRange, destinationBroker);
                        return FutureUtil.failedFuture(new BrokerServiceException.NotAllowedException(
                                "Not allowed unload namespace bundle to inactive destination broker"));
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .thenCompose(__ -> {
                    if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar())) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return validateLeaderBrokerAsync();
                })
                .thenAccept(__ -> {
                    if (ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar())) {
                        return;
                    }
                    // For ExtensibleLoadManager, this operation will be ignored.
                    pulsar().getLoadManager().get().setNamespaceBundleAffinity(bundleRange, destinationBroker);
                });
    }

    public CompletableFuture<Void> internalUnloadNamespaceBundleAsync(String bundleRange,
                                                                      String destinationBrokerParam,
                                                                      boolean authoritative) {
        String destinationBroker = StringUtils.isBlank(destinationBrokerParam) ? null :
                // ensure backward compatibility: strip the possible http:// or https:// prefix
                destinationBrokerParam.replaceFirst("http[s]?://", "");
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> setNamespaceBundleAffinityAsync(bundleRange, destinationBroker))
                .thenAccept(__ -> {
                    checkNotNull(bundleRange, "BundleRange should not be null");
                    log.info("[{}] Unloading namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange);
                })
                .thenApply(__ ->
                    pulsar().getNamespaceService().getNamespaceBundleFactory()
                                    .getBundle(namespaceName.toString(), bundleRange)
                )
                .thenCompose(bundle ->
                    pulsar().getNamespaceService().isNamespaceBundleOwned(bundle)
                          .exceptionally(ex -> {
                            if (log.isDebugEnabled()) {
                                log.debug("Failed to validate cluster ownership for {}-{}, {}",
                                        namespaceName.toString(), bundleRange, ex.getMessage(), ex);
                            }
                            return false;
                          })
                )
                .thenCompose(isOwnedByLocalCluster -> {
                    if (!isOwnedByLocalCluster) {
                        if (namespaceName.isGlobal()) {
                            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                            return validateGlobalNamespaceOwnershipAsync(namespaceName);
                        } else {
                            return validateClusterOwnershipAsync(namespaceName.getCluster())
                                    .thenCompose(__ -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                            namespaceName.getCluster()));
                        }
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies ->
                     isBundleOwnedByAnyBroker(namespaceName, policies.bundles, bundleRange)
                        .thenCompose(flag -> {
                            if (!flag) {
                                log.info("[{}] Namespace bundle is not owned by any broker {}/{}", clientAppId(),
                                        namespaceName, bundleRange);
                                return CompletableFuture.completedFuture(null);
                            }
                            Optional<String> destinationBrokerOpt = Optional.ofNullable(destinationBroker);
                            return validateNamespaceBundleOwnershipAsync(namespaceName, policies.bundles, bundleRange,
                                    authoritative, true)
                                    .thenCompose(nsBundle -> pulsar().getNamespaceService()
                                            .unloadNamespaceBundle(nsBundle, destinationBrokerOpt));
                        }));
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalSplitNamespaceBundleAsync(String bundleName,
                                                                        boolean authoritative, boolean unload,
                                                                        String splitAlgorithmName,
                                                                        List<Long> splitBoundaries) {
        return validateSuperUserAccessAsync()
                .thenAccept(__ -> {
                    checkNotNull(bundleName, "BundleRange should not be null");
                    log.info("[{}] Split namespace bundle {}/{}", clientAppId(), namespaceName, bundleName);
                    List<String> supportedNamespaceBundleSplitAlgorithms =
                            pulsar().getConfig().getSupportedNamespaceBundleSplitAlgorithms();
                    if (StringUtils.isNotBlank(splitAlgorithmName)) {
                        if (!supportedNamespaceBundleSplitAlgorithms.contains(splitAlgorithmName)) {
                            throw new RestException(Status.PRECONDITION_FAILED,
                                    "Unsupported namespace bundle split algorithm, supported algorithms are "
                                            + supportedNamespaceBundleSplitAlgorithms);
                        }
                        if (splitAlgorithmName
                                .equalsIgnoreCase(NamespaceBundleSplitAlgorithm.SPECIFIED_POSITIONS_DIVIDE)
                                && (splitBoundaries == null || splitBoundaries.size() == 0)) {
                            throw new RestException(Status.PRECONDITION_FAILED,
                                    "With specified_positions_divide split algorithm, splitBoundaries must not be "
                                            + "empty");
                        }
                    }
                })
                .thenCompose(__ -> {
                    if (namespaceName.isGlobal()) {
                        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                        return validateGlobalNamespaceOwnershipAsync(namespaceName);
                    } else {
                        return validateClusterOwnershipAsync(namespaceName.getCluster())
                                .thenCompose(ignore -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                        namespaceName.getCluster()));
                    }
                })
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> getBundleRangeAsync(bundleName))
                .thenCompose(bundleRange -> {
                    return getNamespacePoliciesAsync(namespaceName)
                            .thenCompose(policies ->
                                    validateNamespaceBundleOwnershipAsync(namespaceName, policies.bundles, bundleRange,
                                        authoritative, false))
                            .thenCompose(nsBundle -> pulsar().getNamespaceService().splitAndOwnBundle(nsBundle, unload,
                                    pulsar().getNamespaceService()
                                            .getNamespaceBundleSplitAlgorithmByName(splitAlgorithmName),
                                    splitBoundaries));
                });
    }

    protected CompletableFuture<TopicHashPositions> internalGetTopicHashPositionsAsync(String bundleRange,
                                                                                       List<String> topics) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Getting hash position for topic list {}, bundle {}", clientAppId(), topics, bundleRange);
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.PERSISTENCE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies -> {
                    return validateNamespaceBundleOwnershipAsync(namespaceName, policies.bundles, bundleRange,
                            false, true)
                            .thenCompose(nsBundle ->
                                    pulsar().getNamespaceService().getOwnedTopicListForNamespaceBundle(nsBundle))
                            .thenApply(allTopicsInThisBundle -> {
                                Map<String, Long> topicHashPositions = new HashMap<>();
                                if (topics == null || topics.size() == 0) {
                                    allTopicsInThisBundle.forEach(t -> {
                                        topicHashPositions.put(t,
                                                pulsar().getNamespaceService().getNamespaceBundleFactory()
                                                        .getLongHashCode(t));
                                    });
                                } else {
                                    for (String topic : topics.stream().map(Codec::decode).toList()) {
                                        TopicName topicName = TopicName.get(topic);
                                        // partitioned topic
                                        if (topicName.getPartitionIndex() == -1) {
                                            allTopicsInThisBundle.stream()
                                                    .filter(t -> TopicName.get(t).getPartitionedTopicName()
                                                            .equals(TopicName.get(topic).getPartitionedTopicName()))
                                                    .forEach(partition -> {
                                                        topicHashPositions.put(partition,
                                                                pulsar().getNamespaceService()
                                                                        .getNamespaceBundleFactory()
                                                                        .getLongHashCode(partition));
                                                    });
                                        } else { // topic partition
                                            if (allTopicsInThisBundle.contains(topicName.toString())) {
                                                topicHashPositions.put(topic,
                                                        pulsar().getNamespaceService().getNamespaceBundleFactory()
                                                                .getLongHashCode(topic));
                                            }
                                        }
                                    }
                                }
                                return new TopicHashPositions(namespaceName.toString(), bundleRange,
                                        topicHashPositions);
                            });
                });
    }

    private CompletableFuture<String> getBundleRangeAsync(String bundleName) {
        CompletableFuture<NamespaceBundle> future;
        if (BundleType.LARGEST.toString().equals(bundleName)) {
            future = findLargestBundleWithTopicsAsync(namespaceName);
        } else if (BundleType.HOT.toString().equals(bundleName)) {
            future = findHotBundleAsync(namespaceName);
        } else {
            return CompletableFuture.completedFuture(bundleName);
        }
        return future.thenApply(nsBundle -> {
            if (nsBundle == null) {
                throw new RestException(Status.NOT_FOUND,
                        String.format("Bundle range %s not found", bundleName));
            }
            return nsBundle.getBundleRange();
        });
    }

    private CompletableFuture<NamespaceBundle> findLargestBundleWithTopicsAsync(NamespaceName namespaceName) {
        return pulsar().getNamespaceService().getNamespaceBundleFactory()
                .getBundleWithHighestTopicsAsync(namespaceName);
    }

    private CompletableFuture<NamespaceBundle> findHotBundleAsync(NamespaceName namespaceName) {
        return pulsar().getNamespaceService().getNamespaceBundleFactory()
                .getBundleWithHighestThroughputAsync(namespaceName);
    }

    protected void internalSetPublishRate(PublishRate maxPublishMessageRate) {
        validateSuperUserAccess();
        log.info("[{}] Set namespace publish-rate {}/{}", clientAppId(), namespaceName, maxPublishMessageRate);
        updatePolicies(namespaceName, policies -> {
            policies.publishMaxMessageRate.put(pulsar().getConfiguration().getClusterName(), maxPublishMessageRate);
            return policies;
        });
        log.info("[{}] Successfully updated the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                namespaceName);
    }

    protected CompletableFuture<Void> internalSetPublishRateAsync(PublishRate maxPublishMessageRate) {
        log.info("[{}] Set namespace publish-rate {}/{}", clientAppId(), namespaceName, maxPublishMessageRate);
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.WRITE)
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.publishMaxMessageRate.put(pulsar().getConfiguration().getClusterName(), maxPublishMessageRate);
            log.info("[{}] Successfully updated the publish_max_message_rate for cluster on namespace {}",
                    clientAppId(), namespaceName);
            return policies;
        }));
    }

    protected void internalRemovePublishRate() {
        validateSuperUserAccess();
        log.info("[{}] Remove namespace publish-rate {}/{}", clientAppId(), namespaceName, topicName);
        try {
            updatePolicies(namespaceName, policies -> {
                if (policies.publishMaxMessageRate != null) {
                    policies.publishMaxMessageRate.remove(pulsar().getConfiguration().getClusterName());
                }
                return policies;
            });
            log.info("[{}] Successfully remove the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                    namespaceName);
        } catch (Exception e) {
            log.error("[{}] Failed to remove the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> internalRemovePublishRateAsync() {
        log.info("[{}] Remove namespace publish-rate {}/{}", clientAppId(), namespaceName, topicName);
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.WRITE)
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            if (policies.publishMaxMessageRate != null) {
                policies.publishMaxMessageRate.remove(pulsar().getConfiguration().getClusterName());
            }
            log.info("[{}] Successfully remove the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }

    protected CompletableFuture<PublishRate> internalGetPublishRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies ->
                        policies.publishMaxMessageRate.get(pulsar().getConfiguration().getClusterName()));
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalSetTopicDispatchRateAsync(DispatchRateImpl dispatchRate) {
        log.info("[{}] Set namespace dispatch-rate {}/{}", clientAppId(), namespaceName, dispatchRate);
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.WRITE)
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.topicDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
            policies.clusterDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
            log.info("[{}] Successfully updated the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }

    protected CompletableFuture<Void> internalDeleteTopicDispatchRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.WRITE)
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.topicDispatchRate.remove(pulsar().getConfiguration().getClusterName());
            policies.clusterDispatchRate.remove(pulsar().getConfiguration().getClusterName());
            log.info("[{}] Successfully delete the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<DispatchRate> internalGetTopicDispatchRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.topicDispatchRate.get(pulsar().getConfiguration().getClusterName()));
    }

    protected CompletableFuture<Void> internalSetSubscriptionDispatchRateAsync(DispatchRateImpl dispatchRate) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.WRITE)
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.subscriptionDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
                    log.info("[{}] Successfully updated the subscriptionDispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName);
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalDeleteSubscriptionDispatchRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.WRITE)
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.subscriptionDispatchRate.remove(pulsar().getConfiguration().getClusterName());
                    log.info("[{}] Successfully delete the subscriptionDispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName);
                    return policies;
                }));
    }

    protected CompletableFuture<DispatchRate> internalGetSubscriptionDispatchRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies ->
                        policies.subscriptionDispatchRate.get(pulsar().getConfiguration().getClusterName()));
    }

    protected CompletableFuture<Void> internalSetSubscribeRateAsync(SubscribeRate subscribeRate) {
        log.info("[{}] Set namespace subscribe-rate {}/{}", clientAppId(), namespaceName, subscribeRate);
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.WRITE)
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.clusterSubscribeRate.put(pulsar().getConfiguration().getClusterName(), subscribeRate);
            log.info("[{}] Successfully updated the subscribeRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }

    protected CompletableFuture<Void> internalDeleteSubscribeRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.WRITE)
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.clusterSubscribeRate.remove(pulsar().getConfiguration().getClusterName());
            log.info("[{}] Successfully delete the subscribeRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }


    protected CompletableFuture<SubscribeRate> internalGetSubscribeRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.clusterSubscribeRate.get(pulsar().getConfiguration().getClusterName()));
    }
    protected CompletableFuture<Void> setBacklogQuotaAsync(BacklogQuotaType backlogQuotaType,
                                                           BacklogQuota quota) {
        return namespaceResources().setPoliciesAsync(namespaceName, policies -> {
            RetentionPolicies retentionPolicies = policies.retention_policies;
            final BacklogQuotaType quotaType = backlogQuotaType != null ? backlogQuotaType
                    : BacklogQuotaType.destination_storage;
            if (retentionPolicies == null) {
                policies.backlog_quota_map.put(quotaType, quota);
                return policies;
            }
            // If we have retention policies, we have to check the conflict.
            BacklogQuota needCheckQuota = null;
            if (quotaType == BacklogQuotaType.destination_storage) {
                needCheckQuota = quota;
            }
            boolean passCheck = checkBacklogQuota(needCheckQuota, retentionPolicies);
            if (!passCheck) {
                throw new RestException(Response.Status.PRECONDITION_FAILED,
                        "Backlog Quota exceeds configured retention quota for namespace."
                                + " Please increase retention quota and retry");
            }
            policies.backlog_quota_map.put(quotaType, quota);
            return policies;
        });
    }

    protected void internalSetRetention(RetentionPolicies retention) {
        validateRetentionPolicies(retention);
        validateNamespacePolicyOperation(namespaceName, PolicyName.RETENTION, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        try {
            Policies policies = namespaceResources().getPolicies(namespaceName)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                    "Namespace policies does not exist"));
            if (!checkQuotas(policies, retention)) {
                log.warn("[{}] Failed to update retention configuration"
                                + " for namespace {}: conflicts with backlog quota",
                        clientAppId(), namespaceName);
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Retention Quota must exceed configured backlog quota for namespace.");
            }
            policies.retention_policies = retention;
            namespaceResources().setPolicies(namespaceName, p -> policies);
            log.info("[{}] Successfully updated retention configuration: namespace={}, map={}", clientAppId(),
                    namespaceName, objectWriter().writeValueAsString(retention));
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update retention configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> internalDeletePersistenceAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.PERSISTENCE, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> doUpdatePersistenceAsync(null));
    }

    protected CompletableFuture<Void> internalSetPersistenceAsync(PersistencePolicies persistence) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.PERSISTENCE, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> validatePersistencePolicies(persistence))
                .thenCompose(__ -> doUpdatePersistenceAsync(persistence));
    }

    private CompletableFuture<Void> doUpdatePersistenceAsync(PersistencePolicies persistence) {
        return updatePoliciesAsync(namespaceName, policies -> {
            policies.persistence = persistence;
            return policies;
        }).thenAccept(__ -> log.info("[{}] Successfully updated persistence configuration: namespace={}, map={}",
                clientAppId(), namespaceName, persistence)
        );
    }

    protected void internalClearNamespaceBacklog(AsyncResponse asyncResponse, boolean authoritative) {
        validateNamespaceOperation(namespaceName, NamespaceOperation.CLEAR_BACKLOG);

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            for (NamespaceBundle nsBundle : bundles.getBundles()) {
                // check if the bundle is owned by any broker, if not then there is no backlog on this bundle to clear
                if (pulsar().getNamespaceService().checkOwnershipPresent(nsBundle)) {
                    futures.add(pulsar().getAdminClient().namespaces()
                            .clearNamespaceBundleBacklogAsync(namespaceName.toString(), nsBundle.getBundleRange()));
                }
            }
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
            return;
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        FutureUtil.waitForAll(futures).handle((result, exception) -> {
            if (exception != null) {
                log.warn("[{}] Failed to clear backlog on the bundles for namespace {}: {}", clientAppId(),
                        namespaceName, exception.getCause().getMessage());
                if (exception.getCause() instanceof PulsarAdminException) {
                    asyncResponse.resume(new RestException((PulsarAdminException) exception.getCause()));
                    return null;
                } else {
                    asyncResponse.resume(new RestException(exception.getCause()));
                    return null;
                }
            }
            log.info("[{}] Successfully cleared backlog on all the bundles for namespace {}", clientAppId(),
                    namespaceName);
            asyncResponse.resume(Response.noContent().build());
            return null;
        });
    }

    @SuppressWarnings("deprecation")
    protected void internalClearNamespaceBundleBacklog(String bundleRange, boolean authoritative) {
        validateNamespaceOperation(namespaceName, NamespaceOperation.CLEAR_BACKLOG);
        checkNotNull(bundleRange, "BundleRange should not be null");

        Policies policies = getNamespacePolicies(namespaceName);

        if (namespaceName.isGlobal()) {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } else {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange, authoritative, true);

        clearBacklog(namespaceName, bundleRange, null);
        log.info("[{}] Successfully cleared backlog on namespace bundle {}/{}", clientAppId(), namespaceName,
                bundleRange);
    }

    protected void internalClearNamespaceBacklogForSubscription(AsyncResponse asyncResponse, String subscription,
                                                                boolean authoritative) {
        validateNamespaceOperation(namespaceName, NamespaceOperation.CLEAR_BACKLOG);
        checkNotNull(subscription, "Subscription should not be null");

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            for (NamespaceBundle nsBundle : bundles.getBundles()) {
                // check if the bundle is owned by any broker, if not then there is no backlog on this bundle to clear
                if (pulsar().getNamespaceService().checkOwnershipPresent(nsBundle)) {
                    futures.add(pulsar().getAdminClient().namespaces().clearNamespaceBundleBacklogForSubscriptionAsync(
                            namespaceName.toString(), nsBundle.getBundleRange(), subscription));
                }
            }
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
            return;
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        FutureUtil.waitForAll(futures).handle((result, exception) -> {
            if (exception != null) {
                log.warn("[{}] Failed to clear backlog for subscription {} on the bundles for namespace {}: {}",
                        clientAppId(), subscription, namespaceName, exception.getCause().getMessage());
                if (exception.getCause() instanceof PulsarAdminException) {
                    asyncResponse.resume(new RestException((PulsarAdminException) exception.getCause()));
                    return null;
                } else {
                    asyncResponse.resume(new RestException(exception.getCause()));
                    return null;
                }
            }
            log.info("[{}] Successfully cleared backlog for subscription {} on all the bundles for namespace {}",
                    clientAppId(), subscription, namespaceName);
            asyncResponse.resume(Response.noContent().build());
            return null;
        });
    }

    @SuppressWarnings("deprecation")
    protected void internalClearNamespaceBundleBacklogForSubscription(String subscription, String bundleRange,
                                                                      boolean authoritative) {
        validateNamespaceOperation(namespaceName, NamespaceOperation.CLEAR_BACKLOG);
        checkNotNull(subscription, "Subscription should not be null");
        checkNotNull(bundleRange, "BundleRange should not be null");

        Policies policies = getNamespacePolicies(namespaceName);

        if (namespaceName.isGlobal()) {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } else {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange, authoritative, true);

        clearBacklog(namespaceName, bundleRange, subscription);
        log.info("[{}] Successfully cleared backlog for subscription {} on namespace bundle {}/{}", clientAppId(),
                subscription, namespaceName, bundleRange);
    }

    protected void internalUnsubscribeNamespace(AsyncResponse asyncResponse, String subscription,
                                                boolean authoritative) {
        validateNamespaceOperation(namespaceName, NamespaceOperation.UNSUBSCRIBE);
        checkNotNull(subscription, "Subscription should not be null");

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            for (NamespaceBundle nsBundle : bundles.getBundles()) {
                // check if the bundle is owned by any broker, if not then there are no subscriptions
                if (pulsar().getNamespaceService().checkOwnershipPresent(nsBundle)) {
                    futures.add(pulsar().getAdminClient().namespaces().unsubscribeNamespaceBundleAsync(
                            namespaceName.toString(), nsBundle.getBundleRange(), subscription));
                }
            }
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
            return;
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        FutureUtil.waitForAll(futures).handle((result, exception) -> {
            if (exception != null) {
                log.warn("[{}] Failed to unsubscribe {} on the bundles for namespace {}: {}", clientAppId(),
                        subscription, namespaceName, exception.getCause().getMessage());
                if (exception.getCause() instanceof PulsarAdminException) {
                    asyncResponse.resume(new RestException((PulsarAdminException) exception.getCause()));
                    return null;
                } else {
                    asyncResponse.resume(new RestException(exception.getCause()));
                    return null;
                }
            }
            log.info("[{}] Successfully unsubscribed {} on all the bundles for namespace {}", clientAppId(),
                    subscription, namespaceName);
            asyncResponse.resume(Response.noContent().build());
            return null;
        });
    }

    @SuppressWarnings("deprecation")
    protected void internalUnsubscribeNamespaceBundle(String subscription, String bundleRange, boolean authoritative) {
        validateNamespaceOperation(namespaceName, NamespaceOperation.UNSUBSCRIBE);
        checkNotNull(subscription, "Subscription should not be null");
        checkNotNull(bundleRange, "BundleRange should not be null");

        Policies policies = getNamespacePolicies(namespaceName);

        if (namespaceName.isGlobal()) {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } else {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange, authoritative, true);

        unsubscribe(namespaceName, bundleRange, subscription);
        log.info("[{}] Successfully unsubscribed {} on namespace bundle {}/{}", clientAppId(), subscription,
                namespaceName, bundleRange);
    }

    protected void internalSetSubscriptionAuthMode(SubscriptionAuthMode subscriptionAuthMode) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.SUBSCRIPTION_AUTH_MODE, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        SubscriptionAuthMode authMode = subscriptionAuthMode == null ? subscriptionAuthMode = SubscriptionAuthMode.None
                : subscriptionAuthMode;
        try {
            updatePolicies(namespaceName, policies -> {
                policies.subscription_auth_mode = authMode;
                return policies;
            });
            log.info("[{}] Successfully updated subscription auth mode: namespace={}, map={}", clientAppId(),
                    namespaceName, objectWriter().writeValueAsString(authMode));

        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update subscription auth mode for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalModifyEncryptionRequired(boolean encryptionRequired) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.ENCRYPTION, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        try {
            updatePolicies(namespaceName, policies -> {
                policies.encryption_required = encryptionRequired;
                return policies;
            });
            log.info("[{}] Successfully {} on namespace {}", clientAppId(), encryptionRequired ? "true" : "false",
                    namespaceName);
        } catch (Exception e) {
            log.error("[{}] Failed to modify encryption required status on namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    protected Boolean internalGetEncryptionRequired() {
        validateNamespacePolicyOperation(namespaceName, PolicyName.ENCRYPTION, PolicyOperation.READ);
        Policies policies = getNamespacePolicies(namespaceName);
        return policies.encryption_required;
    }

    protected void internalSetInactiveTopic(InactiveTopicPolicies inactiveTopicPolicies) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.INACTIVE_TOPIC, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        internalSetPolicies("inactive_topic_policies", inactiveTopicPolicies);
    }

    protected void internalSetPolicies(String fieldName, Object value) {
        try {
            Policies policies = namespaceResources().getPolicies(namespaceName)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                    "Namespace policies does not exist"));
            Field field = Policies.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(policies, value);
            namespaceResources().setPolicies(namespaceName, p -> policies);
            log.info("[{}] Successfully updated {} configuration: namespace={}, value={}", clientAppId(), fieldName,
                    namespaceName, objectWriter().writeValueAsString(value));

        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update {} configuration for namespace {}", clientAppId(), fieldName
                    , namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetDelayedDelivery(DelayedDeliveryPolicies delayedDeliveryPolicies) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();
        internalSetPolicies("delayed_delivery_policies", delayedDeliveryPolicies);
    }

    protected void internalSetNamespaceAntiAffinityGroup(String antiAffinityGroup) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.ANTI_AFFINITY, PolicyOperation.WRITE);
        checkNotNull(antiAffinityGroup, "AntiAffinityGroup should not be null");
        validatePoliciesReadOnlyAccess();

        log.info("[{}] Setting anti-affinity group {} for {}", clientAppId(), antiAffinityGroup, namespaceName);

        if (isBlank(antiAffinityGroup)) {
            throw new RestException(Status.PRECONDITION_FAILED, "antiAffinityGroup can't be empty");
        }

        try {
            getLocalPolicies().setLocalPoliciesWithCreate(namespaceName, (lp)->
                lp.map(policies -> new LocalPolicies(policies.bundles,
                        policies.bookieAffinityGroup,
                        antiAffinityGroup))
                        .orElseGet(() -> new LocalPolicies(defaultBundle(),
                                null, antiAffinityGroup))
            );
            log.info("[{}] Successfully updated local-policies configuration: namespace={}, map={}", clientAppId(),
                    namespaceName, antiAffinityGroup);
        } catch (Exception e) {
            log.error("[{}] Failed to update local-policy configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected String internalGetNamespaceAntiAffinityGroup() {
        validateNamespacePolicyOperation(namespaceName, PolicyName.ANTI_AFFINITY, PolicyOperation.READ);

        try {
            return getLocalPolicies()
                    .getLocalPolicies(namespaceName)
                    .orElseGet(() -> new LocalPolicies(getBundles(config().getDefaultNumberOfNamespaceBundles())
                            , null, null)).namespaceAntiAffinityGroup;
        } catch (Exception e) {
            log.error("[{}] Failed to get the antiAffinityGroup of namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(Status.NOT_FOUND, "Couldn't find namespace policies");
        }
    }

    protected void internalRemoveNamespaceAntiAffinityGroup() {
        validateNamespacePolicyOperation(namespaceName, PolicyName.ANTI_AFFINITY, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        log.info("[{}] Deleting anti-affinity group for {}", clientAppId(), namespaceName);

        try {
            getLocalPolicies().setLocalPolicies(namespaceName, (policies)->
                new LocalPolicies(policies.bundles,
                        policies.bookieAffinityGroup,
                        null));
            log.info("[{}] Successfully removed anti-affinity group for a namespace={}", clientAppId(), namespaceName);
        } catch (Exception e) {
            log.error("[{}] Failed to remove anti-affinity group for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected List<String> internalGetAntiAffinityNamespaces(String cluster, String antiAffinityGroup,
                                                             String tenant) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.ANTI_AFFINITY, PolicyOperation.READ);
        checkNotNull(cluster, "Cluster should not be null");
        checkNotNull(antiAffinityGroup, "AntiAffinityGroup should not be null");
        checkNotNull(tenant, "Tenant should not be null");

        log.info("[{}]-{} Finding namespaces for {} in {}", clientAppId(), tenant, antiAffinityGroup, cluster);

        if (isBlank(antiAffinityGroup)) {
            throw new RestException(Status.PRECONDITION_FAILED, "anti-affinity group can't be empty.");
        }
        validateClusterExists(cluster);

        try {
            List<String> namespaces = tenantResources().getListOfNamespaces(tenant);

            return namespaces.stream().filter(ns -> {
                Optional<LocalPolicies> policies;
                try {
                    policies = getLocalPolicies().getLocalPolicies(NamespaceName.get(ns));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                String storedAntiAffinityGroup = policies.orElseGet(() ->
                        new LocalPolicies(getBundles(config().getDefaultNumberOfNamespaceBundles()),
                                null, null)).namespaceAntiAffinityGroup;
                return antiAffinityGroup.equalsIgnoreCase(storedAntiAffinityGroup);
            }).collect(Collectors.toList());

        } catch (Exception e) {
            log.warn("Failed to list of properties/namespace from global-zk", e);
            throw new RestException(e);
        }
    }

    private boolean checkQuotas(Policies policies, RetentionPolicies retention) {
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlogQuotaMap = policies.backlog_quota_map;
        if (backlogQuotaMap.isEmpty()) {
            return true;
        }
        BacklogQuota quota = backlogQuotaMap.get(BacklogQuotaType.destination_storage);
        return checkBacklogQuota(quota, retention);
    }

    private void clearBacklog(NamespaceName nsName, String bundleRange, String subscription) {
        try {
            List<Topic> topicList = pulsar().getBrokerService().getAllTopicsFromNamespaceBundle(nsName.toString(),
                    nsName.toString() + "/" + bundleRange);

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            if (subscription != null) {
                if (subscription.startsWith(pulsar().getConfiguration().getReplicatorPrefix())) {
                    subscription = PersistentReplicator.getRemoteCluster(subscription);
                }
                for (Topic topic : topicList) {
                    if (topic instanceof PersistentTopic
                            && !pulsar().getBrokerService().isSystemTopic(TopicName.get(topic.getName()))) {
                        futures.add(((PersistentTopic) topic).clearBacklog(subscription));
                    }
                }
            } else {
                for (Topic topic : topicList) {
                    if (topic instanceof PersistentTopic
                            && !pulsar().getBrokerService().isSystemTopic(TopicName.get(topic.getName()))) {
                        futures.add(((PersistentTopic) topic).clearBacklog());
                    }
                }
            }

            FutureUtil.waitForAll(futures).get();
        } catch (Exception e) {
            log.error("[{}] Failed to clear backlog for namespace {}/{}, subscription: {}", clientAppId(),
                    nsName.toString(), bundleRange, subscription, e);
            throw new RestException(e);
        }
    }

    private void unsubscribe(NamespaceName nsName, String bundleRange, String subscription) {
        try {
            List<Topic> topicList = pulsar().getBrokerService().getAllTopicsFromNamespaceBundle(nsName.toString(),
                    nsName.toString() + "/" + bundleRange);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            if (subscription.startsWith(pulsar().getConfiguration().getReplicatorPrefix())) {
                throw new RestException(Status.PRECONDITION_FAILED, "Cannot unsubscribe a replication cursor");
            } else {
                for (Topic topic : topicList) {
                    Subscription sub = topic.getSubscription(subscription);
                    if (sub != null) {
                        futures.add(sub.delete());
                    }
                }
            }
            FutureUtil.waitForAll(futures).get();
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to unsubscribe {} for namespace {}/{}", clientAppId(), subscription,
                    nsName.toString(), bundleRange, e);
            if (e.getCause() instanceof SubscriptionBusyException) {
                throw new RestException(Status.PRECONDITION_FAILED, "Subscription has active connected consumers");
            }
            throw new RestException(e.getCause());
        }
    }

    protected BundlesData validateBundlesData(BundlesData initialBundles) {
        SortedSet<String> partitions = new TreeSet<String>();
        for (String partition : initialBundles.getBoundaries()) {
            Long partBoundary = Long.decode(partition);
            partitions.add(String.format("0x%08x", partBoundary));
        }
        if (partitions.size() != initialBundles.getBoundaries().size()) {
            if (log.isDebugEnabled()) {
                log.debug("Input bundles included repeated partition points. Ignored.");
            }
        }
        try {
            NamespaceBundleFactory.validateFullRange(partitions);
        } catch (IllegalArgumentException iae) {
            throw new RestException(Status.BAD_REQUEST, "Input bundles do not cover the whole hash range. first:"
                    + partitions.first() + ", last:" + partitions.last());
        }
        List<String> bundles = new ArrayList<>();
        bundles.addAll(partitions);
        return BundlesData.builder()
                .boundaries(bundles)
                .numBundles(bundles.size() - 1)
                .build();
    }

    private CompletableFuture<Void> validatePoliciesAsync(NamespaceName ns, Policies policies) {
        if (ns.isV2() && policies.replication_clusters.isEmpty()) {
            // Default to local cluster
            policies.replication_clusters = Collections.singleton(config().getClusterName());
        }

        // Validate cluster names and permissions
        return policies.replication_clusters.stream()
                    .map(cluster -> validateClusterForTenantAsync(ns.getTenant(), cluster))
                    .reduce(CompletableFuture.completedFuture(null), (a, b) -> a.thenCompose(ignore -> b))
            .thenAccept(__ -> {
                if (policies.message_ttl_in_seconds != null && policies.message_ttl_in_seconds < 0) {
                    throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for message TTL");
                }

                if (policies.bundles != null && policies.bundles.getNumBundles() > 0) {
                    if (policies.bundles.getBoundaries() == null || policies.bundles.getBoundaries().size() == 0) {
                        policies.bundles = getBundles(policies.bundles.getNumBundles());
                    } else {
                        policies.bundles = validateBundlesData(policies.bundles);
                    }
                } else {
                    int defaultNumberOfBundles = config().getDefaultNumberOfNamespaceBundles();
                    policies.bundles = getBundles(defaultNumberOfBundles);
                }

                if (policies.persistence != null) {
                    validatePersistencePolicies(policies.persistence);
                }

                if (policies.retention_policies != null) {
                    validateRetentionPolicies(policies.retention_policies);
                }
            });
    }

    protected void validateRetentionPolicies(RetentionPolicies retention) {
        if (retention == null) {
            return;
        }
        checkArgument(retention.getRetentionSizeInMB() >= -1,
                "Invalid retention policy: size limit must be >= -1");
        checkArgument(retention.getRetentionTimeInMinutes() >= -1,
                "Invalid retention policy: time limit must be >= -1");
        checkArgument((retention.getRetentionTimeInMinutes() != 0 && retention.getRetentionSizeInMB() != 0)
                        || (retention.getRetentionTimeInMinutes() == 0 && retention.getRetentionSizeInMB() == 0),
                "Invalid retention policy: Setting a single time or size limit to 0 is invalid when "
                        + "one of the limits has a non-zero value. Use the value of -1 instead of 0 to ignore a "
                        + "specific limit. To disable retention both limits must be set to 0.");
    }

    protected void internalSetDeduplicationSnapshotInterval(Integer interval) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.DEDUPLICATION_SNAPSHOT, PolicyOperation.WRITE);
        if (interval != null && interval < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "interval must be greater than or equal to 0");
        }
        internalSetPolicies("deduplicationSnapshotIntervalSeconds", interval);
    }

    protected void internalSetMaxProducersPerTopic(Integer maxProducersPerTopic) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.MAX_PRODUCERS, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        try {
            if (maxProducersPerTopic != null && maxProducersPerTopic < 0) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "maxProducersPerTopic must be 0 or more");
            }
            updatePolicies(namespaceName, policies -> {
                policies.max_producers_per_topic = maxProducersPerTopic;
                return policies;
            });
            log.info("[{}] Successfully updated maxProducersPerTopic configuration: namespace={}, value={}",
                    clientAppId(), namespaceName, maxProducersPerTopic);
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxProducersPerTopic configuration for namespace {}",
                    clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Boolean> internalGetDeduplicationAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DEDUPLICATION, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.deduplicationEnabled);
    }

    protected void internalSetMaxConsumersPerTopic(Integer maxConsumersPerTopic) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        try {
            if (maxConsumersPerTopic != null && maxConsumersPerTopic < 0) {
                throw new RestException(Status.PRECONDITION_FAILED, "maxConsumersPerTopic must be 0 or more");
            }
            updatePolicies(namespaceName, policies -> {
                policies.max_consumers_per_topic = maxConsumersPerTopic;
                return policies;
            });
            log.info("[{}] Successfully updated maxConsumersPerTopic configuration: namespace={}, value={}",
                    clientAppId(), namespaceName, maxConsumersPerTopic);

        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxConsumersPerTopic configuration for namespace {}",
                    clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetMaxConsumersPerSubscription(Integer maxConsumersPerSubscription) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.MAX_CONSUMERS, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        try {
            if (maxConsumersPerSubscription != null && maxConsumersPerSubscription < 0) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "maxConsumersPerSubscription must be 0 or more");
            }
            updatePolicies(namespaceName, policies -> {
                policies.max_consumers_per_subscription = maxConsumersPerSubscription;
                return policies;
            });
            log.info("[{}] Successfully updated maxConsumersPerSubscription configuration: namespace={}, value={}",
                    clientAppId(), namespaceName, maxConsumersPerSubscription);
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxConsumersPerSubscription configuration for namespace {}",
                    clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetMaxUnackedMessagesPerConsumer(Integer maxUnackedMessagesPerConsumer) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.MAX_UNACKED, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        if (maxUnackedMessagesPerConsumer != null && maxUnackedMessagesPerConsumer < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedMessagesPerConsumer must be 0 or more");
        }
        try {
            updatePolicies(namespaceName, policies -> {
                policies.max_unacked_messages_per_consumer = maxUnackedMessagesPerConsumer;
                return policies;
            });
            log.info("[{}] Successfully updated maxUnackedMessagesPerConsumer configuration: namespace={}, value={}",
                    clientAppId(), namespaceName, maxUnackedMessagesPerConsumer);

        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxUnackedMessagesPerConsumer configuration for namespace {}",
                    clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetMaxSubscriptionsPerTopic(Integer maxSubscriptionsPerTopic){
        validateNamespacePolicyOperation(namespaceName, PolicyName.MAX_SUBSCRIPTIONS, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        if (maxSubscriptionsPerTopic != null && maxSubscriptionsPerTopic < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxSubscriptionsPerTopic must be 0 or more");
        }
        internalSetPolicies("max_subscriptions_per_topic", maxSubscriptionsPerTopic);
    }

    protected void internalSetMaxUnackedMessagesPerSubscription(Integer maxUnackedMessagesPerSubscription) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.MAX_UNACKED, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        if (maxUnackedMessagesPerSubscription != null && maxUnackedMessagesPerSubscription < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedMessagesPerSubscription must be 0 or more");
        }
        try {
            updatePolicies(namespaceName, policies -> {
                policies.max_unacked_messages_per_subscription = maxUnackedMessagesPerSubscription;
                return policies;
            });
            log.info("[{}] Successfully updated maxUnackedMessagesPerSubscription"
                            + " configuration: namespace={}, value={}",
                    clientAppId(), namespaceName, maxUnackedMessagesPerSubscription);

        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxUnackedMessagesPerSubscription configuration for namespace {}",
                    clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetCompactionThreshold(Long newThreshold) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.COMPACTION, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        try {
            if (newThreshold != null && newThreshold < 0) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "compactionThreshold must be 0 or more");
            }
            updatePolicies(namespaceName, policies -> {
                policies.compaction_threshold = newThreshold;
                return policies;
            });
            log.info("[{}] Successfully updated compactionThreshold configuration: namespace={}, value={}",
                    clientAppId(), namespaceName, newThreshold);

        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update compactionThreshold configuration for namespace {}",
                    clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetOffloadThreshold(long newThreshold) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        try {
            updatePolicies(namespaceName, policies -> {
                if (policies.offload_policies == null) {
                    policies.offload_policies = new OffloadPoliciesImpl();
                }
                ((OffloadPoliciesImpl) policies.offload_policies).setManagedLedgerOffloadThresholdInBytes(newThreshold);
                policies.offload_threshold = newThreshold;
                return policies;
            });
            log.info("[{}] Successfully updated offloadThreshold configuration: namespace={}, value={}",
                    clientAppId(), namespaceName, newThreshold);

        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update offloadThreshold configuration for namespace {}",
                    clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> internalSetOffloadThresholdInSecondsAsync(long newThreshold) {
        CompletableFuture<Void> f = new CompletableFuture<>();

        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD, PolicyOperation.WRITE)
                .thenCompose(v -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(v -> updatePoliciesAsync(namespaceName,
                        policies -> {
                            if (policies.offload_policies == null) {
                                policies.offload_policies = new OffloadPoliciesImpl();
                            }
                            ((OffloadPoliciesImpl) policies.offload_policies)
                                    .setManagedLedgerOffloadThresholdInSeconds(newThreshold);
                            policies.offload_threshold_in_seconds = newThreshold;
                            return policies;
                        })
                )
                .thenAccept(v -> {
                    log.info("[{}] Successfully updated offloadThresholdInSeconds configuration:"
                            + " namespace={}, value={}", clientAppId(), namespaceName, newThreshold);
                    f.complete(null);
                })
                .exceptionally(t -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(t);
                    log.error("[{}] Failed to update offloadThresholdInSeconds configuration for namespace {}",
                            clientAppId(), namespaceName, t);
                    f.completeExceptionally(new RestException(cause));
                    return null;
                });

        return f;
    }

    protected void internalSetOffloadDeletionLag(Long newDeletionLagMs) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        try {
            updatePolicies(namespaceName, policies -> {
                if (policies.offload_policies == null) {
                    policies.offload_policies = new OffloadPoliciesImpl();
                }
                ((OffloadPoliciesImpl) policies.offload_policies)
                        .setManagedLedgerOffloadDeletionLagInMillis(newDeletionLagMs);
                policies.offload_deletion_lag_ms = newDeletionLagMs;
                return policies;
            });
            log.info("[{}] Successfully updated offloadDeletionLagMs configuration: namespace={}, value={}",
                    clientAppId(), namespaceName, newDeletionLagMs);
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update offloadDeletionLag configuration for namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    @Deprecated
    protected SchemaAutoUpdateCompatibilityStrategy internalGetSchemaAutoUpdateCompatibilityStrategy() {
        validateNamespacePolicyOperation(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.READ);
        return getNamespacePolicies(namespaceName).schema_auto_update_compatibility_strategy;
    }

    @Deprecated
    protected void internalSetSchemaAutoUpdateCompatibilityStrategy(SchemaAutoUpdateCompatibilityStrategy strategy) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        mutatePolicy((policies) -> {
                    policies.schema_auto_update_compatibility_strategy = strategy;
                    return policies;
                }, (policies) -> policies.schema_auto_update_compatibility_strategy,
                "schemaAutoUpdateCompatibilityStrategy");
    }

    protected void internalSetSchemaCompatibilityStrategy(SchemaCompatibilityStrategy strategy) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        mutatePolicy((policies) -> {
                    policies.schema_compatibility_strategy = strategy;
                    return policies;
                }, (policies) -> policies.schema_compatibility_strategy,
                "schemaCompatibilityStrategy");
    }

    protected void internalSetSchemaValidationEnforced(boolean schemaValidationEnforced) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        mutatePolicy((policies) -> {
                    policies.schema_validation_enforced = schemaValidationEnforced;
                    return policies;
                }, (policies) -> policies.schema_validation_enforced,
                "schemaValidationEnforced");
    }

    protected void internalSetIsAllowAutoUpdateSchema(boolean isAllowAutoUpdateSchema) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        mutatePolicy((policies) -> {
                    policies.is_allow_auto_update_schema = isAllowAutoUpdateSchema;
                    return policies;
                }, (policies) -> policies.is_allow_auto_update_schema,
                "isAllowAutoUpdateSchema");
    }

    protected void internalSetSubscriptionTypesEnabled(Set<SubscriptionType> subscriptionTypesEnabled) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.SUBSCRIPTION_AUTH_MODE,
                PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        Set<String> subTypes = new HashSet<>();
        subscriptionTypesEnabled.forEach(subscriptionType -> subTypes.add(subscriptionType.name()));
        mutatePolicy((policies) -> {
                    policies.subscription_types_enabled = subTypes;
                    return policies;
                }, (policies) -> policies.subscription_types_enabled,
                "subscriptionTypesEnabled");
    }


    private <T> void mutatePolicy(Function<Policies, Policies> policyTransformation,
                                  Function<Policies, T> getter,
                                  String policyName) {
        try {
            MutableObject exception = new MutableObject(null);
            MutableObject policiesObj = new MutableObject(null);
            updatePolicies(namespaceName, policies -> {
                try {
                    policies = policyTransformation.apply(policies);
                } catch (Exception e) {
                    exception.setValue(e);
                }
                policiesObj.setValue(policies);
                return policies;
            });
            if (exception.getValue() != null) {
                throw (Exception) exception.getValue();
            }
            log.info("[{}] Successfully updated {} configuration: namespace={}, value={}", clientAppId(), policyName,
                    namespaceName, getter.apply((Policies) policiesObj.getValue()));
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update {} configuration for namespace {}",
                    clientAppId(), policyName, namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetOffloadPolicies(AsyncResponse asyncResponse, OffloadPoliciesImpl offloadPolicies) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        validateOffloadPolicies(offloadPolicies);

        try {
            namespaceResources().setPoliciesAsync(namespaceName, policies -> {
                if (Objects.equals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(),
                        OffloadPoliciesImpl.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS)) {
                    offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(policies.offload_deletion_lag_ms);
                } else {
                    policies.offload_deletion_lag_ms = offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis();
                }
                if (Objects.equals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(),
                        OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES)) {
                    offloadPolicies.setManagedLedgerOffloadThresholdInBytes(policies.offload_threshold);
                } else {
                    policies.offload_threshold = offloadPolicies.getManagedLedgerOffloadThresholdInBytes();
                }
                policies.offload_policies = offloadPolicies;
                return policies;
            }).thenApply(r -> {
                log.info("[{}] Successfully updated offload configuration: namespace={}, map={}", clientAppId(),
                        namespaceName, offloadPolicies);
                asyncResponse.resume(Response.noContent().build());
                return null;
            }).exceptionally(e -> {
                log.error("[{}] Failed to update offload configuration for namespace {}", clientAppId(), namespaceName,
                        e);
                asyncResponse.resume(new RestException(e));
                return null;
            });
        } catch (Exception e) {
            log.error("[{}] Failed to update offload configuration for namespace {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(e.getCause() instanceof NotFoundException
                    ? new RestException(Status.CONFLICT, "Concurrent modification")
                    : new RestException(e));
        }
    }

    protected void internalRemoveOffloadPolicies(AsyncResponse asyncResponse) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.OFFLOAD, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();
        try {
            namespaceResources().setPoliciesAsync(namespaceName, (policies) -> {
                policies.offload_policies = null;
                return policies;
            }).thenApply(r -> {
                log.info("[{}] Successfully remove offload configuration: namespace={}", clientAppId(), namespaceName);
                asyncResponse.resume(Response.noContent().build());
                return null;
            }).exceptionally(e -> {
                log.error("[{}] Failed to remove offload configuration for namespace {}", clientAppId(), namespaceName,
                        e);
                asyncResponse.resume(e.getCause() instanceof NotFoundException
                        ? new RestException(Status.CONFLICT, "Concurrent modification")
                        : new RestException(e));
                return null;
            });
        } catch (Exception e) {
            log.error("[{}] Failed to remove offload configuration for namespace {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
        }
    }

    private void validateOffloadPolicies(OffloadPoliciesImpl offloadPolicies) {
        if (offloadPolicies == null) {
            log.warn("[{}] Failed to update offload configuration for namespace {}: offloadPolicies is null",
                    clientAppId(), namespaceName);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "The offloadPolicies must be specified for namespace offload.");
        }
        if (!offloadPolicies.driverSupported()) {
            log.warn("[{}] Failed to update offload configuration for namespace {}: "
                            + "driver is not supported, support value: {}",
                    clientAppId(), namespaceName, OffloadPoliciesImpl.getSupportedDriverNames());
            throw new RestException(Status.PRECONDITION_FAILED,
                    "The driver is not supported, support value: " + OffloadPoliciesImpl.getSupportedDriverNames());
        }
        if (!offloadPolicies.bucketValid()) {
            log.warn("[{}] Failed to update offload configuration for namespace {}: bucket must be specified",
                    clientAppId(), namespaceName);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "The bucket must be specified for namespace offload.");
        }
    }

   protected void internalRemoveMaxTopicsPerNamespace() {
        validateNamespacePolicyOperation(namespaceName, PolicyName.MAX_TOPICS, PolicyOperation.WRITE);
        internalSetMaxTopicsPerNamespace(null);
   }

   protected void internalSetMaxTopicsPerNamespace(Integer maxTopicsPerNamespace) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.MAX_TOPICS, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        if (maxTopicsPerNamespace != null && maxTopicsPerNamespace < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxTopicsPerNamespace must be 0 or more");
        }
        internalSetPolicies("max_topics_per_namespace", maxTopicsPerNamespace);
   }

   protected void internalSetProperty(String key, String value, AsyncResponse asyncResponse) {
       validateAdminAccessForTenantAsync(namespaceName.getTenant())
               .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
               .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                   policies.properties.put(key, value);
                   return policies;
               }))
               .thenAccept(v -> {
                   log.info("[{}] Successfully set property for key {} on namespace {}", clientAppId(), key,
                           namespaceName);
                   asyncResponse.resume(Response.noContent().build());
               }).exceptionally(ex -> {
                   Throwable cause = ex.getCause();
                   log.error("[{}] Failed to set property for key {} on namespace {}", clientAppId(), key,
                           namespaceName, cause);
                   asyncResponse.resume(cause);
                   return null;
               });
   }

   protected void internalSetProperties(Map<String, String> properties, AsyncResponse asyncResponse) {
       validateAdminAccessForTenantAsync(namespaceName.getTenant())
               .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
               .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                   policies.properties.putAll(properties);
                   return policies;
               }))
               .thenAccept(v -> {
                   log.info("[{}] Successfully set {} properties on namespace {}", clientAppId(), properties.size(),
                           namespaceName);
                   asyncResponse.resume(Response.noContent().build());
               }).exceptionally(ex -> {
                   Throwable cause = ex.getCause();
                   log.error("[{}] Failed to set {} properties on namespace {}", clientAppId(), properties.size(),
                           namespaceName, cause);
                   asyncResponse.resume(cause);
                   return null;
               });
   }

   protected void internalGetProperty(String key, AsyncResponse asyncResponse) {
       validateAdminAccessForTenantAsync(namespaceName.getTenant())
               .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
               .thenAccept(policies -> asyncResponse.resume(policies.properties.get(key)))
               .exceptionally(ex -> {
                   Throwable cause = ex.getCause();
                   log.error("[{}] Failed to get property for key {} of namespace {}", clientAppId(), key,
                           namespaceName, cause);
                   asyncResponse.resume(cause);
                   return null;
               });
   }

   protected void internalGetProperties(AsyncResponse asyncResponse) {
       validateAdminAccessForTenantAsync(namespaceName.getTenant())
               .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
               .thenAccept(policies -> asyncResponse.resume(policies.properties))
               .exceptionally(ex -> {
                   Throwable cause = ex.getCause();
                   log.error("[{}] Failed to get properties of namespace {}", clientAppId(), namespaceName, cause);
                   asyncResponse.resume(cause);
                   return null;
               });
   }

   protected void internalRemoveProperty(String key, AsyncResponse asyncResponse) {
       AtomicReference<String> oldVal = new AtomicReference<>(null);
       validateAdminAccessForTenantAsync(namespaceName.getTenant())
               .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
               .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                   oldVal.set(policies.properties.remove(key));
                   return policies;
               })).thenAccept(v -> {
                   asyncResponse.resume(oldVal.get());
                   log.info("[{}] Successfully remove property for key {} on namespace {}", clientAppId(), key,
                           namespaceName);
               }).exceptionally(ex -> {
                   Throwable cause = ex.getCause();
                   log.error("[{}] Failed to remove property for key {} on namespace {}", clientAppId(), key,
                           namespaceName, cause);
                   asyncResponse.resume(cause);
                   return null;
               });
   }

   protected void internalClearProperties(AsyncResponse asyncResponse) {
       AtomicReference<Integer> clearedCount = new AtomicReference<>(0);
       validateAdminAccessForTenantAsync(namespaceName.getTenant())
               .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
               .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                   clearedCount.set(policies.properties.size());
                   policies.properties.clear();
                   return policies;
               }))
               .thenAccept(v -> {
                   asyncResponse.resume(Response.noContent().build());
                   log.info("[{}] Successfully clear {} properties on namespace {}", clientAppId(), clearedCount.get(),
                           namespaceName);
               }).exceptionally(ex -> {
                   Throwable cause = ex.getCause();
                   log.error("[{}] Failed to clear {} properties on namespace {}", clientAppId(), clearedCount.get(),
                           namespaceName, cause);
                   asyncResponse.resume(cause);
                   return null;
               });
   }

   private CompletableFuture<Void> updatePoliciesAsync(NamespaceName ns, Function<Policies, Policies> updateFunction) {
       CompletableFuture<Void> result = new CompletableFuture<>();
       namespaceResources().setPoliciesAsync(ns, updateFunction)
           .thenAccept(v -> {
               log.info("[{}] Successfully updated the policies on namespace {}", clientAppId(), namespaceName);
               result.complete(null);
           })
           .exceptionally(ex -> {
               Throwable cause = ex.getCause();
               if (cause instanceof NotFoundException) {
                   result.completeExceptionally(new RestException(Status.NOT_FOUND, "Namespace does not exist"));
               } else if (cause instanceof BadVersionException) {
                   log.warn("[{}] Failed to update the replication clusters on"
                                   + " namespace {} : concurrent modification", clientAppId(), namespaceName);
                   result.completeExceptionally(new RestException(Status.CONFLICT, "Concurrent modification"));
               } else {
                   log.error("[{}] Failed to update namespace policies {}", clientAppId(), namespaceName, cause);
                   result.completeExceptionally(new RestException(cause));
               }
               return null;
           });
       return result;
   }

   private void updatePolicies(NamespaceName ns, Function<Policies, Policies> updateFunction) {
       // Force to read the data s.t. the watch to the cache content is setup.
       try {
           updatePoliciesAsync(ns, updateFunction).get(namespaceResources().getOperationTimeoutSec(),
                   TimeUnit.SECONDS);
       } catch (Exception e) {
           Throwable cause = e.getCause();
           if (!(cause instanceof RestException)) {
               throw new RestException(cause);
           } else {
               throw (RestException) cause;
           }
       }
   }

    protected void internalSetNamespaceResourceGroup(String rgName) {
        validateNamespacePolicyOperation(namespaceName, PolicyName.RESOURCEGROUP, PolicyOperation.WRITE);
        validatePoliciesReadOnlyAccess();

        if (rgName != null) {
            // check resourcegroup exists.
            try {
                if (!resourceGroupResources().resourceGroupExists(rgName)) {
                    throw new RestException(Status.PRECONDITION_FAILED, "ResourceGroup does not exist");
                }
            } catch (Exception e) {
                log.error("[{}] Invalid ResourceGroup {}: {}", clientAppId(), rgName, e);
                throw new RestException(e);
            }
        }

        internalSetPolicies("resource_group_name", rgName);
    }

    protected void internalScanOffloadedLedgers(OffloaderObjectsScannerUtils.ScannerResultSink sink)
            throws Exception {
        log.info("internalScanOffloadedLedgers {}", namespaceName);
        validateNamespacePolicyOperation(namespaceName, PolicyName.OFFLOAD, PolicyOperation.READ);

        Policies policies = getNamespacePolicies(namespaceName);
        LedgerOffloader managedLedgerOffloader = pulsar()
                .getManagedLedgerOffloader(namespaceName, (OffloadPoliciesImpl) policies.offload_policies);

        String localClusterName = pulsar().getConfiguration().getClusterName();

        OffloaderObjectsScannerUtils.scanOffloadedLedgers(managedLedgerOffloader,
                localClusterName, pulsar().getManagedLedgerFactory(), sink);

    }

    protected CompletableFuture<Void> internalSetReplicateSubscriptionStateAsync(Boolean enabled) {
        return validatePoliciesReadOnlyAccessAsync()
                .thenCompose(__ -> validateNamespacePolicyOperationAsync(namespaceName,
                        PolicyName.REPLICATED_SUBSCRIPTION, PolicyOperation.WRITE))
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.replicate_subscription_state = enabled;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetEntryFiltersPerTopicAsync(EntryFilters entryFilters) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ENTRY_FILTERS, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> validateEntryFilters(entryFilters))
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.entryFilters = entryFilters;
                    return policies;
                }));
    }


    /**
     * Base method for setReplicatorDispatchRate v1 and v2.
     * Notion: don't re-use this logic.
     */
    protected void internalSetReplicatorDispatchRate(AsyncResponse asyncResponse, DispatchRateImpl dispatchRate) {
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.REPLICATION_RATE, PolicyOperation.WRITE)
                .thenAccept(__ -> {
                    log.info("[{}] Set namespace replicator dispatch-rate {}/{}",
                            clientAppId(), namespaceName, dispatchRate);
                }).thenCompose(__ -> namespaceResources().setPoliciesAsync(namespaceName, policies -> {
                    String clusterName = pulsar().getConfiguration().getClusterName();
                    policies.replicatorDispatchRate.put(clusterName, dispatchRate);
                    return policies;
                })).thenAccept(__ -> {
                    asyncResponse.resume(Response.noContent().build());
                    log.info("[{}] Successfully updated the replicatorDispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName);
                }).exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    log.error("[{}] Failed to update the replicatorDispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName, ex);
                    return null;
                });
    }
    /**
     * Base method for getReplicatorDispatchRate v1 and v2.
     * Notion: don't re-use this logic.
     */
    protected void internalGetReplicatorDispatchRate(AsyncResponse asyncResponse) {
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.REPLICATION_RATE, PolicyOperation.READ)
                .thenCompose(__ -> namespaceResources().getPoliciesAsync(namespaceName))
                .thenApply(policiesOpt -> {
                    if (!policiesOpt.isPresent()) {
                        throw new RestException(Response.Status.NOT_FOUND, "Namespace policies does not exist");
                    }
                    String clusterName = pulsar().getConfiguration().getClusterName();
                    return policiesOpt.get().replicatorDispatchRate.get(clusterName);
                }).thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    log.error("[{}] Failed to get replicator dispatch-rate configured for the namespace {}",
                            clientAppId(), namespaceName, ex);
                    return null;
                });
    }
    /**
     * Base method for removeReplicatorDispatchRate v1 and v2.
     * Notion: don't re-use this logic.
     */
    protected void internalRemoveReplicatorDispatchRate(AsyncResponse asyncResponse) {
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.REPLICATION_RATE, PolicyOperation.WRITE)
                .thenCompose(__ -> namespaceResources().setPoliciesAsync(namespaceName, policies -> {
                    String clusterName = pulsar().getConfiguration().getClusterName();
                    policies.replicatorDispatchRate.remove(clusterName);
                    return policies;
                })).thenAccept(__ -> {
                    asyncResponse.resume(Response.noContent().build());
                    log.info("[{}] Successfully delete the replicatorDispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName);
                }).exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    log.error("[{}] Failed to delete the replicatorDispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName, ex);
                    return null;
                });
    }

    /**
     * Base method for getBackLogQuotaMap v1 and v2.
     * Notion: don't re-use this logic.
     */
    protected void internalGetBacklogQuotaMap(AsyncResponse asyncResponse) {
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.BACKLOG, PolicyOperation.READ)
                .thenCompose(__ -> namespaceResources().getPoliciesAsync(namespaceName))
                .thenAccept(policiesOpt -> {
                    Map<BacklogQuotaType, BacklogQuota> backlogQuotaMap = policiesOpt.orElseThrow(() ->
                            new RestException(Response.Status.NOT_FOUND, "Namespace does not exist"))
                            .backlog_quota_map;
                    asyncResponse.resume(backlogQuotaMap);
                })
                .exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    log.error("[{}] Failed to get backlog quota map on namespace {}", clientAppId(), namespaceName, ex);
                    return null;
                });
    }

    /**
     * Base method for setBacklogQuota v1 and v2.
     * Notion: don't re-use this logic.
     */
    protected void internalSetBacklogQuota(AsyncResponse asyncResponse,
                                           BacklogQuotaType backlogQuotaType, BacklogQuota backlogQuota) {
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.BACKLOG, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> setBacklogQuotaAsync(backlogQuotaType, backlogQuota))
                .thenAccept(__ -> {
                    asyncResponse.resume(Response.noContent().build());
                    log.info("[{}] Successfully updated backlog quota map: namespace={}, map={}", clientAppId(),
                            namespaceName, backlogQuota);
                }).exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    log.error("[{}] Failed to update backlog quota map for namespace {}",
                            clientAppId(), namespaceName, ex);
                    return null;
                });
    }

    /**
     * Base method for removeBacklogQuota v1 and v2.
     * Notion: don't re-use this logic.
     */
    protected void internalRemoveBacklogQuota(AsyncResponse asyncResponse, BacklogQuotaType backlogQuotaType) {
        validateNamespacePolicyOperationAsync(namespaceName, PolicyName.BACKLOG, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> namespaceResources().setPoliciesAsync(namespaceName, policies -> {
                    final BacklogQuotaType quotaType = backlogQuotaType != null ? backlogQuotaType
                            : BacklogQuotaType.destination_storage;
                    policies.backlog_quota_map.remove(quotaType);
                    return policies;
                })).thenAccept(__ -> {
                    asyncResponse.resume(Response.noContent().build());
                    log.info("[{}] Successfully removed backlog namespace={}, quota={}", clientAppId(), namespaceName,
                            backlogQuotaType);
                }).exceptionally(ex -> {
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    log.error("[{}] Failed to update backlog quota map for namespace {}",
                            clientAppId(), namespaceName, ex);
                    return null;
                });
    }

    protected CompletableFuture<Void> internalSetNamespaceAllowedClusters(List<String> clusterIds) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ALLOW_CLUSTERS, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                // Allowed clusters in the namespace policy should be included in the allowed clusters in the tenant
                // policy.
                .thenCompose(__ -> FutureUtil.waitForAll(clusterIds.stream().map(clusterId ->
                        validateClusterForTenantAsync(namespaceName.getTenant(), clusterId))
                        .collect(Collectors.toList())))
                // Allowed clusters should include all the existed replication clusters and could not contain global
                // cluster.
                .thenCompose(__ -> {
                    checkNotNull(clusterIds, "ClusterIds should not be null");
                    if (clusterIds.contains("global")) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Cannot specify global in the list of allowed clusters");
                    }
                    return getNamespacePoliciesAsync(this.namespaceName).thenApply(namespacePolicies -> {
                        namespacePolicies.replication_clusters.forEach(replicationCluster -> {
                            if (!clusterIds.contains(replicationCluster)) {
                                throw new RestException(Status.BAD_REQUEST,
                                        String.format("Allowed clusters do not contain the replication cluster %s. "
                                                + "Please remove the replication cluster if the cluster is not allowed "
                                                + "for this namespace", replicationCluster));
                            }
                        });
                        return Sets.newHashSet(clusterIds);
                    });
                })
                // Verify the allowed clusters are valid and they do not contain the peer clusters.
                .thenCompose(allowedClusters -> clustersAsync()
                        .thenCompose(clusters -> {
                            List<CompletableFuture<Void>> futures =
                                    allowedClusters.stream().map(clusterId -> {
                                        if (!clusters.contains(clusterId)) {
                                            throw new RestException(Status.FORBIDDEN,
                                                    "Invalid cluster id: " + clusterId);
                                        }
                                        return validatePeerClusterConflictAsync(clusterId, allowedClusters);
                                    }).collect(Collectors.toList());
                            return FutureUtil.waitForAll(futures).thenApply(__ -> allowedClusters);
                        }))
                // Update allowed clusters into policies.
                .thenCompose(allowedClusterSet -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.allowed_clusters = allowedClusterSet;
                    return policies;
                }));
    }

    protected CompletableFuture<Set<String>> internalGetNamespaceAllowedClustersAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ALLOW_CLUSTERS, PolicyOperation.READ)
                .thenAccept(__ -> {
                    if (!namespaceName.isGlobal()) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Cannot get the allowed clusters for a non-global namespace");
                    }
                }).thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.allowed_clusters);
    }


}
