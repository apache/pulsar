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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.cache.LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NamespacesBase extends AdminResource {

    private static final long MAX_BUNDLES = ((long) 1) << 32;

    protected List<String> internalGetTenantNamespaces(String tenant) {
        validateAdminAccessForTenant(tenant);

        try {
            return getListOfNamespaces(tenant);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to get namespace list for tenant: {} - Does not exist", clientAppId(), tenant);
            throw new RestException(Status.NOT_FOUND, "Property does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to get namespaces list: {}", clientAppId(), e);
            throw new RestException(e);
        }
    }

    protected void internalCreateNamespace(Policies policies) {
        validatePoliciesReadOnlyAccess();
        validateAdminAccessForTenant(namespaceName.getTenant());

        validatePolicies(namespaceName, policies);

        try {
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));

            zkCreateOptimistic(path(POLICIES, namespaceName.toString()), jsonMapper().writeValueAsBytes(policies));
            log.info("[{}] Created namespace {}", clientAppId(), namespaceName);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create namespace {} - already exists", clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Namespace already exists");
        } catch (Exception e) {
            log.error("[{}] Failed to create namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    @SuppressWarnings("deprecation")
    protected void internalDeleteNamespace(AsyncResponse asyncResponse, boolean authoritative) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        // ensure that non-global namespace is directed to the correct cluster
        if (!namespaceName.isGlobal()) {
            validateClusterOwnership(namespaceName.getCluster());
        }

        Entry<Policies, Stat> policiesNode = null;
        Policies policies = null;

        // ensure the local cluster is the only cluster for the global namespace configuration
        try {
            policiesNode = policiesCache().getWithStat(path(POLICIES, namespaceName.toString())).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist."));

            policies = policiesNode.getKey();
            if (namespaceName.isGlobal()) {
                if (policies.replication_clusters.size() > 1) {
                    // There are still more than one clusters configured for the global namespace
                    throw new RestException(Status.PRECONDITION_FAILED, "Cannot delete the global namespace "
                            + namespaceName + ". There are still more than one replication clusters configured.");
                }
                if (policies.replication_clusters.size() == 1
                        && !policies.replication_clusters.contains(config().getClusterName())) {
                    // the only replication cluster is other cluster, redirect
                    String replCluster = Lists.newArrayList(policies.replication_clusters).get(0);
                    ClusterData replClusterData = clustersCache().get(AdminResource.path("clusters", replCluster))
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                    "Cluster " + replCluster + " does not exist"));
                    URL replClusterUrl;
                    if (!config().isTlsEnabled() || !isRequestHttps()) {
                        replClusterUrl = new URL(replClusterData.getServiceUrl());
                    } else if (StringUtils.isNotBlank(replClusterData.getServiceUrlTls())) {
                        replClusterUrl = new URL(replClusterData.getServiceUrlTls());
                    } else {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "The replication cluster does not provide TLS encrypted service");
                    }
                    URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(replClusterUrl.getHost())
                            .port(replClusterUrl.getPort()).replaceQueryParam("authoritative", false).build();
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(), redirect,
                                replCluster);
                    }
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
            return;
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        boolean isEmpty;
        try {
            isEmpty = pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName).join().isEmpty()
                    && getPartitionedTopicList(TopicDomain.persistent).isEmpty()
                    && getPartitionedTopicList(TopicDomain.non_persistent).isEmpty();
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        if (!isEmpty) {
            if (log.isDebugEnabled()) {
                log.debug("Found topics on namespace {}", namespaceName);
            }
            asyncResponse.resume(new RestException(Status.CONFLICT, "Cannot delete non empty namespace"));
            return;
        }

        // set the policies to deleted so that somebody else cannot acquire this namespace
        try {
            policies.deleted = true;
            globalZk().setData(path(POLICIES, namespaceName.toString()), jsonMapper().writeValueAsBytes(policies),
                    policiesNode.getValue().getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
        } catch (Exception e) {
            log.error("[{}] Failed to delete namespace on global ZK {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
            return;
        }

        // remove from owned namespace map and ephemeral node from ZK
        final List<CompletableFuture<Void>> futures = Lists.newArrayList();
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            for (NamespaceBundle bundle : bundles.getBundles()) {
                // check if the bundle is owned by any broker, if not then we do not need to delete the bundle
                if (pulsar().getNamespaceService().getOwner(bundle).isPresent()) {
                    futures.add(pulsar().getAdminClient().namespaces()
                            .deleteNamespaceBundleAsync(namespaceName.toString(), bundle.getBundleRange()));
                }
            }
        } catch (Exception e) {
            log.error("[{}] Failed to remove owned namespace {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
            return;
        }

        FutureUtil.waitForAll(futures).handle((result, exception) -> {
            if (exception != null) {
                if (exception.getCause() instanceof PulsarAdminException) {
                    asyncResponse.resume(new RestException((PulsarAdminException) exception.getCause()));
                    return null;
                } else {
                    log.error("[{}] Failed to remove owned namespace {}", clientAppId(), namespaceName, exception);
                    asyncResponse.resume(new RestException(exception.getCause()));
                    return null;
                }
            }

            try {
                // we have successfully removed all the ownership for the namespace, the policies znode can be deleted
                // now
                final String globalZkPolicyPath = path(POLICIES, namespaceName.toString());
                final String lcaolZkPolicyPath = joinPath(LOCAL_POLICIES_ROOT, namespaceName.toString());
                globalZk().delete(globalZkPolicyPath, -1);

                try {
                    localZk().delete(lcaolZkPolicyPath, -1);
                } catch (NoNodeException nne) {
                    // If the z-node with the modified information is not there anymore, we're already good
                }

                policiesCache().invalidate(globalZkPolicyPath);
                localCacheService().policiesCache().invalidate(lcaolZkPolicyPath);
            } catch (Exception e) {
                log.error("[{}] Failed to remove owned namespace {} from ZK", clientAppId(), namespaceName, e);
                asyncResponse.resume(new RestException(e));
                return null;
            }

            asyncResponse.resume(Response.noContent().build());
            return null;
        });
    }

    @SuppressWarnings("deprecation")
    protected void internalDeleteNamespaceBundle(String bundleRange, boolean authoritative) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        // ensure that non-global namespace is directed to the correct cluster
        if (!namespaceName.isGlobal()) {
            validateClusterOwnership(namespaceName.getCluster());
        }

        Policies policies = getNamespacePolicies(namespaceName);
        // ensure the local cluster is the only cluster for the global namespace configuration
        try {
            if (namespaceName.isGlobal()) {
                if (policies.replication_clusters.size() > 1) {
                    // There are still more than one clusters configured for the global namespace
                    throw new RestException(Status.PRECONDITION_FAILED, "Cannot delete the global namespace "
                            + namespaceName + ". There are still more than one replication clusters configured.");
                }
                if (policies.replication_clusters.size() == 1
                        && !policies.replication_clusters.contains(config().getClusterName())) {
                    // the only replication cluster is other cluster, redirect
                    String replCluster = Lists.newArrayList(policies.replication_clusters).get(0);
                    ClusterData replClusterData = clustersCache().get(AdminResource.path("clusters", replCluster))
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                    "Cluser " + replCluster + " does not exist"));
                    URL replClusterUrl;
                    if (!config().isTlsEnabled() || !isRequestHttps()) {
                        replClusterUrl = new URL(replClusterData.getServiceUrl());
                    } else if (StringUtils.isNotBlank(replClusterData.getServiceUrlTls())) {
                        replClusterUrl = new URL(replClusterData.getServiceUrlTls());
                    } else {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "The replication cluster does not provide TLS encrypted service");
                    }
                    URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(replClusterUrl.getHost())
                            .port(replClusterUrl.getPort()).replaceQueryParam("authoritative", false).build();
                    if(log.isDebugEnabled()) {
                        log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(), redirect, replCluster);
                    }
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            throw new RestException(e);
        }

        NamespaceBundle bundle = validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange,
                authoritative, true);
        try {
            List<String> topics = pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName).join();
            for (String topic : topics) {
                NamespaceBundle topicBundle = pulsar().getNamespaceService()
                        .getBundle(TopicName.get(topic));
                if (bundle.equals(topicBundle)) {
                    throw new RestException(Status.CONFLICT, "Cannot delete non empty bundle");
                }
            }

            // remove from owned namespace map and ephemeral node from ZK
            pulsar().getNamespaceService().removeOwnedServiceUnit(bundle);
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            log.error("[{}] Failed to remove namespace bundle {}/{}", clientAppId(), namespaceName.toString(),
                    bundleRange, e);
            throw new RestException(e);
        }
    }

    protected void internalGrantPermissionOnNamespace(String role, Set<AuthAction> actions) {
        validateAdminAccessForTenant(namespaceName.getTenant());

        try {
            AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
            if (null != authService) {
                authService.grantPermissionAsync(namespaceName, actions, role, null/*additional auth-data json*/)
                    .get();
            } else {
                throw new RestException(Status.NOT_IMPLEMENTED, "Authorization is not enabled");
            }
        } catch (InterruptedException e) {
            log.error("[{}] Failed to get permissions for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalArgumentException) {
                log.warn("[{}] Failed to set permissions for namespace {}: does not exist", clientAppId(),
                        namespaceName);
                throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
            } else if (e.getCause() instanceof IllegalStateException) {
                log.warn("[{}] Failed to set permissions for namespace {}: concurrent modification",
                        clientAppId(), namespaceName);
                throw new RestException(Status.CONFLICT, "Concurrent modification");
            } else {
                log.error("[{}] Failed to get permissions for namespace {}", clientAppId(), namespaceName, e);
                throw new RestException(e);
            }
        }
    }


    protected void internalGrantPermissionOnSubscription(String subscription, Set<String> roles) {
        validateAdminAccessForTenant(namespaceName.getTenant());

        try {
            AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
            if (null != authService) {
                authService.grantSubscriptionPermissionAsync(namespaceName, subscription, roles,
                        null/* additional auth-data json */).get();
            } else {
                throw new RestException(Status.NOT_IMPLEMENTED, "Authorization is not enabled");
            }
        } catch (InterruptedException e) {
            log.error("[{}] Failed to get permissions for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalArgumentException) {
                log.warn("[{}] Failed to set permissions for namespace {}: does not exist", clientAppId(),
                        namespaceName);
                throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
            } else if (e.getCause() instanceof IllegalStateException) {
                log.warn("[{}] Failed to set permissions for namespace {}: concurrent modification", clientAppId(),
                        namespaceName);
                throw new RestException(Status.CONFLICT, "Concurrent modification");
            } else {
                log.error("[{}] Failed to get permissions for namespace {}", clientAppId(), namespaceName, e);
                throw new RestException(e);
            }
        }
    }

    protected void internalRevokePermissionsOnNamespace(String role) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            byte[] content = globalZk().getData(path(POLICIES, namespaceName.toString()), null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies.auth_policies.namespace_auth.remove(role);

            // Write back the new policies into zookeeper
            globalZk().setData(path(POLICIES, namespaceName.toString()), jsonMapper().writeValueAsBytes(policies),
                    nodeStat.getVersion());

            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully revoked access for role {} - namespace {}", clientAppId(), role, namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to revoke permissions for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to revoke permissions on namespace {}: concurrent modification", clientAppId(),
                    namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to revoke permissions on namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalRevokePermissionsOnSubscription(String subscriptionName, String role) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
        if (null != authService) {
            authService.revokeSubscriptionPermissionAsync(namespaceName, subscriptionName, role,
                    null/* additional auth-data json */);
        } else {
            throw new RestException(Status.NOT_IMPLEMENTED, "Authorization is not enabled");
        }
    }

    protected Set<String> internalGetNamespaceReplicationClusters() {
        if (!namespaceName.isGlobal()) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "Cannot get the replication clusters for a non-global namespace");
        }

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.replication_clusters;
    }

    protected void internalSetNamespaceReplicationClusters(List<String> clusterIds) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        Set<String> replicationClusterSet = Sets.newHashSet(clusterIds);
        if (!namespaceName.isGlobal()) {
            throw new RestException(Status.PRECONDITION_FAILED, "Cannot set replication on a non-global namespace");
        }

        if (replicationClusterSet.contains("global")) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "Cannot specify global in the list of replication clusters");
        }

        Set<String> clusters = clusters();
        for (String clusterId : replicationClusterSet) {
            if (!clusters.contains(clusterId)) {
                throw new RestException(Status.FORBIDDEN, "Invalid cluster id: " + clusterId);
            }
            validatePeerClusterConflict(clusterId, replicationClusterSet);
        }

        for (String clusterId : replicationClusterSet) {
            validateClusterForTenant(namespaceName.getTenant(), clusterId);
        }

        Entry<Policies, Stat> policiesNode = null;

        try {
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path(POLICIES, namespaceName.toString())).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().replication_clusters = replicationClusterSet;

            // Write back the new policies into zookeeper
            globalZk().setData(path(POLICIES, namespaceName.toString()),
                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));

            log.info("[{}] Successfully updated the replication clusters on namespace {}", clientAppId(),
                    namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the replication clusters for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to update the replication clusters on namespace {} expected policy node version={} : concurrent modification",
                    clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the replication clusters on namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected void internalSetNamespaceMessageTTL(int messageTTL) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        if (messageTTL < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for message TTL");
        }

        Entry<Policies, Stat> policiesNode = null;

        try {
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path(POLICIES, namespaceName.toString())).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().message_ttl_in_seconds = messageTTL;

            // Write back the new policies into zookeeper
            globalZk().setData(path(POLICIES, namespaceName.toString()),
                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));

            log.info("[{}] Successfully updated the message TTL on namespace {}", clientAppId(), namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the message TTL for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to update the message TTL on namespace {} expected policy node version={} : concurrent modification",
                    clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the message TTL on namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetAutoTopicCreation(AsyncResponse asyncResponse, AutoTopicCreationOverride autoTopicCreationOverride) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        if (!AutoTopicCreationOverride.isValidOverride(autoTopicCreationOverride)) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid configuration for autoTopicCreationOverride");
        }

        // Force to read the data s.t. the watch to the cache content is setup.
        policiesCache().getWithStatAsync(path(POLICIES, namespaceName.toString())).thenApply(
                policies -> {
                    if (policies.isPresent()) {
                        Entry<Policies, Stat> policiesNode = policies.get();
                        policiesNode.getKey().autoTopicCreationOverride = autoTopicCreationOverride;
                        try {
                            // Write back the new policies into zookeeper
                            globalZk().setData(path(POLICIES, namespaceName.toString()),
                                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion(),
                                    (rc, path1, ctx, stat) -> {
                                        if (rc == KeeperException.Code.OK.intValue()) {
                                            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
                                            String autoOverride = autoTopicCreationOverride.allowAutoTopicCreation ? "enabled" : "disabled";
                                            log.info("[{}] Successfully {} autoTopicCreation on namespace {}", clientAppId(), autoOverride, namespaceName);
                                            asyncResponse.resume(Response.noContent().build());
                                        } else {
                                            String errorMsg = String.format(
                                                    "[%s] Failed to modify autoTopicCreation status for namespace %s",
                                                    clientAppId(), namespaceName);
                                            if (rc == KeeperException.Code.NONODE.intValue()) {
                                                log.warn("{} : does not exist", errorMsg);
                                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Namespace does not exist"));
                                            } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                                                log.warn("{} : concurrent modification", errorMsg);
                                                asyncResponse.resume(new RestException(Status.CONFLICT, "Concurrent modification"));
                                            } else {
                                                asyncResponse.resume(KeeperException.create(KeeperException.Code.get(rc), errorMsg));
                                            }
                                        }
                                    }, null);
                            return null;
                        } catch (Exception e) {
                            log.error("[{}] Failed to modify autoTopicCreation status on namespace {}", clientAppId(), namespaceName, e);
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    } else {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
                        return null;
                    }
                }
        ).exceptionally(e -> {
            log.error("[{}] Failed to modify autoTopicCreation status on namespace {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
            return null;
        });
    }

    protected void internalRemoveAutoTopicCreation(AsyncResponse asyncResponse) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        // Force to read the data s.t. the watch to the cache content is setup.
        policiesCache().getWithStatAsync(path(POLICIES, namespaceName.toString())).thenApply(
                policies -> {
                    if (policies.isPresent()) {
                        Entry<Policies, Stat> policiesNode = policies.get();
                        policiesNode.getKey().autoTopicCreationOverride = null;
                        try {
                            // Write back the new policies into zookeeper
                            globalZk().setData(path(POLICIES, namespaceName.toString()),
                                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion(),
                                    (rc, path1, ctx, stat) -> {
                                        if (rc == KeeperException.Code.OK.intValue()) {
                                            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
                                            log.info("[{}] Successfully removed autoTopicCreation override on namespace {}", clientAppId(), namespaceName);
                                            asyncResponse.resume(Response.noContent().build());
                                        } else {
                                            String errorMsg = String.format(
                                                    "[%s] Failed to modify autoTopicCreation status for namespace %s",
                                                    clientAppId(), namespaceName);
                                            if (rc == KeeperException.Code.NONODE.intValue()) {
                                                log.warn("{} : does not exist", errorMsg);
                                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Namespace does not exist"));
                                            } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                                                log.warn("{} : concurrent modification", errorMsg);
                                                asyncResponse.resume(new RestException(Status.CONFLICT, "Concurrent modification"));
                                            } else {
                                                asyncResponse.resume(KeeperException.create(KeeperException.Code.get(rc), errorMsg));
                                            }
                                        }
                                    }, null);
                            return null;
                        } catch (Exception e) {
                            log.error("[{}] Failed to modify autoTopicCreation status on namespace {}", clientAppId(), namespaceName, e);
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    } else {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
                        return null;
                    }
                }
        ).exceptionally(e -> {
            log.error("[{}] Failed to modify autoTopicCreation status on namespace {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
            return null;
        });
    }

    protected void internalSetAutoSubscriptionCreation(AsyncResponse asyncResponse, AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        // Force to read the data s.t. the watch to the cache content is setup.
        policiesCache().getWithStatAsync(path(POLICIES, namespaceName.toString())).thenApply(
                policies -> {
                    if (policies.isPresent()) {
                        Entry<Policies, Stat> policiesNode = policies.get();
                        policiesNode.getKey().autoSubscriptionCreationOverride = autoSubscriptionCreationOverride;
                        try {
                            // Write back the new policies into zookeeper
                            globalZk().setData(path(POLICIES, namespaceName.toString()),
                                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion(),
                                    (rc, path1, ctx, stat) -> {
                                        if (rc == KeeperException.Code.OK.intValue()) {
                                            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
                                            String autoOverride = autoSubscriptionCreationOverride.allowAutoSubscriptionCreation ? "enabled" : "disabled";
                                            log.info("[{}] Successfully {} autoSubscriptionCreation on namespace {}", clientAppId(), autoOverride, namespaceName);
                                            asyncResponse.resume(Response.noContent().build());
                                        } else {
                                            String errorMsg = String.format(
                                                    "[%s] Failed to modify autoSubscriptionCreation status for namespace %s",
                                                    clientAppId(), namespaceName);
                                            if (rc == KeeperException.Code.NONODE.intValue()) {
                                                log.warn("{} : does not exist", errorMsg);
                                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Namespace does not exist"));
                                            } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                                                log.warn("{} : concurrent modification", errorMsg);
                                                asyncResponse.resume(new RestException(Status.CONFLICT, "Concurrent modification"));
                                            } else {
                                                asyncResponse.resume(KeeperException.create(KeeperException.Code.get(rc), errorMsg));
                                            }
                                        }
                                    }, null);
                            return null;
                        } catch (Exception e) {
                            log.error("[{}] Failed to modify autoSubscriptionCreation status on namespace {}", clientAppId(), namespaceName, e);
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    } else {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
                        return null;
                    }
                }
        ).exceptionally(e -> {
            log.error("[{}] Failed to modify autoSubscriptionCreation status on namespace {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
            return null;
        });
    }

    protected void internalRemoveAutoSubscriptionCreation(AsyncResponse asyncResponse) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        // Force to read the data s.t. the watch to the cache content is setup.
        policiesCache().getWithStatAsync(path(POLICIES, namespaceName.toString())).thenApply(
                policies -> {
                    if (policies.isPresent()) {
                        Entry<Policies, Stat> policiesNode = policies.get();
                        policiesNode.getKey().autoSubscriptionCreationOverride = null;
                        try {
                            // Write back the new policies into zookeeper
                            globalZk().setData(path(POLICIES, namespaceName.toString()),
                                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion(),
                                    (rc, path1, ctx, stat) -> {
                                        if (rc == KeeperException.Code.OK.intValue()) {
                                            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
                                            log.info("[{}] Successfully removed autoSubscriptionCreation override on namespace {}", clientAppId(), namespaceName);
                                            asyncResponse.resume(Response.noContent().build());
                                        } else {
                                            String errorMsg = String.format(
                                                    "[%s] Failed to modify autoSubscriptionCreation status for namespace %s",
                                                    clientAppId(), namespaceName);
                                            if (rc == KeeperException.Code.NONODE.intValue()) {
                                                log.warn("{} : does not exist", errorMsg);
                                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Namespace does not exist"));
                                            } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                                                log.warn("{} : concurrent modification", errorMsg);
                                                asyncResponse.resume(new RestException(Status.CONFLICT, "Concurrent modification"));
                                            } else {
                                                asyncResponse.resume(KeeperException.create(KeeperException.Code.get(rc), errorMsg));
                                            }
                                        }
                                    }, null);
                            return null;
                        } catch (Exception e) {
                            log.error("[{}] Failed to modify autoSubscriptionCreation status on namespace {}", clientAppId(), namespaceName, e);
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    } else {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
                        return null;
                    }
                }
        ).exceptionally(e -> {
            log.error("[{}] Failed to modify autoSubscriptionCreation status on namespace {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
            return null;
        });
    }

    protected void internalModifyDeduplication(boolean enableDeduplication) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        Entry<Policies, Stat> policiesNode = null;

        try {
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path(POLICIES, namespaceName.toString())).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().deduplicationEnabled = enableDeduplication;

            // Write back the new policies into zookeeper
            globalZk().setData(path(POLICIES, namespaceName.toString()),
                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));

            log.info("[{}] Successfully {} on namespace {}", clientAppId(),
                    enableDeduplication ? "enabled" : "disabled", namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to modify deduplication status for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to modify deduplication status on namespace {} expected policy node version={} : concurrent modification",
                    clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to modify deduplication status on namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    @SuppressWarnings("deprecation")
    protected void internalUnloadNamespace(AsyncResponse asyncResponse) {
        log.info("[{}] Unloading namespace {}", clientAppId(), namespaceName);

        validateSuperUserAccess();

        if (namespaceName.isGlobal()) {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } else {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        Policies policies = getNamespacePolicies(namespaceName);

        final List<CompletableFuture<Void>> futures = Lists.newArrayList();
        List<String> boundaries = policies.bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            try {
                futures.add(pulsar().getAdminClient().namespaces().unloadNamespaceBundleAsync(namespaceName.toString(),
                        bundle));
            } catch (PulsarServerException e) {
                log.error("[{}] Failed to unload namespace {}", clientAppId(), namespaceName, e);
                asyncResponse.resume(new RestException(e));
                return;
            }
        }

        FutureUtil.waitForAll(futures).handle((result, exception) -> {
            if (exception != null) {
                log.error("[{}] Failed to unload namespace {}", clientAppId(), namespaceName, exception);
                if (exception.getCause() instanceof PulsarAdminException) {
                    asyncResponse.resume(new RestException((PulsarAdminException) exception.getCause()));
                    return null;
                } else {
                    asyncResponse.resume(new RestException(exception.getCause()));
                    return null;
                }
            }
            log.info("[{}] Successfully unloaded all the bundles in namespace {}", clientAppId(), namespaceName);
            asyncResponse.resume(Response.noContent().build());
            return null;
        });
    }

    
    protected void internalSetBookieAffinityGroup(BookieAffinityGroupData bookieAffinityGroup) {
        log.info("[{}] Setting bookie-affinity-group {} for namespace {}", clientAppId(), bookieAffinityGroup,
                this.namespaceName);

        validateSuperUserAccess();

        if (namespaceName.isGlobal()) {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } else {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        try {
            String path = joinPath(LOCAL_POLICIES_ROOT, this.namespaceName.toString());
            Stat nodeStat = new Stat();

            LocalPolicies localPolicies = null;
            int version = -1;
            try {
                byte[] content = pulsar().getLocalZkCache().getZooKeeper().getData(path, false, nodeStat);
                localPolicies = jsonMapper().readValue(content, LocalPolicies.class);
                version = nodeStat.getVersion();
            } catch (KeeperException.NoNodeException e) {
                log.info("local-policies for {} is not setup at path {}", this.namespaceName, path);
                // if policies is not present into localZk then create new policies
                this.pulsar().getLocalZkCacheService().createPolicies(path, false)
                        .get(pulsar().getConfiguration().getZooKeeperOperationTimeoutSeconds(), SECONDS);
                localPolicies = new LocalPolicies();
            }
            localPolicies.bookieAffinityGroup = bookieAffinityGroup;
            byte[] data = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(localPolicies);
            pulsar().getLocalZkCache().getZooKeeper().setData(path, data, Math.toIntExact(version));
            // invalidate namespace's local-policies
            pulsar().getLocalZkCacheService().policiesCache().invalidate(path);
            log.info("[{}] Successfully updated local-policies configuration: namespace={}, map={}", clientAppId(),
                    namespaceName, jsonMapper().writeValueAsString(localPolicies));
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update local-policy configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update persistence configuration for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
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

        String path = joinPath(LOCAL_POLICIES_ROOT, this.namespaceName.toString());
        try {
            Optional<LocalPolicies> policies = pulsar().getLocalZkCacheService().policiesCache().get(path);
            final BookieAffinityGroupData bookkeeperAffinityGroup = policies.orElseThrow(() -> new RestException(Status.NOT_FOUND,
                    "Namespace local-policies does not exist")).bookieAffinityGroup;
            if (bookkeeperAffinityGroup == null) {
                throw new RestException(Status.NOT_FOUND, "bookie-affinity group does not exist");
            }
            return bookkeeperAffinityGroup;
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update local-policy configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace policies does not exist");
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get local-policy configuration for namespace {} at path {}", clientAppId(),
                    namespaceName, path, e);
            throw new RestException(e);
        }
    }

    @SuppressWarnings("deprecation")
    public void internalUnloadNamespaceBundle(String bundleRange, boolean authoritative) {
        log.info("[{}] Unloading namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange);

        validateSuperUserAccess();
        Policies policies = getNamespacePolicies(namespaceName);

        NamespaceBundle bundle = pulsar().getNamespaceService().getNamespaceBundleFactory().getBundle(namespaceName.toString(), bundleRange);
        boolean isOwnedByLocalCluster = false;
        try {
            isOwnedByLocalCluster = pulsar().getNamespaceService().isNamespaceBundleOwned(bundle).get();
        } catch (Exception e) {
            if(log.isDebugEnabled()) {
                log.debug("Failed to validate cluster ownership for {}-{}, {}", namespaceName.toString(), bundleRange, e.getMessage(), e);
            }
        }

        // validate namespace ownership only if namespace is not owned by local-cluster (it happens when broker doesn't
        // receive replication-cluster change watch and still owning bundle
        if (!isOwnedByLocalCluster) {
            if (namespaceName.isGlobal()) {
                // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                validateGlobalNamespaceOwnership(namespaceName);
            } else {
                validateClusterOwnership(namespaceName.getCluster());
                validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
            }
        }

        validatePoliciesReadOnlyAccess();

        if (!isBundleOwnedByAnyBroker(namespaceName, policies.bundles, bundleRange)) {
            log.info("[{}] Namespace bundle is not owned by any broker {}/{}", clientAppId(), namespaceName,
                    bundleRange);
            return;
        }

        NamespaceBundle nsBundle = validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange,
                authoritative, true);
        try {
            pulsar().getNamespaceService().unloadNamespaceBundle(nsBundle);
            log.info("[{}] Successfully unloaded namespace bundle {}", clientAppId(), nsBundle.toString());
        } catch (Exception e) {
            log.error("[{}] Failed to unload namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange, e);
            throw new RestException(e);
        }
    }

    @SuppressWarnings("deprecation")
    protected void internalSplitNamespaceBundle(String bundleRange, boolean authoritative, boolean unload, String splitAlgorithmName) {
        log.info("[{}] Split namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange);

        validateSuperUserAccess();
        Policies policies = getNamespacePolicies(namespaceName);

        if (namespaceName.isGlobal()) {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } else {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        validatePoliciesReadOnlyAccess();
        NamespaceBundle nsBundle = validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange,
                authoritative, true);

        List<String> supportedNamespaceBundleSplitAlgorithms = pulsar().getConfig().getSupportedNamespaceBundleSplitAlgorithms();
        if (StringUtils.isNotBlank(splitAlgorithmName) && !supportedNamespaceBundleSplitAlgorithms.contains(splitAlgorithmName)) {
            throw new RestException(Status.PRECONDITION_FAILED,
                "Unsupported namespace bundle split algorithm, supported algorithms are " + supportedNamespaceBundleSplitAlgorithms);
        }

        try {
            pulsar().getNamespaceService().splitAndOwnBundle(nsBundle, unload, getNamespaceBundleSplitAlgorithmByName(splitAlgorithmName)).get();
            log.info("[{}] Successfully split namespace bundle {}", clientAppId(), nsBundle.toString());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalArgumentException) {
                log.error("[{}] Failed to split namespace bundle {}/{} due to {}", clientAppId(), namespaceName,
                    bundleRange, e.getMessage());
                throw new RestException(Status.PRECONDITION_FAILED, "Split bundle failed due to invalid request");
            } else {
                log.error("[{}] Failed to split namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange, e);
                throw new RestException(e.getCause());
            }
        } catch (Exception e) {
            log.error("[{}] Failed to split namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange, e);
            throw new RestException(e);
        }
    }

    private NamespaceBundleSplitAlgorithm getNamespaceBundleSplitAlgorithmByName(String algorithmName) {
        NamespaceBundleSplitAlgorithm algorithm = NamespaceBundleSplitAlgorithm.of(algorithmName);
        if (algorithm == null) {
            algorithm = NamespaceBundleSplitAlgorithm.of(pulsar().getConfig().getDefaultNamespaceBundleSplitAlgorithm());
        }
        if (algorithm == null) {
            algorithm = NamespaceBundleSplitAlgorithm.rangeEquallyDivide;
        }
        return algorithm;
    }

    protected void internalSetPublishRate(PublishRate maxPublishMessageRate) {
        log.info("[{}] Set namespace publish-rate {}/{}", clientAppId(), namespaceName, maxPublishMessageRate);
        validateSuperUserAccess();

        Entry<Policies, Stat> policiesNode = null;

        try {
            final String path = path(POLICIES, namespaceName.toString());
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().publishMaxMessageRate.put(pulsar().getConfiguration().getClusterName(), maxPublishMessageRate);

            // Write back the new policies into zookeeper
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policiesNode.getKey()),
                    policiesNode.getValue().getVersion());
            policiesCache().invalidate(path);

            log.info("[{}] Successfully updated the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                    namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the publish_max_message_rate for cluster on namespace {}: does not exist",
                    clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to update the publish_max_message_rate for cluster on namespace {} expected policy node version={} : concurrent modification",
                    clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    protected PublishRate internalGetPublishRate() {
        validateAdminAccessForTenant(namespaceName.getTenant());

        Policies policies = getNamespacePolicies(namespaceName);
        PublishRate publishRate = policies.publishMaxMessageRate.get(pulsar().getConfiguration().getClusterName());
        if (publishRate != null) {
            return publishRate;
        } else {
            throw new RestException(Status.NOT_FOUND,
                    "Publish-rate is not configured for cluster " + pulsar().getConfiguration().getClusterName());
        }
    }

    @SuppressWarnings("deprecation")
    protected void internalSetTopicDispatchRate(DispatchRate dispatchRate) {
        log.info("[{}] Set namespace dispatch-rate {}/{}", clientAppId(), namespaceName, dispatchRate);
        validateSuperUserAccess();

        Entry<Policies, Stat> policiesNode = null;

        try {
            final String path = path(POLICIES, namespaceName.toString());
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().topicDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
            policiesNode.getKey().clusterDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);

            // Write back the new policies into zookeeper
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policiesNode.getKey()),
                    policiesNode.getValue().getVersion());
            policiesCache().invalidate(path);

            log.info("[{}] Successfully updated the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the dispatchRate for cluster on namespace {}: does not exist",
                    clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to update the dispatchRate for cluster on namespace {} expected policy node version={} : concurrent modification",
                    clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    @SuppressWarnings("deprecation")
    protected DispatchRate internalGetTopicDispatchRate() {
        validateAdminAccessForTenant(namespaceName.getTenant());

        Policies policies = getNamespacePolicies(namespaceName);
        DispatchRate dispatchRate = policies.topicDispatchRate.get(pulsar().getConfiguration().getClusterName());
        if (dispatchRate == null) {
            dispatchRate = policies.clusterDispatchRate.get(pulsar().getConfiguration().getClusterName());
        }
        if (dispatchRate != null) {
            return dispatchRate;
        } else {
            throw new RestException(Status.NOT_FOUND,
                    "Dispatch-rate is not configured for cluster " + pulsar().getConfiguration().getClusterName());
        }
    }

    protected void internalSetSubscriptionDispatchRate(DispatchRate dispatchRate) {
        log.info("[{}] Set namespace subscription dispatch-rate {}/{}", clientAppId(), namespaceName, dispatchRate);
        validateSuperUserAccess();

        Entry<Policies, Stat> policiesNode = null;

        try {
            final String path = path(POLICIES, namespaceName.toString());
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path).orElseThrow(
                () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().subscriptionDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);

            // Write back the new policies into zookeeper
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policiesNode.getKey()),
                policiesNode.getValue().getVersion());
            policiesCache().invalidate(path);

            log.info("[{}] Successfully updated the subscriptionDispatchRate for cluster on namespace {}", clientAppId(),
                namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the subscriptionDispatchRate for cluster on namespace {}: does not exist",
                clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                "[{}] Failed to update the subscriptionDispatchRate for cluster on namespace {} expected policy node version={} : concurrent modification",
                clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the subscriptionDispatchRate for cluster on namespace {}", clientAppId(),
                namespaceName, e);
            throw new RestException(e);
        }
    }

    protected DispatchRate internalGetSubscriptionDispatchRate() {
        validateAdminAccessForTenant(namespaceName.getTenant());

        Policies policies = getNamespacePolicies(namespaceName);
        DispatchRate dispatchRate = policies.subscriptionDispatchRate.get(pulsar().getConfiguration().getClusterName());
        if (dispatchRate != null) {
            return dispatchRate;
        } else {
            throw new RestException(Status.NOT_FOUND,
                "Subscription-Dispatch-rate is not configured for cluster " + pulsar().getConfiguration().getClusterName());
        }
    }

    protected void internalSetSubscribeRate(SubscribeRate subscribeRate) {
        log.info("[{}] Set namespace subscribe-rate {}/{}", clientAppId(), namespaceName, subscribeRate);
        validateSuperUserAccess();

        Entry<Policies, Stat> policiesNode = null;

        try {
            final String path = path(POLICIES, namespaceName.toString());
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().clusterSubscribeRate.put(pulsar().getConfiguration().getClusterName(), subscribeRate);

            // Write back the new policies into zookeeper
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policiesNode.getKey()),
                    policiesNode.getValue().getVersion());
            policiesCache().invalidate(path);

            log.info("[{}] Successfully updated the subscribeRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the subscribeRate for cluster on namespace {}: does not exist",
                    clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to update the subscribeRate for cluster on namespace {} expected policy node version={} : concurrent modification",
                    clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the subscribeRate for cluster on namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    protected SubscribeRate internalGetSubscribeRate() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        Policies policies = getNamespacePolicies(namespaceName);
        SubscribeRate subscribeRate = policies.clusterSubscribeRate.get(pulsar().getConfiguration().getClusterName());
        if (subscribeRate != null) {
            return subscribeRate;
        } else {
            throw new RestException(Status.NOT_FOUND,
                    "Subscribe-rate is not configured for cluster " + pulsar().getConfiguration().getClusterName());
        }
    }

    protected void internalSetReplicatorDispatchRate(DispatchRate dispatchRate) {
        log.info("[{}] Set namespace replicator dispatch-rate {}/{}", clientAppId(), namespaceName, dispatchRate);
        validateSuperUserAccess();

        Entry<Policies, Stat> policiesNode = null;

        try {
            final String path = path(POLICIES, namespaceName.toString());
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path).orElseThrow(
                () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().replicatorDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);

            // Write back the new policies into zookeeper
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policiesNode.getKey()),
                policiesNode.getValue().getVersion());
            policiesCache().invalidate(path);

            log.info("[{}] Successfully updated the replicatorDispatchRate for cluster on namespace {}", clientAppId(),
                namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the replicatorDispatchRate for cluster on namespace {}: does not exist",
                clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                "[{}] Failed to update the replicatorDispatchRate for cluster on namespace {} expected policy node version={} : concurrent modification",
                clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the replicatorDispatchRate for cluster on namespace {}", clientAppId(),
                namespaceName, e);
            throw new RestException(e);
        }
    }

    protected DispatchRate internalGetReplicatorDispatchRate() {
        validateAdminAccessForTenant(namespaceName.getTenant());

        Policies policies = getNamespacePolicies(namespaceName);
        DispatchRate dispatchRate = policies.replicatorDispatchRate.get(pulsar().getConfiguration().getClusterName());
        if (dispatchRate != null) {
            return dispatchRate;
        } else {
            throw new RestException(Status.NOT_FOUND,
                "replicator-Dispatch-rate is not configured for cluster " + pulsar().getConfiguration().getClusterName());
        }
    }

    protected void internalSetBacklogQuota(BacklogQuotaType backlogQuotaType, BacklogQuota backlogQuota) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        if (backlogQuotaType == null) {
            backlogQuotaType = BacklogQuotaType.destination_storage;
        }

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            RetentionPolicies r = policies.retention_policies;
            if (r != null) {
                Policies p = new Policies();
                p.backlog_quota_map.put(backlogQuotaType, backlogQuota);
                if (!checkQuotas(p, r)) {
                    log.warn(
                            "[{}] Failed to update backlog configuration for namespace {}: conflicts with retention quota",
                            clientAppId(), namespaceName);
                    throw new RestException(Status.PRECONDITION_FAILED,
                            "Backlog Quota exceeds configured retention quota for namespace. Please increase retention quota and retry");
                }
            }
            policies.backlog_quota_map.put(backlogQuotaType, backlogQuota);
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated backlog quota map: namespace={}, map={}", clientAppId(), namespaceName,
                    jsonMapper().writeValueAsString(policies.backlog_quota_map));

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update backlog quota map for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update backlog quota map for namespace {}: concurrent modification", clientAppId(),
                    namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update backlog quota map for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalRemoveBacklogQuota(BacklogQuotaType backlogQuotaType) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        if (backlogQuotaType == null) {
            backlogQuotaType = BacklogQuotaType.destination_storage;
        }

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies.backlog_quota_map.remove(backlogQuotaType);
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully removed backlog namespace={}, quota={}", clientAppId(), namespaceName,
                    backlogQuotaType);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update backlog quota map for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update backlog quota map for namespace {}: concurrent modification", clientAppId(),
                    namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update backlog quota map for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetRetention(RetentionPolicies retention) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (!checkQuotas(policies, retention)) {
                log.warn("[{}] Failed to update retention configuration for namespace {}: conflicts with backlog quota",
                        clientAppId(), namespaceName);
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Retention Quota must exceed configured backlog quota for namespace.");
            }
            policies.retention_policies = retention;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated retention configuration: namespace={}, map={}", clientAppId(),
                    namespaceName, jsonMapper().writeValueAsString(policies.retention_policies));

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update retention configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update retention configuration for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update retention configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected void internalSetPersistence(PersistencePolicies persistence) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();
        validatePersistencePolicies(persistence);

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies.persistence = persistence;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated persistence configuration: namespace={}, map={}", clientAppId(),
                    namespaceName, jsonMapper().writeValueAsString(policies.persistence));

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update persistence configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update persistence configuration for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update persistence configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected PersistencePolicies internalGetPersistence() {
        validateAdminAccessForTenant(namespaceName.getTenant());

        Policies policies = getNamespacePolicies(namespaceName);
        if (policies.persistence == null) {
            return new PersistencePolicies(config().getManagedLedgerDefaultEnsembleSize(),
                    config().getManagedLedgerDefaultWriteQuorum(), config().getManagedLedgerDefaultAckQuorum(), 0.0d);
        } else {
            return policies.persistence;
        }
    }

    protected void internalClearNamespaceBacklog(AsyncResponse asyncResponse, boolean authoritative) {
        validateAdminAccessForTenant(namespaceName.getTenant());

        final List<CompletableFuture<Void>> futures = Lists.newArrayList();
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            for (NamespaceBundle nsBundle : bundles.getBundles()) {
                // check if the bundle is owned by any broker, if not then there is no backlog on this bundle to clear
                if (pulsar().getNamespaceService().getOwner(nsBundle).isPresent()) {
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
        validateAdminAccessForTenant(namespaceName.getTenant());

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
        validateAdminAccessForTenant(namespaceName.getTenant());

        final List<CompletableFuture<Void>> futures = Lists.newArrayList();
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            for (NamespaceBundle nsBundle : bundles.getBundles()) {
                // check if the bundle is owned by any broker, if not then there is no backlog on this bundle to clear
                if (pulsar().getNamespaceService().getOwner(nsBundle).isPresent()) {
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
        validateAdminAccessForTenant(namespaceName.getTenant());

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
        validateAdminAccessForTenant(namespaceName.getTenant());

        final List<CompletableFuture<Void>> futures = Lists.newArrayList();
        try {
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            for (NamespaceBundle nsBundle : bundles.getBundles()) {
                // check if the bundle is owned by any broker, if not then there are no subscriptions
                if (pulsar().getNamespaceService().getOwner(nsBundle).isPresent()) {
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
        validateAdminAccessForTenant(namespaceName.getTenant());

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
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        if (subscriptionAuthMode == null) {
            subscriptionAuthMode = SubscriptionAuthMode.None;
        }

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies.subscription_auth_mode = subscriptionAuthMode;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated subscription auth mode: namespace={}, map={}", clientAppId(),
                    namespaceName, jsonMapper().writeValueAsString(policies.backlog_quota_map));

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update subscription auth mode for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update subscription auth mode for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update subscription auth mode for namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalModifyEncryptionRequired(boolean encryptionRequired) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        Entry<Policies, Stat> policiesNode = null;

        try {
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path(POLICIES, namespaceName.toString())).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().encryption_required = encryptionRequired;

            // Write back the new policies into zookeeper
            globalZk().setData(path(POLICIES, namespaceName.toString()),
                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));

            log.info("[{}] Successfully {} on namespace {}", clientAppId(), encryptionRequired ? "true" : "false",
                    namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to modify encryption required status for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to modify encryption required status on namespace {} expected policy node version={} : concurrent modification",
                    clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to modify encryption required status on namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected DelayedDeliveryPolicies internalGetDelayedDelivery() {
        validateAdminAccessForTenant(namespaceName.getTenant());

        Policies policies = getNamespacePolicies(namespaceName);
        if (policies.delayed_delivery_policies == null) {
            return new DelayedDeliveryPolicies(config().getDelayedDeliveryTickTimeMillis(),
                    config().isDelayedDeliveryEnabled());
        } else {
            return policies.delayed_delivery_policies;
        }
    }

    protected void internalSetDelayedDelivery(DelayedDeliveryPolicies delayedDeliveryPolicies) {
        validateSuperUserAccess();
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);

            policies.delayed_delivery_policies = delayedDeliveryPolicies;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated delayed delivery messages configuration: namespace={}, map={}", clientAppId(),
                    namespaceName, jsonMapper().writeValueAsString(policies.retention_policies));

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update delayed delivery messages configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update delayed delivery messages configuration for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update delayed delivery messages configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected void internalSetNamespaceAntiAffinityGroup(String antiAffinityGroup) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        log.info("[{}] Setting anti-affinity group {} for {}", clientAppId(), antiAffinityGroup, namespaceName);

        if (isBlank(antiAffinityGroup)) {
            throw new RestException(Status.PRECONDITION_FAILED, "antiAffinityGroup can't be empty");
        }

        Map.Entry<Policies, Stat> policiesNode = null;

        try {
            // Force to read the data s.t. the watch to the cache content is setup.
            policiesNode = policiesCache().getWithStat(path(POLICIES, namespaceName.toString())).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist"));
            policiesNode.getKey().antiAffinityGroup = antiAffinityGroup;

            // Write back the new policies into zookeeper
            globalZk().setData(path(POLICIES, namespaceName.toString()),
                    jsonMapper().writeValueAsBytes(policiesNode.getKey()), policiesNode.getValue().getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));

            log.info("[{}] Successfully updated the antiAffinityGroup {} on namespace {}", clientAppId(),
                    antiAffinityGroup, namespaceName);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update the antiAffinityGroup for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn(
                    "[{}] Failed to update the antiAffinityGroup on namespace {} expected policy node version={} : concurrent modification",
                    clientAppId(), namespaceName, policiesNode.getValue().getVersion());

            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to update the antiAffinityGroup on namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected String internalGetNamespaceAntiAffinityGroup() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).antiAffinityGroup;
    }

    protected void internalRemoveNamespaceAntiAffinityGroup() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();

        log.info("[{}] Deleting anti-affinity group for {}", clientAppId(), namespaceName);

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies.antiAffinityGroup = null;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully removed anti-affinity group for a namespace={}", clientAppId(), namespaceName);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to remove anti-affinity group for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to remove anti-affinity group for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (Exception e) {
            log.error("[{}] Failed to remove anti-affinity group for namespace {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected List<String> internalGetAntiAffinityNamespaces(String cluster, String antiAffinityGroup,
            String tenant) {
        validateAdminAccessForTenant(tenant);

        log.info("[{}]-{} Finding namespaces for {} in {}", clientAppId(), tenant, antiAffinityGroup, cluster);

        if (isBlank(antiAffinityGroup)) {
            throw new RestException(Status.PRECONDITION_FAILED, "anti-affinity group can't be empty.");
        }
        validateClusterExists(cluster);

        try {
            List<String> namespaces = getListOfNamespaces(tenant);

            return namespaces.stream().filter(ns -> {
                Optional<Policies> policies;
                try {
                    policies = policiesCache().get(AdminResource.path(POLICIES, ns));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                String storedAntiAffinityGroup = policies.orElse(new Policies()).antiAffinityGroup;
                return antiAffinityGroup.equalsIgnoreCase(storedAntiAffinityGroup);
            }).collect(Collectors.toList());

        } catch (Exception e) {
            log.warn("Failed to list of properties/namespace from global-zk", e);
            throw new RestException(e);
        }
    }

    private void validatePersistencePolicies(PersistencePolicies persistence) {
        try {
            checkNotNull(persistence);
            final ServiceConfiguration config = pulsar().getConfiguration();
            checkArgument(persistence.getBookkeeperEnsemble() <= config.getManagedLedgerMaxEnsembleSize(),
                    "Bookkeeper-Ensemble must be <= %s", config.getManagedLedgerMaxEnsembleSize());
            checkArgument(persistence.getBookkeeperWriteQuorum() <= config.getManagedLedgerMaxWriteQuorum(),
                    "Bookkeeper-WriteQuorum must be <= %s", config.getManagedLedgerMaxWriteQuorum());
            checkArgument(persistence.getBookkeeperAckQuorum() <= config.getManagedLedgerMaxAckQuorum(),
                    "Bookkeeper-AckQuorum must be <= %s", config.getManagedLedgerMaxAckQuorum());
            checkArgument(
                    (persistence.getBookkeeperEnsemble() >= persistence.getBookkeeperWriteQuorum())
                            && (persistence.getBookkeeperWriteQuorum() >= persistence.getBookkeeperAckQuorum()),
                    "Bookkeeper Ensemble (%s) >= WriteQuorum (%s) >= AckQuoru (%s)",
                    persistence.getBookkeeperEnsemble(), persistence.getBookkeeperWriteQuorum(),
                    persistence.getBookkeeperAckQuorum());
        } catch (NullPointerException | IllegalArgumentException e) {
            throw new RestException(Status.PRECONDITION_FAILED, e.getMessage());
        }
    }

    protected RetentionPolicies internalGetRetention() {
        validateAdminAccessForTenant(namespaceName.getTenant());

        Policies policies = getNamespacePolicies(namespaceName);
        if (policies.retention_policies == null) {
            return new RetentionPolicies(config().getDefaultRetentionTimeInMinutes(),
                    config().getDefaultRetentionSizeInMB());
        } else {
            return policies.retention_policies;
        }
    }

    private boolean checkQuotas(Policies policies, RetentionPolicies retention) {
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlog_quota_map = policies.backlog_quota_map;
        if (backlog_quota_map.isEmpty() || retention.getRetentionSizeInMB() == 0 || retention.getRetentionSizeInMB() == -1) {
            return true;
        }
        BacklogQuota quota = backlog_quota_map.get(BacklogQuotaType.destination_storage);
        if (quota == null) {
            quota = pulsar().getBrokerService().getBacklogQuotaManager().getDefaultQuota();
        }
        if (quota.getLimit() < 0 && (retention.getRetentionSizeInMB() > 0 || retention.getRetentionTimeInMinutes() > 0)) {
            return false;
        }
        if (quota.getLimit() >= (retention.getRetentionSizeInMB() * 1024 * 1024)) {
            return false;
        }
        return true;
    }

    private void clearBacklog(NamespaceName nsName, String bundleRange, String subscription) {
        try {
            List<Topic> topicList = pulsar().getBrokerService().getAllTopicsFromNamespaceBundle(nsName.toString(),
                    nsName.toString() + "/" + bundleRange);

            List<CompletableFuture<Void>> futures = Lists.newArrayList();
            if (subscription != null) {
                if (subscription.startsWith(pulsar().getConfiguration().getReplicatorPrefix())) {
                    subscription = PersistentReplicator.getRemoteCluster(subscription);
                }
                for (Topic topic : topicList) {
                    if (topic instanceof PersistentTopic) {
                        futures.add(((PersistentTopic) topic).clearBacklog(subscription));
                    }
                }
            } else {
                for (Topic topic : topicList) {
                    if (topic instanceof PersistentTopic) {
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
            List<CompletableFuture<Void>> futures = Lists.newArrayList();
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

    /**
     * It validates that peer-clusters can't coexist in replication-clusters
     *
     * @param clusterName:
     *            given cluster whose peer-clusters can't be present into replication-cluster list
     * @param replicationClusters:
     *            replication-cluster list
     */
    private void validatePeerClusterConflict(String clusterName, Set<String> replicationClusters) {
        try {
            ClusterData clusterData = clustersCache().get(path("clusters", clusterName)).orElseThrow(
                    () -> new RestException(Status.PRECONDITION_FAILED, "Invalid replication cluster " + clusterName));
            Set<String> peerClusters = clusterData.getPeerClusterNames();
            if (peerClusters != null && !peerClusters.isEmpty()) {
                SetView<String> conflictPeerClusters = Sets.intersection(peerClusters, replicationClusters);
                if (!conflictPeerClusters.isEmpty()) {
                    log.warn("[{}] {}'s peer cluster can't be part of replication clusters {}", clientAppId(),
                            clusterName, conflictPeerClusters);
                    throw new RestException(Status.CONFLICT,
                            String.format("%s's peer-clusters %s can't be part of replication-clusters %s", clusterName,
                                    conflictPeerClusters, replicationClusters));
                }
            }
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.warn("[{}] Failed to get cluster-data for {}", clientAppId(), clusterName, e);
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
        List<String> bundles = Lists.newArrayList();
        bundles.addAll(partitions);
        return new BundlesData(bundles);
    }

    public static BundlesData getBundles(int numBundles) {
        if (numBundles <= 0 || numBundles > MAX_BUNDLES) {
            throw new RestException(Status.BAD_REQUEST,
                    "Invalid number of bundles. Number of numbles has to be in the range of (0, 2^32].");
        }
        Long maxVal = ((long) 1) << 32;
        Long segSize = maxVal / numBundles;
        List<String> partitions = Lists.newArrayList();
        partitions.add(String.format("0x%08x", 0l));
        Long curPartition = segSize;
        for (int i = 0; i < numBundles; i++) {
            if (i != numBundles - 1) {
                partitions.add(String.format("0x%08x", curPartition));
            } else {
                partitions.add(String.format("0x%08x", maxVal - 1));
            }
            curPartition += segSize;
        }
        return new BundlesData(partitions);
    }

    private void validatePolicies(NamespaceName ns, Policies policies) {
        if (ns.isV2() && policies.replication_clusters.isEmpty()) {
            // Default to local cluster
            policies.replication_clusters = Collections.singleton(config().getClusterName());
        }

        // Validate cluster names and permissions
        policies.replication_clusters.forEach(cluster -> validateClusterForTenant(ns.getTenant(), cluster));

        if (policies.message_ttl_in_seconds < 0) {
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
    }


    protected int internalGetMaxProducersPerTopic() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).max_producers_per_topic;
    }

    protected void internalSetMaxProducersPerTopic(int maxProducersPerTopic) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (maxProducersPerTopic < 0) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "maxProducersPerTopic must be 0 or more");
            }
            policies.max_producers_per_topic = maxProducersPerTopic;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated maxProducersPerTopic configuration: namespace={}, value={}", clientAppId(),
                    namespaceName, policies.max_producers_per_topic);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update maxProducersPerTopic configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update maxProducersPerTopic configuration for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxProducersPerTopic configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected int internalGetMaxConsumersPerTopic() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).max_consumers_per_topic;
    }

    protected void internalSetMaxConsumersPerTopic(int maxConsumersPerTopic) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (maxConsumersPerTopic < 0) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "maxConsumersPerTopic must be 0 or more");
            }
            policies.max_consumers_per_topic = maxConsumersPerTopic;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated maxConsumersPerTopic configuration: namespace={}, value={}", clientAppId(),
                    namespaceName, policies.max_consumers_per_topic);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update maxConsumersPerTopic configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update maxConsumersPerTopic configuration for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxConsumersPerTopic configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected int internalGetMaxConsumersPerSubscription() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).max_consumers_per_subscription;
    }

    protected void internalSetMaxConsumersPerSubscription(int maxConsumersPerSubscription) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (maxConsumersPerSubscription < 0) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "maxConsumersPerSubscription must be 0 or more");
            }
            policies.max_consumers_per_subscription = maxConsumersPerSubscription;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated maxConsumersPerSubscription configuration: namespace={}, value={}", clientAppId(),
                    namespaceName, policies.max_consumers_per_subscription);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update maxConsumersPerSubscription configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update maxConsumersPerSubscription configuration for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxConsumersPerSubscription configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected int internalGetMaxUnackedMessagesPerConsumer() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).max_unacked_messages_per_consumer;
    }

    protected void internalSetMaxUnackedMessagesPerConsumer(int maxUnackedMessagesPerConsumer) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (maxUnackedMessagesPerConsumer < 0) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "maxUnackedMessagesPerConsumer must be 0 or more");
            }
            policies.max_unacked_messages_per_consumer = maxUnackedMessagesPerConsumer;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated maxUnackedMessagesPerConsumer configuration: namespace={}, value={}", clientAppId(),
                    namespaceName, policies.max_unacked_messages_per_consumer);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update maxUnackedMessagesPerConsumer configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update maxUnackedMessagesPerConsumer configuration for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxUnackedMessagesPerConsumer configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected int internalGetMaxUnackedMessagesPerSubscription() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).max_unacked_messages_per_subscription;
    }

    protected void internalSetMaxUnackedMessagesPerSubscription(int maxUnackedMessagesPerSubscription) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (maxUnackedMessagesPerSubscription < 0) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "maxUnackedMessagesPerSubscription must be 0 or more");
            }
            policies.max_unacked_messages_per_subscription = maxUnackedMessagesPerSubscription;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated maxUnackedMessagesPerSubscription configuration: namespace={}, value={}", clientAppId(),
                    namespaceName, policies.max_unacked_messages_per_subscription);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update maxUnackedMessagesPerSubscription configuration for namespace {}: does not exist", clientAppId(),
                    namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update maxUnackedMessagesPerSubscription configuration for namespace {}: concurrent modification",
                    clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update maxUnackedMessagesPerSubscription configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            throw new RestException(e);
        }
    }

    protected long internalGetCompactionThreshold() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).compaction_threshold;
    }

    protected void internalSetCompactionThreshold(long newThreshold) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (newThreshold < 0) {
                throw new RestException(Status.PRECONDITION_FAILED,
                        "compactionThreshold must be 0 or more");
            }
            policies.compaction_threshold = newThreshold;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated compactionThreshold configuration: namespace={}, value={}",
                     clientAppId(), namespaceName, policies.compaction_threshold);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update compactionThreshold configuration for namespace {}: does not exist",
                     clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update compactionThreshold configuration for namespace {}: concurrent modification",
                     clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update compactionThreshold configuration for namespace {}",
                      clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected long internalGetOffloadThreshold() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        Policies policies = getNamespacePolicies(namespaceName);
        if (policies.offload_policies == null) {
            return policies.offload_threshold;
        } else {
            return policies.offload_policies.getManagedLedgerOffloadThresholdInBytes();
        }
    }

    protected void internalSetOffloadThreshold(long newThreshold) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);

            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (policies.offload_policies != null) {
                policies.offload_policies.setManagedLedgerOffloadThresholdInBytes(newThreshold);
            }
            policies.offload_threshold = newThreshold;

            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated offloadThreshold configuration: namespace={}, value={}",
                     clientAppId(), namespaceName, policies.offload_threshold);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update offloadThreshold configuration for namespace {}: does not exist",
                     clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update offloadThreshold configuration for namespace {}: concurrent modification",
                     clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update offloadThreshold configuration for namespace {}",
                      clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected Long internalGetOffloadDeletionLag() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        Policies policies = getNamespacePolicies(namespaceName);
        if (policies.offload_policies == null) {
            return policies.offload_deletion_lag_ms;
        } else {
            return policies.offload_policies.getManagedLedgerOffloadDeletionLagInMillis();
        }
    }

    protected void internalSetOffloadDeletionLag(Long newDeletionLagMs) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);

            Policies policies = jsonMapper().readValue(content, Policies.class);
            if (policies.offload_policies != null) {
                policies.offload_policies.setManagedLedgerOffloadDeletionLagInMillis(newDeletionLagMs);
            }
            policies.offload_deletion_lag_ms = newDeletionLagMs;

            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated offloadDeletionLagMs configuration: namespace={}, value={}",
                     clientAppId(), namespaceName, policies.offload_deletion_lag_ms);

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update offloadDeletionLagMs configuration for namespace {}: does not exist",
                     clientAppId(), namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update offloadDeletionLagMs configuration for namespace {}: concurrent modification",
                     clientAppId(), namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update offloadDeletionLag configuration for namespace {}",
                      clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    @Deprecated
    protected SchemaAutoUpdateCompatibilityStrategy internalGetSchemaAutoUpdateCompatibilityStrategy() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).schema_auto_update_compatibility_strategy;
    }

    protected SchemaCompatibilityStrategy internalGetSchemaCompatibilityStrategy() {
        validateAdminAccessForTenant(namespaceName.getTenant());
        Policies policies = getNamespacePolicies(namespaceName);
        SchemaCompatibilityStrategy schemaCompatibilityStrategy = policies.schema_compatibility_strategy;
        if (schemaCompatibilityStrategy == SchemaCompatibilityStrategy.UNDEFINED){
            schemaCompatibilityStrategy = SchemaCompatibilityStrategy
                    .fromAutoUpdatePolicy(policies.schema_auto_update_compatibility_strategy);
        }
        return schemaCompatibilityStrategy;
    }

    @Deprecated
    protected void internalSetSchemaAutoUpdateCompatibilityStrategy(SchemaAutoUpdateCompatibilityStrategy strategy) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        mutatePolicy((policies) -> {
                policies.schema_auto_update_compatibility_strategy = strategy;
                return policies;
            }, (policies) -> policies.schema_auto_update_compatibility_strategy,
            "schemaAutoUpdateCompatibilityStrategy");
    }

    protected void internalSetSchemaCompatibilityStrategy(SchemaCompatibilityStrategy strategy) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        mutatePolicy((policies) -> {
                    policies.schema_compatibility_strategy = strategy;
                    return policies;
                }, (policies) -> policies.schema_compatibility_strategy,
                "schemaCompatibilityStrategy");
    }

    protected boolean internalGetSchemaValidationEnforced() {
        validateSuperUserAccess();
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).schema_validation_enforced;
    }

    protected void internalSetSchemaValidationEnforced(boolean schemaValidationEnforced) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        mutatePolicy((policies) -> {
                policies. schema_validation_enforced = schemaValidationEnforced;
                return policies;
            }, (policies) -> policies. schema_validation_enforced,
            "schemaValidationEnforced");
    }

    protected boolean internalGetIsAllowAutoUpdateSchema() {
        validateSuperUserAccess();
        validateAdminAccessForTenant(namespaceName.getTenant());
        return getNamespacePolicies(namespaceName).is_allow_auto_update_schema;
    }

    protected void internalSetIsAllowAutoUpdateSchema(boolean isAllowAutoUpdateSchema) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        mutatePolicy((policies) -> {
                    policies.is_allow_auto_update_schema = isAllowAutoUpdateSchema;
                    return policies;
                }, (policies) -> policies.is_allow_auto_update_schema,
                "isAllowAutoUpdateSchema");
    }


    private <T> void mutatePolicy(Function<Policies, Policies> policyTransformation,
                                  Function<Policies, T> getter,
                                  String policyName) {
        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);
            policies = policyTransformation.apply(policies);
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion());
            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
            log.info("[{}] Successfully updated {} configuration: namespace={}, value={}",
                     clientAppId(), policyName, namespaceName, getter.apply(policies));

        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update {} configuration for namespace {}: does not exist",
                     clientAppId(), policyName, namespaceName);
            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
        } catch (KeeperException.BadVersionException e) {
            log.warn("[{}] Failed to update {} configuration for namespace {}: concurrent modification",
                     clientAppId(), policyName, namespaceName);
            throw new RestException(Status.CONFLICT, "Concurrent modification");
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update {} configuration for namespace {}",
                      clientAppId(), policyName, namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void internalSetOffloadPolicies(AsyncResponse asyncResponse, OffloadPolicies offloadPolicies) {
        validateAdminAccessForTenant(namespaceName.getTenant());
        validatePoliciesReadOnlyAccess();
        validateOffloadPolicies(offloadPolicies);

        try {
            Stat nodeStat = new Stat();
            final String path = path(POLICIES, namespaceName.toString());
            byte[] content = globalZk().getData(path, null, nodeStat);
            Policies policies = jsonMapper().readValue(content, Policies.class);

            if (offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis()
                    .equals(OffloadPolicies.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS)) {
                offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(policies.offload_deletion_lag_ms);
            } else {
                policies.offload_deletion_lag_ms = offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis();
            }
            if (offloadPolicies.getManagedLedgerOffloadThresholdInBytes() ==
                    OffloadPolicies.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES) {
                offloadPolicies.setManagedLedgerOffloadThresholdInBytes(policies.offload_threshold);
            } else {
                policies.offload_threshold = offloadPolicies.getManagedLedgerOffloadThresholdInBytes();
            }

            policies.offload_policies = offloadPolicies;
            globalZk().setData(path, jsonMapper().writeValueAsBytes(policies), nodeStat.getVersion(),
                    (rc, path1, ctx, stat) -> {
                        if (rc == KeeperException.Code.OK.intValue()) {
                            policiesCache().invalidate(path(POLICIES, namespaceName.toString()));
                        } else {
                            String errorMsg = String.format(
                                    "[%s] Failed to update offload configuration for namespace %s",
                                    clientAppId(), namespaceName);
                            if (rc == KeeperException.Code.NONODE.intValue()) {
                                log.warn("{} : does not exist", errorMsg);
                                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Namespace does not exist"));
                            } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                                log.warn("{} : concurrent modification", errorMsg);
                                asyncResponse.resume(new RestException(Status.CONFLICT, "Concurrent modification"));
                            } else {
                                asyncResponse.resume(KeeperException.create(KeeperException.Code.get(rc), errorMsg));
                            }
                        }
                    }, null);
            log.info("[{}] Successfully updated offload configuration: namespace={}, map={}", clientAppId(),
                    namespaceName, jsonMapper().writeValueAsString(policies.offload_policies));
            asyncResponse.resume(Response.noContent().build());
        } catch (Exception e) {
            log.error("[{}] Failed to update offload configuration for namespace {}", clientAppId(), namespaceName,
                    e);
            asyncResponse.resume(new RestException(e));
        }
    }

    private void validateOffloadPolicies(OffloadPolicies offloadPolicies) {
        if (offloadPolicies == null) {
            log.warn("[{}] Failed to update offload configuration for namespace {}: offloadPolicies is null",
                    clientAppId(), namespaceName);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "The offloadPolicies must be specified for namespace offload.");
        }
        if (!offloadPolicies.driverSupported()) {
            log.warn("[{}] Failed to update offload configuration for namespace {}: " +
                            "driver is not supported, support value: {}",
                    clientAppId(), namespaceName, OffloadPolicies.getSupportedDriverNames());
            throw new RestException(Status.PRECONDITION_FAILED,
                    "The driver is not supported, support value: " + OffloadPolicies.getSupportedDriverNames());
        }
        if (!offloadPolicies.bucketValid()) {
            log.warn("[{}] Failed to update offload configuration for namespace {}: bucket must be specified",
                    clientAppId(), namespaceName);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "The bucket must be specified for namespace offload.");
        }
    }

    protected OffloadPolicies internalGetOffloadPolicies() {
        validateAdminAccessForTenant(namespaceName.getTenant());

        Policies policies = getNamespacePolicies(namespaceName);
        return policies.offload_policies;
    }

    private static final Logger log = LoggerFactory.getLogger(NamespacesBase.class);
}
