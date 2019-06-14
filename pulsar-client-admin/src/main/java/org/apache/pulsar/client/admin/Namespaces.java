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
package org.apache.pulsar.client.admin;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;

/**
 * Admin interface for namespaces management
 */
public interface Namespaces {
    /**
     * Get the list of namespaces.
     * <p>
     * Get the list of all the namespaces for a certain tenant.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>["my-tenant/c1/namespace1",
     *  "my-tenant/global/namespace2",
     *  "my-tenant/c2/namespace3"]</code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getNamespaces(String tenant) throws PulsarAdminException;

    /**
     * Get the list of namespaces.
     * <p>
     * Get the list of all the namespaces for a certain tenant on single cluster.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>["my-tenant/use/namespace1", "my-tenant/use/namespace2"]</code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param cluster
     *            Cluster name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant or cluster does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    List<String> getNamespaces(String tenant, String cluster) throws PulsarAdminException;

    /**
     * Get the list of topics.
     * <p>
     * Get the list of all the topics under a certain namespace.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>["persistent://my-tenant/use/namespace1/my-topic-1",
     *  "persistent://my-tenant/use/namespace1/my-topic-2"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getTopics(String namespace) throws PulsarAdminException;

    /**
     * Get policies for a namespace.
     * <p>
     * Get the dump all the policies specified for a namespace.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>{
     *   "auth_policies" : {
     *     "namespace_auth" : {
     *       "my-role" : [ "produce" ]
     *     },
     *     "destination_auth" : {
     *       "persistent://prop/local/ns1/my-topic" : {
     *         "role-1" : [ "produce" ],
     *         "role-2" : [ "consume" ]
     *       }
     *     }
     *   },
     *   "replication_clusters" : ["use", "usw"],
     *   "message_ttl_in_seconds" : 300
     * }</code>
     * </pre>
     *
     * @see Policies
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Policies getPolicies(String namespace) throws PulsarAdminException;

    /**
     * Create a new namespace.
     * <p>
     * Creates a new empty namespace with no policies attached.
     *
     * @param namespace
     *            Namespace name
     * @param numBundles
     *            Number of bundles
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Tenant or cluster does not exist
     * @throws ConflictException
     *             Namespace already exists
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createNamespace(String namespace, int numBundles) throws PulsarAdminException;

    /**
     * Create a new namespace.
     * <p>
     * Creates a new empty namespace with no policies attached.
     *
     * @param namespace
     *            Namespace name
     * @param bundlesData
     *            Bundles Data
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Tenant or cluster does not exist
     * @throws ConflictException
     *             Namespace already exists
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createNamespace(String namespace, BundlesData bundlesData) throws PulsarAdminException;

    /**
     * Create a new namespace.
     * <p>
     * Creates a new empty namespace with no policies attached.
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Tenant or cluster does not exist
     * @throws ConflictException
     *             Namespace already exists
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createNamespace(String namespace) throws PulsarAdminException;

    /**
     * Create a new namespace.
     * <p>
     * Creates a new empty namespace with no policies attached.
     *
     * @param namespace
     *            Namespace name
     * @param clusters
     *            Clusters in which the namespace will be present. If more than one cluster is present, replication
     *            across clusters will be enabled.
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Tenant or cluster does not exist
     * @throws ConflictException
     *             Namespace already exists
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createNamespace(String namespace, Set<String> clusters) throws PulsarAdminException;

    /**
     * Create a new namespace.
     * <p>
     * Creates a new namespace with the specified policies.
     *
     * @param namespace
     *            Namespace name
     * @param policies
     *            Policies for the namespace
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Tenant or cluster does not exist
     * @throws ConflictException
     *             Namespace already exists
     * @throws PulsarAdminException
     *             Unexpected error
     *
     * @since 2.0
     */
    void createNamespace(String namespace, Policies policies) throws PulsarAdminException;

    /**
     * Delete an existing namespace.
     * <p>
     * The namespace needs to be empty.
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws ConflictException
     *             Namespace is not empty
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteNamespace(String namespace) throws PulsarAdminException;

    /**
     * Delete an existing bundle in a namespace.
     * <p>
     * The bundle needs to be empty.
     *
     * @param namespace
     *            Namespace name
     * @param bundleRange
     *            range of the bundle
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Namespace/bundle does not exist
     * @throws ConflictException
     *             Bundle is not empty
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteNamespaceBundle(String namespace, String bundleRange) throws PulsarAdminException;

    /**
     * Get permissions on a namespace.
     * <p>
     * Retrieve the permissions for a namespace.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>{
     *   "my-role" : [ "produce" ]
     *   "other-role" : [ "consume" ]
     * }</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Map<String, Set<AuthAction>> getPermissions(String namespace) throws PulsarAdminException;

    /**
     * Grant permission on a namespace.
     * <p>
     * Grant a new permission to a client role on a namespace.
     * <p>
     * Request parameter example:
     *
     * <pre>
     * <code>["produce", "consume"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param role
     *            Client role to which grant permission
     * @param actions
     *            Auth actions (produce and consume)
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void grantPermissionOnNamespace(String namespace, String role, Set<AuthAction> actions) throws PulsarAdminException;

    /**
     * Revoke permissions on a namespace.
     * <p>
     * Revoke all permissions to a client role on a namespace.
     *
     * @param namespace
     *            Namespace name
     * @param role
     *            Client role to which remove permissions
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void revokePermissionsOnNamespace(String namespace, String role) throws PulsarAdminException;

    /**
     * Grant permission to role to access subscription's admin-api.
     * @param namespace
     * @param subscription
     * @param roles
     * @throws PulsarAdminException
     */
    void grantPermissionOnSubscription(String namespace, String subscription, Set<String> roles) throws PulsarAdminException;

    /**
     * Revoke permissions on a subscription's admin-api access.
     * @param namespace
     * @param subscription
     * @param role
     * @throws PulsarAdminException
     */
    void revokePermissionOnSubscription(String namespace, String subscription, String role) throws PulsarAdminException;

    /**
     * Get the replication clusters for a namespace.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>["use", "usw", "usc"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PreconditionFailedException
     *             Namespace is not global
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getNamespaceReplicationClusters(String namespace) throws PulsarAdminException;

    /**
     * Set the replication clusters for a namespace.
     * <p>
     * Request example:
     *
     * <pre>
     * <code>["us-west", "us-east", "us-cent"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param clusterIds
     *            Pulsar Cluster Ids
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PreconditionFailedException
     *             Namespace is not global
     * @throws PreconditionFailedException
     *             Invalid cluster ids
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setNamespaceReplicationClusters(String namespace, Set<String> clusterIds) throws PulsarAdminException;

    /**
     * Get the message TTL for a namespace.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>60</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    int getNamespaceMessageTTL(String namespace) throws PulsarAdminException;

    /**
     * Set the messages Time to Live for all the topics within a namespace.
     * <p>
     * Request example:
     *
     * <pre>
     * <code>60</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param ttlInSeconds
     *            TTL values for all messages for all topics in this namespace
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setNamespaceMessageTTL(String namespace, int ttlInSeconds) throws PulsarAdminException;


    /**
     * Set anti-affinity group name for a namespace
     * <p>
     * Request example:
     *
     * @param namespace
     *            Namespace name
     * @param namespaceAntiAffinityGroup
     *            anti-affinity group name for a namespace
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setNamespaceAntiAffinityGroup(String namespace, String namespaceAntiAffinityGroup) throws PulsarAdminException;

    /**
     * Get all namespaces that grouped with given anti-affinity group
     *
     * @param tenant
     *            tenant is only used for authorization. Client has to be admin of any of the tenant to access this
     *            api api.
     * @param cluster
     *            cluster name
     * @param namespaceAntiAffinityGroup
     *            Anti-affinity group name
     * @return list of namespace grouped under a given anti-affinity group
     * @throws PulsarAdminException
     */
    List<String> getAntiAffinityNamespaces(String tenant, String cluster, String namespaceAntiAffinityGroup)
            throws PulsarAdminException;

    /**
     * Get anti-affinity group name for a namespace
     * <p>
     * Response example:
     *
     * <pre>
     * <code>60</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    String getNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException;

    /**
     * Delete anti-affinity group name for a namespace.
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException;

    /**
     * Set the deduplication status for all topics within a namespace.
     * <p>
     * When deduplication is enabled, the broker will prevent to store the same message multiple times.
     * <p>
     * Request example:
     *
     * <pre>
     * <code>true</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param enableDeduplication
     *            wether to enable or disable deduplication feature
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setDeduplicationStatus(String namespace, boolean enableDeduplication) throws PulsarAdminException;

    /**
     * Get the bundles split data.
     *
     * @param namespace
     *            Namespace name
     *
     * @HttpCode 200 Successfully retrieved
     * @HttpCode 403 Don't have admin permission
     * @HttpCode 404 Namespace does not exist
     * @HttpCode 412 Namespace is not setup to split in bundles
     */
    // BundlesData getBundlesData(String namespace);

    /**
     * Get backlog quota map on a namespace.
     * <p>
     * Get backlog quota map on a namespace.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>
     *  {
     *      "namespace_memory" : {
     *          "limit" : "134217728",
     *          "policy" : "consumer_backlog_eviction"
     *      },
     *      "destination_storage" : {
     *          "limit" : "-1",
     *          "policy" : "producer_exception"
     *      }
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Permission denied
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String namespace) throws PulsarAdminException;

    /**
     * Set a backlog quota for all the topics on a namespace.
     * <p>
     * Set a backlog quota on a namespace.
     * <p>
     * The backlog quota can be set on this resource:
     * <p>
     * Request parameter example:
     *
     * <pre>
     * <code>
     * {
     *     "limit" : "134217728",
     *     "policy" : "consumer_backlog_eviction"
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param backlogQuota
     *            the new BacklogQuota
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setBacklogQuota(String namespace, BacklogQuota backlogQuota) throws PulsarAdminException;

    /**
     * Remove a backlog quota policy from a namespace.
     * <p>
     * Remove a backlog quota policy from a namespace.
     * <p>
     * The backlog retention policy will fall back to the default.
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void removeBacklogQuota(String namespace) throws PulsarAdminException;

    /**
     * Set the persistence configuration for all the topics on a namespace.
     * <p>
     * Set the persistence configuration on a namespace.
     * <p>
     * Request parameter example:
     *
     * <pre>
     * <code>
     * {
     *     "bookkeeperEnsemble" : 3,                 // Number of bookies to use for a topic
     *     "bookkeeperWriteQuorum" : 2,              // How many writes to make of each entry
     *     "bookkeeperAckQuorum" : 2,                // Number of acks (guaranteed copies) to wait for each entry
     *     "managedLedgerMaxMarkDeleteRate" : 10.0,  // Throttling rate of mark-delete operation
     *                                               // to avoid high number of updates for each
     *                                               // consumer
     * }
     * </code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param cluster
     *            Cluster name
     * @param namespace
     *            Namespace name
     * @param persistence
     *            Persistence policies object
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setPersistence(String namespace, PersistencePolicies persistence) throws PulsarAdminException;

    /**
     * Get the persistence configuration for a namespace.
     * <p>
     * Get the persistence configuration for a namespace.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>
     * {
     *     "bookkeeperEnsemble" : 3,                 // Number of bookies to use for a topic
     *     "bookkeeperWriteQuorum" : 2,              // How many writes to make of each entry
     *     "bookkeeperAckQuorum" : 2,                // Number of acks (guaranteed copies) to wait for each entry
     *     "managedLedgerMaxMarkDeleteRate" : 10.0,  // Throttling rate of mark-delete operation
     *                                               // to avoid high number of updates for each
     *                                               // consumer
     * }
     * </code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param cluster
     *            Cluster name
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    PersistencePolicies getPersistence(String namespace) throws PulsarAdminException;

    /**
     * Set bookie affinity group for a namespace to isolate namespace write to bookies that are part of given affinity
     * group.
     * 
     * @param namespace
     *            namespace name
     * @param bookieAffinityGroup
     *            bookie affinity group
     * @throws PulsarAdminException
     */
    void setBookieAffinityGroup(String namespace, BookieAffinityGroupData bookieAffinityGroup)
            throws PulsarAdminException;
    
    /**
     * Delete bookie affinity group configured for a namespace.
     * 
     * @param namespace
     * @throws PulsarAdminException
     */
    void deleteBookieAffinityGroup(String namespace) throws PulsarAdminException;

    /**
     * Get bookie affinity group configured for a namespace.
     * 
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    BookieAffinityGroupData getBookieAffinityGroup(String namespace) throws PulsarAdminException;

    /**
     * Set the retention configuration for all the topics on a namespace.
     * <p/>
     * Set the retention configuration on a namespace. This operation requires Pulsar super-user access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setRetention(String namespace, RetentionPolicies retention) throws PulsarAdminException;

    /**
     * Get the retention configuration for a namespace.
     * <p/>
     * Get the retention configuration for a namespace.
     * <p/>
     * Response example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    RetentionPolicies getRetention(String namespace) throws PulsarAdminException;

    /**
     * Unload a namespace from the current serving broker.
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PreconditionFailedException
     *             Namespace is already unloaded
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void unload(String namespace) throws PulsarAdminException;

    /**
     * Get the replication configuration version for a given namespace
     *
     * @param namespace
     * @return Replication configuration version
     * @throws PulsarAdminException
     *             Unexpected error
     */
    String getReplicationConfigVersion(String namespace) throws PulsarAdminException;

    /**
     * Unload namespace bundle
     *
     * @param namespace
     * @bundle range of bundle to unload
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void unloadNamespaceBundle(String namespace, String bundle) throws PulsarAdminException;

    /**
     * Split namespace bundle
     *
     * @param namespace
     * @param range of bundle to split
     * @param unload newly split bundles from the broker
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void splitNamespaceBundle(String namespace, String bundle, boolean unloadSplitBundles) throws PulsarAdminException;

    /**
     * Set message-dispatch-rate (topics under this namespace can dispatch this many messages per second)
     *
     * @param namespace
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException;

    /** Get message-dispatch-rate (topics under this namespace can dispatch this many messages per second)
    *
    * @param namespace
    * @returns messageRate
    *            number of messages per second
    * @throws PulsarAdminException
    *             Unexpected error
    */
    DispatchRate getDispatchRate(String namespace) throws PulsarAdminException;

    /**
     * Set namespace-subscribe-rate (topics under this namespace will limit by subscribeRate)
     *
     * @param namespace
     * @param subscribeRate
     *            consumer subscribe limit by this subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSubscribeRate(String namespace, SubscribeRate subscribeRate) throws PulsarAdminException;

    /** Get namespace-subscribe-rate (topics under this namespace allow subscribe times per consumer in a period)
     *
     * @param namespace
     * @returns subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    SubscribeRate getSubscribeRate(String namespace) throws PulsarAdminException;

    /**
     * Set subscription-message-dispatch-rate (subscriptions under this namespace can dispatch this many messages per second)
     *
     * @param namespace
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSubscriptionDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException;

    /** Get subscription-message-dispatch-rate (subscriptions under this namespace can dispatch this many messages per second)
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getSubscriptionDispatchRate(String namespace) throws PulsarAdminException;

    /**
     * Set replicator-message-dispatch-rate (Replicators under this namespace can dispatch this many messages per second)
     *
     * @param namespace
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setReplicatorDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException;

    /** Get replicator-message-dispatch-rate (Replicators under this namespace can dispatch this many messages per second)
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getReplicatorDispatchRate(String namespace) throws PulsarAdminException;

    /**
     * Clear backlog for all topics on a namespace
     *
     * @param namespace
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBacklog(String namespace) throws PulsarAdminException;

    /**
     * Clear backlog for a given subscription on all topics on a namespace
     *
     * @param namespace
     * @param subscription
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBacklogForSubscription(String namespace, String subscription) throws PulsarAdminException;

    /**
     * Clear backlog for all topics on a namespace bundle
     *
     * @param namespace
     * @param bundle
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBundleBacklog(String namespace, String bundle) throws PulsarAdminException;

    /**
     * Clear backlog for a given subscription on all topics on a namespace bundle
     *
     * @param namespace
     * @param bundle
     * @param subscription
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBundleBacklogForSubscription(String namespace, String bundle, String subscription)
            throws PulsarAdminException;

    /**
     * Unsubscribes the given subscription on all topics on a namespace
     *
     * @param namespace
     * @param subscription
     * @throws PulsarAdminException
     */
    void unsubscribeNamespace(String namespace, String subscription) throws PulsarAdminException;

    /**
     * Unsubscribes the given subscription on all topics on a namespace bundle
     *
     * @param namespace
     * @param bundle
     * @param subscription
     * @throws PulsarAdminException
     */
    void unsubscribeNamespaceBundle(String namespace, String bundle, String subscription) throws PulsarAdminException;

    /**
     * Set the encryption required status for all topics within a namespace.
     * <p>
     * When encryption required is true, the broker will prevent to store unencrypted messages.
     * <p>
     * Request example:
     *
     * <pre>
     * <code>true</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param encryptionRequired
     *            whether message encryption is required or not
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setEncryptionRequiredStatus(String namespace, boolean encryptionRequired) throws PulsarAdminException;

     /**
     * Set the given subscription auth mode on all topics on a namespace
     *
     * @param namespace
     * @param subscriptionAuthMode
     * @throws PulsarAdminException
     */
    void setSubscriptionAuthMode(String namespace, SubscriptionAuthMode subscriptionAuthMode) throws PulsarAdminException;

    /**
     * Get the maxProducersPerTopic for a namespace.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    int getMaxProducersPerTopic(String namespace) throws PulsarAdminException;

    /**
     * Set maxProducersPerTopic for a namespace.
     * <p>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param maxProducersPerTopic
     *            maxProducersPerTopic value for a namespace
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setMaxProducersPerTopic(String namespace, int maxProducersPerTopic) throws PulsarAdminException;

    /**
     * Get the maxProducersPerTopic for a namespace.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    int getMaxConsumersPerTopic(String namespace) throws PulsarAdminException;

    /**
     * Set maxConsumersPerTopic for a namespace.
     * <p>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param maxConsumersPerTopic
     *            maxConsumersPerTopic value for a namespace
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setMaxConsumersPerTopic(String namespace, int maxConsumersPerTopic) throws PulsarAdminException;

    /**
     * Get the maxConsumersPerSubscription for a namespace.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    int getMaxConsumersPerSubscription(String namespace) throws PulsarAdminException;

    /**
     * Set maxConsumersPerSubscription for a namespace.
     * <p>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param maxConsumersPerSubscription
     *            maxConsumersPerSubscription value for a namespace
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setMaxConsumersPerSubscription(String namespace, int maxConsumersPerSubscription) throws PulsarAdminException;

    /**
     * Get the compactionThreshold for a namespace. The maximum number of bytes topics in the namespace
     * can have before compaction is triggered. 0 disables.
     * <p>
     * Response example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    long getCompactionThreshold(String namespace) throws PulsarAdminException;

    /**
     * Set the compactionThreshold for a namespace. The maximum number of bytes topics in the namespace
     * can have before compaction is triggered. 0 disables.
     * <p>
     * Request example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param compactionThreshold
     *            maximum number of backlog bytes before compaction is triggered
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setCompactionThreshold(String namespace, long compactionThreshold) throws PulsarAdminException;

    /**
     * Get the offloadThreshold for a namespace. The maximum number of bytes stored on the pulsar cluster for topics
     * in the namespace before data starts being offloaded to longterm storage.
     *
     * <p>
     * Response example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    long getOffloadThreshold(String namespace) throws PulsarAdminException;

    /**
     * Set the offloadThreshold for a namespace. The maximum number of bytes stored on the pulsar cluster for topics
     * in the namespace before data starts being offloaded to longterm storage.
     *
     * Negative values disabled automatic offloading. Setting a threshold of 0 will offload data as soon as possible.
     * <p>
     * Request example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param offloadThreshold
     *            maximum number of bytes stored before offloading is triggered
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setOffloadThreshold(String namespace, long compactionThreshold) throws PulsarAdminException;

    /**
     * Get the offload deletion lag for a namespace, in milliseconds.
     * The number of milliseconds to wait before deleting a ledger segment which has been offloaded from
     * the Pulsar cluster's local storage (i.e. BookKeeper).
     *
     * If the offload deletion lag has not been set for the namespace, the method returns 'null'
     * and the namespace will use the configured default of the pulsar broker.
     *
     * A negative value disables deletion of the local ledger completely, though it will still be deleted
     * if it exceeds the topics retention policy, along with the offloaded copy.
     *
     * <p>
     * Response example:
     *
     * <pre>
     * <code>3600000</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @return the offload deletion lag for the namespace in milliseconds, or null if not set
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Long getOffloadDeleteLagMs(String namespace) throws PulsarAdminException;

    /**
     * Set the offload deletion lag for a namespace.
     *
     * The offload deletion lag is the amount of time to wait after offloading a ledger segment to long term storage,
     * before deleting its copy stored on the Pulsar cluster's local storage (i.e. BookKeeper).
     *
     * A negative value disables deletion of the local ledger completely, though it will still be deleted
     * if it exceeds the topics retention policy, along with the offloaded copy.
     *
     * @param namespace
     *            Namespace name
     * @param lag the duration to wait before deleting the local copy
     * @param unit the timeunit of the duration
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setOffloadDeleteLag(String namespace, long lag, TimeUnit unit) throws PulsarAdminException;

    /**
     * Clear the offload deletion lag for a namespace.
     *
     * The namespace will fall back to using the configured default of the pulsar broker.
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearOffloadDeleteLag(String namespace) throws PulsarAdminException;

    /**
     * Get the strategy used to check the a new schema provided by a producer is compatible with the current schema
     * before it is installed.
     *
     * <p>If this is
     * {@link org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy#AutoUpdateDisabled},
     * then all new schemas provided via the producer are rejected, and schemas must be updated through the REST api.
     *
     * @param namespace The namespace in whose policy we are interested
     * @return the strategy used to check compatibility
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    SchemaAutoUpdateCompatibilityStrategy getSchemaAutoUpdateCompatibilityStrategy(String namespace)
            throws PulsarAdminException;

    /**
     * Set the strategy used to check the a new schema provided by a producer is compatible with the current schema
     * before it is installed.
     *
     * <p>To disable all new schema updates through the producer, set this to
     * {@link org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy#AutoUpdateDisabled}.
     *
     * @param namespace The namespace in whose policy should be set
     * @param autoUpdate true if connecting producers can automatically update the schema, false otherwise
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSchemaAutoUpdateCompatibilityStrategy(String namespace,
                                                  SchemaAutoUpdateCompatibilityStrategy strategy)
            throws PulsarAdminException;

    /**
     * Get schema validation enforced for namespace.
     * @return the schema validation enforced flag
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant or Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */

    boolean getSchemaValidationEnforced(String namespace)
            throws PulsarAdminException;
    /**
     * Set schema validation enforced for namespace.
     * if a producer without a schema attempts to produce to a topic with schema in this the namespace, the
     * producer will be failed to connect. PLEASE be carefully on using this, since non-java clients don't
     * support schema. if you enable this setting, it will cause non-java clients failed to produce.
     *
     * @param namespace pulsar namespace name
     * @param schemaValidationEnforced flag to enable or disable schema validation for the given namespace
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant or Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */

    void setSchemaValidationEnforced(String namespace, boolean schemaValidationEnforced)
            throws PulsarAdminException;
}
