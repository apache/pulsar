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

import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;

/**
 * Admin interface for namespaces management
 */
public interface Namespaces {
    /**
     * Get the list of namespaces.
     * <p>
     * Get the list of all the namespaces for a certain property.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>["my-property/c1/namespace1",
     *  "my-property/global/namespace2",
     *  "my-property/c2/namespace3"]</code>
     * </pre>
     *
     * @param property
     *            Property name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Property does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getNamespaces(String property) throws PulsarAdminException;

    /**
     * Get the list of namespaces.
     * <p>
     * Get the list of all the namespaces for a certain property on single cluster.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>["my-property/use/namespace1", "my-property/use/namespace2"]</code>
     * </pre>
     *
     * @param property
     *            Property name
     * @param cluster
     *            Cluster name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Property or cluster does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getNamespaces(String property, String cluster) throws PulsarAdminException;

    /**
     * Get the list of destinations.
     * <p>
     * Get the list of all the destinations under a certain namespace.
     * <p>
     * Response Example:
     *
     * <pre>
     * <code>["persistent://my-property/use/namespace1/my-topic-1",
     *  "persistent://my-property/use/namespace1/my-topic-2"]</code>
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
    List<String> getDestinations(String namespace) throws PulsarAdminException;

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
     *             Property or cluster does not exist
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
     *             Property or cluster does not exist
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
     *             Property or cluster does not exist
     * @throws ConflictException
     *             Namespace already exists
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createNamespace(String namespace) throws PulsarAdminException;

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
    void setNamespaceReplicationClusters(String namespace, List<String> clusterIds) throws PulsarAdminException;

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
     * Set the replication clusters for a namespace.
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
     * Set a backlog quota for all the destinations on a namespace.
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
    public void setBacklogQuota(String namespace, BacklogQuota backlogQuota) throws PulsarAdminException;

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
    public void removeBacklogQuota(String namespace) throws PulsarAdminException;

    /**
     * Set the persistence configuration for all the destinations on a namespace.
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
     * @param property
     *            Property name
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
    public void setPersistence(String namespace, PersistencePolicies persistence) throws PulsarAdminException;

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
     * @param property
     *            Property name
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
    public PersistencePolicies getPersistence(String namespace) throws PulsarAdminException;

    /**
     * Set the retention configuration for all the destinations on a namespace.
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
     * @bundle range of bundle to split
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void splitNamespaceBundle(String namespace, String bundle) throws PulsarAdminException;

    /**
     * Clear backlog for all destinations on a namespace
     *
     * @param namespace
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBacklog(String namespace) throws PulsarAdminException;

    /**
     * Clear backlog for a given subscription on all destinations on a namespace
     *
     * @param namespace
     * @param subscription
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBacklogForSubscription(String namespace, String subscription) throws PulsarAdminException;

    /**
     * Clear backlog for all destinations on a namespace bundle
     *
     * @param namespace
     * @param bundle
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBundleBacklog(String namespace, String bundle) throws PulsarAdminException;

    /**
     * Clear backlog for a given subscription on all destinations on a namespace bundle
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
     * Unsubscribes the given subscription on all destinations on a namespace
     *
     * @param namespace
     * @param subscription
     * @throws PulsarAdminException
     */
    void unsubscribeNamespace(String namespace, String subscription) throws PulsarAdminException;

    /**
     * Unsubscribes the given subscription on all destinations on a namespace bundle
     *
     * @param namespace
     * @param bundle
     * @param subscription
     * @throws PulsarAdminException
     */
    void unsubscribeNamespaceBundle(String namespace, String bundle, String subscription) throws PulsarAdminException;
}
