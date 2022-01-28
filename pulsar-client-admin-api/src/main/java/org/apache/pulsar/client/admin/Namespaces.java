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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;

/**
 * Admin interface for namespaces management.
 */
public interface Namespaces {
    /**
     * Get the list of namespaces.
     * <p/>
     * Get the list of all the namespaces for a certain tenant.
     * <p/>
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
     * Get the list of namespaces asynchronously.
     * <p/>
     * Get the list of all the namespaces for a certain tenant.
     * <p/>
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
     */
    CompletableFuture<List<String>> getNamespacesAsync(String tenant);

    /**
     * Get the list of namespaces.
     * <p/>
     * Get the list of all the namespaces for a certain tenant on single cluster.
     * <p/>
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
     * <p/>
     * Get the list of all the topics under a certain namespace.
     * <p/>
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
     * Get the list of topics asynchronously.
     * <p/>
     * Get the list of all the topics under a certain namespace.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["persistent://my-tenant/use/namespace1/my-topic-1",
     *  "persistent://my-tenant/use/namespace1/my-topic-2"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<List<String>> getTopicsAsync(String namespace);

    /**
     * Get the list of bundles.
     * <p/>
     * Get the list of all the bundles under a certain namespace.
     * <p/>
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
    BundlesData getBundles(String namespace) throws PulsarAdminException;

    /**
     * Get the list of bundles asynchronously.
     * <p/>
     * Get the list of all the bundles under a certain namespace.
     * <p/>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<BundlesData> getBundlesAsync(String namespace);

    /**
     * Get policies for a namespace.
     * <p/>
     * Get the dump all the policies specified for a namespace.
     * <p/>
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
     * Get policies for a namespace asynchronously.
     * <p/>
     * Get the dump all the policies specified for a namespace.
     * <p/>
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
     */
    CompletableFuture<Policies> getPoliciesAsync(String namespace);

    /**
     * Create a new namespace.
     * <p/>
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
     * <p/>
     * Creates a new empty namespace with no policies attached.
     *
     * @param namespace
     *            Namespace name
     * @param numBundles
     *            Number of bundles
     */
    CompletableFuture<Void> createNamespaceAsync(String namespace, int numBundles);

    /**
     * Create a new namespace.
     * <p/>
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
     * Create a new namespace asynchronously.
     * <p/>
     * Creates a new empty namespace with no policies attached.
     *
     * @param namespace
     *            Namespace name
     * @param bundlesData
     *            Bundles Data
     */
    CompletableFuture<Void> createNamespaceAsync(String namespace, BundlesData bundlesData);

    /**
     * Create a new namespace.
     * <p/>
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
     * Create a new namespace asynchronously.
     * <p/>
     * Creates a new empty namespace with no policies attached.
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Void> createNamespaceAsync(String namespace);

    /**
     * Create a new namespace.
     * <p/>
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
     * Create a new namespace asynchronously.
     * <p/>
     * Creates a new empty namespace with no policies attached.
     *
     * @param namespace
     *            Namespace name
     * @param clusters
     *            Clusters in which the namespace will be present. If more than one cluster is present, replication
     *            across clusters will be enabled.
     */
    CompletableFuture<Void> createNamespaceAsync(String namespace, Set<String> clusters);

    /**
     * Create a new namespace.
     * <p/>
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
     * Create a new namespace asynchronously.
     * <p/>
     * Creates a new namespace with the specified policies.
     *
     * @param namespace
     *            Namespace name
     * @param policies
     *            Policies for the namespace
     */
    CompletableFuture<Void> createNamespaceAsync(String namespace, Policies policies);

    /**
     * Delete an existing namespace.
     * <p/>
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
     * Delete an existing namespace.
     * <p/>
     * Force flag deletes namespace forcefully by force deleting all topics under it.
     *
     * @param namespace
     *            Namespace name
     * @param force
     *            Delete namespace forcefully
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
    void deleteNamespace(String namespace, boolean force) throws PulsarAdminException;

    /**
     * Delete an existing namespace asynchronously.
     * <p/>
     * The namespace needs to be empty.
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Void> deleteNamespaceAsync(String namespace);

    /**
     * Delete an existing namespace asynchronously.
     * <p/>
     * Force flag deletes namespace forcefully by force deleting all topics under it.
     *
     * @param namespace
     *            Namespace name
     * @param force
     *            Delete namespace forcefully
     */
    CompletableFuture<Void> deleteNamespaceAsync(String namespace, boolean force);

    /**
     * Delete an existing bundle in a namespace.
     * <p/>
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
     * Delete an existing bundle in a namespace.
     * <p/>
     * Force flag deletes bundle forcefully.
     *
     * @param namespace
     *            Namespace name
     * @param bundleRange
     *            range of the bundle
     * @param force
     *            Delete bundle forcefully
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
    void deleteNamespaceBundle(String namespace, String bundleRange, boolean force) throws PulsarAdminException;

    /**
     * Delete an existing bundle in a namespace asynchronously.
     * <p/>
     * The bundle needs to be empty.
     *
     * @param namespace
     *            Namespace name
     * @param bundleRange
     *            range of the bundle
     *
     * @return a future that can be used to track when the bundle is deleted
     */
    CompletableFuture<Void> deleteNamespaceBundleAsync(String namespace, String bundleRange);

    /**
     * Delete an existing bundle in a namespace asynchronously.
     * <p/>
     * Force flag deletes bundle forcefully.
     *
     * @param namespace
     *            Namespace name
     * @param bundleRange
     *            range of the bundle
     * @param force
     *            Delete bundle forcefully
     *
     * @return a future that can be used to track when the bundle is deleted
     */
    CompletableFuture<Void> deleteNamespaceBundleAsync(String namespace, String bundleRange, boolean force);

    /**
     * Get permissions on a namespace.
     * <p/>
     * Retrieve the permissions for a namespace.
     * <p/>
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
     * Get permissions on a namespace asynchronously.
     * <p/>
     * Retrieve the permissions for a namespace.
     * <p/>
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
     */
    CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(String namespace);

    /**
     * Grant permission on a namespace.
     * <p/>
     * Grant a new permission to a client role on a namespace.
     * <p/>
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
     * Grant permission on a namespace asynchronously.
     * <p/>
     * Grant a new permission to a client role on a namespace.
     * <p/>
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
     */
    CompletableFuture<Void> grantPermissionOnNamespaceAsync(String namespace, String role, Set<AuthAction> actions);

    /**
     * Revoke permissions on a namespace.
     * <p/>
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
     * Revoke permissions on a namespace asynchronously.
     * <p/>
     * Revoke all permissions to a client role on a namespace.
     *
     * @param namespace
     *            Namespace name
     * @param role
     *            Client role to which remove permissions
     */
    CompletableFuture<Void> revokePermissionsOnNamespaceAsync(String namespace, String role);

    /**
     * Get permission to role to access subscription's admin-api.
     * @param namespace
     * @throws PulsarAdminException
     */
    Map<String, Set<String>> getPermissionOnSubscription(String namespace) throws PulsarAdminException;

    /**
     * Get permission to role to access subscription's admin-api asynchronously.
     * @param namespace
     */
    CompletableFuture<Map<String, Set<String>>> getPermissionOnSubscriptionAsync(String namespace);

    /**
     * Grant permission to role to access subscription's admin-api.
     * @param namespace
     * @param subscription
     * @param roles
     * @throws PulsarAdminException
     */
    void grantPermissionOnSubscription(String namespace, String subscription, Set<String> roles)
            throws PulsarAdminException;

    /**
     * Grant permission to role to access subscription's admin-api asynchronously.
     * @param namespace
     * @param subscription
     * @param roles
     */
    CompletableFuture<Void> grantPermissionOnSubscriptionAsync(
            String namespace, String subscription, Set<String> roles);

    /**
     * Revoke permissions on a subscription's admin-api access.
     * @param namespace
     * @param subscription
     * @param role
     * @throws PulsarAdminException
     */
    void revokePermissionOnSubscription(String namespace, String subscription, String role) throws PulsarAdminException;

    /**
     * Revoke permissions on a subscription's admin-api access asynchronously.
     * @param namespace
     * @param subscription
     * @param role
     */
    CompletableFuture<Void> revokePermissionOnSubscriptionAsync(String namespace, String subscription, String role);

    /**
     * Get the replication clusters for a namespace.
     * <p/>
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
     * Get the replication clusters for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>["use", "usw", "usc"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<List<String>> getNamespaceReplicationClustersAsync(String namespace);

    /**
     * Set the replication clusters for a namespace.
     * <p/>
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
     * Set the replication clusters for a namespace asynchronously.
     * <p/>
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
     */
    CompletableFuture<Void> setNamespaceReplicationClustersAsync(String namespace, Set<String> clusterIds);

    /**
     * Get the message TTL for a namespace.
     * <p/>
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
    Integer getNamespaceMessageTTL(String namespace) throws PulsarAdminException;

    /**
     * Get the message TTL for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>60</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Integer> getNamespaceMessageTTLAsync(String namespace);

    /**
     * Set the messages Time to Live for all the topics within a namespace.
     * <p/>
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
     * Set the messages Time to Live for all the topics within a namespace asynchronously.
     * <p/>
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
     */
    CompletableFuture<Void> setNamespaceMessageTTLAsync(String namespace, int ttlInSeconds);

    /**
     * Remove the messages Time to Live for all the topics within a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeNamespaceMessageTTL(String namespace) throws PulsarAdminException;

    /**
     * Remove the messages Time to Live for all the topics within a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeNamespaceMessageTTLAsync(String namespace);

    /**
     * Get the subscription expiration time for a namespace.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>1440</code>
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
    Integer getSubscriptionExpirationTime(String namespace) throws PulsarAdminException;

    /**
     * Get the subscription expiration time for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>1440</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Integer> getSubscriptionExpirationTimeAsync(String namespace);

    /**
     * Set the subscription expiration time in minutes for all the topics within a namespace.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>1440</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param expirationTime
     *            Expiration time values for all subscriptions for all topics in this namespace
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSubscriptionExpirationTime(String namespace, int expirationTime) throws PulsarAdminException;

    /**
     * Set the subscription expiration time in minutes for all the topics within a namespace asynchronously.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>1440</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param expirationTime
     *            Expiration time values for all subscriptions for all topics in this namespace
     */
    CompletableFuture<Void> setSubscriptionExpirationTimeAsync(String namespace, int expirationTime);

    /**
     * Remove the subscription expiration time for a namespace.
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
    void removeSubscriptionExpirationTime(String namespace) throws PulsarAdminException;

    /**
     * Remove the subscription expiration time for a namespace asynchronously.
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Void> removeSubscriptionExpirationTimeAsync(String namespace);

    /**
     * Set anti-affinity group name for a namespace.
     * <p/>
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
     * Set anti-affinity group name for a namespace asynchronously.
     * <p/>
     * Request example:
     *
     * @param namespace
     *            Namespace name
     * @param namespaceAntiAffinityGroup
     *            anti-affinity group name for a namespace
     */
    CompletableFuture<Void> setNamespaceAntiAffinityGroupAsync(String namespace, String namespaceAntiAffinityGroup);

    /**
     * Get all namespaces that grouped with given anti-affinity group.
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
     * Get all namespaces that grouped with given anti-affinity group asynchronously.
     *
     * @param tenant
     *            tenant is only used for authorization. Client has to be admin of any of the tenant to access this
     *            api api.
     * @param cluster
     *            cluster name
     * @param namespaceAntiAffinityGroup
     *            Anti-affinity group name
     * @return list of namespace grouped under a given anti-affinity group
     */
    CompletableFuture<List<String>> getAntiAffinityNamespacesAsync(
            String tenant, String cluster, String namespaceAntiAffinityGroup);

    /**
     * Get anti-affinity group name for a namespace.
     * <p/>
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
     * Get anti-affinity group name for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>60</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<String> getNamespaceAntiAffinityGroupAsync(String namespace);

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
     * Delete anti-affinity group name for a namespace.
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Void> deleteNamespaceAntiAffinityGroupAsync(String namespace);

    /**
     * Remove the deduplication status for all topics within a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeDeduplicationStatus(String namespace) throws PulsarAdminException;

    /**
     * Get the deduplication status for all topics within a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeDeduplicationStatusAsync(String namespace);
    /**
     * Get the deduplication status for all topics within a namespace .
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    Boolean getDeduplicationStatus(String namespace) throws PulsarAdminException;

    /**
     * Get the deduplication status for all topics within a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Boolean> getDeduplicationStatusAsync(String namespace);
    /**
     * Set the deduplication status for all topics within a namespace.
     * <p/>
     * When deduplication is enabled, the broker will prevent to store the same message multiple times.
     * <p/>
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
     * Set the deduplication status for all topics within a namespace asynchronously.
     * <p/>
     * When deduplication is enabled, the broker will prevent to store the same message multiple times.
     * <p/>
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
     */
    CompletableFuture<Void> setDeduplicationStatusAsync(String namespace, boolean enableDeduplication);

    /**
     * Sets the autoTopicCreation policy for a given namespace, overriding broker settings.
     * <p/>
     * When autoTopicCreationOverride is enabled, new topics will be created upon connection,
     * regardless of the broker level configuration.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>
     *  {
     *      "allowAutoTopicCreation" : true,
     *      "topicType" : "partitioned",
     *      "defaultNumPartitions": 2
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param autoTopicCreationOverride
     *            Override policies for auto topic creation
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setAutoTopicCreation(String namespace, AutoTopicCreationOverride autoTopicCreationOverride)
            throws PulsarAdminException;

    /**
     * Sets the autoTopicCreation policy for a given namespace, overriding broker settings asynchronously.
     * <p/>
     * When autoTopicCreationOverride is enabled, new topics will be created upon connection,
     * regardless of the broker level configuration.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>
     *  {
     *      "allowAutoTopicCreation" : true,
     *      "topicType" : "partitioned",
     *      "defaultNumPartitions": 2
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param autoTopicCreationOverride
     *            Override policies for auto topic creation
     */
    CompletableFuture<Void> setAutoTopicCreationAsync(
            String namespace, AutoTopicCreationOverride autoTopicCreationOverride);

    /**
     * Get the autoTopicCreation info within a namespace.
     *
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    AutoTopicCreationOverride getAutoTopicCreation(String namespace) throws PulsarAdminException;

    /**
     * Get the autoTopicCreation info within a namespace asynchronously.
     *
     * @param namespace
     * @return
     */
    CompletableFuture<AutoTopicCreationOverride> getAutoTopicCreationAsync(String namespace);

    /**
     * Removes the autoTopicCreation policy for a given namespace.
     * <p/>
     * Allowing the broker to dictate the auto-creation policy.
     * <p/>
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
    void removeAutoTopicCreation(String namespace) throws PulsarAdminException;

    /**
     * Removes the autoTopicCreation policy for a given namespace asynchronously.
     * <p/>
     * Allowing the broker to dictate the auto-creation policy.
     * <p/>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Void> removeAutoTopicCreationAsync(String namespace);

    /**
     * Sets the autoSubscriptionCreation policy for a given namespace, overriding broker settings.
     * <p/>
     * When autoSubscriptionCreationOverride is enabled, new subscriptions will be created upon connection,
     * regardless of the broker level configuration.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>
     *  {
     *      "allowAutoSubscriptionCreation" : true
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param autoSubscriptionCreationOverride
     *            Override policies for auto subscription creation
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setAutoSubscriptionCreation(
            String namespace, AutoSubscriptionCreationOverride autoSubscriptionCreationOverride)
            throws PulsarAdminException;

    /**
     * Sets the autoSubscriptionCreation policy for a given namespace, overriding broker settings asynchronously.
     * <p/>
     * When autoSubscriptionCreationOverride is enabled, new subscriptions will be created upon connection,
     * regardless of the broker level configuration.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>
     *  {
     *      "allowAutoSubscriptionCreation" : true
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param autoSubscriptionCreationOverride
     *            Override policies for auto subscription creation
     */
    CompletableFuture<Void> setAutoSubscriptionCreationAsync(
            String namespace, AutoSubscriptionCreationOverride autoSubscriptionCreationOverride);

    /**
     * Get the autoSubscriptionCreation info within a namespace.
     *
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    AutoSubscriptionCreationOverride getAutoSubscriptionCreation(String namespace) throws PulsarAdminException;

    /**
     * Get the autoSubscriptionCreation info within a namespace asynchronously.
     *
     * @param namespace
     * @return
     */
    CompletableFuture<AutoSubscriptionCreationOverride> getAutoSubscriptionCreationAsync(String namespace);

    /**
     * Sets the subscriptionTypesEnabled policy for a given namespace, overriding broker settings.
     *
     * Request example:
     *
     * <pre>
     * <code>
     *  {
     *      "subscriptionTypesEnabled" : {"Shared", "Failover"}
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param subscriptionTypesEnabled
     *            is enable subscription types
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSubscriptionTypesEnabled(String namespace,
                                     Set<SubscriptionType> subscriptionTypesEnabled) throws PulsarAdminException;

    /**
     * Sets the subscriptionTypesEnabled policy for a given namespace, overriding broker settings.
     *
     * Request example:
     *
     * <pre>
     * <code>
     *  {
     *      "subscriptionTypesEnabled" : {"Shared", "Failover"}
     *  }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param subscriptionTypesEnabled
     *            is enable subscription types
     */
    CompletableFuture<Void> setSubscriptionTypesEnabledAsync(String namespace,
                                                        Set<SubscriptionType> subscriptionTypesEnabled);

    /**
     * Get the subscriptionTypesEnabled policy for a given namespace, overriding broker settings.
     *
     * @param namespace
     *            Namespace name
     * @return subscription types {@link Set<SubscriptionType>} the subscription types
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Set<SubscriptionType> getSubscriptionTypesEnabled(String namespace) throws PulsarAdminException;

    /**
     * Get the subscriptionTypesEnabled policy for a given namespace, overriding broker settings.
     *
     * @param namespace
     *            Namespace name
     * @return the future of subscription types {@link Set<SubscriptionType>} the subscription types
     */
    CompletableFuture<Set<SubscriptionType>> getSubscriptionTypesEnabledAsync(String namespace);

    /**
     * Removes the subscriptionTypesEnabled policy for a given namespace.
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
     * @return
     */
    void removeSubscriptionTypesEnabled(String namespace) throws PulsarAdminException;

    /**
     * Removes the subscriptionTypesEnabled policy for a given namespace.
     *
     * @param namespace
     *            Namespace name
     * @return
     */
    CompletableFuture<Void> removeSubscriptionTypesEnabledAsync(String namespace);

    /**
     * Removes the autoSubscriptionCreation policy for a given namespace.
     * <p/>
     * Allowing the broker to dictate the subscription auto-creation policy.
     * <p/>
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
    void removeAutoSubscriptionCreation(String namespace) throws PulsarAdminException;

    /**
     * Removes the autoSubscriptionCreation policy for a given namespace asynchronously.
     * <p/>
     * Allowing the broker to dictate the subscription auto-creation policy.
     * <p/>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Void> removeAutoSubscriptionCreationAsync(String namespace);

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
     * <p/>
     * Get backlog quota map on a namespace.
     * <p/>
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
     * Get backlog quota map on a namespace asynchronously.
     * <p/>
     * Get backlog quota map on a namespace.
     * <p/>
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
     */
    CompletableFuture<Map<BacklogQuota.BacklogQuotaType, BacklogQuota>> getBacklogQuotaMapAsync(String namespace);

    /**
     * Set a backlog quota for all the topics on a namespace.
     * <p/>
     * Set a backlog quota on a namespace.
     * <p/>
     * The backlog quota can be set on this resource:
     * <p/>
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
    void setBacklogQuota(String namespace, BacklogQuota backlogQuota, BacklogQuota.BacklogQuotaType backlogQuotaType)
            throws PulsarAdminException;

    default void setBacklogQuota(String namespace, BacklogQuota backlogQuota) throws PulsarAdminException {
        setBacklogQuota(namespace, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
    }

    /**
     * Set a backlog quota for all the topics on a namespace asynchronously.
     * <p/>
     * Set a backlog quota on a namespace.
     * <p/>
     * The backlog quota can be set on this resource:
     * <p/>
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
     */
    CompletableFuture<Void> setBacklogQuotaAsync(String namespace, BacklogQuota backlogQuota,
                                                 BacklogQuota.BacklogQuotaType backlogQuotaType);

    default CompletableFuture<Void> setBacklogQuotaAsync(String namespace, BacklogQuota backlogQuota) {
        return setBacklogQuotaAsync(namespace, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
    }

    /**
     * Remove a backlog quota policy from a namespace.
     * <p/>
     * Remove a backlog quota policy from a namespace.
     * <p/>
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
    void removeBacklogQuota(String namespace, BacklogQuota.BacklogQuotaType backlogQuotaType)
            throws PulsarAdminException;

    default void removeBacklogQuota(String namespace) throws PulsarAdminException {
        removeBacklogQuota(namespace, BacklogQuota.BacklogQuotaType.destination_storage);
    }

    /**
     * Remove a backlog quota policy from a namespace asynchronously.
     * <p/>
     * Remove a backlog quota policy from a namespace.
     * <p/>
     * The backlog retention policy will fall back to the default.
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Void> removeBacklogQuotaAsync(String namespace, BacklogQuota.BacklogQuotaType backlogQuotaType);

    default CompletableFuture<Void> removeBacklogQuotaAsync(String namespace) {
        return removeBacklogQuotaAsync(namespace, BacklogQuota.BacklogQuotaType.destination_storage);
    }


    /**
     * Remove the persistence configuration on a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removePersistence(String namespace) throws PulsarAdminException;

    /**
     * Remove the persistence configuration on a namespace asynchronously.
     * @param namespace
     */
    CompletableFuture<Void> removePersistenceAsync(String namespace);

    /**
     * Set the persistence configuration for all the topics on a namespace.
     * <p/>
     * Set the persistence configuration on a namespace.
     * <p/>
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
     * Set the persistence configuration for all the topics on a namespace asynchronously.
     * <p/>
     * Set the persistence configuration on a namespace.
     * <p/>
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
     * @param namespace
     *            Namespace name
     * @param persistence
     *            Persistence policies object
     */
    CompletableFuture<Void> setPersistenceAsync(String namespace, PersistencePolicies persistence);

    /**
     * Get the persistence configuration for a namespace.
     * <p/>
     * Get the persistence configuration for a namespace.
     * <p/>
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
     * Get the persistence configuration for a namespace asynchronously.
     * <p/>
     * Get the persistence configuration for a namespace.
     * <p/>
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
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<PersistencePolicies> getPersistenceAsync(String namespace);

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
     * Set bookie affinity group for a namespace to isolate namespace write to bookies that are part of given affinity
     * group asynchronously.
     *
     * @param namespace
     *            namespace name
     * @param bookieAffinityGroup
     *            bookie affinity group
     */
    CompletableFuture<Void> setBookieAffinityGroupAsync(String namespace, BookieAffinityGroupData bookieAffinityGroup);

    /**
     * Delete bookie affinity group configured for a namespace.
     *
     * @param namespace
     * @throws PulsarAdminException
     */
    void deleteBookieAffinityGroup(String namespace) throws PulsarAdminException;

    /**
     * Delete bookie affinity group configured for a namespace asynchronously.
     *
     * @param namespace
     */
    CompletableFuture<Void> deleteBookieAffinityGroupAsync(String namespace);

    /**
     * Get bookie affinity group configured for a namespace.
     *
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    BookieAffinityGroupData getBookieAffinityGroup(String namespace) throws PulsarAdminException;

    /**
     * Get bookie affinity group configured for a namespace asynchronously.
     *
     * @param namespace
     * @return
     */
    CompletableFuture<BookieAffinityGroupData> getBookieAffinityGroupAsync(String namespace);

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
     * Set the retention configuration for all the topics on a namespace asynchronously.
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
     */
    CompletableFuture<Void> setRetentionAsync(String namespace, RetentionPolicies retention);

    /**
     * Remove the retention configuration for all the topics on a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeRetention(String namespace) throws PulsarAdminException;

    /**
     * Remove the retention configuration for all the topics on a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeRetentionAsync(String namespace);

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
     * Get the retention configuration for a namespace asynchronously.
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
     */
    CompletableFuture<RetentionPolicies> getRetentionAsync(String namespace);

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
     * Unload a namespace from the current serving broker asynchronously.
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Void> unloadAsync(String namespace);

    /**
     * Get the replication configuration version for a given namespace.
     *
     * @param namespace
     * @return Replication configuration version
     * @throws PulsarAdminException
     *             Unexpected error
     */
    String getReplicationConfigVersion(String namespace) throws PulsarAdminException;

    /**
     * Get the replication configuration version for a given namespace asynchronously.
     *
     * @param namespace
     * @return Replication configuration version
     */
    CompletableFuture<String> getReplicationConfigVersionAsync(String namespace);

    /**
     * Unload namespace bundle.
     *
     * @param namespace
     * @param bundle
     *           range of bundle to unload
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void unloadNamespaceBundle(String namespace, String bundle) throws PulsarAdminException;

    /**
     * Unload namespace bundle asynchronously.
     *
     * @param namespace
     * @param bundle
     *           range of bundle to unload
     *
     * @return a future that can be used to track when the bundle is unloaded
     */
    CompletableFuture<Void> unloadNamespaceBundleAsync(String namespace, String bundle);

    /**
     * Split namespace bundle.
     *
     * @param namespace
     * @param bundle range of bundle to split
     * @param unloadSplitBundles
     * @param splitAlgorithmName
     * @throws PulsarAdminException
     */
    void splitNamespaceBundle(String namespace, String bundle, boolean unloadSplitBundles, String splitAlgorithmName)
            throws PulsarAdminException;

    /**
     * Split namespace bundle asynchronously.
     *
     * @param namespace
     * @param bundle range of bundle to split
     * @param unloadSplitBundles
     * @param splitAlgorithmName
     */
    CompletableFuture<Void> splitNamespaceBundleAsync(
            String namespace, String bundle, boolean unloadSplitBundles, String splitAlgorithmName);

    /**
     * Set message-publish-rate (topics under this namespace can publish this many messages per second).
     *
     * @param namespace
     * @param publishMsgRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setPublishRate(String namespace, PublishRate publishMsgRate) throws PulsarAdminException;

    /**
     * Remove message-publish-rate (topics under this namespace can publish this many messages per second).
     *
     * @param namespace
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void removePublishRate(String namespace) throws PulsarAdminException;

    /**
     * Set message-publish-rate (topics under this namespace can publish this many messages per second) asynchronously.
     *
     * @param namespace
     * @param publishMsgRate
     *            number of messages per second
     */
    CompletableFuture<Void> setPublishRateAsync(String namespace, PublishRate publishMsgRate);

    /**
     * Remove message-publish-rate asynchronously.
     * <p/>
     * topics under this namespace can publish this many messages per second
     * @param namespace
     */
    CompletableFuture<Void> removePublishRateAsync(String namespace);

    /**
     * Get message-publish-rate (topics under this namespace can publish this many messages per second).
     *
     * @param namespace
     * @return number of messages per second
     * @throws PulsarAdminException Unexpected error
     */
    PublishRate getPublishRate(String namespace) throws PulsarAdminException;

    /**
     * Get message-publish-rate (topics under this namespace can publish this many messages per second) asynchronously.
     *
     * @param namespace
     * @return number of messages per second
     */
    CompletableFuture<PublishRate> getPublishRateAsync(String namespace);

    /**
     * Remove message-dispatch-rate.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeDispatchRate(String namespace) throws PulsarAdminException;

    /**
     * Remove message-dispatch-rate asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeDispatchRateAsync(String namespace);
    /**
     * Set message-dispatch-rate (topics under this namespace can dispatch this many messages per second).
     *
     * @param namespace
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException;

    /**
     * Set message-dispatch-rate asynchronously.
     * <p/>
     * topics under this namespace can dispatch this many messages per second
     *
     * @param namespace
     * @param dispatchRate
     *            number of messages per second
     */
    CompletableFuture<Void> setDispatchRateAsync(String namespace, DispatchRate dispatchRate);

    /**
     * Get message-dispatch-rate (topics under this namespace can dispatch this many messages per second).
     *
     * @param namespace
     * @returns messageRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getDispatchRate(String namespace) throws PulsarAdminException;

    /**
     * Get message-dispatch-rate asynchronously.
     * <p/>
     * Topics under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @returns messageRate
     *            number of messages per second
     */
    CompletableFuture<DispatchRate> getDispatchRateAsync(String namespace);

    /**
     * Set namespace-subscribe-rate (topics under this namespace will limit by subscribeRate).
     *
     * @param namespace
     * @param subscribeRate
     *            consumer subscribe limit by this subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSubscribeRate(String namespace, SubscribeRate subscribeRate) throws PulsarAdminException;

    /**
     * Set namespace-subscribe-rate (topics under this namespace will limit by subscribeRate) asynchronously.
     *
     * @param namespace
     * @param subscribeRate
     *            consumer subscribe limit by this subscribeRate
     */
    CompletableFuture<Void> setSubscribeRateAsync(String namespace, SubscribeRate subscribeRate);

    /**
     * Remove namespace-subscribe-rate (topics under this namespace will limit by subscribeRate).
     *
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeSubscribeRate(String namespace) throws PulsarAdminException;

    /**
     * Remove namespace-subscribe-rate (topics under this namespace will limit by subscribeRate) asynchronously.
     *
     * @param namespace
     */
    CompletableFuture<Void> removeSubscribeRateAsync(String namespace);

    /**
     * Get namespace-subscribe-rate (topics under this namespace allow subscribe times per consumer in a period).
     *
     * @param namespace
     * @returns subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    SubscribeRate getSubscribeRate(String namespace) throws PulsarAdminException;

    /**
     * Get namespace-subscribe-rate asynchronously.
     * <p/>
     * Topics under this namespace allow subscribe times per consumer in a period.
     *
     * @param namespace
     * @returns subscribeRate
     */
    CompletableFuture<SubscribeRate> getSubscribeRateAsync(String namespace);

    /**
     * Remove subscription-message-dispatch-rate.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeSubscriptionDispatchRate(String namespace) throws PulsarAdminException;

    /**
     * Remove subscription-message-dispatch-rate asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeSubscriptionDispatchRateAsync(String namespace);

    /**
     * Set subscription-message-dispatch-rate.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second
     *
     * @param namespace
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSubscriptionDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException;

    /**
     * Set subscription-message-dispatch-rate asynchronously.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @param dispatchRate
     *            number of messages per second
     */
    CompletableFuture<Void> setSubscriptionDispatchRateAsync(String namespace, DispatchRate dispatchRate);

    /**
     * Get subscription-message-dispatch-rate.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getSubscriptionDispatchRate(String namespace) throws PulsarAdminException;

    /**
     * Get subscription-message-dispatch-rate asynchronously.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     */
    CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String namespace);

    /**
     * Set replicator-message-dispatch-rate.
     * <p/>
     * Replicators under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setReplicatorDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException;

    /**
     * Set replicator-message-dispatch-rate asynchronously.
     * <p/>
     * Replicators under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @param dispatchRate
     *            number of messages per second
     */
    CompletableFuture<Void> setReplicatorDispatchRateAsync(String namespace, DispatchRate dispatchRate);

    /**
     * Remove replicator-message-dispatch-rate.
     *
     * @param namespace
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void removeReplicatorDispatchRate(String namespace) throws PulsarAdminException;

    /**
     * Set replicator-message-dispatch-rate asynchronously.
     *
     * @param namespace
     */
    CompletableFuture<Void> removeReplicatorDispatchRateAsync(String namespace);

    /**
     * Get replicator-message-dispatch-rate.
     * <p/>
     * Replicators under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DispatchRate getReplicatorDispatchRate(String namespace) throws PulsarAdminException;

    /**
     * Get replicator-message-dispatch-rate asynchronously.
     * <p/>
     * Replicators under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     */
    CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String namespace);

    /**
     * Clear backlog for all topics on a namespace.
     *
     * @param namespace
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBacklog(String namespace) throws PulsarAdminException;

    /**
     * Clear backlog for all topics on a namespace asynchronously.
     *
     * @param namespace
     */
    CompletableFuture<Void> clearNamespaceBacklogAsync(String namespace);

    /**
     * Clear backlog for a given subscription on all topics on a namespace.
     *
     * @param namespace
     * @param subscription
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBacklogForSubscription(String namespace, String subscription) throws PulsarAdminException;

    /**
     * Clear backlog for a given subscription on all topics on a namespace asynchronously.
     *
     * @param namespace
     * @param subscription
     */
    CompletableFuture<Void> clearNamespaceBacklogForSubscriptionAsync(String namespace, String subscription);

    /**
     * Clear backlog for all topics on a namespace bundle.
     *
     * @param namespace
     * @param bundle
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void clearNamespaceBundleBacklog(String namespace, String bundle) throws PulsarAdminException;

    /**
     * Clear backlog for all topics on a namespace bundle asynchronously.
     *
     * @param namespace
     * @param bundle
     *
     * @return a future that can be used to track when the bundle is cleared
     */
    CompletableFuture<Void> clearNamespaceBundleBacklogAsync(String namespace, String bundle);

    /**
     * Clear backlog for a given subscription on all topics on a namespace bundle.
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
     * Clear backlog for a given subscription on all topics on a namespace bundle asynchronously.
     *
     * @param namespace
     * @param bundle
     * @param subscription
     *
     * @return a future that can be used to track when the bundle is cleared
     */
    CompletableFuture<Void> clearNamespaceBundleBacklogForSubscriptionAsync(String namespace, String bundle,
            String subscription);

    /**
     * Unsubscribe the given subscription on all topics on a namespace.
     *
     * @param namespace
     * @param subscription
     * @throws PulsarAdminException
     */
    void unsubscribeNamespace(String namespace, String subscription) throws PulsarAdminException;

    /**
     * Unsubscribe the given subscription on all topics on a namespace asynchronously.
     *
     * @param namespace
     * @param subscription
     */
    CompletableFuture<Void> unsubscribeNamespaceAsync(String namespace, String subscription);

    /**
     * Unsubscribe the given subscription on all topics on a namespace bundle.
     *
     * @param namespace
     * @param bundle
     * @param subscription
     * @throws PulsarAdminException
     */
    void unsubscribeNamespaceBundle(String namespace, String bundle, String subscription) throws PulsarAdminException;

    /**
     * Unsubscribe the given subscription on all topics on a namespace bundle asynchronously.
     *
     * @param namespace
     * @param bundle
     * @param subscription
     *
     * @return a future that can be used to track when the subscription is unsubscribed
     */
    CompletableFuture<Void> unsubscribeNamespaceBundleAsync(String namespace, String bundle, String subscription);

    /**
     * Set the encryption required status for all topics within a namespace.
     * <p/>
     * When encryption required is true, the broker will prevent to store unencrypted messages.
     * <p/>
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
     * Get the encryption required status within a namespace.
     *
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    Boolean getEncryptionRequiredStatus(String namespace) throws PulsarAdminException;

    /**
     * Get the encryption required status within a namespace asynchronously.
     *
     * @param namespace
     * @return
     */
    CompletableFuture<Boolean> getEncryptionRequiredStatusAsync(String namespace);

    /**
     * Set the encryption required status for all topics within a namespace asynchronously.
     * <p/>
     * When encryption required is true, the broker will prevent to store unencrypted messages.
     * <p/>
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
     */
    CompletableFuture<Void> setEncryptionRequiredStatusAsync(String namespace, boolean encryptionRequired);

    /**
     * Get the delayed delivery messages for all topics within a namespace.
     * <p/>
     * If disabled, messages will be immediately delivered and there will
     * be no tracking overhead.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>
     * {
     *     "active" : true,   // Enable or disable delayed delivery for messages on a namespace
     *     "tickTime" : 1000, // The tick time for when retrying on delayed delivery messages
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @return delayedDeliveryPolicies
     *            Whether to enable the delayed delivery for messages.
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    DelayedDeliveryPolicies getDelayedDelivery(String namespace) throws PulsarAdminException;

    /**
     * Get the delayed delivery messages for all topics within a namespace asynchronously.
     * <p/>
     * If disabled, messages will be immediately delivered and there will
     * be no tracking overhead.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>
     * {
     *     "active" : true,   // Enable or disable delayed delivery for messages on a namespace
     *     "tickTime" : 1000, // The tick time for when retrying on delayed delivery messages
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @return delayedDeliveryPolicies
     *            Whether to enable the delayed delivery for messages.
     */
    CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryAsync(String namespace);

    /**
     * Set the delayed delivery messages for all topics within a namespace.
     * <p/>
     * If disabled, messages will be immediately delivered and there will
     * be no tracking overhead.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>
     * {
     *     "tickTime" : 1000, // Enable or disable delayed delivery for messages on a namespace
     *     "active" : true,   // The tick time for when retrying on delayed delivery messages
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param delayedDeliveryPolicies
     *            Whether to enable the delayed delivery for messages.
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setDelayedDeliveryMessages(String namespace, DelayedDeliveryPolicies delayedDeliveryPolicies)
            throws PulsarAdminException;

    /**
     * Set the delayed delivery messages for all topics within a namespace asynchronously.
     * <p/>
     * If disabled, messages will be immediately delivered and there will
     * be no tracking overhead.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>
     * {
     *     "tickTime" : 1000, // Enable or disable delayed delivery for messages on a namespace
     *     "active" : true,   // The tick time for when retrying on delayed delivery messages
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param delayedDeliveryPolicies
     *            Whether to enable the delayed delivery for messages.
     */
    CompletableFuture<Void> setDelayedDeliveryMessagesAsync(
            String namespace, DelayedDeliveryPolicies delayedDeliveryPolicies);

    /**
     * Remove the delayed delivery messages for all topics within a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeDelayedDeliveryMessages(String namespace) throws PulsarAdminException;
    /**
     * Remove the delayed delivery messages for all topics within a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeDelayedDeliveryMessagesAsync(String namespace);

    /**
     * Get the inactive deletion strategy for all topics within a namespace synchronously.
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    InactiveTopicPolicies getInactiveTopicPolicies(String namespace) throws PulsarAdminException;

    /**
     * remove InactiveTopicPolicies from a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String namespace);

    /**
     * Remove inactive topic policies from a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeInactiveTopicPolicies(String namespace) throws PulsarAdminException;

    /**
     * Get the inactive deletion strategy for all topics within a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String namespace);

    /**
     * As same as setInactiveTopicPoliciesAsync，but it is synchronous.
     * @param namespace
     * @param inactiveTopicPolicies
     */
    void setInactiveTopicPolicies(
            String namespace, InactiveTopicPolicies inactiveTopicPolicies) throws PulsarAdminException;

    /**
     * You can set the inactive deletion strategy at the namespace level.
     * Its priority is higher than the inactive deletion strategy at the broker level.
     * All topics under this namespace will follow this strategy.
     * @param namespace
     * @param inactiveTopicPolicies
     * @return
     */
    CompletableFuture<Void> setInactiveTopicPoliciesAsync(
            String namespace, InactiveTopicPolicies inactiveTopicPolicies);
    /**
     * Set the given subscription auth mode on all topics on a namespace.
     *
     * @param namespace
     * @param subscriptionAuthMode
     * @throws PulsarAdminException
     */
    void setSubscriptionAuthMode(String namespace, SubscriptionAuthMode subscriptionAuthMode)
            throws PulsarAdminException;

    /**
     * Set the given subscription auth mode on all topics on a namespace asynchronously.
     *
     * @param namespace
     * @param subscriptionAuthMode
     */
    CompletableFuture<Void> setSubscriptionAuthModeAsync(String namespace, SubscriptionAuthMode subscriptionAuthMode);

    /**
     * Get the subscriptionAuthMode within a namespace.
     *
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    SubscriptionAuthMode getSubscriptionAuthMode(String namespace) throws PulsarAdminException;

    /**
     * Get the subscriptionAuthMode within a namespace asynchronously.
     *
     * @param namespace
     * @return
     */
    CompletableFuture<SubscriptionAuthMode> getSubscriptionAuthModeAsync(String namespace);

    /**
     * Get the deduplicationSnapshotInterval for a namespace.
     *
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    Integer getDeduplicationSnapshotInterval(String namespace) throws PulsarAdminException;

    /**
     * Get the deduplicationSnapshotInterval for a namespace asynchronously.
     *
     * @param namespace
     * @return
     */
    CompletableFuture<Integer> getDeduplicationSnapshotIntervalAsync(String namespace);

    /**
     * Set the deduplicationSnapshotInterval for a namespace.
     *
     * @param namespace
     * @param interval
     * @throws PulsarAdminException
     */
    void setDeduplicationSnapshotInterval(String namespace, Integer interval) throws PulsarAdminException;

    /**
     * Set the deduplicationSnapshotInterval for a namespace asynchronously.
     *
     * @param namespace
     * @param interval
     * @return
     */
    CompletableFuture<Void> setDeduplicationSnapshotIntervalAsync(String namespace, Integer interval);

    /**
     * Remove the deduplicationSnapshotInterval for a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeDeduplicationSnapshotInterval(String namespace) throws PulsarAdminException;

    /**
     * Remove the deduplicationSnapshotInterval for a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeDeduplicationSnapshotIntervalAsync(String namespace);

    /**
     * Get the maxSubscriptionsPerTopic for a namespace.
     *
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    Integer getMaxSubscriptionsPerTopic(String namespace) throws PulsarAdminException;

    /**
     * Get the maxSubscriptionsPerTopic for a namespace asynchronously.
     *
     * @param namespace
     * @return
     */
    CompletableFuture<Integer> getMaxSubscriptionsPerTopicAsync(String namespace);

    /**
     * Set the maxSubscriptionsPerTopic for a namespace.
     *
     * @param namespace
     * @param maxSubscriptionsPerTopic
     * @throws PulsarAdminException
     */
    void setMaxSubscriptionsPerTopic(String namespace, int maxSubscriptionsPerTopic) throws PulsarAdminException;

    /**
     * Set the maxSubscriptionsPerTopic for a namespace asynchronously.
     *
     * @param namespace
     * @param maxSubscriptionsPerTopic
     * @return
     */
    CompletableFuture<Void> setMaxSubscriptionsPerTopicAsync(String namespace, int maxSubscriptionsPerTopic);

    /**
     * Remove the maxSubscriptionsPerTopic for a namespace.
     *
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeMaxSubscriptionsPerTopic(String namespace) throws PulsarAdminException;

    /**
     * Remove the maxSubscriptionsPerTopic for a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeMaxSubscriptionsPerTopicAsync(String namespace);

    /**
     * Get the maxProducersPerTopic for a namespace.
     * <p/>
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
    Integer getMaxProducersPerTopic(String namespace) throws PulsarAdminException;

    /**
     * Get the maxProducersPerTopic for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Integer> getMaxProducersPerTopicAsync(String namespace);

    /**
     * Set maxProducersPerTopic for a namespace.
     * <p/>
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
     * Set maxProducersPerTopic for a namespace asynchronously.
     * <p/>
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
     */
    CompletableFuture<Void> setMaxProducersPerTopicAsync(String namespace, int maxProducersPerTopic);

    /**
     * Remove maxProducersPerTopic for a namespace.
     * @param namespace Namespace name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void removeMaxProducersPerTopic(String namespace) throws PulsarAdminException;

    /**
     * Set maxProducersPerTopic for a namespace asynchronously.
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Void> removeMaxProducersPerTopicAsync(String namespace);

    /**
     * Get the maxProducersPerTopic for a namespace.
     * <p/>
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
    Integer getMaxConsumersPerTopic(String namespace) throws PulsarAdminException;

    /**
     * Get the maxProducersPerTopic for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Integer> getMaxConsumersPerTopicAsync(String namespace);

    /**
     * Set maxConsumersPerTopic for a namespace.
     * <p/>
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
     * Set maxConsumersPerTopic for a namespace asynchronously.
     * <p/>
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
     */
    CompletableFuture<Void> setMaxConsumersPerTopicAsync(String namespace, int maxConsumersPerTopic);

    /**
     * Remove maxConsumersPerTopic for a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeMaxConsumersPerTopic(String namespace) throws PulsarAdminException;

    /**
     * Remove maxConsumersPerTopic for a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeMaxConsumersPerTopicAsync(String namespace);

    /**
     * Get the maxConsumersPerSubscription for a namespace.
     * <p/>
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
    Integer getMaxConsumersPerSubscription(String namespace) throws PulsarAdminException;

    /**
     * Get the maxConsumersPerSubscription for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Integer> getMaxConsumersPerSubscriptionAsync(String namespace);

    /**
     * Set maxConsumersPerSubscription for a namespace.
     * <p/>
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
     * Set maxConsumersPerSubscription for a namespace asynchronously.
     * <p/>
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
     */
    CompletableFuture<Void> setMaxConsumersPerSubscriptionAsync(String namespace, int maxConsumersPerSubscription);

    /**
     * Remove maxConsumersPerSubscription for a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeMaxConsumersPerSubscription(String namespace) throws PulsarAdminException;

    /**
     * Remove maxConsumersPerSubscription for a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeMaxConsumersPerSubscriptionAsync(String namespace);

    /**
     * Get the maxUnackedMessagesPerConsumer for a namespace.
     * <p/>
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
    Integer getMaxUnackedMessagesPerConsumer(String namespace) throws PulsarAdminException;

    /**
     * Get the maxUnackedMessagesPerConsumer for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Integer> getMaxUnackedMessagesPerConsumerAsync(String namespace);

    /**
     * Set maxUnackedMessagesPerConsumer for a namespace.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param maxUnackedMessagesPerConsumer
     *            maxUnackedMessagesPerConsumer value for a namespace
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setMaxUnackedMessagesPerConsumer(String namespace, int maxUnackedMessagesPerConsumer)
            throws PulsarAdminException;

    /**
     * Set maxUnackedMessagesPerConsumer for a namespace asynchronously.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param maxUnackedMessagesPerConsumer
     *            maxUnackedMessagesPerConsumer value for a namespace
     */
    CompletableFuture<Void> setMaxUnackedMessagesPerConsumerAsync(String namespace, int maxUnackedMessagesPerConsumer);

    /**
     * Remove maxUnackedMessagesPerConsumer for a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeMaxUnackedMessagesPerConsumer(String namespace)
            throws PulsarAdminException;

    /**
     * Remove maxUnackedMessagesPerConsumer for a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeMaxUnackedMessagesPerConsumerAsync(
            String namespace);
    /**
     * Get the maxUnackedMessagesPerSubscription for a namespace.
     * <p/>
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
    Integer getMaxUnackedMessagesPerSubscription(String namespace) throws PulsarAdminException;

    /**
     * Get the maxUnackedMessagesPerSubscription for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Integer> getMaxUnackedMessagesPerSubscriptionAsync(String namespace);

    /**
     * Set maxUnackedMessagesPerSubscription for a namespace.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param maxUnackedMessagesPerSubscription
     *            Max number of unacknowledged messages allowed per shared subscription.
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setMaxUnackedMessagesPerSubscription(String namespace, int maxUnackedMessagesPerSubscription)
            throws PulsarAdminException;

    /**
     * Set maxUnackedMessagesPerSubscription for a namespace asynchronously.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param maxUnackedMessagesPerSubscription
     *            Max number of unacknowledged messages allowed per shared subscription.
     */
    CompletableFuture<Void> setMaxUnackedMessagesPerSubscriptionAsync(
            String namespace, int maxUnackedMessagesPerSubscription);

    /**
     * Remove maxUnackedMessagesPerSubscription for a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeMaxUnackedMessagesPerSubscription(String namespace)
            throws PulsarAdminException;

    /**
     * Remove maxUnackedMessagesPerSubscription for a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeMaxUnackedMessagesPerSubscriptionAsync(
            String namespace);

    /**
     * Get the compactionThreshold for a namespace. The maximum number of bytes topics in the namespace
     * can have before compaction is triggered. 0 disables.
     * <p/>
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
    Long getCompactionThreshold(String namespace) throws PulsarAdminException;

    /**
     * Get the compactionThreshold for a namespace asynchronously. The maximum number of bytes topics in the namespace
     * can have before compaction is triggered. 0 disables.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Long> getCompactionThresholdAsync(String namespace);

    /**
     * Set the compactionThreshold for a namespace. The maximum number of bytes topics in the namespace
     * can have before compaction is triggered. 0 disables.
     * <p/>
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
     * Set the compactionThreshold for a namespace asynchronously. The maximum number of bytes topics in the namespace
     * can have before compaction is triggered. 0 disables.
     * <p/>
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
     */
    CompletableFuture<Void> setCompactionThresholdAsync(String namespace, long compactionThreshold);

    /**
     * Delete the compactionThreshold for a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeCompactionThreshold(String namespace) throws PulsarAdminException;

    /**
     * Delete the compactionThreshold for a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeCompactionThresholdAsync(String namespace);

    /**
     * Get the offloadThreshold for a namespace. The maximum number of bytes stored on the pulsar cluster for topics
     * in the namespace before data starts being offloaded to longterm storage.
     *
     * <p/>
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
     * Get the offloadThreshold for a namespace asynchronously.
     * <p/>
     * The maximum number of bytes stored on the pulsar cluster for topics
     * in the namespace before data starts being offloaded to longterm storage.
     *
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<Long> getOffloadThresholdAsync(String namespace);

    /**
     * Set the offloadThreshold for a namespace.
     * <p/>
     * The maximum number of bytes stored on the pulsar cluster for topics
     * in the namespace before data starts being offloaded to longterm storage.
     * <p/>
     * Negative values disabled automatic offloading. Setting a threshold of 0 will offload data as soon as possible.
     * <p/>
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
    void setOffloadThreshold(String namespace, long offloadThreshold) throws PulsarAdminException;

    /**
     * Set the offloadThreshold for a namespace asynchronously.
     * <p/>
     * The maximum number of bytes stored on the pulsar cluster for topics
     * in the namespace before data starts being offloaded to longterm storage.
     * <p/>
     * Negative values disabled automatic offloading. Setting a threshold of 0 will offload data as soon as possible.
     * <p/>
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
     */
    CompletableFuture<Void> setOffloadThresholdAsync(String namespace, long offloadThreshold);

    /**
     * Get the offload deletion lag for a namespace, in milliseconds.
     * The number of milliseconds to wait before deleting a ledger segment which has been offloaded from
     * the Pulsar cluster's local storage (i.e. BookKeeper).
     * <p/>
     * If the offload deletion lag has not been set for the namespace, the method returns 'null'
     * and the namespace will use the configured default of the pulsar broker.
     * <p/>
     * A negative value disables deletion of the local ledger completely, though it will still be deleted
     * if it exceeds the topics retention policy, along with the offloaded copy.
     *
     * <p/>
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
     * Get the offload deletion lag asynchronously for a namespace, in milliseconds.
     * <p/>
     * The number of milliseconds to wait before deleting a ledger segment which has been offloaded from
     * the Pulsar cluster's local storage (i.e. BookKeeper).
     * <p/>
     * If the offload deletion lag has not been set for the namespace, the method returns 'null'
     * and the namespace will use the configured default of the pulsar broker.
     * <p/>
     * A negative value disables deletion of the local ledger completely, though it will still be deleted
     * if it exceeds the topics retention policy, along with the offloaded copy.
     *
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>3600000</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @return the offload deletion lag for the namespace in milliseconds, or null if not set
     */
    CompletableFuture<Long> getOffloadDeleteLagMsAsync(String namespace);

    /**
     * Set the offload deletion lag for a namespace.
     * <p/>
     * The offload deletion lag is the amount of time to wait after offloading a ledger segment to long term storage,
     * before deleting its copy stored on the Pulsar cluster's local storage (i.e. BookKeeper).
     * <p/>
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
     * Set the offload deletion lag for a namespace asynchronously.
     * <p/>
     * The offload deletion lag is the amount of time to wait after offloading a ledger segment to long term storage,
     * before deleting its copy stored on the Pulsar cluster's local storage (i.e. BookKeeper).
     * <p/>
     * A negative value disables deletion of the local ledger completely, though it will still be deleted
     * if it exceeds the topics retention policy, along with the offloaded copy.
     *
     * @param namespace
     *            Namespace name
     * @param lag the duration to wait before deleting the local copy
     * @param unit the timeunit of the duration
     */
    CompletableFuture<Void> setOffloadDeleteLagAsync(String namespace, long lag, TimeUnit unit);

    /**
     * Clear the offload deletion lag for a namespace.
     * <p/>
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
     * Clear the offload deletion lag for a namespace asynchronously.
     * <p/>
     * The namespace will fall back to using the configured default of the pulsar broker.
     */
    CompletableFuture<Void> clearOffloadDeleteLagAsync(String namespace);

    /**
     * Get the strategy used to check the a new schema provided by a producer is compatible with the current schema
     * before it is installed.
     * <p/>
     * If this is
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
    @Deprecated
    SchemaAutoUpdateCompatibilityStrategy getSchemaAutoUpdateCompatibilityStrategy(String namespace)
            throws PulsarAdminException;

    /**
     * Set the strategy used to check the a new schema provided by a producer is compatible with the current schema
     * before it is installed.
     * <p/>
     * To disable all new schema updates through the producer, set this to
     * {@link org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy#AutoUpdateDisabled}.
     *
     * @param namespace The namespace in whose policy should be set
     * @param strategy
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setSchemaAutoUpdateCompatibilityStrategy(String namespace,
                                                  SchemaAutoUpdateCompatibilityStrategy strategy)
            throws PulsarAdminException;

    /**
     * Get schema validation enforced for namespace.
     * @param namespace namespace for this command.
     * @return the schema validation enforced flag
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant or Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    boolean getSchemaValidationEnforced(String namespace) throws PulsarAdminException;

    /**
     * Get schema validation enforced for namespace asynchronously.
     * @param namespace namespace for this command.
     *
     * @return the schema validation enforced flag
     */
    CompletableFuture<Boolean> getSchemaValidationEnforcedAsync(String namespace);

    /**
     * Get schema validation enforced for namespace.
     * @param namespace namespace for this command.
     * @param applied applied for this command.
     * @return the schema validation enforced flag
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant or Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    boolean getSchemaValidationEnforced(String namespace, boolean applied) throws PulsarAdminException;

    /**
     * Get schema validation enforced for namespace asynchronously.
     * @param namespace namespace for this command.
     * @param applied applied for this command.
     *
     * @return the schema validation enforced flag
     */
    CompletableFuture<Boolean> getSchemaValidationEnforcedAsync(String namespace, boolean applied);

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

    /**
     * Set schema validation enforced for namespace asynchronously.
     * if a producer without a schema attempts to produce to a topic with schema in this the namespace, the
     * producer will be failed to connect. PLEASE be carefully on using this, since non-java clients don't
     * support schema. if you enable this setting, it will cause non-java clients failed to produce.
     *
     * @param namespace pulsar namespace name
     * @param schemaValidationEnforced flag to enable or disable schema validation for the given namespace
     */
    CompletableFuture<Void> setSchemaValidationEnforcedAsync(String namespace, boolean schemaValidationEnforced);

    /**
     * Get the strategy used to check the a new schema provided by a producer is compatible with the current schema
     * before it is installed.
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
    SchemaCompatibilityStrategy getSchemaCompatibilityStrategy(String namespace)
            throws PulsarAdminException;

    /**
     * Get the strategy used to check the a new schema provided by a producer is compatible with the current schema
     * before it is installed asynchronously.
     *
     * @param namespace The namespace in whose policy we are interested
     * @return the strategy used to check compatibility
     */
    CompletableFuture<SchemaCompatibilityStrategy> getSchemaCompatibilityStrategyAsync(String namespace);

    /**
     * Set the strategy used to check the a new schema provided by a producer is compatible with the current schema
     * before it is installed.
     *
     * @param namespace The namespace in whose policy should be set
     * @param strategy The schema compatibility strategy
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setSchemaCompatibilityStrategy(String namespace,
                                               SchemaCompatibilityStrategy strategy)
            throws PulsarAdminException;

    /**
     * Set the strategy used to check the a new schema provided by a producer is compatible with the current schema
     * before it is installed asynchronously.
     *
     * @param namespace The namespace in whose policy should be set
     * @param strategy The schema compatibility strategy
     */
    CompletableFuture<Void> setSchemaCompatibilityStrategyAsync(String namespace,
                                        SchemaCompatibilityStrategy strategy);

    /**
     * Get whether allow auto update schema.
     *
     * @param namespace pulsar namespace name
     * @return the schema validation enforced flag
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant or Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    boolean getIsAllowAutoUpdateSchema(String namespace)
            throws PulsarAdminException;

    /**
     * Get whether allow auto update schema asynchronously.
     *
     * @param namespace pulsar namespace name
     * @return the schema validation enforced flag
     */
    CompletableFuture<Boolean> getIsAllowAutoUpdateSchemaAsync(String namespace);

    /**
     * Set whether to allow automatic schema updates.
     * <p/>
     * The flag is when producer bring a new schema and the schema pass compatibility check
     * whether allow schema auto registered
     *
     * @param namespace pulsar namespace name
     * @param isAllowAutoUpdateSchema flag to enable or disable auto update schema
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Tenant or Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setIsAllowAutoUpdateSchema(String namespace, boolean isAllowAutoUpdateSchema)
            throws PulsarAdminException;

    /**
     * Set whether to allow automatic schema updates asynchronously.
     * <p/>
     * The flag is when producer bring a new schema and the schema pass compatibility check
     * whether allow schema auto registered
     *
     * @param namespace pulsar namespace name
     * @param isAllowAutoUpdateSchema flag to enable or disable auto update schema
     */
    CompletableFuture<Void> setIsAllowAutoUpdateSchemaAsync(String namespace, boolean isAllowAutoUpdateSchema);

    /**
     * Set the offload configuration for all the topics in a namespace.
     * <p/>
     * Set the offload configuration in a namespace. This operation requires pulsar tenant access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "region" : "us-east-2",                   // The long term storage region
     *     "bucket" : "bucket",                      // Bucket to place offloaded ledger into
     *     "endpoint" : "endpoint",                  // Alternative endpoint to connect to
     *     "maxBlockSize" : 1024,                    // Max Block Size, default 64MB
     *     "readBufferSize" : 1024,                  // Read Buffer Size, default 1MB
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param offloadPolicies
     *            Offload configuration
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
    void setOffloadPolicies(String namespace, OffloadPolicies offloadPolicies) throws PulsarAdminException;

    /**
     * Remove the offload configuration for a namespace.
     * <p/>
     * Remove the offload configuration in a namespace. This operation requires pulsar tenant access.
     * <p/>
     *
     * @param namespace Namespace name
     * @throws NotAuthorizedException Don't have admin permission
     * @throws NotFoundException      Namespace does not exist
     * @throws ConflictException      Concurrent modification
     * @throws PulsarAdminException   Unexpected error
     */
    void removeOffloadPolicies(String namespace) throws PulsarAdminException;

    /**
     * Set the offload configuration for all the topics in a namespace asynchronously.
     * <p/>
     * Set the offload configuration in a namespace. This operation requires pulsar tenant access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "region" : "us-east-2",                   // The long term storage region
     *     "bucket" : "bucket",                      // Bucket to place offloaded ledger into
     *     "endpoint" : "endpoint",                  // Alternative endpoint to connect to
     *     "maxBlockSize" : 1024,                    // Max Block Size, default 64MB
     *     "readBufferSize" : 1024,                  // Read Buffer Size, default 1MB
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param offloadPolicies
     *            Offload configuration
     */
    CompletableFuture<Void> setOffloadPoliciesAsync(String namespace, OffloadPolicies offloadPolicies);

    /**
     * Remove the offload configuration for a namespace asynchronously.
     * <p/>
     * Remove the offload configuration in a namespace. This operation requires pulsar tenant access.
     * <p/>
     *
     * @param namespace Namespace name
     * @throws NotAuthorizedException Don't have admin permission
     * @throws NotFoundException      Namespace does not exist
     * @throws ConflictException      Concurrent modification
     * @throws PulsarAdminException   Unexpected error
     */
    CompletableFuture<Void> removeOffloadPoliciesAsync(String namespace);

    /**
     * Get the offload configuration for a namespace.
     * <p/>
     * Get the offload configuration for a namespace.
     * <p/>
     * Response example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "region" : "us-east-2",                   // The long term storage region
     *     "bucket" : "bucket",                      // Bucket to place offloaded ledger into
     *     "endpoint" : "endpoint",                  // Alternative endpoint to connect to
     *     "maxBlockSize" : 1024,                    // Max Block Size, default 64MB
     *     "readBufferSize" : 1024,                  // Read Buffer Size, default 1MB
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
    OffloadPolicies getOffloadPolicies(String namespace) throws PulsarAdminException;

    /**
     * Get the offload configuration for a namespace asynchronously.
     * <p/>
     * Get the offload configuration for a namespace.
     * <p/>
     * Response example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "region" : "us-east-2",                   // The long term storage region
     *     "bucket" : "bucket",                      // Bucket to place offloaded ledger into
     *     "endpoint" : "endpoint",                  // Alternative endpoint to connect to
     *     "maxBlockSize" : 1024,                    // Max Block Size, default 64MB
     *     "readBufferSize" : 1024,                  // Read Buffer Size, default 1MB
     * }
     * </code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String namespace);

    /**
     * Get maxTopicsPerNamespace for a namespace.
     * <p/>
     * Response example:
     *
     * <pre>
     *     <code>0</code>
     * </pre>
     * @param namespace
     *              Namespace name
     * @return
     * @throws NotAuthorizedException
     *              Don't have admin permission
     * @throws NotFoundException
     *              Namespace dost not exist
     * @throws PulsarAdminException
     *              Unexpected error
     */
    int getMaxTopicsPerNamespace(String namespace) throws PulsarAdminException;

    /**
     * Get maxTopicsPerNamespace for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     *     <code>0</code>
     * </pre>
     * @param namespace
     *          Namespace name
     * @return
     */
    CompletableFuture<Integer> getMaxTopicsPerNamespaceAsync(String namespace);

    /**
     * Set maxTopicsPerNamespace for a namespace.
     * <p/>
     * Request example:
     *
     * <pre>
     *     <code>100</code>
     * </pre>
     *
     * @param namespace
     *              Namespace name
     * @param maxTopicsPerNamespace
     *              maxTopicsPerNamespace value for a namespace
     *
     * @throws NotAuthorizedException
     *              Don't have admin permission
     * @throws NotFoundException
     *              Namespace does not exist
     * @throws PulsarAdminException
     *              Unexpected error
     */
    void setMaxTopicsPerNamespace(String namespace, int maxTopicsPerNamespace) throws PulsarAdminException;

    /**
     * Set maxTopicsPerNamespace for a namespace asynchronously.
     * <p/>
     * Request example:
     *
     * <pre>
     *     <code>100</code>
     * </pre>
     *
     * @param namespace
     *              Namespace name
     * @param maxTopicsPerNamespace
     *              maxTopicsPerNamespace value for a namespace
     * @return
     */
    CompletableFuture<Void> setMaxTopicsPerNamespaceAsync(String namespace, int maxTopicsPerNamespace);

    /**
     * remove maxTopicsPerNamespace for a namespace.
     *
     * @param namespace
     *              Namespace name
     * @throws NotAuthorizedException
     *              Don't have admin permission
     * @throws NotFoundException
     *              Namespace does not exist
     * @throws PulsarAdminException
     *              Unexpected error
     */
    void removeMaxTopicsPerNamespace(String namespace) throws PulsarAdminException;

    /**
     * remove maxTopicsPerNamespace for a namespace asynchronously.
     *
     * @param namespace
     *              Namespace name
     * @@throws NotAuthorizedException
     *              Don't have admin permission
     * @throws NotFoundException
     *              Namespace does not exist
     * @throws PulsarAdminException
     *              Unexpected error
     */
    CompletableFuture<Void> removeMaxTopicsPerNamespaceAsync(String namespace);

    /**
<<<<<<< HEAD
     * Set key value pair property for a namespace.
     * If the property absents, a new property will added. Otherwise, the new value will overwrite.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().setProperty("a", "a");
     *     admin.namespaces().setProperty("b", "b");
     * </pre>
     *
     * @param namespace
     *              Namespace name
     * @param key
     *              key of the property
     * @param value
     *              value of the property
     */
    CompletableFuture<Void> setPropertyAsync(String namespace, String key, String value);

    /**
     * Set key value pair property for a namespace.
     * If the property absents, a new property will added. Otherwise, the new value will overwrite.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().setProperty("a", "a");
     *     admin.namespaces().setProperty("b", "b");
     * </pre>
     *
     * @param namespace
     *              Namespace name
     * @param key
     *              key of the property
     * @param value
     *              value of the property
     */
    void setProperty(String namespace, String key, String value) throws PulsarAdminException;

    /**
     * Set key value pair properties for a namespace asynchronously.
     * If the property absents, a new property will added. Otherwise, the new value will overwrite.
     *
     * @param namespace
     *              Namespace name
     * @param properties
     *              key value pair properties
     */
    CompletableFuture<Void> setPropertiesAsync(String namespace, Map<String, String> properties);

    /**
     * Set key value pair properties for a namespace.
     * If the property absents, a new property will added. Otherwise, the new value will overwrite.
     *
     * @param namespace
     *              Namespace name
     * @param properties
     *              key value pair properties
     */
    void setProperties(String namespace, Map<String, String> properties) throws PulsarAdminException;

    /**
     * Get property value for a given key.
     * If the property absents, will return null.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().getProperty("a");
     *     admin.namespaces().getProperty("b");
     * </pre>
     *
     * @param namespace
     *              Namespace name
     * @param key
     *              key of the property
     *
     * @return value of the property.
     */
    CompletableFuture<String> getPropertyAsync(String namespace, String key);

    /**
     * Get property value for a given key.
     * If the property absents, will return null.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().getProperty("a");
     *     admin.namespaces().getProperty("b");
     * </pre>
     *
     * @param namespace
     *              Namespace name
     * @param key
     *              key of the property
     *
     * @return value of the property.
     */
    String getProperty(String namespace, String key) throws PulsarAdminException;

    /**
     * Get all properties of a namespace asynchronously.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().getPropertiesAsync();
     * </pre>
     *
     * @param namespace
     *              Namespace name
     *
     * @return key value pair properties.
     */
    CompletableFuture<Map<String, String>> getPropertiesAsync(String namespace);

    /**
     * Get all properties of a namespace.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().getProperties();
     * </pre>
     *
     * @param namespace
     *              Namespace name
     *
     * @return key value pair properties.
     */
    Map<String, String> getProperties(String namespace) throws PulsarAdminException;

    /**
     * Remove a property for a given key.
     * Return value of the property if the property exists, otherwise return null.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().removeProperty("a");
     *     admin.namespaces().removeProperty("b");
     * </pre>
     *
     * @param namespace
     *              Namespace name
     * @param key
     *              key of the property
     *
     * @return value of the property.
     */
    CompletableFuture<String> removePropertyAsync(String namespace, String key);

    /**
     * Remove a property for a given key.
     * Return value of the property if the property exists, otherwise return null.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().removeProperty("a");
     *     admin.namespaces().removeProperty("b");
     * </pre>
     *
     * @param namespace
     *              Namespace name
     * @param key
     *              key of the property
     *
     * @return value of the property.
     */
    String removeProperty(String namespace, String key) throws PulsarAdminException;

    /**
     * Clear all properties of a namespace asynchronously.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().clearPropertiesAsync();
     * </pre>
     *
     * @param namespace
     *              Namespace name
     */
    CompletableFuture<Void> clearPropertiesAsync(String namespace);

    /**
     * Clear all properties of a namespace.
     *
     * <p/>
     * Example:
     *
     * <pre>
     *     admin.namespaces().clearProperties();
     * </pre>
     *
     * @param namespace
     *              Namespace name
     */
    void clearProperties(String namespace) throws PulsarAdminException;

    /**
     * Get the ResourceGroup for a namespace.
     * <p/>
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
     * @return
     */
    String getNamespaceResourceGroup(String namespace) throws PulsarAdminException;

    /**
     * Get the ResourceGroup for a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>60</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     */
    CompletableFuture<String> getNamespaceResourceGroupAsync(String namespace);

    /**
     * Set the ResourceGroup for a namespace.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>60</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param resourcegroupname
     *            ResourceGroup name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void setNamespaceResourceGroup(String namespace, String resourcegroupname) throws PulsarAdminException;

    /**
     * Set the ResourceGroup for a namespace asynchronously.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>60</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @param resourcegroupname
     *            TTL values for all messages for all topics in this namespace
     */
    CompletableFuture<Void> setNamespaceResourceGroupAsync(String namespace, String resourcegroupname);

    /**
     * Remove the ResourceGroup on  a namespace.
     * @param namespace
     * @throws PulsarAdminException
     */
    void removeNamespaceResourceGroup(String namespace) throws PulsarAdminException;

    /**
     * Remove the ResourceGroup on a namespace asynchronously.
     * @param namespace
     * @return
     */
    CompletableFuture<Void> removeNamespaceResourceGroupAsync(String namespace);
}
