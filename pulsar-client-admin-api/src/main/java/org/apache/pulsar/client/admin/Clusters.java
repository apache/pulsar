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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;

/**
 * Admin interface for clusters management.
 */
public interface Clusters {
    /**
     * Get the list of clusters.
     * <p/>
     * Get the list of all the Pulsar clusters.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["c1", "c2", "c3"]</code>
     * </pre>
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getClusters() throws PulsarAdminException;

    /**
     * Get the list of clusters asynchronously.
     * <p/>
     * Get the list of all the Pulsar clusters.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["c1", "c2", "c3"]</code>
     * </pre>
     *
     */
    CompletableFuture<List<String>> getClustersAsync();

    /**
     * Get the configuration data for the specified cluster.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>{ serviceUrl : "http://my-broker.example.com:8080/" }</code>
     * </pre>
     *
     * @param cluster
     *            Cluster name
     *
     * @return the cluster configuration
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to get the configuration of the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    ClusterData getCluster(String cluster) throws PulsarAdminException;

    /**
     * Get the configuration data for the specified cluster asynchronously.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>{ serviceUrl : "http://my-broker.example.com:8080/" }</code>
     * </pre>
     *
     * @param cluster
     *            Cluster name
     *
     * @return the cluster configuration
     *
     */
    CompletableFuture<ClusterData> getClusterAsync(String cluster);

    /**
     * Create a new cluster.
     * <p/>
     * Provisions a new cluster. This operation requires Pulsar super-user privileges.
     * <p/>
     * The name cannot contain '/' characters.
     *
     * @param cluster
     *            Cluster name
     * @param clusterData
     *            the cluster configuration object
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws ConflictException
     *             Cluster already exists
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createCluster(String cluster, ClusterData clusterData) throws PulsarAdminException;

    /**
     * Create a new cluster asynchronously.
     * <p/>
     * Provisions a new cluster. This operation requires Pulsar super-user privileges.
     * <p/>
     * The name cannot contain '/' characters.
     *
     * @param cluster
     *            Cluster name
     * @param clusterData
     *            the cluster configuration object
     *
     */
    CompletableFuture<Void> createClusterAsync(String cluster, ClusterData clusterData);

    /**
     * Update the configuration for a cluster.
     * <p/>
     * This operation requires Pulsar super-user privileges.
     *
     * @param cluster
     *            Cluster name
     * @param clusterData
     *            the cluster configuration object
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateCluster(String cluster, ClusterData clusterData) throws PulsarAdminException;

    /**
     * Update the configuration for a cluster asynchronously.
     * <p/>
     * This operation requires Pulsar super-user privileges.
     *
     * @param cluster
     *            Cluster name
     * @param clusterData
     *            the cluster configuration object
     *
     */
    CompletableFuture<Void> updateClusterAsync(String cluster, ClusterData clusterData);

    /**
     * Update peer cluster names.
     * <p/>
     * This operation requires Pulsar super-user privileges.
     *
     * @param cluster
     *            Cluster name
     * @param peerClusterNames
     *            list of peer cluster names
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updatePeerClusterNames(String cluster, LinkedHashSet<String> peerClusterNames) throws PulsarAdminException;

    /**
     * Update peer cluster names asynchronously.
     * <p/>
     * This operation requires Pulsar super-user privileges.
     *
     * @param cluster
     *            Cluster name
     * @param peerClusterNames
     *            list of peer cluster names
     *
     */
    CompletableFuture<Void> updatePeerClusterNamesAsync(String cluster, LinkedHashSet<String> peerClusterNames);

    /**
     * Get peer-cluster names.
     * <p/>
     *
     * @param cluster
     *            Cluster name
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Domain doesn't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Set<String> getPeerClusterNames(String cluster) throws PulsarAdminException;

    /**
     * Get peer-cluster names asynchronously.
     * <p/>
     *
     * @param cluster
     *            Cluster name
     * @return
     *
     */
    CompletableFuture<Set<String>> getPeerClusterNamesAsync(String cluster);

    /**
     * Delete an existing cluster.
     * <p/>
     * Delete a cluster
     *
     * @param cluster
     *            Cluster name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Cluster does not exist
     * @throws PreconditionFailedException
     *             Cluster is not empty
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteCluster(String cluster) throws PulsarAdminException;

    /**
     * Delete an existing cluster asynchronously.
     * <p/>
     * Delete a cluster
     *
     * @param cluster
     *            Cluster name
     *
     */
    CompletableFuture<Void> deleteClusterAsync(String cluster);

    /**
     * Get the namespace isolation policies of a cluster.
     * <p/>
     *
     * @param cluster
     *            Cluster name
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Policies don't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(String cluster)
            throws PulsarAdminException;

    /**
     * Get the namespace isolation policies of a cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *            Cluster name
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Policies don't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    CompletableFuture<Map<String, NamespaceIsolationData>> getNamespaceIsolationPoliciesAsync(String cluster);

    /**
     * Create a namespace isolation policy for a cluster.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param policyName
     *          Policy name
     *
     * @param namespaceIsolationData
     *          Namespace isolation policy configuration
     *
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Cluster doesn't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createNamespaceIsolationPolicy(
            String cluster, String policyName, NamespaceIsolationData namespaceIsolationData)
            throws PulsarAdminException;

    /**
     * Create a namespace isolation policy for a cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param policyName
     *          Policy name
     *
     * @param namespaceIsolationData
     *          Namespace isolation policy configuration
     *
     * @return
     */
    CompletableFuture<Void> createNamespaceIsolationPolicyAsync(
            String cluster, String policyName, NamespaceIsolationData namespaceIsolationData);

    /**
     * Returns list of active brokers with namespace-isolation policies attached to it.
     *
     * @param cluster
     * @return
     * @throws PulsarAdminException
     */
    List<BrokerNamespaceIsolationData> getBrokersWithNamespaceIsolationPolicy(String cluster)
            throws PulsarAdminException;

    /**
     * Returns list of active brokers with namespace-isolation policies attached to it asynchronously.
     *
     * @param cluster
     * @return
     */
    CompletableFuture<List<BrokerNamespaceIsolationData>> getBrokersWithNamespaceIsolationPolicyAsync(
            String cluster);

    /**
     * Returns active broker with namespace-isolation policies attached to it.
     *
     * @param cluster
     * @param broker the broker name in the form host:port.
     * @return
     * @throws PulsarAdminException
     */
    BrokerNamespaceIsolationData getBrokerWithNamespaceIsolationPolicy(String cluster, String broker)
            throws PulsarAdminException;

    /**
     * Returns active broker with namespace-isolation policies attached to it asynchronously.
     *
     * @param cluster
     * @param broker
     * @return
     */
    CompletableFuture<BrokerNamespaceIsolationData> getBrokerWithNamespaceIsolationPolicyAsync(
            String cluster, String broker);

    /**
     * Update a namespace isolation policy for a cluster.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param policyName
     *          Policy name
     *
     * @param namespaceIsolationData
     *          Namespace isolation policy configuration
     *
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Cluster doesn't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateNamespaceIsolationPolicy(
            String cluster, String policyName, NamespaceIsolationData namespaceIsolationData)
            throws PulsarAdminException;

    /**
     * Update a namespace isolation policy for a cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param policyName
     *          Policy name
     *
     * @param namespaceIsolationData
     *          Namespace isolation policy configuration
     *
     * @return
     *
     */
    CompletableFuture<Void> updateNamespaceIsolationPolicyAsync(
            String cluster, String policyName, NamespaceIsolationData namespaceIsolationData);

    /**
     * Delete a namespace isolation policy for a cluster.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param policyName
     *          Policy name
     *
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Cluster doesn't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */

    void deleteNamespaceIsolationPolicy(String cluster, String policyName) throws PulsarAdminException;

    /**
     * Delete a namespace isolation policy for a cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param policyName
     *          Policy name
     *
     * @return
     */

    CompletableFuture<Void> deleteNamespaceIsolationPolicyAsync(String cluster, String policyName);

    /**
     * Get a single namespace isolation policy for a cluster.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param policyName
     *          Policy name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Policy doesn't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    NamespaceIsolationData getNamespaceIsolationPolicy(String cluster, String policyName)
            throws PulsarAdminException;

    /**
     * Get a single namespace isolation policy for a cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param policyName
     *          Policy name
     *
     */
    CompletableFuture<NamespaceIsolationData> getNamespaceIsolationPolicyAsync(String cluster,
                                                                               String policyName);

    /**
     * Create a domain into cluster.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param domainName
     *          domain name
     *
     * @param domain
     *          Domain configurations
     *
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws ConflictException
     *             Broker already exist into other domain
     *
     * @throws NotFoundException
     *             Cluster doesn't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createFailureDomain(String cluster, String domainName, FailureDomain domain)
            throws PulsarAdminException;

    /**
     * Create a domain into cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param domainName
     *          domain name
     *
     * @param domain
     *          Domain configurations
     *
     * @return
     *
     */
    CompletableFuture<Void> createFailureDomainAsync(String cluster, String domainName, FailureDomain domain);

    /**
     * Update a domain into cluster.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param domainName
     *          domain name
     *
     * @param domain
     *          Domain configurations
     *
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws ConflictException
     *             Broker already exist into other domain
     *
     * @throws NotFoundException
     *             Cluster doesn't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateFailureDomain(String cluster, String domainName, FailureDomain domain)
            throws PulsarAdminException;

    /**
     * Update a domain into cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param domainName
     *          domain name
     *
     * @param domain
     *          Domain configurations
     *
     * @return
     *
     */
    CompletableFuture<Void> updateFailureDomainAsync(String cluster, String domainName, FailureDomain domain);

    /**
     * Delete a domain in cluster.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param domainName
     *          Domain name
     *
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Cluster doesn't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteFailureDomain(String cluster, String domainName) throws PulsarAdminException;

    /**
     * Delete a domain in cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *          Cluster name
     *
     * @param domainName
     *          Domain name
     *
     * @return
     *
     */
    CompletableFuture<Void> deleteFailureDomainAsync(String cluster, String domainName);

    /**
     * Get all registered domains in cluster.
     * <p/>
     *
     * @param cluster
     *            Cluster name
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Cluster don't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Map<String, FailureDomain> getFailureDomains(String cluster) throws PulsarAdminException;

    /**
     * Get all registered domains in cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *            Cluster name
     * @return
     *
     */
    CompletableFuture<Map<String, FailureDomain>> getFailureDomainsAsync(String cluster);

    /**
     * Get the domain registered into a cluster.
     * <p/>
     *
     * @param cluster
     *            Cluster name
     * @return
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     *
     * @throws NotFoundException
     *             Domain doesn't exist
     *
     * @throws PreconditionFailedException
     *             Cluster doesn't exist
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    FailureDomain getFailureDomain(String cluster, String domainName) throws PulsarAdminException;

    /**
     * Get the domain registered into a cluster asynchronously.
     * <p/>
     *
     * @param cluster
     *            Cluster name
     * @return
     *
     */
    CompletableFuture<FailureDomain> getFailureDomainAsync(String cluster, String domainName);

}
