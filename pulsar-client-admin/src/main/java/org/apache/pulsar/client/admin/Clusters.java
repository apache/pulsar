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

import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;

/**
 * Admin interface for clusters management.
 */
public interface Clusters {
    /**
     * Get the list of clusters.
     * <p>
     * Get the list of all the Pulsar clusters.
     * <p>
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
     * Get the configuration data for the specified cluster.
     * <p>
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
     * Create a new cluster.
     * <p>
     * Provisions a new cluster. This operation requires Pulsar super-user privileges.
     * <p>
     * The name cannot contain '/' characters.
     *
     * @param cluster
     *            Cluster name
     * @param clusterData
     *            the cluster configuration object
     *
     * @throws NotAuthorized
     *             You don't have admin permission to create the cluster
     * @throws ConflictException
     *             Cluster already exists
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createCluster(String cluster, ClusterData clusterData) throws PulsarAdminException;

    /**
     * Update the configuration for a cluster.
     * <p>
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
     * Delete an existing cluster
     * <p>
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
     * Get the namespace isolation policies of a cluster
     * <p>
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
    Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(String cluster) throws PulsarAdminException;


    /**
     * Create a namespace isolation policy for a cluster
     * <p>
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
    void createNamespaceIsolationPolicy(String cluster, String policyName, NamespaceIsolationData namespaceIsolationData)
            throws PulsarAdminException;


    /**
     * Update a namespace isolation policy for a cluster
     * <p>
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
    void updateNamespaceIsolationPolicy(String cluster, String policyName, NamespaceIsolationData namespaceIsolationData)
            throws PulsarAdminException;


    /**
     * Delete a namespace isolation policy for a cluster
     * <p>
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
     * Get a single namespace isolation policy for a cluster
     * <p>
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
    NamespaceIsolationData getNamespaceIsolationPolicy(String cluster, String policyName) throws PulsarAdminException;

}
