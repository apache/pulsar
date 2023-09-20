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
package org.apache.pulsar.client.admin.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterData.ClusterUrl;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;

public class ClustersImpl extends BaseResource implements Clusters {

    private final WebTarget adminClusters;

    public ClustersImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminClusters = web.path("/admin/v2/clusters");
    }

    @Override
    public List<String> getClusters() throws PulsarAdminException {
        return sync(this::getClustersAsync);
    }

    @Override
    public CompletableFuture<List<String>> getClustersAsync() {
        WebTarget path = this.adminClusters;
        return asyncGetRequest(path, new FutureCallback<List<String>>(){});
    }

    @Override
    public ClusterData getCluster(String cluster) throws PulsarAdminException {
        return sync(() -> getClusterAsync(cluster));
    }

    @Override
    public CompletableFuture<ClusterData> getClusterAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster);
        return asyncGetRequest(path, new FutureCallback<ClusterDataImpl>(){})
                .thenApply(clusterData -> clusterData);
    }

    @Override
    public void createCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        sync(() -> createClusterAsync(cluster, clusterData));
    }

    @Override
    public CompletableFuture<Void> createClusterAsync(String cluster, ClusterData clusterData) {
        WebTarget path = adminClusters.path(cluster);
        return asyncPutRequest(path, Entity.entity((ClusterDataImpl) clusterData, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updateCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        sync(() -> updateClusterAsync(cluster, clusterData));
    }

    @Override
    public CompletableFuture<Void> updateClusterAsync(String cluster, ClusterData clusterData) {
        WebTarget path = adminClusters.path(cluster);
        return asyncPostRequest(path, Entity.entity((ClusterDataImpl) clusterData, MediaType.APPLICATION_JSON_TYPE));
    }

    @Override
    public void updatePeerClusterNames(
            String cluster, LinkedHashSet<String> peerClusterNames) throws PulsarAdminException {
        sync(() -> updatePeerClusterNamesAsync(cluster, peerClusterNames));
    }

    @Override
    public CompletableFuture<Void> updatePeerClusterNamesAsync(String cluster, LinkedHashSet<String> peerClusterNames) {
        WebTarget path = adminClusters.path(cluster).path("peers");
        return asyncPostRequest(path, Entity.entity(peerClusterNames, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updateClusterMigration(String cluster, boolean isMigrated, ClusterUrl clusterUrl)
            throws PulsarAdminException {
        sync(() -> updateClusterMigrationAsync(cluster, isMigrated, clusterUrl));
    }

    @Override
    public CompletableFuture<Void> updateClusterMigrationAsync(String cluster, boolean isMigrated,
            ClusterUrl clusterUrl) {
        WebTarget path = adminClusters.path(cluster).path("migrate").queryParam("migrated", isMigrated);
        return asyncPostRequest(path, Entity.entity(clusterUrl, MediaType.APPLICATION_JSON));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getPeerClusterNames(String cluster) throws PulsarAdminException {
        return sync(() -> getPeerClusterNamesAsync(cluster));
    }

    @Override
    public CompletableFuture<Set<String>> getPeerClusterNamesAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster).path("peers");
        return asyncGetRequest(path, new FutureCallback<Set<String>>(){});
    }

    @Override
    public void deleteCluster(String cluster) throws PulsarAdminException {
        sync(() -> deleteClusterAsync(cluster));
    }

    @Override
    public CompletableFuture<Void> deleteClusterAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(String cluster)
            throws PulsarAdminException {
        return sync(() -> getNamespaceIsolationPoliciesAsync(cluster));
    }

    @Override
    public CompletableFuture<Map<String, NamespaceIsolationData>> getNamespaceIsolationPoliciesAsync(
            String cluster) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies");

        return asyncGetRequest(path, new FutureCallback<Map<String, NamespaceIsolationDataImpl>>() {})
                .thenApply(HashMap::new);
    }

    @Override
    public List<BrokerNamespaceIsolationData> getBrokersWithNamespaceIsolationPolicy(String cluster)
            throws PulsarAdminException {
        return sync(() -> getBrokersWithNamespaceIsolationPolicyAsync(cluster));
    }

    @Override
    public CompletableFuture<List<BrokerNamespaceIsolationData>> getBrokersWithNamespaceIsolationPolicyAsync(
            String cluster) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers");
        return asyncGetRequest(path, new FutureCallback<List<BrokerNamespaceIsolationDataImpl>>() {})
                .thenApply(ArrayList::new);
    }

    @Override
    public BrokerNamespaceIsolationData getBrokerWithNamespaceIsolationPolicy(String cluster, String broker)
            throws PulsarAdminException {
        return sync(() -> getBrokerWithNamespaceIsolationPolicyAsync(cluster, broker));
    }

    @Override
    public CompletableFuture<BrokerNamespaceIsolationData> getBrokerWithNamespaceIsolationPolicyAsync(
            String cluster, String broker) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers").path(broker);
        return asyncGetRequest(path, new FutureCallback<BrokerNamespaceIsolationDataImpl>(){})
                .thenApply(brokerNamespaceIsolationData -> brokerNamespaceIsolationData);
    }

    @Override
    public void createNamespaceIsolationPolicy(String cluster, String policyName,
            NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        setNamespaceIsolationPolicy(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public CompletableFuture<Void> createNamespaceIsolationPolicyAsync(
            String cluster, String policyName, NamespaceIsolationData namespaceIsolationData) {
        return setNamespaceIsolationPolicyAsync(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public void updateNamespaceIsolationPolicy(String cluster, String policyName,
           NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        setNamespaceIsolationPolicy(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public CompletableFuture<Void> updateNamespaceIsolationPolicyAsync(
            String cluster, String policyName, NamespaceIsolationData namespaceIsolationData) {
        return setNamespaceIsolationPolicyAsync(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public void deleteNamespaceIsolationPolicy(String cluster, String policyName) throws PulsarAdminException {
        sync(() -> deleteNamespaceIsolationPolicyAsync(cluster, policyName));
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceIsolationPolicyAsync(String cluster, String policyName) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        return asyncDeleteRequest(path);
    }

    private void setNamespaceIsolationPolicy(String cluster, String policyName,
             NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        sync(() -> setNamespaceIsolationPolicyAsync(cluster, policyName, namespaceIsolationData));
    }

    private CompletableFuture<Void> setNamespaceIsolationPolicyAsync(String cluster, String policyName,
             NamespaceIsolationData namespaceIsolationData) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        return asyncPostRequest(path, Entity.entity(namespaceIsolationData, MediaType.APPLICATION_JSON));
    }

    @Override
    public NamespaceIsolationData getNamespaceIsolationPolicy(String cluster, String policyName)
            throws PulsarAdminException {
        return sync(() -> getNamespaceIsolationPolicyAsync(cluster, policyName));
    }

    @Override
    public CompletableFuture<NamespaceIsolationData> getNamespaceIsolationPolicyAsync(
            String cluster, String policyName) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        return asyncGetRequest(path, new FutureCallback<NamespaceIsolationDataImpl>(){})
                .thenApply(namespaceIsolationData -> namespaceIsolationData);
    }

    @Override
    public void createFailureDomain(String cluster, String domainName, FailureDomain domain)
            throws PulsarAdminException {
        setDomain(cluster, domainName, domain);
    }

    @Override
    public CompletableFuture<Void> createFailureDomainAsync(String cluster, String domainName,
                                                            FailureDomain domain) {
        return setDomainAsync(cluster, domainName, domain);
    }

    @Override
    public void updateFailureDomain(String cluster, String domainName, FailureDomain domain)
            throws PulsarAdminException {
        setDomain(cluster, domainName, domain);
    }

    @Override
    public CompletableFuture<Void> updateFailureDomainAsync(String cluster, String domainName,
                                                            FailureDomain domain) {
        return setDomainAsync(cluster, domainName, domain);
    }

    @Override
    public void deleteFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        sync(() -> deleteFailureDomainAsync(cluster, domainName));
    }

    @Override
    public CompletableFuture<Void> deleteFailureDomainAsync(String cluster, String domainName) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, FailureDomain> getFailureDomains(String cluster) throws PulsarAdminException {
        return sync(() -> getFailureDomainsAsync(cluster));
    }

    @Override
    public CompletableFuture<Map<String, FailureDomain>> getFailureDomainsAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains");
        return asyncGetRequest(path, new FutureCallback<Map<String, FailureDomainImpl>>() {})
                .thenApply(HashMap::new);
    }

    @Override
    public FailureDomain getFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        return sync(() -> getFailureDomainAsync(cluster, domainName));
    }

    @Override
    public CompletableFuture<FailureDomain> getFailureDomainAsync(String cluster, String domainName) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        return asyncGetRequest(path, new FutureCallback<FailureDomainImpl>(){})
                .thenApply(failureDomain -> failureDomain);
    }

    private void setDomain(String cluster, String domainName,
                           FailureDomain domain) throws PulsarAdminException {
        sync(() -> setDomainAsync(cluster, domainName, domain));
    }

    private CompletableFuture<Void> setDomainAsync(String cluster, String domainName,
                                                   FailureDomain domain) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        return asyncPostRequest(path, Entity.entity((FailureDomainImpl) domain, MediaType.APPLICATION_JSON));
    }
}
