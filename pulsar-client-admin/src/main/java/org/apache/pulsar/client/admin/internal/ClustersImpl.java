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
package org.apache.pulsar.client.admin.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataInterface;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataInterface;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FailureDomainInterface;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataInterface;

public class ClustersImpl extends BaseResource implements Clusters {

    private final WebTarget adminClusters;

    public ClustersImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminClusters = web.path("/admin/v2/clusters");
    }

    @Override
    public List<String> getClusters() throws PulsarAdminException {
        try {
            return getClustersAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> getClustersAsync() {
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(adminClusters,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> clusters) {
                        future.complete(clusters);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public ClusterDataInterface getCluster(String cluster) throws PulsarAdminException {
        try {
            return getClusterAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<ClusterDataInterface> getClusterAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster);
        final CompletableFuture<ClusterDataInterface> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<ClusterData>() {
                    @Override
                    public void completed(ClusterData clusterData) {
                        future.complete(clusterData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createCluster(String cluster, ClusterDataInterface clusterData) throws PulsarAdminException {
        try {
            createClusterAsync(cluster, clusterData).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> createClusterAsync(String cluster, ClusterDataInterface clusterData) {
        WebTarget path = adminClusters.path(cluster);
        return asyncPutRequest(path, Entity.entity((ClusterData) clusterData, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updateCluster(String cluster, ClusterDataInterface clusterData) throws PulsarAdminException {
        try {
            updateClusterAsync(cluster, clusterData).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updateClusterAsync(String cluster, ClusterDataInterface clusterData) {
        WebTarget path = adminClusters.path(cluster);
        return asyncPostRequest(path, Entity.entity((ClusterData) clusterData, MediaType.APPLICATION_JSON_TYPE));
    }

    @Override
    public void updatePeerClusterNames(
            String cluster, LinkedHashSet<String> peerClusterNames) throws PulsarAdminException {
        try {
            updatePeerClusterNamesAsync(cluster, peerClusterNames).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updatePeerClusterNamesAsync(String cluster, LinkedHashSet<String> peerClusterNames) {
        WebTarget path = adminClusters.path(cluster).path("peers");
        return asyncPostRequest(path, Entity.entity(peerClusterNames, MediaType.APPLICATION_JSON));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getPeerClusterNames(String cluster) throws PulsarAdminException {
        try {
            return getPeerClusterNamesAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Set<String>> getPeerClusterNamesAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster).path("peers");
        final CompletableFuture<Set<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Set<String>>() {
                    @Override
                    public void completed(Set<String> clusterNames) {
                        future.complete(clusterNames);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void deleteCluster(String cluster) throws PulsarAdminException {
        try {
            deleteClusterAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deleteClusterAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, NamespaceIsolationDataInterface> getNamespaceIsolationPolicies(String cluster)
            throws PulsarAdminException {
        try {
            return getNamespaceIsolationPoliciesAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Map<String, NamespaceIsolationDataInterface>> getNamespaceIsolationPoliciesAsync(
            String cluster) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies");
        final CompletableFuture<Map<String, NamespaceIsolationDataInterface>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, NamespaceIsolationData>>() {
                    @Override
                    public void completed(Map<String, NamespaceIsolationData> stringNamespaceIsolationDataMap) {
                        Map<String, NamespaceIsolationDataInterface> result = new HashMap<>();
                        stringNamespaceIsolationDataMap.forEach(result::put);
                        future.complete(result);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<BrokerNamespaceIsolationDataInterface> getBrokersWithNamespaceIsolationPolicy(String cluster)
            throws PulsarAdminException {
        try {
            return getBrokersWithNamespaceIsolationPolicyAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<List<BrokerNamespaceIsolationDataInterface>> getBrokersWithNamespaceIsolationPolicyAsync(
            String cluster) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers");
        final CompletableFuture<List<BrokerNamespaceIsolationDataInterface>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<BrokerNamespaceIsolationData>>() {
                    @Override
                    public void completed(List<BrokerNamespaceIsolationData> brokerNamespaceIsolationData) {
                        List<BrokerNamespaceIsolationDataInterface> data =
                                new ArrayList<>(brokerNamespaceIsolationData);
                        future.complete(data);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public BrokerNamespaceIsolationDataInterface getBrokerWithNamespaceIsolationPolicy(String cluster, String broker)
            throws PulsarAdminException {
        try {
            return getBrokerWithNamespaceIsolationPolicyAsync(cluster, broker)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<BrokerNamespaceIsolationDataInterface> getBrokerWithNamespaceIsolationPolicyAsync(
            String cluster, String broker) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers").path(broker);
        final CompletableFuture<BrokerNamespaceIsolationDataInterface> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BrokerNamespaceIsolationData>() {
                    @Override
                    public void completed(BrokerNamespaceIsolationData brokerNamespaceIsolationData) {
                        future.complete(brokerNamespaceIsolationData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createNamespaceIsolationPolicy(String cluster, String policyName,
            NamespaceIsolationDataInterface namespaceIsolationData) throws PulsarAdminException {
        setNamespaceIsolationPolicy(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public CompletableFuture<Void> createNamespaceIsolationPolicyAsync(
            String cluster, String policyName, NamespaceIsolationDataInterface namespaceIsolationData) {
        return setNamespaceIsolationPolicyAsync(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public void updateNamespaceIsolationPolicy(String cluster, String policyName,
           NamespaceIsolationDataInterface namespaceIsolationData) throws PulsarAdminException {
        setNamespaceIsolationPolicy(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public CompletableFuture<Void> updateNamespaceIsolationPolicyAsync(
            String cluster, String policyName, NamespaceIsolationDataInterface namespaceIsolationData) {
        return setNamespaceIsolationPolicyAsync(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public void deleteNamespaceIsolationPolicy(String cluster, String policyName) throws PulsarAdminException {
        try {
            deleteNamespaceIsolationPolicyAsync(cluster, policyName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceIsolationPolicyAsync(String cluster, String policyName) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        return asyncDeleteRequest(path);
    }

    private void setNamespaceIsolationPolicy(String cluster, String policyName,
             NamespaceIsolationDataInterface namespaceIsolationData) throws PulsarAdminException {
        try {
            setNamespaceIsolationPolicyAsync(cluster, policyName, namespaceIsolationData)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    private CompletableFuture<Void> setNamespaceIsolationPolicyAsync(String cluster, String policyName,
             NamespaceIsolationDataInterface namespaceIsolationData) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        return asyncPostRequest(path, Entity.entity(namespaceIsolationData, MediaType.APPLICATION_JSON));
    }

    @Override
    public NamespaceIsolationDataInterface getNamespaceIsolationPolicy(String cluster, String policyName)
            throws PulsarAdminException {
        try {
            return getNamespaceIsolationPolicyAsync(cluster, policyName)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<NamespaceIsolationDataInterface> getNamespaceIsolationPolicyAsync(
            String cluster, String policyName) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        final CompletableFuture<NamespaceIsolationDataInterface> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<NamespaceIsolationData>() {
                    @Override
                    public void completed(NamespaceIsolationData namespaceIsolationData) {
                        future.complete(namespaceIsolationData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createFailureDomain(String cluster, String domainName, FailureDomainInterface domain)
            throws PulsarAdminException {
        setDomain(cluster, domainName, domain);
    }

    @Override
    public CompletableFuture<Void> createFailureDomainAsync(String cluster, String domainName,
                                                            FailureDomainInterface domain) {
        return setDomainAsync(cluster, domainName, domain);
    }

    @Override
    public void updateFailureDomain(String cluster, String domainName, FailureDomainInterface domain)
            throws PulsarAdminException {
        setDomain(cluster, domainName, domain);
    }

    @Override
    public CompletableFuture<Void> updateFailureDomainAsync(String cluster, String domainName,
                                                            FailureDomainInterface domain) {
        return setDomainAsync(cluster, domainName, domain);
    }

    @Override
    public void deleteFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        try {
            deleteFailureDomainAsync(cluster, domainName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deleteFailureDomainAsync(String cluster, String domainName) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, FailureDomainInterface> getFailureDomains(String cluster) throws PulsarAdminException {
        try {
            return getFailureDomainsAsync(cluster).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Map<String, FailureDomainInterface>> getFailureDomainsAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains");
        final CompletableFuture<Map<String, FailureDomainInterface>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, FailureDomain>>() {
                    @Override
                    public void completed(Map<String, FailureDomain> failureDomains) {
                        Map<String, FailureDomainInterface> result = new HashMap<>(failureDomains);
                        future.complete(result);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public FailureDomainInterface getFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        try {
            return getFailureDomainAsync(cluster, domainName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<FailureDomainInterface> getFailureDomainAsync(String cluster, String domainName) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        final CompletableFuture<FailureDomainInterface> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<FailureDomain>() {
                    @Override
                    public void completed(FailureDomain failureDomain) {
                        future.complete(failureDomain);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    private void setDomain(String cluster, String domainName,
                           FailureDomainInterface domain) throws PulsarAdminException {
        try {
            setDomainAsync(cluster, domainName, domain).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    private CompletableFuture<Void> setDomainAsync(String cluster, String domainName,
                                                   FailureDomainInterface domain) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        return asyncPostRequest(path, Entity.entity((FailureDomain) domain, MediaType.APPLICATION_JSON));
    }
}
