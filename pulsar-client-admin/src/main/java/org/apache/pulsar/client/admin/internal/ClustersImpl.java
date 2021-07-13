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
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
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
    public ClusterData getCluster(String cluster) throws PulsarAdminException {
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
    public CompletableFuture<ClusterData> getClusterAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster);
        final CompletableFuture<ClusterData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<ClusterDataImpl>() {
                    @Override
                    public void completed(ClusterDataImpl clusterData) {
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
    public void createCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
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
    public CompletableFuture<Void> createClusterAsync(String cluster, ClusterData clusterData) {
        WebTarget path = adminClusters.path(cluster);
        return asyncPutRequest(path, Entity.entity((ClusterDataImpl) clusterData, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updateCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
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
    public CompletableFuture<Void> updateClusterAsync(String cluster, ClusterData clusterData) {
        WebTarget path = adminClusters.path(cluster);
        return asyncPostRequest(path, Entity.entity((ClusterDataImpl) clusterData, MediaType.APPLICATION_JSON_TYPE));
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
    public Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(String cluster)
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
    public CompletableFuture<Map<String, NamespaceIsolationData>> getNamespaceIsolationPoliciesAsync(
            String cluster) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies");
        final CompletableFuture<Map<String, NamespaceIsolationData>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, NamespaceIsolationDataImpl>>() {
                    @Override
                    public void completed(Map<String, NamespaceIsolationDataImpl> stringNamespaceIsolationDataMap) {
                        Map<String, NamespaceIsolationData> result = new HashMap<>();
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
    public List<BrokerNamespaceIsolationData> getBrokersWithNamespaceIsolationPolicy(String cluster)
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
    public CompletableFuture<List<BrokerNamespaceIsolationData>> getBrokersWithNamespaceIsolationPolicyAsync(
            String cluster) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers");
        final CompletableFuture<List<BrokerNamespaceIsolationData>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<BrokerNamespaceIsolationDataImpl>>() {
                    @Override
                    public void completed(List<BrokerNamespaceIsolationDataImpl> brokerNamespaceIsolationData) {
                        List<BrokerNamespaceIsolationData> data =
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
    public BrokerNamespaceIsolationData getBrokerWithNamespaceIsolationPolicy(String cluster, String broker)
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
    public CompletableFuture<BrokerNamespaceIsolationData> getBrokerWithNamespaceIsolationPolicyAsync(
            String cluster, String broker) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers").path(broker);
        final CompletableFuture<BrokerNamespaceIsolationData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BrokerNamespaceIsolationDataImpl>() {
                    @Override
                    public void completed(BrokerNamespaceIsolationDataImpl brokerNamespaceIsolationData) {
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
             NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
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
             NamespaceIsolationData namespaceIsolationData) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        return asyncPostRequest(path, Entity.entity(namespaceIsolationData, MediaType.APPLICATION_JSON));
    }

    @Override
    public NamespaceIsolationData getNamespaceIsolationPolicy(String cluster, String policyName)
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
    public CompletableFuture<NamespaceIsolationData> getNamespaceIsolationPolicyAsync(
            String cluster, String policyName) {
        WebTarget path = adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName);
        final CompletableFuture<NamespaceIsolationData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<NamespaceIsolationDataImpl>() {
                    @Override
                    public void completed(NamespaceIsolationDataImpl namespaceIsolationData) {
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
    public Map<String, FailureDomain> getFailureDomains(String cluster) throws PulsarAdminException {
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
    public CompletableFuture<Map<String, FailureDomain>> getFailureDomainsAsync(String cluster) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains");
        final CompletableFuture<Map<String, FailureDomain>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, FailureDomainImpl>>() {
                    @Override
                    public void completed(Map<String, FailureDomainImpl> failureDomains) {
                        Map<String, FailureDomain> result = new HashMap<>(failureDomains);
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
    public FailureDomain getFailureDomain(String cluster, String domainName) throws PulsarAdminException {
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
    public CompletableFuture<FailureDomain> getFailureDomainAsync(String cluster, String domainName) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        final CompletableFuture<FailureDomain> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<FailureDomainImpl>() {
                    @Override
                    public void completed(FailureDomainImpl failureDomain) {
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
                           FailureDomain domain) throws PulsarAdminException {
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
                                                   FailureDomain domain) {
        WebTarget path = adminClusters.path(cluster).path("failureDomains").path(domainName);
        return asyncPostRequest(path, Entity.entity((FailureDomainImpl) domain, MediaType.APPLICATION_JSON));
    }
}
