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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;

public class ClustersImpl extends BaseResource implements Clusters {

    private final WebTarget adminClusters;

    public ClustersImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminClusters = web.path("/admin/v2/clusters");
    }

    @Override
    public List<String> getClusters() throws PulsarAdminException {
        try {
            return request(adminClusters).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public ClusterData getCluster(String cluster) throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster)).get(ClusterData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster))
                    .put(Entity.entity(clusterData, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster)).post(Entity.entity(clusterData, MediaType.APPLICATION_JSON),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updatePeerClusterNames(String cluster, LinkedHashSet<String> peerClusterNames) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster).path("peers")).post(Entity.entity(peerClusterNames, MediaType.APPLICATION_JSON),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }

    }

	@Override
    @SuppressWarnings("unchecked")
	public Set<String> getPeerClusterNames(String cluster) throws PulsarAdminException {
		try {
			return request(adminClusters.path(cluster).path("peers")).get(LinkedHashSet.class);
		} catch (Exception e) {
			throw getApiException(e);
		}
	}

    @Override
    public void deleteCluster(String cluster) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster)).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(String cluster) throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster).path("namespaceIsolationPolicies")).get(
                    new GenericType<Map<String, NamespaceIsolationData>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }


    @Override
    public List<BrokerNamespaceIsolationData> getBrokersWithNamespaceIsolationPolicy(String cluster)
            throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers"))
                    .get(new GenericType<List<BrokerNamespaceIsolationData>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public BrokerNamespaceIsolationData getBrokerWithNamespaceIsolationPolicy(String cluster, String broker)
            throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster).path("namespaceIsolationPolicies").path("brokers").path(broker))
                    .get(BrokerNamespaceIsolationData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createNamespaceIsolationPolicy(String cluster, String policyName,
            NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        setNamespaceIsolationPolicy(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public void updateNamespaceIsolationPolicy(String cluster, String policyName,
            NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        setNamespaceIsolationPolicy(cluster, policyName, namespaceIsolationData);
    }

    @Override
    public void deleteNamespaceIsolationPolicy(String cluster, String policyName) throws PulsarAdminException {
        request(adminClusters.path(cluster)
                .path("namespaceIsolationPolicies").path(policyName)).delete(ErrorData.class);
    }

    private void setNamespaceIsolationPolicy(String cluster, String policyName,
            NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName)).post(
                    Entity.entity(namespaceIsolationData, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public NamespaceIsolationData getNamespaceIsolationPolicy(String cluster, String policyName)
            throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster).path("namespaceIsolationPolicies").path(policyName)).get(
                    NamespaceIsolationData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createFailureDomain(String cluster, String domainName, FailureDomain domain) throws PulsarAdminException {
        setDomain(cluster, domainName, domain);
    }

    @Override
    public void updateFailureDomain(String cluster, String domainName, FailureDomain domain) throws PulsarAdminException {
        setDomain(cluster, domainName, domain);
    }

    @Override
    public void deleteFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        request(adminClusters.path(cluster).path("failureDomains").path(domainName)).delete(ErrorData.class);
    }

    @Override
    public Map<String, FailureDomain> getFailureDomains(String cluster) throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster).path("failureDomains"))
                    .get(new GenericType<Map<String, FailureDomain>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FailureDomain getFailureDomain(String cluster, String domainName) throws PulsarAdminException {
        try {
            return request(adminClusters.path(cluster).path("failureDomains")
                    .path(domainName)).get(FailureDomain.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    private void setDomain(String cluster, String domainName,
            FailureDomain domain) throws PulsarAdminException {
        try {
            request(adminClusters.path(cluster).path("failureDomains").path(domainName)).post(
                    Entity.entity(domain, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
