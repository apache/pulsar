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

import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;

public class ClustersImpl extends BaseResource implements Clusters {

    private final WebTarget clusters;

    public ClustersImpl(WebTarget web, Authentication auth) {
        super(auth);
        clusters = web.path("/clusters");
    }

    @Override
    public List<String> getClusters() throws PulsarAdminException {
        try {
            return request(clusters).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public ClusterData getCluster(String cluster) throws PulsarAdminException {
        try {
            return request(clusters.path(cluster)).get(ClusterData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        try {
            request(clusters.path(cluster))
                    .put(Entity.entity(clusterData, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateCluster(String cluster, ClusterData clusterData) throws PulsarAdminException {
        try {
            request(clusters.path(cluster)).post(Entity.entity(clusterData, MediaType.APPLICATION_JSON),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteCluster(String cluster) throws PulsarAdminException {
        try {
            request(clusters.path(cluster)).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(String cluster) throws PulsarAdminException {
        try {
            return request(clusters.path(cluster).path("namespaceIsolationPolicies")).get(
                    new GenericType<Map<String, NamespaceIsolationData>>() {
                    });
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
        request(clusters.path(cluster).path("namespaceIsolationPolicies").path(policyName)).delete(ErrorData.class);
    }

    private void setNamespaceIsolationPolicy(String cluster, String policyName,
            NamespaceIsolationData namespaceIsolationData) throws PulsarAdminException {
        try {
            request(clusters.path(cluster).path("namespaceIsolationPolicies").path(policyName)).post(
                    Entity.entity(namespaceIsolationData, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public NamespaceIsolationData getNamespaceIsolationPolicy(String cluster, String policyName)
            throws PulsarAdminException {
        try {
            return request(clusters.path(cluster).path("namespaceIsolationPolicies").path(policyName)).get(
                    NamespaceIsolationData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
