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
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;

public class NamespacesImpl extends BaseResource implements Namespaces {

    private final WebTarget namespaces;

    public NamespacesImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.namespaces = web.path("/namespaces");
    }

    @Override
    public List<String> getNamespaces(String property) throws PulsarAdminException {
        try {
            return request(namespaces.path(property)).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getNamespaces(String property, String cluster) throws PulsarAdminException {
        try {
            return request(namespaces.path(property).path(cluster)).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getDestinations(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            return request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName())
                    .path("destinations")).get(new GenericType<List<String>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Policies getPolicies(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            return request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()))
                    .get(Policies.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createNamespace(String namespace, int numBundles) throws PulsarAdminException {
        createNamespace(namespace, new BundlesData(numBundles));
    }

    @Override
    public void createNamespace(String namespace, BundlesData bundlesData) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()))
                    .put(Entity.entity(bundlesData, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createNamespace(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()))
                    .put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteNamespace(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteNamespaceBundle(String namespace, String bundleRange) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path(bundleRange))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<String, Set<AuthAction>> getPermissions(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            return request(
                    namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("permissions"))
                            .get(new GenericType<Map<String, Set<AuthAction>>>() {
                            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void grantPermissionOnNamespace(String namespace, String role, Set<AuthAction> actions)
            throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("permissions")
                    .path(role)).post(Entity.entity(actions, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void revokePermissionsOnNamespace(String namespace, String role) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("permissions")
                    .path(role)).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getNamespaceReplicationClusters(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            return request(
                    namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("replication"))
                            .get(new GenericType<List<String>>() {
                            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setNamespaceReplicationClusters(String namespace, List<String> clusterIds) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("replication"))
                    .post(Entity.entity(clusterIds, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public int getNamespaceMessageTTL(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            return request(
                    namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("messageTTL"))
                            .get(new GenericType<Integer>() {
                            });

        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setNamespaceMessageTTL(String namespace, int ttlInSeconds) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("messageTTL"))
                    .post(Entity.entity(ttlInSeconds, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            return request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName())
                    .path("backlogQuotaMap")).get(new GenericType<Map<BacklogQuotaType, BacklogQuota>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setBacklogQuota(String namespace, BacklogQuota backlogQuota) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName())
                    .path("backlogQuota")).post(Entity.entity(backlogQuota, MediaType.APPLICATION_JSON),
                            ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void removeBacklogQuota(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("backlogQuota")
                    .queryParam("backlogQuotaType", BacklogQuotaType.destination_storage.toString()))
                            .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setPersistence(String namespace, PersistencePolicies persistence) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("persistence"))
                    .post(Entity.entity(persistence, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public PersistencePolicies getPersistence(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            return request(
                    namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("persistence"))
                            .get(PersistencePolicies.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setRetention(String namespace, RetentionPolicies retention) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("retention"))
                    .post(Entity.entity(retention, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }

    }

    @Override
    public RetentionPolicies getRetention(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            return request(
                    namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("retention"))
                            .get(RetentionPolicies.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void unload(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("unload"))
                    .put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public String getReplicationConfigVersion(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            return request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName())
                    .path("configversion")).get(String.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void unloadNamespaceBundle(String namespace, String bundle) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path(bundle)
                    .path("unload")).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void splitNamespaceBundle(String namespace, String bundle) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path(bundle)
                    .path("split")).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void clearNamespaceBacklog(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName())
                    .path("clearBacklog")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void clearNamespaceBacklogForSubscription(String namespace, String subscription)
            throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("clearBacklog")
                    .path(subscription)).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void clearNamespaceBundleBacklog(String namespace, String bundle) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path(bundle)
                    .path("clearBacklog")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void clearNamespaceBundleBacklogForSubscription(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path(bundle)
                    .path("clearBacklog").path(subscription)).post(Entity.entity("", MediaType.APPLICATION_JSON),
                            ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void unsubscribeNamespace(String namespace, String subscription) throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path("unsubscribe")
                    .path(subscription)).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void unsubscribeNamespaceBundle(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        try {
            NamespaceName ns = new NamespaceName(namespace);
            request(namespaces.path(ns.getProperty()).path(ns.getCluster()).path(ns.getLocalName()).path(bundle)
                    .path("unsubscribe").path(subscription)).post(Entity.entity("", MediaType.APPLICATION_JSON),
                            ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
