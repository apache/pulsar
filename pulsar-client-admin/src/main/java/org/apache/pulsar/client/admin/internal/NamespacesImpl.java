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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;

public class NamespacesImpl extends BaseResource implements Namespaces {

    private final WebTarget adminNamespaces;
    private final WebTarget adminV2Namespaces;

    public NamespacesImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminNamespaces = web.path("/admin/namespaces");
        adminV2Namespaces = web.path("/admin/v2/namespaces");
    }

    @Override
    public List<String> getNamespaces(String tenant) throws PulsarAdminException {
        try {
            WebTarget path = adminV2Namespaces.path(tenant);
            return request(path).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getNamespaces(String tenant, String cluster) throws PulsarAdminException {
        try {
            WebTarget path = adminNamespaces.path(tenant).path(cluster);
            return request(path).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getTopics(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            String action = ns.isV2() ? "topics" : "destinations";
            WebTarget path = namespacePath(ns, action);
            return request(path).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Policies getPolicies(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);
            return request(path).get(Policies.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createNamespace(String namespace, Set<String> clusters) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);

            if (ns.isV2()) {
                // For V2 API we pass full Policy class instance
                Policies policies = new Policies();
                policies.replication_clusters = clusters;
                request(path).put(Entity.entity(policies, MediaType.APPLICATION_JSON), ErrorData.class);
            } else {
                // For V1 API, we pass the BundlesData on creation
                request(path).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
                // For V1, we need to do it in 2 steps
                setNamespaceReplicationClusters(namespace, clusters);
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createNamespace(String namespace, int numBundles) throws PulsarAdminException {
        createNamespace(namespace, new BundlesData(numBundles));
    }

    @Override
    public void createNamespace(String namespace, Policies policies) throws PulsarAdminException {
        NamespaceName ns = NamespaceName.get(namespace);
        checkArgument(ns.isV2(), "Create namespace with policies is only supported on newer namespaces");

        try {
            WebTarget path = namespacePath(ns);

            // For V2 API we pass full Policy class instance
            request(path).put(Entity.entity(policies, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createNamespace(String namespace, BundlesData bundlesData) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);

            if (ns.isV2()) {
                // For V2 API we pass full Policy class instance
                Policies policies = new Policies();
                policies.bundles = bundlesData;
                request(path).put(Entity.entity(policies, MediaType.APPLICATION_JSON), ErrorData.class);
            } else {
                // For V1 API, we pass the BundlesData on creation
                request(path).put(Entity.entity(bundlesData, MediaType.APPLICATION_JSON), ErrorData.class);
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createNamespace(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);
            request(path).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteNamespace(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns);
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteNamespaceBundle(String namespace, String bundleRange) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundleRange);
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<String, Set<AuthAction>> getPermissions(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions");
            return request(path).get(new GenericType<Map<String, Set<AuthAction>>>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void grantPermissionOnNamespace(String namespace, String role, Set<AuthAction> actions)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions", role);
            request(path).post(Entity.entity(actions, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void revokePermissionsOnNamespace(String namespace, String role) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions", role);
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }


    @Override
    public void grantPermissionOnSubscription(String namespace, String subscription, Set<String> roles)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions", "subscription", subscription);
            request(path).post(Entity.entity(roles, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void revokePermissionOnSubscription(String namespace, String subscription, String role) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "permissions", subscription, role);
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getNamespaceReplicationClusters(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "replication");
            return request(path).get(new GenericType<List<String>>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setNamespaceReplicationClusters(String namespace, Set<String> clusterIds) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "replication");
            request(path).post(Entity.entity(clusterIds, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public int getNamespaceMessageTTL(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "messageTTL");
            return request(path).get(new GenericType<Integer>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setNamespaceMessageTTL(String namespace, int ttlInSeconds) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "messageTTL");
            request(path).post(Entity.entity(ttlInSeconds, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setNamespaceAntiAffinityGroup(String namespace, String namespaceAntiAffinityGroup)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "antiAffinity");
            request(path).post(Entity.entity(namespaceAntiAffinityGroup, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public String getNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "antiAffinity");
            return request(path).get(new GenericType<String>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getAntiAffinityNamespaces(String tenant, String cluster, String namespaceAntiAffinityGroup)
            throws PulsarAdminException {
        try {
            WebTarget path = adminNamespaces.path(cluster).path("antiAffinity").path(namespaceAntiAffinityGroup);
            return request(path.queryParam("property", tenant)).get(new GenericType<List<String>>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "antiAffinity");
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setDeduplicationStatus(String namespace, boolean enableDeduplication) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "deduplication");
            request(path).post(Entity.entity(enableDeduplication, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "backlogQuotaMap");
            return request(path).get(new GenericType<Map<BacklogQuotaType, BacklogQuota>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setBacklogQuota(String namespace, BacklogQuota backlogQuota) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "backlogQuota");
            request(path).post(Entity.entity(backlogQuota, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void removeBacklogQuota(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "backlogQuota");
            request(path.queryParam("backlogQuotaType", BacklogQuotaType.destination_storage.toString()))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setPersistence(String namespace, PersistencePolicies persistence) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "persistence");
            request(path).post(Entity.entity(persistence, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setBookieAffinityGroup(String namespace, BookieAffinityGroupData bookieAffinityGroup) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "persistence", "bookieAffinity");
            request(path).post(Entity.entity(bookieAffinityGroup, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteBookieAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "persistence", "bookieAffinity");
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public BookieAffinityGroupData getBookieAffinityGroup(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "persistence", "bookieAffinity");
            return request(path).get(BookieAffinityGroupData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public PersistencePolicies getPersistence(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "persistence");
            return request(path).get(PersistencePolicies.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setRetention(String namespace, RetentionPolicies retention) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "retention");
            request(path).post(Entity.entity(retention, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }

    }

    @Override
    public RetentionPolicies getRetention(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "retention");
            return request(path).get(RetentionPolicies.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void unload(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "unload");
            request(path).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public String getReplicationConfigVersion(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "configversion");
            return request(path).get(String.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void unloadNamespaceBundle(String namespace, String bundle) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "unload");
            request(path).put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void splitNamespaceBundle(String namespace, String bundle, boolean unloadSplitBundles)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "split");
            request(path.queryParam("unload", Boolean.toString(unloadSplitBundles)))
                    .put(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "dispatchRate");
            request(path).post(Entity.entity(dispatchRate, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public DispatchRate getDispatchRate(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "dispatchRate");
            return request(path).get(DispatchRate.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setSubscribeRate(String namespace, SubscribeRate subscribeRate) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscribeRate");
            request(path).post(Entity.entity(subscribeRate, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public SubscribeRate getSubscribeRate(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscribeRate");
            return request(path).get(SubscribeRate.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setSubscriptionDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscriptionDispatchRate");
            request(path).post(Entity.entity(dispatchRate, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscriptionDispatchRate");
            return request(path).get(DispatchRate.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setReplicatorDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "replicatorDispatchRate");
            request(path).post(Entity.entity(dispatchRate, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public DispatchRate getReplicatorDispatchRate(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "replicatorDispatchRate");
            return request(path).get(DispatchRate.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void clearNamespaceBacklog(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "clearBacklog");
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void clearNamespaceBacklogForSubscription(String namespace, String subscription)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "clearBacklog", subscription);
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void clearNamespaceBundleBacklog(String namespace, String bundle) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "clearBacklog");
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void clearNamespaceBundleBacklogForSubscription(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "clearBacklog", subscription);
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void unsubscribeNamespace(String namespace, String subscription) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "unsubscribe", subscription);
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void unsubscribeNamespaceBundle(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle, "unsubscribe", subscription);
            request(path).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setSubscriptionAuthMode(String namespace, SubscriptionAuthMode subscriptionAuthMode) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "subscriptionAuthMode");
            request(path).post(Entity.entity(subscriptionAuthMode, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setEncryptionRequiredStatus(String namespace, boolean encryptionRequired) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "encryptionRequired");
            request(path).post(Entity.entity(encryptionRequired, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public int getMaxProducersPerTopic(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxProducersPerTopic");
            return request(path).get(Integer.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setMaxProducersPerTopic(String namespace, int maxProducersPerTopic) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxProducersPerTopic");
            request(path).post(Entity.entity(maxProducersPerTopic, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public int getMaxConsumersPerTopic(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxConsumersPerTopic");
            return request(path).get(Integer.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setMaxConsumersPerTopic(String namespace, int maxConsumersPerTopic) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxConsumersPerTopic");
            request(path).post(Entity.entity(maxConsumersPerTopic, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public int getMaxConsumersPerSubscription(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxConsumersPerSubscription");
            return request(path).get(Integer.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setMaxConsumersPerSubscription(String namespace, int maxConsumersPerSubscription) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "maxConsumersPerSubscription");
            request(path).post(Entity.entity(maxConsumersPerSubscription, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public long getCompactionThreshold(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "compactionThreshold");
            return request(path).get(Long.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setCompactionThreshold(String namespace, long compactionThreshold) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "compactionThreshold");
            request(path).put(Entity.entity(compactionThreshold, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public long getOffloadThreshold(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadThreshold");
            return request(path).get(Long.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setOffloadThreshold(String namespace, long offloadThreshold) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadThreshold");
            request(path).put(Entity.entity(offloadThreshold, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Long getOffloadDeleteLagMs(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
            return request(path).get(Long.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setOffloadDeleteLag(String namespace, long lag, TimeUnit unit) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
            request(path).put(Entity.entity(TimeUnit.MILLISECONDS.convert(lag, unit), MediaType.APPLICATION_JSON),
                              ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void clearOffloadDeleteLag(String namespace) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
            request(path).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public SchemaAutoUpdateCompatibilityStrategy getSchemaAutoUpdateCompatibilityStrategy(String namespace)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "schemaAutoUpdateCompatibilityStrategy");
            return request(path).get(SchemaAutoUpdateCompatibilityStrategy.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setSchemaAutoUpdateCompatibilityStrategy(String namespace,
                                                         SchemaAutoUpdateCompatibilityStrategy strategy)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "schemaAutoUpdateCompatibilityStrategy");
            request(path).put(Entity.entity(strategy, MediaType.APPLICATION_JSON),
                              ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public boolean getSchemaValidationEnforced(String namespace)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "schemaValidationEnforced");
            return request(path).get(Boolean.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void setSchemaValidationEnforced(String namespace, boolean schemaValidationEnforced)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, "schemaValidationEnforced");
            request(path).post(Entity.entity(schemaValidationEnforced, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    private WebTarget namespacePath(NamespaceName namespace, String... parts) {
        final WebTarget base = namespace.isV2() ? adminV2Namespaces : adminNamespaces;
        WebTarget namespacePath = base.path(namespace.toString());
        namespacePath = WebTargets.addParts(namespacePath, parts);
        return namespacePath;
    }
}
