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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
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
        return sync(() -> getNamespacesAsync(tenant));
    }

    @Override
    public CompletableFuture<List<String>> getNamespacesAsync(String tenant) {
        WebTarget path = adminV2Namespaces.path(tenant);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> namespaces) {
                        future.complete(namespaces);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<String> getNamespaces(String tenant, String cluster) throws PulsarAdminException {
        return sync(() -> getNamespacesAsync(tenant, cluster));
    }

    public CompletableFuture<List<String>> getNamespacesAsync(String tenant, String cluster) {
        WebTarget path = adminNamespaces.path(tenant).path(cluster);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {

                    @Override
                    public void completed(List<String> namespaces) {
                        future.complete(namespaces);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<String> getTopics(String namespace) throws PulsarAdminException {
        return sync(() -> getTopicsAsync(namespace));
    }

    @Override
    public BundlesData getBundles(String namespace) throws PulsarAdminException {
        return sync(() -> getBundlesAsync(namespace));
    }

    @Override
    public  CompletableFuture<BundlesData> getBundlesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        String action = "bundles";
        WebTarget path = namespacePath(ns, action);
        final CompletableFuture<BundlesData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BundlesData>() {
                    @Override
                    public void completed(BundlesData bundles) {
                        future.complete(bundles);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public CompletableFuture<List<String>> getTopicsAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        String action = ns.isV2() ? "topics" : "destinations";
        WebTarget path = namespacePath(ns, action);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> topics) {
                        future.complete(topics);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Policies getPolicies(String namespace) throws PulsarAdminException {
        return sync(() -> getPoliciesAsync(namespace));
    }

    @Override
    public CompletableFuture<Policies> getPoliciesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);
        final CompletableFuture<Policies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Policies>() {
                    @Override
                    public void completed(Policies policies) {
                        future.complete(policies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createNamespace(String namespace, Set<String> clusters) throws PulsarAdminException {
        sync(() -> createNamespaceAsync(namespace, clusters));
    }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String namespace, Set<String> clusters) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);

        if (ns.isV2()) {
            // For V2 API we pass full Policy class instance
            Policies policies = new Policies();
            policies.replication_clusters = clusters;
            return asyncPutRequest(path, Entity.entity(policies, MediaType.APPLICATION_JSON));
        } else {
            // For V1 API, we pass the BundlesData on creation
            return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON)).thenAccept(ignore -> {
                // For V1, we need to do it in 2 steps
                setNamespaceReplicationClustersAsync(namespace, clusters);
            });
        }
    }

    @Override
    public void createNamespace(String namespace, int numBundles) throws PulsarAdminException {
        createNamespace(namespace, BundlesData.builder().numBundles(numBundles).build());
    }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String namespace, int numBundles) {
        return createNamespaceAsync(namespace, BundlesData.builder().numBundles(numBundles).build());
    }

    @Override
    public void createNamespace(String namespace, Policies policies) throws PulsarAdminException {
        sync(() -> createNamespaceAsync(namespace, policies));
    }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String namespace, Policies policies) {
        NamespaceName ns = NamespaceName.get(namespace);
        checkArgument(ns.isV2(), "Create namespace with policies is only supported on newer namespaces");
        WebTarget path = namespacePath(ns);
        // For V2 API we pass full Policy class instance
        return asyncPutRequest(path, Entity.entity(policies, MediaType.APPLICATION_JSON));
    }

    @Override
    public void createNamespace(String namespace, BundlesData bundlesData) throws PulsarAdminException {
        sync(() -> createNamespaceAsync(namespace, bundlesData));
    }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String namespace, BundlesData bundlesData) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);

        if (ns.isV2()) {
            // For V2 API we pass full Policy class instance
            Policies policies = new Policies();
            policies.bundles = bundlesData;
            return asyncPutRequest(path, Entity.entity(policies, MediaType.APPLICATION_JSON));
        } else {
            // For V1 API, we pass the BundlesData on creation
            return asyncPutRequest(path, Entity.entity(bundlesData, MediaType.APPLICATION_JSON));
        }
    }

    @Override
    public void createNamespace(String namespace) throws PulsarAdminException {
        sync(() -> createNamespaceAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteNamespace(String namespace) throws PulsarAdminException {
        sync(() -> deleteNamespaceAsync(namespace));
    }

    @Override
    public void deleteNamespace(String namespace, boolean force) throws PulsarAdminException {
        sync(() -> deleteNamespaceAsync(namespace, force));
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceAsync(String namespace) {
        return deleteNamespaceAsync(namespace, false);
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceAsync(String namespace, boolean force) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns);
        path = path.queryParam("force", force);
        return asyncDeleteRequest(path);
    }

    @Override
    public void deleteNamespaceBundle(String namespace, String bundleRange) throws PulsarAdminException {
        sync(() -> deleteNamespaceBundleAsync(namespace, bundleRange));
    }

    @Override
    public void deleteNamespaceBundle(String namespace, String bundleRange, boolean force) throws PulsarAdminException {
        sync(() -> deleteNamespaceBundleAsync(namespace, bundleRange, force));
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceBundleAsync(String namespace, String bundleRange) {
        return deleteNamespaceBundleAsync(namespace, bundleRange, false);
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceBundleAsync(String namespace, String bundleRange, boolean force) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundleRange);
        path = path.queryParam("force", force);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, Set<AuthAction>> getPermissions(String namespace) throws PulsarAdminException {
        return sync(() -> getPermissionsAsync(namespace));
    }

    @Override
    public CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions");
        final CompletableFuture<Map<String, Set<AuthAction>>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, Set<AuthAction>>>() {
                    @Override
                    public void completed(Map<String, Set<AuthAction>> permissions) {
                        future.complete(permissions);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void grantPermissionOnNamespace(String namespace, String role, Set<AuthAction> actions)
            throws PulsarAdminException {
        sync(() -> grantPermissionOnNamespaceAsync(namespace, role, actions));
    }

    @Override
    public CompletableFuture<Void> grantPermissionOnNamespaceAsync(
            String namespace, String role, Set<AuthAction> actions) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions", role);
        return asyncPostRequest(path, Entity.entity(actions, MediaType.APPLICATION_JSON));
    }

    @Override
    public void revokePermissionsOnNamespace(String namespace, String role) throws PulsarAdminException {
        sync(() -> revokePermissionsOnNamespaceAsync(namespace, role));
    }

    @Override
    public CompletableFuture<Void> revokePermissionsOnNamespaceAsync(String namespace, String role) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions", role);
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<String, Set<String>> getPermissionOnSubscription(String namespace) throws PulsarAdminException {
        return sync(() -> getPermissionOnSubscriptionAsync(namespace));
    }

    @Override
    public CompletableFuture<Map<String, Set<String>>> getPermissionOnSubscriptionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions", "subscription");
        final CompletableFuture<Map<String, Set<String>>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<String, Set<String>>>() {
                    @Override
                    public void completed(Map<String, Set<String>> permissions) {
                        future.complete(permissions);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void grantPermissionOnSubscription(String namespace, String subscription, Set<String> roles)
            throws PulsarAdminException {
        sync(() -> grantPermissionOnSubscriptionAsync(namespace, subscription, roles));
    }

    @Override
    public CompletableFuture<Void> grantPermissionOnSubscriptionAsync(
            String namespace, String subscription, Set<String> roles) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions", "subscription", subscription);
        return asyncPostRequest(path, Entity.entity(roles, MediaType.APPLICATION_JSON));
    }

    @Override
    public void revokePermissionOnSubscription(
            String namespace, String subscription, String role) throws PulsarAdminException {
        sync(() -> revokePermissionOnSubscriptionAsync(namespace, subscription, role));
    }

    @Override
    public CompletableFuture<Void> revokePermissionOnSubscriptionAsync(
            String namespace, String subscription, String role) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "permissions", subscription, role);
        return asyncDeleteRequest(path);
    }

    @Override
    public List<String> getNamespaceReplicationClusters(String namespace) throws PulsarAdminException {
        return sync(() -> getNamespaceReplicationClustersAsync(namespace));
    }

    @Override
    public CompletableFuture<List<String>> getNamespaceReplicationClustersAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "replication");
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
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
    public void setNamespaceReplicationClusters(String namespace, Set<String> clusterIds) throws PulsarAdminException {
        sync(() -> setNamespaceReplicationClustersAsync(namespace, clusterIds));
    }

    @Override
    public CompletableFuture<Void> setNamespaceReplicationClustersAsync(String namespace, Set<String> clusterIds) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "replication");
        return asyncPostRequest(path, Entity.entity(clusterIds, MediaType.APPLICATION_JSON));
    }

    @Override
    public Integer getNamespaceMessageTTL(String namespace) throws PulsarAdminException {
        return sync(() -> getNamespaceMessageTTLAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getNamespaceMessageTTLAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "messageTTL");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer ttl) {
                        future.complete(ttl);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setNamespaceMessageTTL(String namespace, int ttlInSeconds) throws PulsarAdminException {
        sync(() -> setNamespaceMessageTTLAsync(namespace, ttlInSeconds));
    }

    @Override
    public CompletableFuture<Void> setNamespaceMessageTTLAsync(String namespace, int ttlInSeconds) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "messageTTL");
        return asyncPostRequest(path, Entity.entity(ttlInSeconds, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeNamespaceMessageTTL(String namespace) throws PulsarAdminException {
        sync(() -> removeNamespaceMessageTTLAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeNamespaceMessageTTLAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "messageTTL");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getSubscriptionExpirationTime(String namespace) throws PulsarAdminException {
        return sync(() -> getSubscriptionExpirationTimeAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getSubscriptionExpirationTimeAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionExpirationTime");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<Integer>() {
            @Override
            public void completed(Integer expirationTime) {
                future.complete(expirationTime);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public void setSubscriptionExpirationTime(String namespace, int expirationTime)
            throws PulsarAdminException {
        sync(() -> setSubscriptionExpirationTimeAsync(namespace, expirationTime));
    }

    @Override
    public CompletableFuture<Void> setSubscriptionExpirationTimeAsync(String namespace, int expirationTime) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionExpirationTime");
        return asyncPostRequest(path, Entity.entity(expirationTime, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeSubscriptionExpirationTime(String namespace) throws PulsarAdminException {
        sync(() -> removeSubscriptionExpirationTimeAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeSubscriptionExpirationTimeAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionExpirationTime");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setNamespaceAntiAffinityGroup(String namespace, String namespaceAntiAffinityGroup)
            throws PulsarAdminException {
        sync(() -> setNamespaceAntiAffinityGroupAsync(namespace, namespaceAntiAffinityGroup));
    }

    @Override
    public CompletableFuture<Void> setNamespaceAntiAffinityGroupAsync(
            String namespace, String namespaceAntiAffinityGroup) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "antiAffinity");
        return asyncPostRequest(path, Entity.entity(namespaceAntiAffinityGroup, MediaType.APPLICATION_JSON));
    }

    @Override
    public String getNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException {
        return sync(() -> getNamespaceAntiAffinityGroupAsync(namespace));
    }

    @Override
    public CompletableFuture<String> getNamespaceAntiAffinityGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "antiAffinity");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String s) {
                        future.complete(s);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public List<String> getAntiAffinityNamespaces(String tenant, String cluster, String namespaceAntiAffinityGroup)
            throws PulsarAdminException {
        return sync(() -> getAntiAffinityNamespacesAsync(tenant, cluster, namespaceAntiAffinityGroup));
    }

    @Override
    public CompletableFuture<List<String>> getAntiAffinityNamespacesAsync(
            String tenant, String cluster, String namespaceAntiAffinityGroup) {
        WebTarget path = adminNamespaces.path(cluster)
                .path("antiAffinity").path(namespaceAntiAffinityGroup).queryParam("property", tenant);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<List<String>>() {
                    @Override
                    public void completed(List<String> antiNamespaces) {
                        future.complete(antiNamespaces);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void deleteNamespaceAntiAffinityGroup(String namespace) throws PulsarAdminException {
        sync(() -> deleteNamespaceAntiAffinityGroupAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceAntiAffinityGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "antiAffinity");
        return asyncDeleteRequest(path);
    }

    @Override
    public void removeDeduplicationStatus(String namespace) throws PulsarAdminException {
        sync(() -> removeDeduplicationStatusAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeDeduplicationStatusAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "deduplication");
        return asyncDeleteRequest(path);
    }

    @Override
    public Boolean getDeduplicationStatus(String namespace) throws PulsarAdminException {
        return sync(() -> getDeduplicationStatusAsync(namespace));
    }

    @Override
    public CompletableFuture<Boolean> getDeduplicationStatusAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "deduplication");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Boolean>() {
                    @Override
                    public void completed(Boolean enabled) {
                        future.complete(enabled);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setDeduplicationStatus(String namespace, boolean enableDeduplication) throws PulsarAdminException {
        sync(() -> setDeduplicationStatusAsync(namespace, enableDeduplication));
    }

    @Override
    public CompletableFuture<Void> setDeduplicationStatusAsync(String namespace, boolean enableDeduplication) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "deduplication");
        return asyncPostRequest(path, Entity.entity(enableDeduplication, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setAutoTopicCreation(String namespace,
                                     AutoTopicCreationOverride autoTopicCreationOverride) throws PulsarAdminException {
        sync(() -> setAutoTopicCreationAsync(namespace, autoTopicCreationOverride));
    }

    @Override
    public CompletableFuture<Void> setAutoTopicCreationAsync(
            String namespace, AutoTopicCreationOverride autoTopicCreationOverride) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoTopicCreation");
        return asyncPostRequest(path, Entity.entity(autoTopicCreationOverride, MediaType.APPLICATION_JSON));
    }

    @Override
    public AutoTopicCreationOverride getAutoTopicCreation(String namespace) throws PulsarAdminException {
        return sync(() -> getAutoTopicCreationAsync(namespace));
    }

    @Override
    public CompletableFuture<AutoTopicCreationOverride> getAutoTopicCreationAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoTopicCreation");
        final CompletableFuture<AutoTopicCreationOverride> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<AutoTopicCreationOverride>() {
                    @Override
                    public void completed(AutoTopicCreationOverride autoTopicCreationOverride) {
                        future.complete(autoTopicCreationOverride);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void removeAutoTopicCreation(String namespace) throws PulsarAdminException {
        sync(() -> removeAutoTopicCreationAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeAutoTopicCreationAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoTopicCreation");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setAutoSubscriptionCreation(String namespace,
            AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) throws PulsarAdminException {
        sync(() -> setAutoSubscriptionCreationAsync(namespace, autoSubscriptionCreationOverride));
    }

    @Override
    public CompletableFuture<Void> setAutoSubscriptionCreationAsync(String namespace,
            AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoSubscriptionCreation");
        return asyncPostRequest(path, Entity.entity(autoSubscriptionCreationOverride, MediaType.APPLICATION_JSON));
    }

    @Override
    public AutoSubscriptionCreationOverride getAutoSubscriptionCreation(String namespace) throws PulsarAdminException {
        return sync(() -> getAutoSubscriptionCreationAsync(namespace));
    }

    @Override
    public CompletableFuture<AutoSubscriptionCreationOverride> getAutoSubscriptionCreationAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoSubscriptionCreation");
        final CompletableFuture<AutoSubscriptionCreationOverride> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<AutoSubscriptionCreationOverride>() {
                    @Override
                    public void completed(AutoSubscriptionCreationOverride autoSubscriptionCreation) {
                        future.complete(autoSubscriptionCreation);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }


    @Override
    public void setSubscriptionTypesEnabled(
            String namespace, Set<SubscriptionType> subscriptionTypesEnabled) throws PulsarAdminException {
        sync(() -> setSubscriptionTypesEnabledAsync(namespace, subscriptionTypesEnabled));
    }

    @Override
    public CompletableFuture<Void> setSubscriptionTypesEnabledAsync(String namespace,
                                                                    Set<SubscriptionType> subscriptionTypes) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionTypesEnabled");
        return asyncPostRequest(path, Entity.entity(subscriptionTypes, MediaType.APPLICATION_JSON));
    }

    @Override
    public Set<SubscriptionType> getSubscriptionTypesEnabled(String namespace) throws PulsarAdminException {
        return sync(() -> getSubscriptionTypesEnabledAsync(namespace));
    }

    @Override
    public CompletableFuture<Set<SubscriptionType>> getSubscriptionTypesEnabledAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionTypesEnabled");
        final CompletableFuture<Set<SubscriptionType>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Set<SubscriptionType>>() {
                    @Override
                    public void completed(Set<SubscriptionType> subscriptionTypesEnabled) {
                        future.complete(subscriptionTypesEnabled);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void removeSubscriptionTypesEnabled(String namespace) throws PulsarAdminException {
        sync(() -> removeSubscriptionTypesEnabledAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeSubscriptionTypesEnabledAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionTypesEnabled");
        return asyncDeleteRequest(path);
    }

    @Override
    public void removeAutoSubscriptionCreation(String namespace) throws PulsarAdminException {
        sync(() -> removeAutoSubscriptionCreationAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeAutoSubscriptionCreationAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "autoSubscriptionCreation");
        return asyncDeleteRequest(path);
    }

    @Override
    public Map<BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String namespace) throws PulsarAdminException {
        return sync(() -> getBacklogQuotaMapAsync(namespace));
    }

    @Override
    public CompletableFuture<Map<BacklogQuotaType, BacklogQuota>> getBacklogQuotaMapAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "backlogQuotaMap");
        final CompletableFuture<Map<BacklogQuotaType, BacklogQuota>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Map<BacklogQuotaType, BacklogQuota>>() {
                    @Override
                    public void completed(Map<BacklogQuotaType, BacklogQuota> quotaMap) {
                        future.complete(quotaMap);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setBacklogQuota(String namespace, BacklogQuota backlogQuota,
                                BacklogQuota.BacklogQuotaType backlogQuotaType) throws PulsarAdminException {
        sync(() -> setBacklogQuotaAsync(namespace, backlogQuota, backlogQuotaType));
    }

    @Override
    public CompletableFuture<Void> setBacklogQuotaAsync(String namespace, BacklogQuota backlogQuota,
                                                        BacklogQuota.BacklogQuotaType backlogQuotaType) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "backlogQuota");
        return asyncPostRequest(path.queryParam("backlogQuotaType", backlogQuotaType.toString()),
                Entity.entity(backlogQuota, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeBacklogQuota(String namespace, BacklogQuota.BacklogQuotaType backlogQuotaType)
            throws PulsarAdminException {
        sync(() -> removeBacklogQuotaAsync(namespace, backlogQuotaType));
    }

    @Override
    public void removeInactiveTopicPolicies(String namespace) throws PulsarAdminException {
        sync(() -> removeInactiveTopicPoliciesAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "inactiveTopicPolicies");
        return asyncDeleteRequest(path);
    }

    @Override
    public CompletableFuture<Void> removeBacklogQuotaAsync(String namespace,
                                                           BacklogQuota.BacklogQuotaType backlogQuotaType) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "backlogQuota")
                .queryParam("backlogQuotaType", backlogQuotaType.toString());
        return asyncDeleteRequest(path);
    }

    @Override
    public void removePersistence(String namespace) throws PulsarAdminException {
        sync(() -> removePersistenceAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removePersistenceAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setPersistence(String namespace, PersistencePolicies persistence) throws PulsarAdminException {
        sync(() -> setPersistenceAsync(namespace, persistence));
    }

    @Override
    public CompletableFuture<Void> setPersistenceAsync(String namespace, PersistencePolicies persistence) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence");
        return asyncPostRequest(path, Entity.entity(persistence, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setBookieAffinityGroup(
            String namespace, BookieAffinityGroupData bookieAffinityGroup) throws PulsarAdminException {
        sync(() -> setBookieAffinityGroupAsync(namespace, bookieAffinityGroup));
    }

    @Override
    public CompletableFuture<Void> setBookieAffinityGroupAsync(
            String namespace, BookieAffinityGroupData bookieAffinityGroup) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence", "bookieAffinity");
        return asyncPostRequest(path, Entity.entity(bookieAffinityGroup, MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteBookieAffinityGroup(String namespace) throws PulsarAdminException {
        sync(() -> deleteBookieAffinityGroupAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> deleteBookieAffinityGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence", "bookieAffinity");
        return asyncDeleteRequest(path);
    }

    @Override
    public BookieAffinityGroupData getBookieAffinityGroup(String namespace) throws PulsarAdminException {
        return sync(() -> getBookieAffinityGroupAsync(namespace));
    }

    @Override
    public CompletableFuture<BookieAffinityGroupData> getBookieAffinityGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence", "bookieAffinity");
        final CompletableFuture<BookieAffinityGroupData> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<BookieAffinityGroupData>() {
                    @Override
                    public void completed(BookieAffinityGroupData bookieAffinityGroupData) {
                        future.complete(bookieAffinityGroupData);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public PersistencePolicies getPersistence(String namespace) throws PulsarAdminException {
        return sync(() -> getPersistenceAsync(namespace));
    }

    @Override
    public CompletableFuture<PersistencePolicies> getPersistenceAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "persistence");
        final CompletableFuture<PersistencePolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<PersistencePolicies>() {
                    @Override
                    public void completed(PersistencePolicies persistencePolicies) {
                        future.complete(persistencePolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setRetention(String namespace, RetentionPolicies retention) throws PulsarAdminException {
        sync(() -> setRetentionAsync(namespace, retention));
    }

    @Override
    public CompletableFuture<Void> setRetentionAsync(String namespace, RetentionPolicies retention) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "retention");
        return asyncPostRequest(path, Entity.entity(retention, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeRetention(String namespace) throws PulsarAdminException {
        sync(() -> removeRetentionAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeRetentionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "retention");
        return asyncDeleteRequest(path);
    }

    @Override
    public RetentionPolicies getRetention(String namespace) throws PulsarAdminException {
        return sync(() -> getRetentionAsync(namespace));
    }

    @Override
    public CompletableFuture<RetentionPolicies> getRetentionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "retention");
        final CompletableFuture<RetentionPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<RetentionPolicies>() {
                    @Override
                    public void completed(RetentionPolicies retentionPolicies) {
                        future.complete(retentionPolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void unload(String namespace) throws PulsarAdminException {
        sync(() -> unloadAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> unloadAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "unload");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public String getReplicationConfigVersion(String namespace) throws PulsarAdminException {
        return sync(() -> getReplicationConfigVersionAsync(namespace));
    }

    @Override
    public CompletableFuture<String> getReplicationConfigVersionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "configversion");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String s) {
                        future.complete(s);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void unloadNamespaceBundle(String namespace, String bundle) throws PulsarAdminException {
        sync(() -> unloadNamespaceBundleAsync(namespace, bundle));
    }

    @Override
    public CompletableFuture<Void> unloadNamespaceBundleAsync(String namespace, String bundle) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "unload");
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void splitNamespaceBundle(
            String namespace, String bundle, boolean unloadSplitBundles, String splitAlgorithmName)
            throws PulsarAdminException {
        sync(() -> splitNamespaceBundleAsync(namespace, bundle, unloadSplitBundles, splitAlgorithmName));
    }

    @Override
    public CompletableFuture<Void> splitNamespaceBundleAsync(
            String namespace, String bundle, boolean unloadSplitBundles, String splitAlgorithmName) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "split")
                .queryParam("unload", Boolean.toString(unloadSplitBundles))
                .queryParam("splitAlgorithmName", splitAlgorithmName);
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void setPublishRate(String namespace, PublishRate publishMsgRate) throws PulsarAdminException {
        sync(() -> setPublishRateAsync(namespace, publishMsgRate));
    }

    @Override
    public void removePublishRate(String namespace) throws PulsarAdminException {
        sync(() -> removePublishRateAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> setPublishRateAsync(String namespace, PublishRate publishMsgRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "publishRate");
        return asyncPostRequest(path, Entity.entity(publishMsgRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<Void> removePublishRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "publishRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public PublishRate getPublishRate(String namespace) throws PulsarAdminException {
        return sync(() -> getPublishRateAsync(namespace));
    }

    @Override
    public CompletableFuture<PublishRate> getPublishRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "publishRate");
        final CompletableFuture<PublishRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<PublishRate>() {
                    @Override
                    public void completed(PublishRate publishRate) {
                        future.complete(publishRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void removeDispatchRate(String namespace) throws PulsarAdminException {
        sync(() -> removeDispatchRateAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeDispatchRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "dispatchRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public void setDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        sync(() -> setDispatchRateAsync(namespace, dispatchRate));
    }

    @Override
    public CompletableFuture<Void> setDispatchRateAsync(String namespace, DispatchRate dispatchRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "dispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public DispatchRate getDispatchRate(String namespace) throws PulsarAdminException {
        return sync(() -> getDispatchRateAsync(namespace));
    }

    @Override
    public CompletableFuture<DispatchRate> getDispatchRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "dispatchRate");
        final CompletableFuture<DispatchRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DispatchRate>() {
                    @Override
                    public void completed(DispatchRate dispatchRate) {
                        future.complete(dispatchRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setSubscribeRate(String namespace, SubscribeRate subscribeRate) throws PulsarAdminException {
        sync(() -> setSubscribeRateAsync(namespace, subscribeRate));
    }

    @Override
    public CompletableFuture<Void> setSubscribeRateAsync(String namespace, SubscribeRate subscribeRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscribeRate");
        return asyncPostRequest(path, Entity.entity(subscribeRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeSubscribeRate(String namespace) throws PulsarAdminException {
        sync(() -> removeSubscribeRateAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeSubscribeRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscribeRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public SubscribeRate getSubscribeRate(String namespace) throws PulsarAdminException {
        return sync(() -> getSubscribeRateAsync(namespace));
    }

    @Override
    public CompletableFuture<SubscribeRate> getSubscribeRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscribeRate");
        final CompletableFuture<SubscribeRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<SubscribeRate>() {
                    @Override
                    public void completed(SubscribeRate subscribeRate) {
                        future.complete(subscribeRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void removeSubscriptionDispatchRate(String namespace) throws PulsarAdminException {
        sync(() -> removeSubscriptionDispatchRateAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeSubscriptionDispatchRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionDispatchRate");
        return asyncDeleteRequest(path);
    }


    @Override
    public void setSubscriptionDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        sync(() -> setSubscriptionDispatchRateAsync(namespace, dispatchRate));
    }

    @Override
    public CompletableFuture<Void> setSubscriptionDispatchRateAsync(String namespace, DispatchRate dispatchRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionDispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String namespace) throws PulsarAdminException {
        return sync(() -> getSubscriptionDispatchRateAsync(namespace));
    }

    @Override
    public CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionDispatchRate");
        final CompletableFuture<DispatchRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DispatchRate>() {
                    @Override
                    public void completed(DispatchRate dispatchRate) {
                        future.complete(dispatchRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setReplicatorDispatchRate(String namespace, DispatchRate dispatchRate) throws PulsarAdminException {
        sync(() -> setReplicatorDispatchRateAsync(namespace, dispatchRate));
    }

    @Override
    public CompletableFuture<Void> setReplicatorDispatchRateAsync(String namespace, DispatchRate dispatchRate) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "replicatorDispatchRate");
        return asyncPostRequest(path, Entity.entity(dispatchRate, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeReplicatorDispatchRate(String namespace) throws PulsarAdminException {
        sync(() -> removeReplicatorDispatchRateAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeReplicatorDispatchRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "replicatorDispatchRate");
        return asyncDeleteRequest(path);
    }

    @Override
    public DispatchRate getReplicatorDispatchRate(String namespace) throws PulsarAdminException {
        return sync(() -> getReplicatorDispatchRateAsync(namespace));
    }

    @Override
    public CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "replicatorDispatchRate");
        final CompletableFuture<DispatchRate> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DispatchRate>() {
                    @Override
                    public void completed(DispatchRate dispatchRate) {
                        future.complete(dispatchRate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void clearNamespaceBacklog(String namespace) throws PulsarAdminException {
        sync(() -> clearNamespaceBacklogAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> clearNamespaceBacklogAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "clearBacklog");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void clearNamespaceBacklogForSubscription(String namespace, String subscription)
            throws PulsarAdminException {
        sync(() -> clearNamespaceBacklogForSubscriptionAsync(namespace, subscription));
    }

    @Override
    public CompletableFuture<Void> clearNamespaceBacklogForSubscriptionAsync(String namespace, String subscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "clearBacklog", subscription);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void clearNamespaceBundleBacklog(String namespace, String bundle) throws PulsarAdminException {
        sync(() -> clearNamespaceBundleBacklogAsync(namespace, bundle));
    }

    @Override
    public CompletableFuture<Void> clearNamespaceBundleBacklogAsync(String namespace, String bundle) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "clearBacklog");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void clearNamespaceBundleBacklogForSubscription(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        sync(() -> clearNamespaceBundleBacklogForSubscriptionAsync(namespace, bundle, subscription));
    }

    @Override
    public CompletableFuture<Void> clearNamespaceBundleBacklogForSubscriptionAsync(String namespace, String bundle,
            String subscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "clearBacklog", subscription);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void unsubscribeNamespace(String namespace, String subscription) throws PulsarAdminException {
        sync(() -> unsubscribeNamespaceAsync(namespace, subscription));
    }

    @Override
    public CompletableFuture<Void> unsubscribeNamespaceAsync(String namespace, String subscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "unsubscribe", subscription);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void unsubscribeNamespaceBundle(String namespace, String bundle, String subscription)
            throws PulsarAdminException {
        sync(() -> unsubscribeNamespaceBundleAsync(namespace, bundle, subscription));
    }

    @Override
    public CompletableFuture<Void> unsubscribeNamespaceBundleAsync(String namespace, String bundle,
            String subscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, bundle, "unsubscribe", subscription);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void setSubscriptionAuthMode(String namespace, SubscriptionAuthMode subscriptionAuthMode)
            throws PulsarAdminException {
        sync(() -> setSubscriptionAuthModeAsync(namespace, subscriptionAuthMode));
    }

    @Override
    public CompletableFuture<Void> setSubscriptionAuthModeAsync(
            String namespace, SubscriptionAuthMode subscriptionAuthMode) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionAuthMode");
        return asyncPostRequest(path, Entity.entity(subscriptionAuthMode, MediaType.APPLICATION_JSON));
    }

    @Override
    public SubscriptionAuthMode getSubscriptionAuthMode(String namespace) throws PulsarAdminException {
        return sync(() -> getSubscriptionAuthModeAsync(namespace));
    }

    @Override
    public CompletableFuture<SubscriptionAuthMode> getSubscriptionAuthModeAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "subscriptionAuthMode");
        final CompletableFuture<SubscriptionAuthMode> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<SubscriptionAuthMode>() {
                    @Override
                    public void completed(SubscriptionAuthMode subscriptionAuthMode) {
                        future.complete(subscriptionAuthMode);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setEncryptionRequiredStatus(String namespace, boolean encryptionRequired) throws PulsarAdminException {
        sync(() -> setEncryptionRequiredStatusAsync(namespace, encryptionRequired));
    }

    @Override
    public CompletableFuture<Void> setEncryptionRequiredStatusAsync(String namespace, boolean encryptionRequired) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "encryptionRequired");
        return asyncPostRequest(path, Entity.entity(encryptionRequired, MediaType.APPLICATION_JSON));
    }

    @Override
    public Boolean getEncryptionRequiredStatus(String namespace) throws PulsarAdminException {
        return sync(() -> getEncryptionRequiredStatusAsync(namespace));
    }

    @Override
    public CompletableFuture<Boolean> getEncryptionRequiredStatusAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "encryptionRequired");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Boolean>() {
                    @Override
                    public void completed(Boolean enabled) {
                        future.complete(enabled);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public DelayedDeliveryPolicies getDelayedDelivery(String namespace) throws PulsarAdminException {
        return sync(() -> getDelayedDeliveryAsync(namespace));
    }

    @Override
    public CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "delayedDelivery");
        final CompletableFuture<DelayedDeliveryPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<DelayedDeliveryPolicies>() {
                    @Override
                    public void completed(DelayedDeliveryPolicies delayedDeliveryPolicies) {
                        future.complete(delayedDeliveryPolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setDelayedDeliveryMessages(
            String namespace, DelayedDeliveryPolicies delayedDeliveryPolicies) throws PulsarAdminException {
        sync(() -> setDelayedDeliveryMessagesAsync(namespace, delayedDeliveryPolicies));
    }

    @Override
    public CompletableFuture<Void> setDelayedDeliveryMessagesAsync(
            String namespace, DelayedDeliveryPolicies delayedDeliveryPolicies) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "delayedDelivery");
        return asyncPostRequest(path, Entity.entity(delayedDeliveryPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeDelayedDeliveryMessages(String namespace) throws PulsarAdminException {
        sync(() -> removeDelayedDeliveryMessagesAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeDelayedDeliveryMessagesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "delayedDelivery");
        return asyncDeleteRequest(path);
    }

    @Override
    public InactiveTopicPolicies getInactiveTopicPolicies(String namespace) throws PulsarAdminException {
        return sync(() -> getInactiveTopicPoliciesAsync(namespace));
    }

    @Override
    public CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "inactiveTopicPolicies");
        final CompletableFuture<InactiveTopicPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<InactiveTopicPolicies>() {
                    @Override
                    public void completed(InactiveTopicPolicies inactiveTopicPolicies) {
                        future.complete(inactiveTopicPolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setInactiveTopicPolicies(
            String namespace, InactiveTopicPolicies inactiveTopicPolicies) throws PulsarAdminException {
        sync(() -> setInactiveTopicPoliciesAsync(namespace, inactiveTopicPolicies));
    }

    @Override
    public CompletableFuture<Void> setInactiveTopicPoliciesAsync(
            String namespace, InactiveTopicPolicies inactiveTopicPolicies) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "inactiveTopicPolicies");
        return asyncPostRequest(path, Entity.entity(inactiveTopicPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public Integer getDeduplicationSnapshotInterval(String namespace) throws PulsarAdminException {
        return sync(() -> getDeduplicationSnapshotIntervalAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getDeduplicationSnapshotIntervalAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "deduplicationSnapshotInterval");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer interval) {
                        future.complete(interval);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setDeduplicationSnapshotInterval(String namespace, Integer interval) throws PulsarAdminException {
        sync(() -> setDeduplicationSnapshotIntervalAsync(namespace, interval));
    }

    @Override
    public CompletableFuture<Void> setDeduplicationSnapshotIntervalAsync(String namespace, Integer interval) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "deduplicationSnapshotInterval");
        return asyncPostRequest(path, Entity.entity(interval, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeDeduplicationSnapshotInterval(String namespace) throws PulsarAdminException {
        setDeduplicationSnapshotInterval(namespace, null);
    }

    @Override
    public CompletableFuture<Void> removeDeduplicationSnapshotIntervalAsync(String namespace) {
        return setDeduplicationSnapshotIntervalAsync(namespace, null);
    }

    @Override
    public Integer getMaxSubscriptionsPerTopic(String namespace) throws PulsarAdminException {
        return sync(() -> getMaxSubscriptionsPerTopicAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getMaxSubscriptionsPerTopicAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxSubscriptionsPerTopic");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer maxSubscriptionsPerTopic) {
                        future.complete(maxSubscriptionsPerTopic);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxSubscriptionsPerTopic(String namespace, int maxSubscriptionsPerTopic)
            throws PulsarAdminException {
        sync(() -> setMaxSubscriptionsPerTopicAsync(namespace, maxSubscriptionsPerTopic));
    }

    @Override
    public CompletableFuture<Void> setMaxSubscriptionsPerTopicAsync(String namespace, int maxSubscriptionsPerTopic) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxSubscriptionsPerTopic");
        return asyncPostRequest(path, Entity.entity(maxSubscriptionsPerTopic, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxSubscriptionsPerTopic(String namespace) throws PulsarAdminException {
        sync(() -> removeMaxSubscriptionsPerTopicAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeMaxSubscriptionsPerTopicAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxSubscriptionsPerTopic");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxProducersPerTopic(String namespace) throws PulsarAdminException {
        return sync(() -> getMaxProducersPerTopicAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getMaxProducersPerTopicAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxProducersPerTopic");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxProducersPerTopic(String namespace, int maxProducersPerTopic) throws PulsarAdminException {
        sync(() -> setMaxProducersPerTopicAsync(namespace, maxProducersPerTopic));
    }

    @Override
    public CompletableFuture<Void> setMaxProducersPerTopicAsync(String namespace, int maxProducersPerTopic) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxProducersPerTopic");
        return asyncPostRequest(path, Entity.entity(maxProducersPerTopic, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxProducersPerTopic(String namespace) throws PulsarAdminException {
        sync(() -> removeMaxProducersPerTopicAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeMaxProducersPerTopicAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxProducersPerTopic");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxConsumersPerTopic(String namespace) throws PulsarAdminException {
        return sync(() -> getMaxConsumersPerTopicAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersPerTopicAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerTopic");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxConsumersPerTopic(String namespace, int maxConsumersPerTopic) throws PulsarAdminException {
        sync(() -> setMaxConsumersPerTopicAsync(namespace, maxConsumersPerTopic));
    }

    @Override
    public CompletableFuture<Void> setMaxConsumersPerTopicAsync(String namespace, int maxConsumersPerTopic) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerTopic");
        return asyncPostRequest(path, Entity.entity(maxConsumersPerTopic, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxConsumersPerTopic(String namespace) throws PulsarAdminException {
        sync(() -> removeMaxConsumersPerTopicAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeMaxConsumersPerTopicAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerTopic");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxConsumersPerSubscription(String namespace) throws PulsarAdminException {
        return sync(() -> getMaxConsumersPerSubscriptionAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersPerSubscriptionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerSubscription");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxConsumersPerSubscription(String namespace, int maxConsumersPerSubscription)
            throws PulsarAdminException {
        sync(() -> setMaxConsumersPerSubscriptionAsync(namespace, maxConsumersPerSubscription));
    }

    @Override
    public CompletableFuture<Void> setMaxConsumersPerSubscriptionAsync(
            String namespace, int maxConsumersPerSubscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerSubscription");
        return asyncPostRequest(path, Entity.entity(maxConsumersPerSubscription, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxConsumersPerSubscription(String namespace)
            throws PulsarAdminException {
        sync(() -> removeMaxConsumersPerSubscriptionAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeMaxConsumersPerSubscriptionAsync(
            String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxConsumersPerSubscription");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxUnackedMessagesPerConsumer(String namespace) throws PulsarAdminException {
        return sync(() -> getMaxUnackedMessagesPerConsumerAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesPerConsumerAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerConsumer");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxUnackedMessagesPerConsumer(String namespace, int maxUnackedMessagesPerConsumer)
            throws PulsarAdminException {
        sync(() -> setMaxUnackedMessagesPerConsumerAsync(namespace, maxUnackedMessagesPerConsumer));
    }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesPerConsumerAsync(
            String namespace, int maxUnackedMessagesPerConsumer) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerConsumer");
        return asyncPostRequest(path, Entity.entity(maxUnackedMessagesPerConsumer, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxUnackedMessagesPerConsumer(String namespace) throws PulsarAdminException {
        sync(() -> removeMaxUnackedMessagesPerConsumerAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeMaxUnackedMessagesPerConsumerAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerConsumer");
        return asyncDeleteRequest(path);
    }

    @Override
    public Integer getMaxUnackedMessagesPerSubscription(String namespace) throws PulsarAdminException {
        return sync(() -> getMaxUnackedMessagesPerSubscriptionAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesPerSubscriptionAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerSubscription");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer max) {
                        future.complete(max);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxUnackedMessagesPerSubscription(String namespace, int maxUnackedMessagesPerSubscription)
            throws PulsarAdminException {
        sync(() -> setMaxUnackedMessagesPerSubscriptionAsync(namespace, maxUnackedMessagesPerSubscription));
    }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesPerSubscriptionAsync(
            String namespace, int maxUnackedMessagesPerSubscription) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerSubscription");
        return asyncPostRequest(path, Entity.entity(maxUnackedMessagesPerSubscription, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxUnackedMessagesPerSubscription(String namespace)
            throws PulsarAdminException {
        sync(() -> removeMaxUnackedMessagesPerSubscriptionAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeMaxUnackedMessagesPerSubscriptionAsync(
            String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxUnackedMessagesPerSubscription");
        return asyncDeleteRequest(path);
    }

    @Override
    public Long getCompactionThreshold(String namespace) throws PulsarAdminException {
        return sync(() -> getCompactionThresholdAsync(namespace));
    }

    @Override
    public CompletableFuture<Long> getCompactionThresholdAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "compactionThreshold");
        final CompletableFuture<Long> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Long>() {
                    @Override
                    public void completed(Long threshold) {
                        future.complete(threshold);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setCompactionThreshold(String namespace, long compactionThreshold) throws PulsarAdminException {
        sync(() -> setCompactionThresholdAsync(namespace, compactionThreshold));
    }

    @Override
    public CompletableFuture<Void> setCompactionThresholdAsync(String namespace, long compactionThreshold) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "compactionThreshold");
        return asyncPutRequest(path, Entity.entity(compactionThreshold, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeCompactionThreshold(String namespace) throws PulsarAdminException {
        sync(() -> removeCompactionThresholdAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeCompactionThresholdAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "compactionThreshold");
        return asyncDeleteRequest(path);
    }

    @Override
    public long getOffloadThreshold(String namespace) throws PulsarAdminException {
        return sync(() -> getOffloadThresholdAsync(namespace));
    }

    @Override
    public CompletableFuture<Long> getOffloadThresholdAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadThreshold");
        final CompletableFuture<Long> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Long>() {
                    @Override
                    public void completed(Long threshold) {
                        future.complete(threshold);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setOffloadThreshold(String namespace, long offloadThreshold) throws PulsarAdminException {
        sync(() -> setOffloadThresholdAsync(namespace, offloadThreshold));
    }

    @Override
    public CompletableFuture<Void> setOffloadThresholdAsync(String namespace, long offloadThreshold) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadThreshold");
        return asyncPutRequest(path, Entity.entity(offloadThreshold, MediaType.APPLICATION_JSON));
    }

    @Override
    public Long getOffloadDeleteLagMs(String namespace) throws PulsarAdminException {
        return sync(() -> getOffloadDeleteLagMsAsync(namespace));
    }

    @Override
    public CompletableFuture<Long> getOffloadDeleteLagMsAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
        final CompletableFuture<Long> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Long>() {
                    @Override
                    public void completed(Long lag) {
                        future.complete(lag);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setOffloadDeleteLag(String namespace, long lag, TimeUnit unit) throws PulsarAdminException {
        sync(() -> setOffloadDeleteLagAsync(namespace, lag, unit));
    }

    @Override
    public CompletableFuture<Void> setOffloadDeleteLagAsync(String namespace, long lag, TimeUnit unit) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
        return asyncPutRequest(path, Entity.entity(
                TimeUnit.MILLISECONDS.convert(lag, unit), MediaType.APPLICATION_JSON));
    }

    @Override
    public void clearOffloadDeleteLag(String namespace) throws PulsarAdminException {
        sync(() -> clearOffloadDeleteLagAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> clearOffloadDeleteLagAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadDeletionLagMs");
        return asyncDeleteRequest(path);
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
    public boolean getSchemaValidationEnforced(String namespace) throws PulsarAdminException {
        return getSchemaValidationEnforced(namespace, false);
    }

    @Override
    public CompletableFuture<Boolean> getSchemaValidationEnforcedAsync(String namespace) {
        return getSchemaValidationEnforcedAsync(namespace, false);
    }

    @Override
    public boolean getSchemaValidationEnforced(String namespace, boolean applied)
            throws PulsarAdminException {
        return sync(() -> getSchemaValidationEnforcedAsync(namespace, applied));
    }

    @Override
    public CompletableFuture<Boolean> getSchemaValidationEnforcedAsync(String namespace, boolean applied) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "schemaValidationEnforced");
        path = path.queryParam("applied", applied);
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Boolean>() {
                    @Override
                    public void completed(Boolean enforced) {
                        future.complete(enforced);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setSchemaValidationEnforced(String namespace, boolean schemaValidationEnforced)
            throws PulsarAdminException {
        sync(() -> setSchemaValidationEnforcedAsync(namespace, schemaValidationEnforced));
    }

    @Override
    public CompletableFuture<Void> setSchemaValidationEnforcedAsync(
            String namespace, boolean schemaValidationEnforced) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "schemaValidationEnforced");
        return asyncPostRequest(path, Entity.entity(schemaValidationEnforced, MediaType.APPLICATION_JSON));
    }

    @Override
    public SchemaCompatibilityStrategy getSchemaCompatibilityStrategy(String namespace) throws PulsarAdminException {
        return sync(() -> getSchemaCompatibilityStrategyAsync(namespace));
    }

    @Override
    public CompletableFuture<SchemaCompatibilityStrategy> getSchemaCompatibilityStrategyAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "schemaCompatibilityStrategy");
        final CompletableFuture<SchemaCompatibilityStrategy> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<SchemaCompatibilityStrategy>() {
                    @Override
                    public void completed(SchemaCompatibilityStrategy schemaCompatibilityStrategy) {
                        future.complete(schemaCompatibilityStrategy);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setSchemaCompatibilityStrategy(String namespace, SchemaCompatibilityStrategy strategy)
            throws PulsarAdminException {
        sync(() -> setSchemaCompatibilityStrategyAsync(namespace, strategy));
    }

    @Override
    public CompletableFuture<Void> setSchemaCompatibilityStrategyAsync(
            String namespace, SchemaCompatibilityStrategy strategy) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "schemaCompatibilityStrategy");
        return asyncPutRequest(path, Entity.entity(strategy, MediaType.APPLICATION_JSON));
    }

    @Override
    public boolean getIsAllowAutoUpdateSchema(String namespace) throws PulsarAdminException {
        return sync(() -> getIsAllowAutoUpdateSchemaAsync(namespace));
    }

    @Override
    public CompletableFuture<Boolean> getIsAllowAutoUpdateSchemaAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "isAllowAutoUpdateSchema");
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Boolean>() {
                    @Override
                    public void completed(Boolean allowAutoUpdate) {
                        future.complete(allowAutoUpdate);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setIsAllowAutoUpdateSchema(String namespace, boolean isAllowAutoUpdateSchema)
            throws PulsarAdminException {
        sync(() -> setIsAllowAutoUpdateSchemaAsync(namespace, isAllowAutoUpdateSchema));
    }

    @Override
    public CompletableFuture<Void> setIsAllowAutoUpdateSchemaAsync(String namespace, boolean isAllowAutoUpdateSchema) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "isAllowAutoUpdateSchema");
        return asyncPostRequest(path, Entity.entity(isAllowAutoUpdateSchema, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setOffloadPolicies(String namespace, OffloadPolicies offloadPolicies)
            throws PulsarAdminException {
        sync(() -> setOffloadPoliciesAsync(namespace, offloadPolicies));
    }

    @Override
    public void removeOffloadPolicies(String namespace) throws PulsarAdminException {
        sync(() -> removeOffloadPoliciesAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> setOffloadPoliciesAsync(String namespace, OffloadPolicies offloadPolicies) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadPolicies");
        return asyncPostRequest(path, Entity.entity(offloadPolicies, MediaType.APPLICATION_JSON));
    }

    @Override
    public CompletableFuture<Void> removeOffloadPoliciesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "removeOffloadPolicies");
        return asyncDeleteRequest(path);
    }

    @Override
    public OffloadPolicies getOffloadPolicies(String namespace) throws PulsarAdminException {
        return sync(() -> getOffloadPoliciesAsync(namespace));
    }

    @Override
    public CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "offloadPolicies");
        final CompletableFuture<OffloadPolicies> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<OffloadPoliciesImpl>() {
                    @Override
                    public void completed(OffloadPoliciesImpl offloadPolicies) {
                        future.complete(offloadPolicies);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public int getMaxTopicsPerNamespace(String namespace) throws PulsarAdminException {
        return sync(() -> getMaxTopicsPerNamespaceAsync(namespace));
    }

    @Override
    public CompletableFuture<Integer> getMaxTopicsPerNamespaceAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxTopicsPerNamespace");
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Integer>() {
                    @Override
                    public void completed(Integer maxTopicsPerNamespace) {
                        future.complete(maxTopicsPerNamespace);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void setMaxTopicsPerNamespace(String namespace, int maxTopicsPerNamespace) throws PulsarAdminException {
        sync(() -> setMaxTopicsPerNamespaceAsync(namespace, maxTopicsPerNamespace));
    }

    @Override
    public CompletableFuture<Void> setMaxTopicsPerNamespaceAsync(String namespace, int maxTopicsPerNamespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxTopicsPerNamespace");
        return asyncPostRequest(path, Entity.entity(maxTopicsPerNamespace, MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeMaxTopicsPerNamespace(String namespace) throws PulsarAdminException {
        sync(() -> removeMaxTopicsPerNamespaceAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeMaxTopicsPerNamespaceAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "maxTopicsPerNamespace");
        return asyncDeleteRequest(path);
    }

    @Override
    public CompletableFuture<Void> setPropertyAsync(String namespace, String key, String value) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "property", key, value);
        return asyncPutRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void setProperty(String namespace, String key, String value) throws PulsarAdminException {
        sync(() -> setPropertyAsync(namespace, key, value));
    }

    @Override
    public CompletableFuture<Void> setPropertiesAsync(String namespace, Map<String, String> properties) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "properties");
        return asyncPutRequest(path, Entity.entity(properties, MediaType.APPLICATION_JSON));
    }

    @Override
    public void setProperties(String namespace, Map<String, String> properties) throws PulsarAdminException {
        sync(() -> setPropertiesAsync(namespace, properties));
    }

    @Override
    public String getNamespaceResourceGroup(String namespace) throws PulsarAdminException {
        return sync(() -> getNamespaceResourceGroupAsync(namespace));
    }

    @Override
    public CompletableFuture<String> getPropertyAsync(String namespace, String key) {
        NamespaceName ns = NamespaceName.get(namespace);
        final CompletableFuture<String> future = new CompletableFuture<>();
        WebTarget path = namespacePath(ns, "property", key);
        asyncGetRequest(path, new InvocationCallback<String>() {
            @Override
            public void completed(String value) {
                future.complete(StringUtils.isEmpty(value) ? null : value);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public String getProperty(String namespace, String key) throws PulsarAdminException {
        return sync(() -> getPropertyAsync(namespace, key));
    }

    @Override
    public CompletableFuture<Map<String, String>> getPropertiesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        final CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
        WebTarget path = namespacePath(ns, "properties");
        asyncGetRequest(path, new InvocationCallback<Map<String, String>>() {
            @Override
            public void completed(Map<String, String> value) {
                future.complete(value);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<String> getNamespaceResourceGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "resourcegroup");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String rgName) {
                        future.complete(rgName);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Map<String, String> getProperties(String namespace) throws PulsarAdminException {
        return sync(() -> getPropertiesAsync(namespace));
    }

    @Override
    public void setNamespaceResourceGroup(String namespace, String resourcegroupname) throws PulsarAdminException {
        sync(() -> setNamespaceResourceGroupAsync(namespace, resourcegroupname));
    }

    @Override
    public CompletableFuture<String> removePropertyAsync(String namespace, String key) {
        NamespaceName ns = NamespaceName.get(namespace);
        final CompletableFuture<String> future = new CompletableFuture<>();
        WebTarget path = namespacePath(ns, "property", key);
        asyncDeleteRequest(path, new InvocationCallback<String>() {
            @Override
            public void completed(String value) {
                future.complete(StringUtils.isEmpty(value) ? null : value);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public String removeProperty(String namespace, String key) throws PulsarAdminException {
        return sync(() -> removePropertyAsync(namespace, key));
    }

    @Override
    public CompletableFuture<Void> setNamespaceResourceGroupAsync(String namespace, String resourcegroupname) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "resourcegroup", resourcegroupname);
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void removeNamespaceResourceGroup(String namespace) throws PulsarAdminException {
        sync(() -> removeNamespaceResourceGroupAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> clearPropertiesAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        final CompletableFuture<String> future = new CompletableFuture<>();
        WebTarget path = namespacePath(ns, "properties");
        return asyncDeleteRequest(path);
    }

    @Override
    public void clearProperties(String namespace) throws PulsarAdminException {
        sync(() -> clearPropertiesAsync(namespace));
    }

    @Override
    public CompletableFuture<Void> removeNamespaceResourceGroupAsync(String namespace) {
        NamespaceName ns = NamespaceName.get(namespace);
        WebTarget path = namespacePath(ns, "resourcegroup");
        return asyncDeleteRequest(path);
    }

    private WebTarget namespacePath(NamespaceName namespace, String... parts) {
        final WebTarget base = namespace.isV2() ? adminV2Namespaces : adminNamespaces;
        WebTarget namespacePath = base.path(namespace.toString());
        namespacePath = WebTargets.addParts(namespacePath, parts);
        return namespacePath;
    }
}
