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
package org.apache.pulsar.broker.authorization;

import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorization service that manages pluggable authorization provider and authorize requests accordingly.
 *
 */
public class AuthorizationService {
    private static final Logger log = LoggerFactory.getLogger(AuthorizationService.class);

    private final AuthorizationProvider provider;
    private final ServiceConfiguration conf;

    public AuthorizationService(ServiceConfiguration conf, PulsarResources pulsarResources)
            throws PulsarServerException {
        this.conf = conf;
        try {
            final String providerClassname = conf.getAuthorizationProvider();
            if (StringUtils.isNotBlank(providerClassname)) {
                provider = (AuthorizationProvider) Class.forName(providerClassname)
                        .getDeclaredConstructor().newInstance();
                provider.initialize(conf, pulsarResources);
                log.info("{} has been loaded.", providerClassname);
            } else {
                throw new PulsarServerException("No authorization providers are present.");
            }
        } catch (PulsarServerException e) {
            throw e;
        } catch (Throwable e) {
            throw new PulsarServerException("Failed to load an authorization provider.", e);
        }
    }

    public CompletableFuture<Boolean> isSuperUser(String user, AuthenticationDataSource authenticationData) {
        return provider.isSuperUser(user, authenticationData, conf);
    }

    public CompletableFuture<Boolean> isTenantAdmin(String tenant, String role, TenantInfo tenantInfo,
                                                    AuthenticationDataSource authenticationData) {
        return provider.isTenantAdmin(tenant, role, tenantInfo, authenticationData);
    }

    /**
     *
     * Grant authorization-action permission on a namespace to the given client.
     *
     * NOTE: used to complete with {@link IllegalArgumentException} when namespace not found or with
     * {@link IllegalStateException} when failed to grant permission.
     *
     * @param namespace
     * @param actions
     * @param role
     * @param authDataJson
     *            additional authdata in json for targeted authorization provider
     * @completesWith null when the permissions are updated successfully.
     * @completesWith {@link MetadataStoreException} when the MetadataStore is not updated.
     */
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions, String role,
                                                        String authDataJson) {
        return provider.grantPermissionAsync(namespace, actions, role, authDataJson);
    }

    /**
     * Grant permission to roles that can access subscription-admin api.
     *
     * @param namespace
     * @param subscriptionName
     * @param roles
     * @param authDataJson
     *            additional authdata in json for targeted authorization provider
     * @return
     */
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
                                                                    Set<String> roles, String authDataJson) {
        return provider.grantSubscriptionPermissionAsync(namespace, subscriptionName, roles, authDataJson);
    }

    /**
     * Revoke subscription admin-api access for a role.
     *
     * @param namespace
     * @param subscriptionName
     * @param role
     * @return
     */
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
                                                                     String role, String authDataJson) {
        return provider.revokeSubscriptionPermissionAsync(namespace, subscriptionName, role, authDataJson);
    }

    /**
     * Grant authorization-action permission on a topic to the given client.
     *
     * NOTE: used to complete with {@link IllegalArgumentException} when namespace not found or with
     * {@link IllegalStateException} when failed to grant permission.
     *
     * @param topicname
     * @param role
     * @param authDataJson
     *            additional authdata in json for targeted authorization provider
     * @completesWith null when the permissions are updated successfully.
     * @completesWith {@link MetadataStoreException} when the MetadataStore is not updated.
     */
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicname, Set<AuthAction> actions, String role,
                                                        String authDataJson) {
        return provider.grantPermissionAsync(topicname, actions, role, authDataJson);
    }

    /**
     * Check if the specified role has permission to send messages to the specified fully qualified topic name.
     *
     * @param topicName
     *            the fully qualified topic name associated with the topic.
     * @param role
     *            the app id used to send messages to the topic.
     */
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData) {

        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        return provider.isSuperUser(role, authenticationData, conf).thenComposeAsync(isSuperUser -> {
            if (isSuperUser) {
                return CompletableFuture.completedFuture(true);
            } else {
                return provider.canProduceAsync(topicName, role, authenticationData);
            }
        });
    }

    /**
     * Check if the specified role has permission to receive messages from the specified fully qualified topic name.
     *
     * @param topicName
     *            the fully qualified topic name associated with the topic.
     * @param role
     *            the app id used to receive messages from the topic.
     * @param subscription
     *            the subscription name defined by the client
     */
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData,
                                                      String subscription) {
        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        return provider.isSuperUser(role, authenticationData, conf).thenComposeAsync(isSuperUser -> {
            if (isSuperUser) {
                return CompletableFuture.completedFuture(true);
            } else {
                return provider.canConsumeAsync(topicName, role, authenticationData, subscription);
            }
        });
    }

    public boolean canProduce(TopicName topicName, String role, AuthenticationDataSource authenticationData)
            throws Exception {
        try {
            return canProduceAsync(topicName, role, authenticationData).get(
                    conf.getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException e) {
            log.warn("Time-out {} sec while checking authorization on {} ",
                    conf.getMetadataStoreOperationTimeoutSeconds(), topicName);
            throw e;
        } catch (Exception e) {
            log.warn("Producer-client  with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                    e.getMessage());
            throw e;
        }
    }

    public boolean canConsume(TopicName topicName, String role, AuthenticationDataSource authenticationData,
                              String subscription) throws Exception {
        try {
            return canConsumeAsync(topicName, role, authenticationData, subscription)
                    .get(conf.getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException e) {
            log.warn("Time-out {} sec while checking authorization on {} ",
                    conf.getMetadataStoreOperationTimeoutSeconds(), topicName);
            throw e;
        } catch (Exception e) {
            log.warn("Consumer-client  with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                    e.getMessage());
            throw e;
        }
    }

    /**
     * Check whether the specified role can perform a lookup for the specified topic.
     *
     * For that the caller needs to have producer or consumer permission.
     *
     * @param topicName
     * @param role
     * @return
     * @throws Exception
     */
    public boolean canLookup(TopicName topicName, String role, AuthenticationDataSource authenticationData)
            throws Exception {
        try {
            return canLookupAsync(topicName, role, authenticationData)
                    .get(conf.getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException e) {
            log.warn("Time-out {} sec while checking authorization on {} ",
                    conf.getMetadataStoreOperationTimeoutSeconds(), topicName);
            throw e;
        } catch (Exception e) {
            log.warn("Role - {} failed to get lookup permissions for topic - {}. {}", role, topicName,
                    e.getMessage());
            throw e;
        }
    }

    /**
     * Check whether the specified role can perform a lookup for the specified topic.
     *
     * For that the caller needs to have producer or consumer permission.
     *
     * @param topicName
     * @param role
     * @return
     * @throws Exception
     */
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                                                     AuthenticationDataSource authenticationData) {
        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        return provider.isSuperUser(role, authenticationData, conf).thenComposeAsync(isSuperUser -> {
            if (isSuperUser) {
                return CompletableFuture.completedFuture(true);
            } else {
                return provider.canLookupAsync(topicName, role, authenticationData);
            }
        });
    }

    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role,
                                                            AuthenticationDataSource authenticationData) {
        return provider.allowFunctionOpsAsync(namespaceName, role, authenticationData);
    }

    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role,
                                                          AuthenticationDataSource authenticationData) {
        return provider.allowSourceOpsAsync(namespaceName, role, authenticationData);
    }

    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role,
                                                        AuthenticationDataSource authenticationData) {
        return provider.allowSinkOpsAsync(namespaceName, role, authenticationData);
    }

    private static void validateOriginalPrincipal(Set<String> proxyRoles, String authenticatedPrincipal,
                                                  String originalPrincipal) {
        if (proxyRoles.contains(authenticatedPrincipal)) {
            // Request has come from a proxy
            if (StringUtils.isBlank(originalPrincipal)) {
                log.warn("Original principal empty in request authenticated as {}", authenticatedPrincipal);
                throw new RestException(Response.Status.UNAUTHORIZED, "Original principal cannot be empty if the "
                        + "request is via proxy.");
            }
            if (proxyRoles.contains(originalPrincipal)) {
                log.warn("Original principal {} cannot be a proxy role ({})", originalPrincipal, proxyRoles);
                throw new RestException(Response.Status.UNAUTHORIZED, "Original principal cannot be a proxy role");
            }
        }
    }

    private boolean isProxyRole(String role) {
        return role != null && conf.getProxyRoles().contains(role);
    }

    /**
     * Grant authorization-action permission on a tenant to the given client.
     *
     * @param tenantName tenant name
     * @param operation tenant operation
     * @param role role name
     * @param authData
     *            additional authdata in json for targeted authorization provider
     * @return IllegalArgumentException when tenant not found
     * @throws IllegalStateException
     *             when failed to grant permission
     */
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName,
                                                                TenantOperation operation,
                                                                String role,
                                                                AuthenticationDataSource authData) {
        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        return provider.allowTenantOperationAsync(tenantName, role, operation, authData);
    }

    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName,
                                                                TenantOperation operation,
                                                                String originalRole,
                                                                String role,
                                                                AuthenticationDataSource authData) {
        validateOriginalPrincipal(conf.getProxyRoles(), role, originalRole);
        if (isProxyRole(role)) {
            CompletableFuture<Boolean> isRoleAuthorizedFuture = allowTenantOperationAsync(
                    tenantName, operation, role, authData);
            CompletableFuture<Boolean> isOriginalAuthorizedFuture = allowTenantOperationAsync(
                    tenantName, operation, originalRole, authData);
            return isRoleAuthorizedFuture.thenCombine(isOriginalAuthorizedFuture,
                    (isRoleAuthorized, isOriginalAuthorized) -> isRoleAuthorized && isOriginalAuthorized);
        } else {
            return allowTenantOperationAsync(tenantName, operation, role, authData);
        }
    }

    /**
     * @deprecated - will be removed after 2.12. Use async variant.
     */
    @Deprecated
    public boolean allowTenantOperation(String tenantName,
                                        TenantOperation operation,
                                        String originalRole,
                                        String role,
                                        AuthenticationDataSource authData) throws Exception {
        try {
            return allowTenantOperationAsync(
                    tenantName, operation, originalRole, role, authData).get(
                            conf.getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException e) {
            throw new RestException(e);
        } catch (ExecutionException e) {
            throw new RestException(e.getCause());
        }
    }

    /**
     * Grant authorization-action permission on a namespace to the given client.
     *
     * @param namespaceName
     * @param operation
     * @param role
     * @param authData
     *            additional authdata in json for targeted authorization provider
     * @return IllegalArgumentException when namespace not found
     * @throws IllegalStateException
     *             when failed to grant permission
     */
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
                                                                   NamespaceOperation operation,
                                                                   String role,
                                                                   AuthenticationDataSource authData) {
        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        return provider.allowNamespaceOperationAsync(namespaceName, role, operation, authData);
    }

    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
                                                                   NamespaceOperation operation,
                                                                   String originalRole,
                                                                   String role,
                                                                   AuthenticationDataSource authData) {
        validateOriginalPrincipal(conf.getProxyRoles(), role, originalRole);
        if (isProxyRole(role)) {
            CompletableFuture<Boolean> isRoleAuthorizedFuture = allowNamespaceOperationAsync(
                    namespaceName, operation, role, authData);
            CompletableFuture<Boolean> isOriginalAuthorizedFuture = allowNamespaceOperationAsync(
                    namespaceName, operation, originalRole, authData);
            return isRoleAuthorizedFuture.thenCombine(isOriginalAuthorizedFuture,
                    (isRoleAuthorized, isOriginalAuthorized) -> isRoleAuthorized && isOriginalAuthorized);
        } else {
            return allowNamespaceOperationAsync(namespaceName, operation, role, authData);
        }
    }

    /**
     * Grant authorization-action permission on a namespace to the given client.
     *
     * @param namespaceName
     * @param operation
     * @param role
     * @param authData
     *            additional authdata in json for targeted authorization provider
     * @return IllegalArgumentException when namespace not found
     * @throws IllegalStateException
     *             when failed to grant permission
     */
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                         PolicyName policy,
                                                                         PolicyOperation operation,
                                                                         String role,
                                                                         AuthenticationDataSource authData) {
        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        return provider.allowNamespacePolicyOperationAsync(namespaceName, policy, operation, role, authData);
    }

    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                         PolicyName policy,
                                                                         PolicyOperation operation,
                                                                         String originalRole,
                                                                         String role,
                                                                         AuthenticationDataSource authData) {
        validateOriginalPrincipal(conf.getProxyRoles(), role, originalRole);
        if (isProxyRole(role)) {
            CompletableFuture<Boolean> isRoleAuthorizedFuture = allowNamespacePolicyOperationAsync(
                    namespaceName, policy, operation, role, authData);
            CompletableFuture<Boolean> isOriginalAuthorizedFuture = allowNamespacePolicyOperationAsync(
                    namespaceName, policy, operation, originalRole, authData);
            return isRoleAuthorizedFuture.thenCombine(isOriginalAuthorizedFuture,
                    (isRoleAuthorized, isOriginalAuthorized) -> isRoleAuthorized && isOriginalAuthorized);
        } else {
            return allowNamespacePolicyOperationAsync(namespaceName, policy, operation, role, authData);
        }
    }

    /**
     * @deprecated - will be removed after 2.12. Use async variant.
     */
    @Deprecated
    public boolean allowNamespacePolicyOperation(NamespaceName namespaceName,
                                                 PolicyName policy,
                                                 PolicyOperation operation,
                                                 String originalRole,
                                                 String role,
                                                 AuthenticationDataSource authData) throws Exception {
        try {
            return allowNamespacePolicyOperationAsync(
                    namespaceName, policy, operation, originalRole, role, authData).get(
                            conf.getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException e) {
            throw new RestException(e);
        } catch (ExecutionException e) {
            throw new RestException(e.getCause());
        }
    }

    /**
     * Grant authorization-action permission on a topic to the given client.
     *
     * @param topicName
     * @param policy
     * @param operation
     * @param role
     * @param authData additional authdata in json for targeted authorization provider
     * @throws IllegalStateException when failed to grant permission
     */
    public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topicName,
                                                                     PolicyName policy,
                                                                     PolicyOperation operation,
                                                                     String role,
                                                                     AuthenticationDataSource authData) {
        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }
        return provider.allowTopicPolicyOperationAsync(topicName, role, policy, operation, authData);
    }

    public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topicName,
                                                                     PolicyName policy,
                                                                     PolicyOperation operation,
                                                                     String originalRole,
                                                                     String role,
                                                                     AuthenticationDataSource authData) {
        try {
            validateOriginalPrincipal(conf.getProxyRoles(), role, originalRole);
        } catch (RestException e) {
            return FutureUtil.failedFuture(e);
        }
        if (isProxyRole(role)) {
            CompletableFuture<Boolean> isRoleAuthorizedFuture = allowTopicPolicyOperationAsync(
                    topicName, policy, operation, role, authData);
            CompletableFuture<Boolean> isOriginalAuthorizedFuture = allowTopicPolicyOperationAsync(
                    topicName, policy, operation, originalRole, authData);
            return isRoleAuthorizedFuture.thenCombine(isOriginalAuthorizedFuture,
                    (isRoleAuthorized, isOriginalAuthorized) -> isRoleAuthorized && isOriginalAuthorized);
        } else {
            return allowTopicPolicyOperationAsync(topicName, policy, operation, role, authData);
        }
    }


    /**
     * @deprecated - will be removed after 2.12. Use async variant.
     */
    @Deprecated
    public Boolean allowTopicPolicyOperation(TopicName topicName,
                                             PolicyName policy,
                                             PolicyOperation operation,
                                             String originalRole,
                                             String role,
                                             AuthenticationDataSource authData) throws Exception {
        try {
            return allowTopicPolicyOperationAsync(
                    topicName, policy, operation, originalRole, role, authData).get(
                            conf.getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException e) {
            throw new RestException(e);
        } catch (ExecutionException e) {
            throw new RestException(e.getCause());
        }
    }

    /**
     * Grant authorization-action permission on a topic to the given client.
     *
     * @param topicName
     * @param operation
     * @param role
     * @param authData
     *            additional authdata in json for targeted authorization provider
     * @return IllegalArgumentException when namespace not found
     * @throws IllegalStateException
     *             when failed to grant permission
     */
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topicName,
                                                               TopicOperation operation,
                                                               String role,
                                                               AuthenticationDataSource authData) {
        if (log.isDebugEnabled()) {
            log.debug("Check if role {} is allowed to execute topic operation {} on topic {}",
                    role, operation, topicName);
        }
        if (!this.conf.isAuthorizationEnabled()) {
            return CompletableFuture.completedFuture(true);
        }

        CompletableFuture<Boolean> allowFuture =
                provider.allowTopicOperationAsync(topicName, role, operation, authData);
        if (log.isDebugEnabled()) {
            return allowFuture.whenComplete((allowed, exception) -> {
                if (exception == null) {
                    if (allowed) {
                        log.debug("Topic operation {} on topic {} is allowed: role = {}",
                                operation, topicName, role);
                    } else {
                        log.debug("Topic operation {} on topic {} is NOT allowed: role = {}",
                                operation, topicName, role);
                    }
                } else {
                    log.debug("Failed to check if topic operation {} on topic {} is allowed:"
                                    + " role = {}",
                            operation, topicName, role, exception);
                }
            });
        } else {
            return allowFuture;
        }
    }

    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topicName,
                                                               TopicOperation operation,
                                                               String originalRole,
                                                               String role,
                                                               AuthenticationDataSource authData) {
        validateOriginalPrincipal(conf.getProxyRoles(), role, originalRole);
        if (isProxyRole(role)) {
            CompletableFuture<Boolean> isRoleAuthorizedFuture = allowTopicOperationAsync(
                    topicName, operation, role, authData);
            CompletableFuture<Boolean> isOriginalAuthorizedFuture = allowTopicOperationAsync(
                    topicName, operation, originalRole, authData);
            return isRoleAuthorizedFuture.thenCombine(isOriginalAuthorizedFuture,
                    (isRoleAuthorized, isOriginalAuthorized) -> isRoleAuthorized && isOriginalAuthorized);
        } else {
            return allowTopicOperationAsync(topicName, operation, role, authData);
        }
    }

    /**
     * @deprecated - will be removed after 2.12. Use async variant.
     */
    @Deprecated
    public Boolean allowTopicOperation(TopicName topicName,
                                       TopicOperation operation,
                                       String originalRole,
                                       String role,
                                       AuthenticationDataSource authData) throws Exception {
        try {
            return allowTopicOperationAsync(topicName, operation, originalRole, role, authData).get(
                    conf.getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (InterruptedException e) {
            throw new RestException(e);
        } catch (ExecutionException e) {
            throw new RestException(e.getCause());
        }
    }
}
