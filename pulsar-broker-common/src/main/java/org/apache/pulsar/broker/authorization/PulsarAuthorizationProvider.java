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
package org.apache.pulsar.broker.authorization;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.Response;
import com.google.common.base.Joiner;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default authorization provider that stores authorization policies under local-zookeeper.
 *
 */
public class PulsarAuthorizationProvider implements AuthorizationProvider {
    private static final Logger log = LoggerFactory.getLogger(PulsarAuthorizationProvider.class);

    public ServiceConfiguration conf;
    private PulsarResources pulsarResources;
    private static final String POLICY_ROOT = "/admin/policies/";
    public static final String POLICIES = "policies";
    private static final String POLICIES_READONLY_FLAG_PATH = "/admin/flags/policies-readonly";

    public PulsarAuthorizationProvider() {
    }

    public PulsarAuthorizationProvider(ServiceConfiguration conf, ConfigurationCacheService configCache)
            throws IOException {
        initialize(conf, configCache);
    }

    @Override
    public void initialize(ServiceConfiguration conf, ConfigurationCacheService configCache) throws IOException {
        checkNotNull(conf, "ServiceConfiguration can't be null");
        checkNotNull(configCache, "ConfigurationCacheService can't be null");
        this.conf = conf;
        this.pulsarResources = configCache.getPulsarResources();

    }

    /**
     * Check if the specified role has permission to send messages to the specified fully qualified topic name.
     *
     * @param topicName
     *            the fully qualified topic name associated with the topic.
     * @param role
     *            the app id used to send messages to the topic.
     */
    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData) {
        return checkAuthorization(topicName, role, AuthAction.produce);
    }

    /**
     * Check if the specified role has permission to receive messages from the specified fully qualified topic
     * name.
     *
     * @param topicName
     *            the fully qualified topic name associated with the topic.
     * @param role
     *            the app id used to receive messages from the topic.
     * @param subscription
     *            the subscription name defined by the client
     */
    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData, String subscription) {
        CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
        try {
            pulsarResources.getNamespaceResources().getAsync(POLICY_ROOT + topicName.getNamespace()).thenAccept(policies -> {
                if (!policies.isPresent()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Policies node couldn't be found for topic : {}", topicName);
                    }
                } else {
                    if (isNotBlank(subscription)) {
                        // validate if role is authorize to access subscription. (skip validatation if authorization
                        // list is empty)
                        Set<String> roles = policies.get().auth_policies
                                .getSubscriptionAuthentication().get(subscription);
                        if (roles != null && !roles.isEmpty() && !roles.contains(role)) {
                            log.warn("[{}] is not authorized to subscribe on {}-{}", role, topicName, subscription);
                            permissionFuture.complete(false);
                            return;
                        }

                        // validate if subscription-auth mode is configured
                        switch (policies.get().subscription_auth_mode) {
                        case Prefix:
                            if (!subscription.startsWith(role)) {
                                PulsarServerException ex = new PulsarServerException(String.format(
                                        "Failed to create consumer - The subscription name needs to be prefixed by the authentication role, like %s-xxxx for topic: %s",
                                        role, topicName));
                                permissionFuture.completeExceptionally(ex);
                                return;
                            }
                            break;
                        default:
                            break;
                        }
                    }
                }
                // check namespace and topic level consume-permissions
                checkAuthorization(topicName, role, AuthAction.consume).thenAccept(isAuthorized -> {
                    permissionFuture.complete(isAuthorized);
                }).exceptionally(ex -> {
                    log.warn("Client with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                            ex.getMessage());
                    permissionFuture.completeExceptionally(ex);
                    return null;
                });
            }).exceptionally(ex -> {
                log.warn("Client with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                        ex.getMessage());
                permissionFuture.completeExceptionally(ex);
                return null;
            });
        } catch (Exception e) {
            log.warn("Client  with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                    e.getMessage());
            permissionFuture.completeExceptionally(e);
        }
        return permissionFuture;
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
    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData) {
        CompletableFuture<Boolean> finalResult = new CompletableFuture<Boolean>();
        canProduceAsync(topicName, role, authenticationData).whenComplete((produceAuthorized, ex) -> {
            if (ex == null) {
                if (produceAuthorized) {
                    finalResult.complete(produceAuthorized);
                    return;
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Topic [{}] Role [{}] exception occurred while trying to check Produce permissions. {}",
                            topicName.toString(), role, ex.getMessage());
                }
            }
            canConsumeAsync(topicName, role, authenticationData, null).whenComplete((consumeAuthorized, e) -> {
                if (e == null) {
                    if (consumeAuthorized) {
                        finalResult.complete(consumeAuthorized);
                        return;
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Topic [{}] Role [{}] exception occurred while trying to check Consume permissions. {}",
                                topicName.toString(), role, e.getMessage());

                    }
                    finalResult.completeExceptionally(e);
                    return;
                }
                finalResult.complete(false);
            });
        });
        return finalResult;
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return allowTheSpecifiedActionOpsAsync(namespaceName, role, authenticationData, AuthAction.functions);
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return allowTheSpecifiedActionOpsAsync(namespaceName, role, authenticationData, AuthAction.sources);
    }

    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return allowTheSpecifiedActionOpsAsync(namespaceName, role, authenticationData, AuthAction.sinks);
    }

    private CompletableFuture<Boolean> allowTheSpecifiedActionOpsAsync(NamespaceName namespaceName, String role,
                                                                       AuthenticationDataSource authenticationData,
                                                                       AuthAction authAction) {
        CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
        try {
            pulsarResources.getNamespaceResources().getAsync(POLICY_ROOT + namespaceName.toString()).thenAccept(policies -> {
                if (!policies.isPresent()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Policies node couldn't be found for namespace : {}", namespaceName);
                    }
                } else {
                    Map<String, Set<AuthAction>> namespaceRoles = policies.get()
                            .auth_policies.getNamespaceAuthentication();
                    Set<AuthAction> namespaceActions = namespaceRoles.get(role);
                    if (namespaceActions != null && namespaceActions.contains(authAction)) {
                        // The role has namespace level permission
                        permissionFuture.complete(true);
                        return;
                    }

                    // Using wildcard
                    if (conf.isAuthorizationAllowWildcardsMatching()) {
                        if (checkWildcardPermission(role, authAction, namespaceRoles)) {
                            // The role has namespace level permission by wildcard match
                            permissionFuture.complete(true);
                            return;
                        }
                    }
                }
                permissionFuture.complete(false);
            }).exceptionally(ex -> {
                log.warn("Client  with Role - {} failed to get permissions for namespace - {}. {}", role, namespaceName,
                        ex.getMessage());
                permissionFuture.completeExceptionally(ex);
                return null;
            });
        } catch (Exception e) {
            log.warn("Client  with Role - {} failed to get permissions for namespace - {}. {}", role, namespaceName,
                    e.getMessage());
            permissionFuture.completeExceptionally(e);
        }
        return permissionFuture;
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions,
            String role, String authDataJson) {
        return grantPermissionAsync(topicName.getNamespaceObject(), actions, role, authDataJson);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespaceName, Set<AuthAction> actions,
            String role, String authDataJson) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            validatePoliciesReadOnlyAccess();
        } catch (Exception e) {
            result.completeExceptionally(e);
        }

        final String policiesPath = String.format("/%s/%s/%s", "admin", POLICIES, namespaceName.toString());
        try {
            pulsarResources.getNamespaceResources().set(policiesPath, (policies)->{
                policies.auth_policies.getNamespaceAuthentication().put(role, actions);
                return policies;
            });
            log.info("[{}] Successfully granted access for role {}: {} - namespace {}", role, role, actions,
                    namespaceName);
            result.complete(null);
        } catch (NotFoundException e) {
            log.warn("[{}] Failed to set permissions for namespace {}: does not exist", role, namespaceName);
            result.completeExceptionally(new IllegalArgumentException("Namespace does not exist" + namespaceName));
        } catch (BadVersionException e) {
            log.warn("[{}] Failed to set permissions for namespace {}: concurrent modification", role, namespaceName);
            result.completeExceptionally(new IllegalStateException(
                    "Concurrent modification on zk path: " + policiesPath + ", " + e.getMessage()));
        } catch (Exception e) {
            log.error("[{}] Failed to get permissions for namespace {}", role, namespaceName, e);
            result.completeExceptionally(
                    new IllegalStateException("Failed to get permissions for namespace " + namespaceName));
        }

        return result;
    }

    @Override
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
            Set<String> roles, String authDataJson) {
        return updateSubscriptionPermissionAsync(namespace, subscriptionName, roles, false);
    }

    @Override
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
            String role, String authDataJson) {
        return updateSubscriptionPermissionAsync(namespace, subscriptionName, Collections.singleton(role), true);
    }

    private CompletableFuture<Void> updateSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName, Set<String> roles,
            boolean remove) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            validatePoliciesReadOnlyAccess();
        } catch (Exception e) {
            result.completeExceptionally(e);
        }

        final String policiesPath = String.format("/%s/%s/%s", "admin", POLICIES, namespace.toString());

        try {
            Policies policies = pulsarResources.getNamespaceResources().get(policiesPath)
                    .orElseThrow(() -> new NotFoundException(policiesPath + " not found"));
            if (remove) {
                if (policies.auth_policies.getSubscriptionAuthentication().get(subscriptionName) != null) {
                    policies.auth_policies.getSubscriptionAuthentication().get(subscriptionName).removeAll(roles);
                }else {
                    log.info("[{}] Couldn't find role {} while revoking for sub = {}", namespace, subscriptionName, roles);
                    result.completeExceptionally(new IllegalArgumentException("couldn't find subscription"));
                    return result;
                }
            } else {
                policies.auth_policies.getSubscriptionAuthentication().put(subscriptionName, roles);
            }
            pulsarResources.getNamespaceResources().set(policiesPath, (data)->policies);

            log.info("[{}] Successfully granted access for role {} for sub = {}", namespace, subscriptionName, roles);
            result.complete(null);
        } catch (NotFoundException e) {
            log.warn("[{}] Failed to set permissions for namespace {}: does not exist", subscriptionName, namespace);
            result.completeExceptionally(new IllegalArgumentException("Namespace does not exist" + namespace));
        } catch (BadVersionException e) {
            log.warn("[{}] Failed to set permissions for {} on namespace {}: concurrent modification", subscriptionName, roles, namespace);
            result.completeExceptionally(new IllegalStateException(
                    "Concurrent modification on zk path: " + policiesPath + ", " + e.getMessage()));
        } catch (Exception e) {
            log.error("[{}] Failed to get permissions for role {} on namespace {}", subscriptionName, roles, namespace, e);
            result.completeExceptionally(
                    new IllegalStateException("Failed to get permissions for namespace " + namespace));
        }

        return result;
    }

    private CompletableFuture<Boolean> checkAuthorization(TopicName topicName, String role, AuthAction action) {
        return checkPermission(topicName, role, action)
                .thenApply(isPermission -> isPermission && checkCluster(topicName));
    }

    private boolean checkCluster(TopicName topicName) {
        if (topicName.isGlobal() || conf.getClusterName().equals(topicName.getCluster())) {
            return true;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Topic [{}] does not belong to local cluster [{}]", topicName.toString(),
                        conf.getClusterName());
            }
            return false;
        }
    }

    public CompletableFuture<Boolean> checkPermission(TopicName topicName, String role, AuthAction action) {
        CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
        try {
            pulsarResources.getNamespaceResources().getAsync(POLICY_ROOT + topicName.getNamespace()).thenAccept(policies -> {
                if (!policies.isPresent()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Policies node couldn't be found for topic : {}", topicName);
                    }
                } else {
                    Map<String, Set<AuthAction>> namespaceRoles = policies.get().auth_policies
                            .getNamespaceAuthentication();
                    Set<AuthAction> namespaceActions = namespaceRoles.get(role);
                    if (namespaceActions != null && namespaceActions.contains(action)) {
                        // The role has namespace level permission
                        permissionFuture.complete(true);
                        return;
                    }

                    Map<String, Set<AuthAction>> topicRoles = policies.get().auth_policies.getTopicAuthentication()
                            .get(topicName.toString());
                    if (topicRoles != null && role != null) {
                        // Topic has custom policy
                        Set<AuthAction> topicActions = topicRoles.get(role);
                        if (topicActions != null && topicActions.contains(action)) {
                            // The role has topic level permission
                            permissionFuture.complete(true);
                            return;
                        }
                    }

                    // Using wildcard
                    if (conf.isAuthorizationAllowWildcardsMatching()) {
                        if (checkWildcardPermission(role, action, namespaceRoles)) {
                            // The role has namespace level permission by wildcard match
                            permissionFuture.complete(true);
                            return;
                        }

                        if (topicRoles != null && checkWildcardPermission(role, action, topicRoles)) {
                            // The role has topic level permission by wildcard match
                            permissionFuture.complete(true);
                            return;
                        }
                    }

                    // If the partition number of the partitioned topic having topic level policy is updated,
                    // the new sub partitions may not inherit the policy of the partition topic.
                    // We can also check the permission of partitioned topic.
                    // For https://github.com/apache/pulsar/issues/10300
                    if (topicName.isPartitioned()) {
                        topicRoles = policies.get().auth_policies
                                .getTopicAuthentication().get(topicName.getPartitionedTopicName());
                        if (topicRoles != null) {
                            // Topic has custom policy
                            Set<AuthAction> topicActions = topicRoles.get(role);
                            if (topicActions != null && topicActions.contains(action)) {
                                // The role has topic level permission
                                permissionFuture.complete(true);
                                return;
                            }
                        }
                    }
                }
                permissionFuture.complete(false);
            }).exceptionally(ex -> {
                log.warn("Client with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                        ex.getMessage());
                permissionFuture.completeExceptionally(ex);
                return null;
            });
        } catch (Exception e) {
            log.warn("Client with Role - {} failed to get permissions for topic - {}. {}", role, topicName,
                    e.getMessage());
            permissionFuture.completeExceptionally(e);
        }
        return permissionFuture;
    }

    private boolean checkWildcardPermission(String checkedRole, AuthAction checkedAction,
            Map<String, Set<AuthAction>> permissionMap) {
        for (Map.Entry<String, Set<AuthAction>> permissionData : permissionMap.entrySet()) {
            String permittedRole = permissionData.getKey();
            Set<AuthAction> permittedActions = permissionData.getValue();

            // Prefix match
            if (checkedRole != null) {
                if (permittedRole.charAt(permittedRole.length() - 1) == '*'
                        && checkedRole.startsWith(permittedRole.substring(0, permittedRole.length() - 1))
                        && permittedActions.contains(checkedAction)) {
                    return true;
                }

                // Suffix match
                if (permittedRole.charAt(0) == '*' && checkedRole.endsWith(permittedRole.substring(1))
                        && permittedActions.contains(checkedAction)) {
                    return true;
                }
            }
        }
        return false;
    }


    @Override
    public void close() throws IOException {
        // No-op
    }

    private void validatePoliciesReadOnlyAccess() {
        boolean arePoliciesReadOnly = true;

        try {
            arePoliciesReadOnly = pulsarResources.getNamespaceResources().exists(POLICIES_READONLY_FLAG_PATH);
        } catch (Exception e) {
            log.warn("Unable to fetch contents of [{}] from global zookeeper", POLICIES_READONLY_FLAG_PATH, e);
            throw new IllegalStateException("Unable to fetch content from global zk");
        }

        if (arePoliciesReadOnly) {
            if (log.isDebugEnabled()) {
                log.debug("Policies are read-only. Broker cannot do read-write operations");
            }
            throw new IllegalStateException("policies are in readonly mode");
        }
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName,
                                                                String role,
                                                                TenantOperation operation,
                                                                AuthenticationDataSource authData) {
        return validateTenantAdminAccess(tenantName, role, authData);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
                                                                   String role,
                                                                   NamespaceOperation operation,
                                                                   AuthenticationDataSource authData) {
        CompletableFuture<Boolean> isAuthorizedFuture;
        switch (operation) {
            case PACKAGES:
                isAuthorizedFuture = allowTheSpecifiedActionOpsAsync(namespaceName, role, authData, AuthAction.packages);
                break;
            default:
                isAuthorizedFuture = CompletableFuture.completedFuture(false);
        }
        CompletableFuture<Boolean> isTenantAdminFuture = validateTenantAdminAccess(namespaceName.getTenant(), role, authData);
        return isTenantAdminFuture.thenCombine(isAuthorizedFuture, (isTenantAdmin, isAuthorized) -> {
            if (log.isDebugEnabled()) {
                log.debug("Verify if role {} is allowed to {} to topic {}: isTenantAdmin={}, isAuthorized={}",
                        role, operation, namespaceName, isTenantAdmin, isAuthorized);
            }
            return isTenantAdmin || isAuthorized;
        });
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                         PolicyName policy,
                                                                         PolicyOperation operation,
                                                                         String role,
                                                                         AuthenticationDataSource authData) {
        return validateTenantAdminAccess(namespaceName.getTenant(), role, authData);
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topicName,
                                                               String role,
                                                               TopicOperation operation,
                                                               AuthenticationDataSource authData) {
        log.debug("Check allowTopicOperationAsync [" + operation.name() + "] on [" + topicName.toString() + "].");

        CompletableFuture<Boolean> isAuthorizedFuture;

        switch (operation) {
            case LOOKUP:
            case GET_STATS:
                isAuthorizedFuture = canLookupAsync(topicName, role, authData);
                break;
            case PRODUCE:
                isAuthorizedFuture = canProduceAsync(topicName, role, authData);
                break;
            case GET_SUBSCRIPTIONS:
            case CONSUME:
            case SUBSCRIBE:
            case UNSUBSCRIBE:
            case SKIP:
            case EXPIRE_MESSAGES:
            case PEEK_MESSAGES:
            case RESET_CURSOR:
                isAuthorizedFuture = canConsumeAsync(topicName, role, authData, authData.getSubscription());
                break;
            case TERMINATE:
            case COMPACT:
            case OFFLOAD:
            case UNLOAD:
            case ADD_BUNDLE_RANGE:
            case GET_BUNDLE_RANGE:
            case DELETE_BUNDLE_RANGE:
                return validateTenantAdminAccess(topicName.getTenant(), role, authData);
            default:
                return FutureUtil.failedFuture(
                        new IllegalStateException("TopicOperation [" + operation.name() + "] is not supported."));
        }

        return validateTenantAdminAccess(topicName.getTenant(), role, authData)
                .thenCompose(isSuperUserOrAdmin -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Verify if role {} is allowed to {} to topic {}: isSuperUserOrAdmin={}",
                                role, operation, topicName, isSuperUserOrAdmin);
                    }
                    if (isSuperUserOrAdmin) {
                        return CompletableFuture.completedFuture(true);
                    } else {
                        return isAuthorizedFuture;
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topicName,
                                                                     String role,
                                                                     PolicyName policyName,
                                                                     PolicyOperation policyOperation,
                                                                     AuthenticationDataSource authData) {
        return validateTenantAdminAccess(topicName.getTenant(), role, authData);
    }

    private static String path(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append("/admin/");
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    public CompletableFuture<Boolean> validateTenantAdminAccess(String tenantName,
                                                                 String role,
                                                                 AuthenticationDataSource authData) {
        return isSuperUser(role, authData, conf)
                .thenCompose(isSuperUser -> {
                    if (isSuperUser) {
                        return CompletableFuture.completedFuture(true);
                    } else {
                        try {
                            TenantInfo tenantInfo = pulsarResources.getTenantResources()
                                    .get(path(POLICIES, tenantName))
                                    .orElseThrow(() -> new RestException(Response.Status.NOT_FOUND, "Tenant does not exist"));
                            return isTenantAdmin(tenantName, role, tenantInfo, authData);
                        } catch (NotFoundException e) {
                            log.warn("Failed to get tenant info data for non existing tenant {}", tenantName);
                            throw new RestException(Response.Status.NOT_FOUND, "Tenant does not exist");
                        } catch (Exception e) {
                            log.error("Failed to get tenant {}", tenantName, e);
                            throw new RestException(e);
                        }
                    }
                });
    }

}
