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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.TokenAuthenticationProvider;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
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
 * Authorizes roles in JWTs authenticated by a {@link TokenAuthenticationProvider}.
 *
 * <p>Authorization obtains roles asynchronously from the existing authentication provider's validated
 * token, using its configured keys, issuer, audience and clock-skew handling. Proxies must forward
 * the original credentials and brokers must authenticate them.
 */
public class MultiRolesTokenAuthorizationProvider extends PulsarAuthorizationProvider {
    private static final Logger log = LoggerFactory.getLogger(MultiRolesTokenAuthorizationProvider.class);

    // When symmetric key is configured
    static final String CONF_TOKEN_SETTING_PREFIX = "tokenSettingPrefix";

    // The token's claim that corresponds to the "role" string
    static final String CONF_TOKEN_AUTH_CLAIM = "tokenAuthClaim";

    static final String DEFAULT_ROLE_CLAIM = "roles";

    private String roleClaim = DEFAULT_ROLE_CLAIM;
    private TokenAuthenticationProvider authenticationProvider;

    @Deprecated
    @Override
    public void initialize(ServiceConfiguration conf, PulsarResources pulsarResources) throws IOException {
        initialize(new InitialContext(conf, pulsarResources, null));
    }

    @Override
    public void initialize(InitialContext context) throws IOException {
        ServiceConfiguration conf = context.config();
        String prefix = (String) conf.getProperty(CONF_TOKEN_SETTING_PREFIX);
        if (null == prefix) {
            prefix = "";
        }
        String confTokenAuthClaimSettingName = prefix + CONF_TOKEN_AUTH_CLAIM;
        Object tokenAuthClaim = conf.getProperty(confTokenAuthClaimSettingName);
        if (tokenAuthClaim != null && StringUtils.isNotBlank((String) tokenAuthClaim)) {
            this.roleClaim = (String) tokenAuthClaim;
        }

        if (!conf.isAuthenticationEnabled()) {
            throw new IOException("MultiRolesTokenAuthorizationProvider requires authenticationEnabled=true");
        }
        // Token and OpenID share the "token" method; the service returns their AuthenticationProviderList
        // when both are configured.
        AuthenticationProvider sharedProvider = context.authenticationService() == null ? null
                : context.authenticationService()
                        .getAuthenticationProvider(TokenAuthenticationProvider.AUTH_METHOD_NAME);
        if (!(sharedProvider instanceof TokenAuthenticationProvider tokenProvider)) {
            throw new IOException("MultiRolesTokenAuthorizationProvider requires an initialized token authentication "
                    + "provider in AuthorizationProvider.InitialContext");
        }
        authenticationProvider = tokenProvider;

        super.initialize(context);
    }

    @Override
    public CompletableFuture<Boolean> isSuperUser(String role, AuthenticationDataSource authenticationData,
                                                  ServiceConfiguration serviceConfiguration) {
        // if superUser role contains in config, return true.
        Set<String> superUserRoles = serviceConfiguration.getSuperUserRoles();
        if (superUserRoles.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        if (role != null && superUserRoles.contains(role)) {
            return CompletableFuture.completedFuture(true);
        }
        return getRolesAsync(role, authenticationData)
                .thenApply(roles -> roles.stream().anyMatch(superUserRoles::contains));
    }

    @Override
    public CompletableFuture<Boolean> validateTenantAdminAccess(String tenantName, String role,
                                                                AuthenticationDataSource authData) {
        if (role != null && conf.getSuperUserRoles().contains(role)) {
            return CompletableFuture.completedFuture(true);
        }
        return getRolesAsync(role, authData)
                .thenCompose(roles -> {
                    if (roles.stream().anyMatch(conf.getSuperUserRoles()::contains)) {
                        return CompletableFuture.completedFuture(true);
                    }
                    if (roles.isEmpty()) {
                        return CompletableFuture.completedFuture(false);
                    }

                    return pulsarResources.getTenantResources()
                            .getTenantAsync(tenantName)
                            // Failing to read the tenant is a broker-side fault, so it is handled here, on the
                            // stage that can actually fail. Keeping this handler off the stage below prevents the
                            // expected "tenant does not exist" rejection from being reported as an error.
                            .exceptionally(ex -> {
                                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                                if (cause instanceof MetadataStoreException.NotFoundException) {
                                    log.warn("Failed to get tenant info data for non existing tenant {}", tenantName);
                                    return Optional.empty();
                                }
                                log.error("Failed to get tenant {}", tenantName, cause);
                                throw new RestException(cause);
                            })
                            .thenCompose(op -> {
                                if (op.isPresent()) {
                                    TenantInfo tenantInfo = op.get();
                                    if (tenantInfo.getAdminRoles() == null || tenantInfo.getAdminRoles().isEmpty()) {
                                        return CompletableFuture.completedFuture(false);
                                    }

                                    return CompletableFuture.completedFuture(roles.stream()
                                            .anyMatch(n -> tenantInfo.getAdminRoles().contains(n)));
                                }
                                // A client naming a tenant that does not exist is a client error, not a broker
                                // fault: reject it without logging. Any client can trigger this at will, and the
                                // caller (e.g. ServerCnx) already logs the rejection at its own level.
                                throw new RestException(Response.Status.NOT_FOUND, "Tenant does not exist");
                            });
                });
    }

    private CompletableFuture<Set<String>> getRolesAsync(String role, AuthenticationDataSource authData) {
        if (authData == null || (authData instanceof AuthenticationDataSubscription subscription
                && subscription.getAuthData() == null)) {
            return CompletableFuture.completedFuture(
                    role == null ? Collections.emptySet() : Collections.singleton(role));
        }
        try {
            return authenticationProvider.authenticateRolesAsync(authData, roleClaim)
                    .exceptionally(error -> {
                        log.debug("Unable to extract additional roles from JWT token");
                        return Collections.emptySet();
                    });
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }
    }

    public CompletableFuture<Boolean> authorize(String role, AuthenticationDataSource authenticationData,
                                                Function<String, CompletableFuture<Boolean>> authorizeFunc) {
        if (role != null && conf.getSuperUserRoles().contains(role)) {
            return CompletableFuture.completedFuture(true);
        }
        return getRolesAsync(role, authenticationData)
                .thenCompose(roles -> {
                    if (roles.stream().anyMatch(conf.getSuperUserRoles()::contains)) {
                        return CompletableFuture.completedFuture(true);
                    }
                    if (roles.isEmpty()) {
                        return CompletableFuture.completedFuture(false);
                    }
                    List<CompletableFuture<Boolean>> futures = new ArrayList<>(roles.size());
                    if (roles.size() == 1) {
                        roles.forEach(r -> futures.add(authorizeFunc.apply(r)));
                    } else {
                        roles.forEach(r -> futures.add(authorizeFunc.apply(r).exceptionally(ex -> false)));
                    }
                    return FutureUtil.waitForAny(futures, ret -> (boolean) ret).thenApply(v -> v.isPresent());
                });
    }

    /**
     * Check if the specified role has permission to send messages to the specified fully qualified topic name.
     *
     * @param topicName the fully qualified topic name associated with the topic.
     * @param role      the app id used to send messages to the topic.
     */
    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData) {
        return authorize(role, authenticationData, r -> super.canProduceAsync(topicName, r, authenticationData));
    }

    /**
     * Check if the specified role has permission to receive messages from the specified fully qualified topic
     * name.
     *
     * @param topicName    the fully qualified topic name associated with the topic.
     * @param role         the app id used to receive messages from the topic.
     * @param subscription the subscription name defined by the client
     */
    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData,
                                                      String subscription) {
        return authorize(role, authenticationData, r -> super.canConsumeAsync(topicName, r, authenticationData,
                subscription));
    }

    /**
     * Check whether the specified role can perform a lookup for the specified topic.
     * <p>
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
        return authorize(role, authenticationData, r -> super.canLookupAsync(topicName, r, authenticationData));
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role,
                                                            AuthenticationDataSource authenticationData) {
        return authorize(role, authenticationData,
                r -> super.allowFunctionOpsAsync(namespaceName, r, authenticationData));
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role,
                                                          AuthenticationDataSource authenticationData) {
        return authorize(role, authenticationData,
                r -> super.allowSourceOpsAsync(namespaceName, r, authenticationData));
    }

    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role,
                                                        AuthenticationDataSource authenticationData) {
        return authorize(role, authenticationData, r -> super.allowSinkOpsAsync(namespaceName, r, authenticationData));
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName,
                                                                String role,
                                                                TenantOperation operation,
                                                                AuthenticationDataSource authData) {
        return authorize(role, authData, r -> super.allowTenantOperationAsync(tenantName, r, operation, authData));
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
                                                                   String role,
                                                                   NamespaceOperation operation,
                                                                   AuthenticationDataSource authData) {
        return authorize(role, authData,
                r -> super.allowNamespaceOperationAsync(namespaceName, r, operation, authData));
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                         PolicyName policy,
                                                                         PolicyOperation operation,
                                                                         String role,
                                                                         AuthenticationDataSource authData) {
        return authorize(role, authData,
                r -> super.allowNamespacePolicyOperationAsync(namespaceName, policy, operation, r, authData));
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topicName,
                                                               String role,
                                                               TopicOperation operation,
                                                               AuthenticationDataSource authData) {
        return authorize(role, authData, r -> super.allowTopicOperationAsync(topicName, r, operation, authData));
    }

    @Override
    public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topicName,
                                                                     String role,
                                                                     PolicyName policyName,
                                                                     PolicyOperation policyOperation,
                                                                     AuthenticationDataSource authData) {
        return authorize(role, authData,
                r -> super.allowTopicPolicyOperationAsync(topicName, r, policyName, policyOperation, authData));
    }
}
