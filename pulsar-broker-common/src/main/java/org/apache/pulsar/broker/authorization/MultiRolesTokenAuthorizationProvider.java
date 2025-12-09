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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
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


public class MultiRolesTokenAuthorizationProvider extends PulsarAuthorizationProvider {
    private static final Logger log = LoggerFactory.getLogger(MultiRolesTokenAuthorizationProvider.class);

    static final String HTTP_HEADER_NAME = "Authorization";
    static final String HTTP_HEADER_VALUE_PREFIX = "Bearer ";

    // When symmetric key is configured
    static final String CONF_TOKEN_SETTING_PREFIX = "tokenSettingPrefix";

    // The token's claim that corresponds to the "role" string
    static final String CONF_TOKEN_AUTH_CLAIM = "tokenAuthClaim";

    static final String DEFAULT_ROLE_CLAIM = "roles";

    private static final ObjectMapper mapper = new ObjectMapper();

    private final JwtParser parser;
    private String roleClaim;

    public MultiRolesTokenAuthorizationProvider() {
        this.roleClaim = DEFAULT_ROLE_CLAIM;
        this.parser = Jwts.parser().unsecured().build();
    }

    @Override
    public void initialize(ServiceConfiguration conf, PulsarResources pulsarResources) throws IOException {
        String prefix = (String) conf.getProperty(CONF_TOKEN_SETTING_PREFIX);
        if (null == prefix) {
            prefix = "";
        }
        String confTokenAuthClaimSettingName = prefix + CONF_TOKEN_AUTH_CLAIM;
        Object tokenAuthClaim = conf.getProperty(confTokenAuthClaimSettingName);
        if (tokenAuthClaim != null && StringUtils.isNotBlank((String) tokenAuthClaim)) {
            this.roleClaim = (String) tokenAuthClaim;
        }

        super.initialize(conf, pulsarResources);
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
        Set<String> roles = getRoles(role, authenticationData);
        if (roles.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        return CompletableFuture.completedFuture(roles.stream().anyMatch(superUserRoles::contains));
    }

    @Override
    public CompletableFuture<Boolean> validateTenantAdminAccess(String tenantName, String role,
                                                                AuthenticationDataSource authData) {
        return isSuperUser(role, authData, conf)
                .thenCompose(isSuperUser -> {
                    if (isSuperUser) {
                        return CompletableFuture.completedFuture(true);
                    }
                    Set<String> roles = getRoles(role, authData);
                    if (roles.isEmpty()) {
                        return CompletableFuture.completedFuture(false);
                    }

                    return pulsarResources.getTenantResources()
                            .getTenantAsync(tenantName)
                            .thenCompose(op -> {
                                if (op.isPresent()) {
                                    TenantInfo tenantInfo = op.get();
                                    if (tenantInfo.getAdminRoles() == null || tenantInfo.getAdminRoles().isEmpty()) {
                                        return CompletableFuture.completedFuture(false);
                                    }

                                    return CompletableFuture.completedFuture(roles.stream()
                                            .anyMatch(n -> tenantInfo.getAdminRoles().contains(n)));
                                } else {
                                    throw new RestException(Response.Status.NOT_FOUND, "Tenant does not exist");
                                }
                            }).exceptionally(ex -> {
                                Throwable cause = ex.getCause();
                                if (cause instanceof MetadataStoreException.NotFoundException) {
                                    log.warn("Failed to get tenant info data for non existing tenant {}", tenantName);
                                    throw new RestException(Response.Status.NOT_FOUND, "Tenant does not exist");
                                }
                                log.error("Failed to get tenant {}", tenantName, cause);
                                throw new RestException(cause);
                            });
                });
    }

    private Set<String> getRoles(String role, AuthenticationDataSource authData) {
        if (authData == null || (authData instanceof AuthenticationDataSubscription
                && ((AuthenticationDataSubscription) authData).getAuthData() == null)) {
            return Collections.singleton(role);
        }

        String token = null;

        if (authData.hasDataFromCommand()) {
            // Authenticate Pulsar binary connection
            token = authData.getCommandData();
            if (StringUtils.isBlank(token)) {
                return Collections.emptySet();
            }
        } else if (authData.hasDataFromHttp()) {
            // The format here should be compliant to RFC-6750
            // (https://tools.ietf.org/html/rfc6750#section-2.1). Eg: Authorization: Bearer xxxxxxxxxxxxx
            String httpHeaderValue = authData.getHttpHeader(HTTP_HEADER_NAME);
            if (httpHeaderValue == null || !httpHeaderValue.startsWith(HTTP_HEADER_VALUE_PREFIX)) {
                return Collections.emptySet();
            }

            // Remove prefix
            token = httpHeaderValue.substring(HTTP_HEADER_VALUE_PREFIX.length());
        }

        if (token == null) {
            return Collections.emptySet();
        }

        String[] splitToken = token.split("\\.");
        if (splitToken.length < 2) {
            log.warn("Unable to extract additional roles from JWT token");
            return Collections.emptySet();
        }
        String unsignedToken = replaceAlgWithNoneInHeader(splitToken[0]) + "." + splitToken[1] + ".";
        Object jwtRole = parser.parseUnsecuredClaims(unsignedToken).getBody().get(roleClaim);
        if (jwtRole instanceof String) {
            return new HashSet<String>(Collections.singletonList((String) jwtRole));
        } else if (jwtRole instanceof Collection) {
            return new HashSet<>((Collection<String>) jwtRole);
        } else {
            return Collections.emptySet();
        }
    }

    // replace alg with none in header so that it can be parsed in unsecured mode
    private static String replaceAlgWithNoneInHeader(String header) {
        try {
            JsonNode jsonNode = mapper.readTree(Base64.getDecoder().decode(header));
            ((ObjectNode) jsonNode).put("alg", "none");
            return Base64.getEncoder().withoutPadding().encodeToString(mapper.writeValueAsBytes(jsonNode));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public CompletableFuture<Boolean> authorize(String role, AuthenticationDataSource authenticationData,
                                                Function<String, CompletableFuture<Boolean>> authorizeFunc) {
        return isSuperUser(role, authenticationData, conf)
                .thenCompose(superUser -> {
                    if (superUser) {
                        return CompletableFuture.completedFuture(true);
                    }
                    Set<String> roles = getRoles(role, authenticationData);
                    if (roles.isEmpty()) {
                        return CompletableFuture.completedFuture(false);
                    }
                    List<CompletableFuture<Boolean>> futures = new ArrayList<>(roles.size());
                    roles.forEach(r -> futures.add(authorizeFunc.apply(r)));
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
