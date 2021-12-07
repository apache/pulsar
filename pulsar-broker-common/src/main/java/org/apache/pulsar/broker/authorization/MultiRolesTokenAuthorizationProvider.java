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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.RequiredTypeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class MultiRolesTokenAuthorizationProvider extends PulsarAuthorizationProvider {
    private static final Logger log = LoggerFactory.getLogger(MultiRolesTokenAuthorizationProvider.class);

    static final String HTTP_HEADER_NAME = "Authorization";
    static final String HTTP_HEADER_VALUE_PREFIX = "Bearer ";

    // When symmetric key is configured
    static final String CONF_TOKEN_SETTING_PREFIX = "tokenSettingPrefix";

    // The token's claim that corresponds to the "role" string
    static final String CONF_TOKEN_AUTH_CLAIM = "tokenAuthClaim";

    private JwtParser parser;
    private String roleClaim;

    public MultiRolesTokenAuthorizationProvider() {
        this.roleClaim = Claims.SUBJECT;
        this.parser = Jwts.parserBuilder().build();
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

    private List<String> getRoles(AuthenticationDataSource authData) {
        String token = null;

        if (authData.hasDataFromCommand()) {
            // Authenticate Pulsar binary connection
            token = authData.getCommandData();
            if (StringUtils.isBlank(token)) {
                return Collections.emptyList();
            }
        } else if (authData.hasDataFromHttp()) {
            // The format here should be compliant to RFC-6750
            // (https://tools.ietf.org/html/rfc6750#section-2.1). Eg: Authorization: Bearer xxxxxxxxxxxxx
            String httpHeaderValue = authData.getHttpHeader(HTTP_HEADER_NAME);
            if (httpHeaderValue == null || !httpHeaderValue.startsWith(HTTP_HEADER_VALUE_PREFIX)) {
                return Collections.emptyList();
            }

            // Remove prefix
            token = httpHeaderValue.substring(HTTP_HEADER_VALUE_PREFIX.length());
        }

        if (token == null)
            return Collections.emptyList();

        String[] splitToken = token.split("\\.");
        String unsignedToken = splitToken[0] + "." + splitToken[1] + ".";

        Jwt<?, Claims> jwt = parser.parseClaimsJwt(unsignedToken);
        try {
            Collections.singletonList(jwt.getBody().get(roleClaim, String.class));
        } catch (RequiredTypeException requiredTypeException) {
            try {
                List list = jwt.getBody().get(roleClaim, List.class);
                if (list != null) {
                    return list;
                }
            } catch (RequiredTypeException requiredTypeException1) {
                return Collections.emptyList();
            }
        }

        return Collections.emptyList();
    }

    public CompletableFuture<Boolean> authorize(AuthenticationDataSource authenticationData, Function<String, CompletableFuture<Boolean>> authorizeFunc) {
        List<String> roles = getRoles(authenticationData);
        List<CompletableFuture<Boolean>> futures = new ArrayList<>(roles.size());
        roles.forEach(r -> futures.add(authorizeFunc.apply(r)));
        return CompletableFuture.supplyAsync(() -> {
            do {
                try {
                    List<CompletableFuture<Boolean>> doneFutures = new ArrayList<>();
                    FutureUtil.waitForAny(futures).get();
                    for (CompletableFuture<Boolean> future : futures) {
                        if (!future.isDone()) continue;
                        doneFutures.add(future);
                        if (future.get()) {
                            futures.forEach(f -> {
                                if (!f.isDone()) f.cancel(false);
                            });
                            return true;
                        }
                    }
                    futures.removeAll(doneFutures);
                } catch (InterruptedException | ExecutionException ignored) {
                }
            } while (!futures.isEmpty());
            return false;
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
        return authorize(authenticationData, r -> super.canProduceAsync(topicName, r, authenticationData));
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
                                                      AuthenticationDataSource authenticationData, String subscription) {
        return authorize(authenticationData, r -> super.canConsumeAsync(topicName, r, authenticationData, subscription));
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
        return authorize(authenticationData, r -> super.canLookupAsync(topicName, r, authenticationData));
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return authorize(authenticationData, r -> super.allowFunctionOpsAsync(namespaceName, r, authenticationData));
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return authorize(authenticationData, r -> super.allowSourceOpsAsync(namespaceName, r, authenticationData));
    }

    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return authorize(authenticationData, r -> super.allowSinkOpsAsync(namespaceName, r, authenticationData));
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName,
                                                                String role,
                                                                TenantOperation operation,
                                                                AuthenticationDataSource authData) {
        return authorize(authData, r -> super.allowTenantOperationAsync(tenantName, r, operation, authData));
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
                                                                   String role,
                                                                   NamespaceOperation operation,
                                                                   AuthenticationDataSource authData) {
        return authorize(authData, r -> super.allowNamespaceOperationAsync(namespaceName, r, operation, authData));
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                         PolicyName policy,
                                                                         PolicyOperation operation,
                                                                         String role,
                                                                         AuthenticationDataSource authData) {
        return authorize(authData, r -> super.allowNamespacePolicyOperationAsync(namespaceName, policy, operation, r, authData));
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topicName,
                                                               String role,
                                                               TopicOperation operation,
                                                               AuthenticationDataSource authData) {
        return authorize(authData, r -> super.allowTopicOperationAsync(topicName, r, operation, authData));
    }

    @Override
    public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topicName,
                                                                     String role,
                                                                     PolicyName policyName,
                                                                     PolicyOperation policyOperation,
                                                                     AuthenticationDataSource authData) {
        return authorize(authData, r -> super.allowTopicPolicyOperationAsync(topicName, r, policyName, policyOperation, authData));
    }
}
