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
package org.apache.pulsar.broker.auth;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockAuthorizationProvider implements AuthorizationProvider {
    private static final Logger log = LoggerFactory.getLogger(MockAuthorizationProvider.class);

    @Override
    public void close() {}

    @Override
    public CompletableFuture<Boolean> isSuperUser(String role,
                                                  AuthenticationDataSource authenticationData,
                                                  ServiceConfiguration serviceConfiguration) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> isSuperUser(String role, ServiceConfiguration serviceConfiguration) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> isTenantAdmin(String tenant, String role, TenantInfo tenantInfo,
                                                    AuthenticationDataSource authenticationData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData,
                                                      String subscription) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role,
                                                            AuthenticationDataSource authenticationData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role,
                                                          AuthenticationDataSource authenticationData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role,
                                                        AuthenticationDataSource authenticationData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions, String role,
                                                        String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace,
                                                                    String subscriptionName, Set<String> roles,
                                                                    String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
                                                                     String role, String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions, String role,
                                                        String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName, String originalRole, String role,
                                                                TenantOperation operation,
                                                                AuthenticationDataSource authData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowTenantOperation(String tenantName, String originalRole, String role, TenantOperation operation,
                                        AuthenticationDataSource authData) {
        return roleAuthorized(role);
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName, String role,
                                                                TenantOperation operation,
                                                                AuthenticationDataSource authData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowTenantOperation(String tenantName, String role, TenantOperation operation,
                                        AuthenticationDataSource authData) {
        return roleAuthorized(role);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
                                                                   String role,
                                                                   NamespaceOperation operation,
                                                                   AuthenticationDataSource authData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowNamespaceOperation(NamespaceName namespaceName,
                                           String role,
                                           NamespaceOperation operation,
                                           AuthenticationDataSource authData) {
        return roleAuthorized(role);
    }


    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
                                                                   String originalRole,
                                                                   String role,
                                                                   NamespaceOperation operation,
                                                                   AuthenticationDataSource authData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowNamespaceOperation(NamespaceName namespaceName,
                                           String originalRole,
                                           String role,
                                           NamespaceOperation operation,
                                           AuthenticationDataSource authData) {
        return roleAuthorized(role);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                         PolicyName policy,
                                                                         PolicyOperation operation,
                                                                         String role,
                                                                         AuthenticationDataSource authData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowNamespacePolicyOperation(NamespaceName namespaceName,
                                                 PolicyName policy,
                                                 PolicyOperation operation,
                                                 String role,
                                                 AuthenticationDataSource authData) {
        return roleAuthorized(role);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                         PolicyName policy,
                                                                         PolicyOperation operation,
                                                                         String originalRole,
                                                                         String role,
                                                                         AuthenticationDataSource authData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowNamespacePolicyOperation(NamespaceName namespaceName,
                                                 PolicyName policy,
                                                 PolicyOperation operation,
                                                 String originalRole,
                                                 String role,
                                                 AuthenticationDataSource authData) {
        return roleAuthorized(role);
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic,
                                                                String role,
                                                                TopicOperation operation,
                                                                AuthenticationDataSource authData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowTopicOperation(TopicName topicName,
                                        String role,
                                        TopicOperation operation,
                                        AuthenticationDataSource authData) {
        return roleAuthorized(role);
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic,
                                                                String originalRole,
                                                                String role,
                                                                TopicOperation operation,
                                                                AuthenticationDataSource authData) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowTopicOperation(TopicName topicName,
                                       String originalRole,
                                       String role,
                                       TopicOperation operation,
                                       AuthenticationDataSource authData) {
        return roleAuthorized(role);
    }

    CompletableFuture<Boolean> roleAuthorizedAsync(String role) {
        CompletableFuture<Boolean> promise = new CompletableFuture<>();
        try {
            promise.complete(roleAuthorized(role));
        } catch (Exception e) {
            promise.completeExceptionally(e);
        }
        return promise;
    }

    boolean roleAuthorized(String role) {
        String[] parts = role.split("\\.");
        if (parts.length == 2) {
            switch (parts[1]) {
                case "pass":
                    return true;
                case "fail":
                    return false;
                case "error":
                    throw new RuntimeException("Error in authn");
            }
        }
        throw new IllegalArgumentException(
                "Not a valid principle. Should be [pass|fail|error].[pass|fail|error], found " + role);
    }
}
