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

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Mock implementation of the authorization provider interface used for testing.
 * A role is authorized if it starts with "pass".
 */
public class MockAuthorizationProvider implements AuthorizationProvider {

    private CompletableFuture<Boolean> shouldPass(String role) {
        return CompletableFuture.completedFuture(role != null && role.startsWith("pass"));
    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData) {
        return shouldPass(role);
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData, String subscription) {
        return shouldPass(role);
    }

    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                                                     AuthenticationDataSource authenticationData) {
        return shouldPass(role);
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role,
                                                            AuthenticationDataSource authenticationData) {
        return shouldPass(role);
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role,
                                                          AuthenticationDataSource authenticationData) {
        return shouldPass(role);
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName, String role, TenantOperation operation, AuthenticationDataSource authData) {
        return shouldPass(role);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName, String role, NamespaceOperation operation, AuthenticationDataSource authData) {
        return shouldPass(role);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName, PolicyName policy, PolicyOperation operation, String role, AuthenticationDataSource authData) {
        return shouldPass(role);
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic, String role, TopicOperation operation, AuthenticationDataSource authData) {
        return shouldPass(role);
    }

    @Override
    public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topic, String role, PolicyName policy, PolicyOperation operation, AuthenticationDataSource authData) {
        return shouldPass(role);
    }


    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        return null;
    }


    @Override
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions, String role,
                                                        String authDataJson) {
        return null;
    }

    @Override
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
                                                                    Set<String> roles, String authDataJson) {
        return null;
    }

    @Override
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
                                                                     String role, String authDataJson) {
        return null;
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions, String role,
                                                        String authDataJson) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
