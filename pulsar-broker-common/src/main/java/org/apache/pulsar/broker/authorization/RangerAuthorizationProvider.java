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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

@Slf4j
public class RangerAuthorizationProvider implements AuthorizationProvider {

    private static final String RANGER_PLUGIN_TYPE = "pulsar";
    private static final String RANGER_SQOOP_AUTHORIZER_IMPL_CLASSNAME =
            "org.apache.pulsar.broker.authz.ranger.RangerAuthorizationProvider";

    private AuthorizationProvider authorizationProvider = null;
    private static RangerPluginClassLoader rangerPluginClassLoader = null;

    @Override
    public void initialize(ServiceConfiguration conf, ConfigurationCacheService configCache)
            throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("==> RangerAuthorizationProvider.initialize()");
        }

        try {

            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<AuthorizationProvider> cls = (Class<AuthorizationProvider>) Class.forName(
                    RANGER_SQOOP_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

            activatePluginClassLoader();

            authorizationProvider = cls.newInstance();
            authorizationProvider.initialize(conf, configCache);
        } catch (Exception e) {
            log.error("Error Enabling RangerAuthorizationProvider", e);
        } finally {
            deactivatePluginClassLoader();
        }

        if (log.isDebugEnabled()) {
            log.debug("<== RangerAuthorizationProvider.initialize()");
        }
    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData) {
        return authorizationProvider.canProduceAsync(topicName, role, authenticationData);
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData, String subscription) {
        return authorizationProvider.canConsumeAsync(topicName, role, authenticationData, subscription);
    }

    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
            AuthenticationDataSource authenticationData) {
        return authorizationProvider.canLookupAsync(topicName, role, authenticationData);
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName,
                                                            String role, AuthenticationDataSource authenticationData) {
        return authorizationProvider.allowFunctionOpsAsync(namespaceName, role, authenticationData);
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName,
                                                          String role, AuthenticationDataSource authenticationData) {
        return authorizationProvider.allowSourceOpsAsync(namespaceName, role, authenticationData);
    }

    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName,
                                                        String role, AuthenticationDataSource authenticationData) {
        return authorizationProvider.allowSinkOpsAsync(namespaceName, role, authenticationData);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace,
            Set<AuthAction> actions, String role, String authDataJson) {
        return authorizationProvider.grantPermissionAsync(namespace, actions, role, authDataJson);
    }

    @Override
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace,
            String subscriptionName, Set<String> roles, String authDataJson) {
        return authorizationProvider.grantSubscriptionPermissionAsync(namespace, subscriptionName, roles, authDataJson);
    }

    @Override
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace,
            String subscriptionName, String role, String authDataJson) {
        return authorizationProvider.revokeSubscriptionPermissionAsync(namespace, subscriptionName, role, authDataJson);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicName,
            Set<AuthAction> actions, String role, String authDataJson) {
        return authorizationProvider.grantPermissionAsync(topicName, actions, role, authDataJson);
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic,
            String role, TopicOperation operation, AuthenticationDataSource authData) {
        return authorizationProvider.allowTopicOperationAsync(topic, role, operation, authData);
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName,
            String role, TenantOperation operation, AuthenticationDataSource authData) {
        return authorizationProvider.allowTenantOperationAsync(tenantName, role, operation, authData);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
            String role, NamespaceOperation operation, AuthenticationDataSource authData) {
        return authorizationProvider.allowNamespaceOperationAsync(namespaceName, role, operation, authData);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
            PolicyName policy, PolicyOperation operation, String role, AuthenticationDataSource authData) {
        return authorizationProvider.allowNamespacePolicyOperationAsync(
                namespaceName, policy, operation, role, authData);
    }

    @Override
    public void close() throws IOException {
        authorizationProvider.close();
    }

    private void activatePluginClassLoader() {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader() {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.deactivate();
        }
    }
}
