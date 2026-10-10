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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataForwarded;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSubscription;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BrokerOperation;
import org.apache.pulsar.common.policies.data.ClusterOperation;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AuthorizationServiceTest {

    AuthorizationService authorizationService;

    @BeforeClass
    void beforeClass() throws PulsarServerException {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setAuthorizationEnabled(true);
        // Consider both of these proxy roles to make testing more comprehensive
        HashSet<String> proxyRoles = new HashSet<>();
        proxyRoles.add("pass.proxy");
        proxyRoles.add("fail.proxy");
        conf.setProxyRoles(proxyRoles);
        conf.setAuthorizationProvider(MockAuthorizationProvider.class.getName());
        authorizationService = new AuthorizationService(conf, null);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testContextDelegatesToLegacyInitializer() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        PulsarResources resources = mock(PulsarResources.class);
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthorizationProvider provider = mock(AuthorizationProvider.class, CALLS_REAL_METHODS);
        provider.initialize(AuthorizationProvider.InitialContext.builder()
                .config(config).pulsarResources(resources).authenticationService(authenticationService).build());
        verify(provider).initialize(config, resources);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPulsarProviderInitializers() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        PulsarResources resources = mock(PulsarResources.class);
        try (PulsarAuthorizationProvider provider = new PulsarAuthorizationProvider()) {
            provider.initialize(AuthorizationProvider.InitialContext.builder()
                    .config(config).pulsarResources(resources).build());
            assertThat(provider.conf).isSameAs(config);
            assertThat(provider.pulsarResources).isSameAs(resources);

            ServiceConfiguration legacyConfig = new ServiceConfiguration();
            PulsarResources legacyResources = mock(PulsarResources.class);
            provider.initialize(legacyConfig, legacyResources);
            assertThat(provider.conf).isSameAs(legacyConfig);
            assertThat(provider.pulsarResources).isSameAs(legacyResources);
        }
    }

    public static class LegacyPulsarProvider extends PulsarAuthorizationProvider {
        private int initializationCount;

        @Override
        @SuppressWarnings("deprecation")
        public void initialize(ServiceConfiguration config, PulsarResources resources) throws IOException {
            super.initialize(config, resources);
            initializationCount++;
        }
    }

    @Test
    public void testContextInitializesLegacyPulsarSubclass() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        PulsarResources resources = mock(PulsarResources.class);
        try (LegacyPulsarProvider provider = new LegacyPulsarProvider()) {
            provider.initialize(new AuthorizationProvider.InitialContext(config, resources, null));
            assertThat(provider.initializationCount).isEqualTo(1);
            assertThat(provider.conf).isSameAs(config);
            assertThat(provider.pulsarResources).isSameAs(resources);
        }
    }

    @Test
    public void testIsSuperUserOrTenantAdminUsesProviderAdminChecks() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setAuthorizationEnabled(true);
        config.setSuperUserRoles(Set.of("super-role"));
        config.setAuthorizationProvider(MockAuthorizationProvider.class.getName());
        PulsarResources resources = mock(PulsarResources.class);
        TenantResources tenantResources = mock(TenantResources.class);
        when(resources.getTenantResources()).thenReturn(tenantResources);
        when(tenantResources.getTenantAsync("tenant")).thenReturn(CompletableFuture.completedFuture(
                Optional.of(TenantInfo.builder().adminRoles(Set.of("admin-role")).build())));
        AuthorizationService service = new AuthorizationService(config, resources);

        assertTrue(service.isSuperUserOrTenantAdmin("tenant", "super-role", null).get());
        assertTrue(service.isSuperUserOrTenantAdmin("tenant", "admin-role", null).get());
        // the mock provider allows tenant operations for this role, but it is not a tenant admin
        assertFalse(service.isSuperUserOrTenantAdmin("tenant", "pass.client", null).get());
    }

    /**
     * See {@link MockAuthorizationProvider} for the implementation of the mock authorization provider.
     */
    @DataProvider(name = "roles")
    public Object[][] encryptionProvider() {
        return new Object[][]{
                // Schema: role, originalRole, whether authorization should pass

                // Client conditions where original role isn't passed or is blank
                {"pass.client", null, Boolean.TRUE},
                {"pass.client", " ", Boolean.TRUE},
                {"fail.client", null, Boolean.FALSE},
                {"fail.client", " ", Boolean.FALSE},

                // Proxy conditions where original role isn't passed or is blank
                {"pass.proxy", null, Boolean.FALSE},
                {"pass.proxy", " ", Boolean.FALSE},
                {"fail.proxy", null, Boolean.FALSE},
                {"fail.proxy", " ", Boolean.FALSE},

                // Normal proxy and client conditions
                {"pass.proxy", "pass.client", Boolean.TRUE},
                {"pass.proxy", "fail.client", Boolean.FALSE},
                {"fail.proxy", "pass.client", Boolean.FALSE},
                {"fail.proxy", "fail.client", Boolean.FALSE},

                // Not proxy with original principal
                {"pass.not-proxy", "pass.client", Boolean.FALSE}, // non proxy role can't pass original role
                {"pass.not-proxy", "fail.client", Boolean.FALSE},
                {"fail.not-proxy", "pass.client", Boolean.FALSE},
                {"fail.not-proxy", "fail.client", Boolean.FALSE},

                // Covers an unlikely scenario, but valid in the context of this test
                {null, "pass.proxy", Boolean.FALSE},
        };
    }

    private void checkResult(boolean expected, boolean actual) {
        if (expected) {
            assertTrue(actual);
        } else {
            assertFalse(actual);
        }
    }

    @Test(dataProvider = "roles")
    public void testAllowTenantOperationAsync(String role, String originalRole, boolean shouldPass) throws Exception {
        boolean isAuthorized = authorizationService.allowTenantOperationAsync("tenant",
                TenantOperation.DELETE_NAMESPACE, originalRole, role, null).get();
        checkResult(shouldPass, isAuthorized);
    }

    @Test(dataProvider = "roles")
    public void testNamespaceOperationAsync(String role, String originalRole, boolean shouldPass) throws Exception {
        boolean isAuthorized = authorizationService.allowNamespaceOperationAsync(NamespaceName.get("public/default"),
                NamespaceOperation.PACKAGES, originalRole, role, null).get();
        checkResult(shouldPass, isAuthorized);
    }

    @Test(dataProvider = "roles")
    public void testTopicOperationAsync(String role, String originalRole, boolean shouldPass) throws Exception {
        boolean isAuthorized = authorizationService.allowTopicOperationAsync(TopicName.get("topic"),
                TopicOperation.PRODUCE, originalRole, role, null).get();
        checkResult(shouldPass, isAuthorized);
    }

    @Test(dataProvider = "roles")
    public void testNamespacePolicyOperationAsync(String role, String originalRole, boolean shouldPass)
            throws Exception {
        boolean isAuthorized = authorizationService.allowNamespacePolicyOperationAsync(
                NamespaceName.get("public/default"), PolicyName.ALL, PolicyOperation.READ, originalRole, role, null)
                .get();
        checkResult(shouldPass, isAuthorized);
    }

    @Test(dataProvider = "roles")
    public void testTopicPolicyOperationAsync(String role, String originalRole, boolean shouldPass) throws Exception {
        boolean isAuthorized = authorizationService.allowTopicPolicyOperationAsync(TopicName.get("topic"),
                PolicyName.ALL, PolicyOperation.READ, originalRole, role, null).get();
        checkResult(shouldPass, isAuthorized);
    }

    /**
     * Allows the proxy role only with the request auth data and the original principal only with forwarded auth data
     * that keeps the request data, and the subscription only for consume operations.
     */
    public static class ProxiedAuthDataProvider extends MockAuthorizationProvider {
        static final String PROXY_ROLE = "pass.proxy";
        static final String SUBSCRIPTION = "sub";

        private CompletableFuture<Boolean> check(String role, AuthenticationDataSource authData) {
            return check(role, authData, false);
        }

        private CompletableFuture<Boolean> check(String role, AuthenticationDataSource authData,
                                                 boolean expectSubscription) {
            if (PROXY_ROLE.equals(role)) {
                return CompletableFuture.completedFuture(authData != null && authData.hasDataFromHttp());
            }
            AuthenticationDataSource data = authData;
            if (expectSubscription) {
                if (!(data instanceof AuthenticationDataSubscription subscriptionData)
                        || !SUBSCRIPTION.equals(subscriptionData.getSubscription())) {
                    return CompletableFuture.completedFuture(false);
                }
                data = subscriptionData.getAuthData();
            }
            return CompletableFuture.completedFuture(data instanceof AuthenticationDataForwarded forwarded
                    && forwarded.getProxiedRequestData() != null
                    && forwarded.getProxiedRequestData().hasDataFromHttp());
        }

        @Override
        public CompletableFuture<Boolean> isSuperUser(String role, AuthenticationDataSource authenticationData,
                                                      ServiceConfiguration serviceConfiguration) {
            return check(role, authenticationData);
        }

        @Override
        public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName, String role,
                                                                    TenantOperation operation,
                                                                    AuthenticationDataSource authData) {
            return check(role, authData);
        }

        @Override
        public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName, String role,
                                                                       NamespaceOperation operation,
                                                                       AuthenticationDataSource authData) {
            return check(role, authData);
        }

        @Override
        public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                             PolicyName policy,
                                                                             PolicyOperation operation, String role,
                                                                             AuthenticationDataSource authData) {
            return check(role, authData);
        }

        @Override
        public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic, String role,
                                                                   TopicOperation operation,
                                                                   AuthenticationDataSource authData) {
            return check(role, authData, operation == TopicOperation.CONSUME);
        }

        @Override
        public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topic, String role,
                                                                         PolicyName policy,
                                                                         PolicyOperation operation,
                                                                         AuthenticationDataSource authData) {
            return check(role, authData);
        }

        @Override
        public CompletableFuture<Boolean> allowBrokerOperationAsync(String clusterName, String brokerId,
                                                                    BrokerOperation brokerOperation, String role,
                                                                    AuthenticationDataSource authData) {
            return check(role, authData);
        }

        @Override
        public CompletableFuture<Boolean> allowClusterOperationAsync(String clusterName,
                                                                     ClusterOperation clusterOperation, String role,
                                                                     AuthenticationDataSource authData) {
            return check(role, authData);
        }

        @Override
        public CompletableFuture<Boolean> allowClusterPolicyOperationAsync(String clusterName, String role,
                                                                           PolicyName policy,
                                                                           PolicyOperation operation,
                                                                           AuthenticationDataSource authData) {
            return check(role, authData);
        }
    }

    @Test
    public void testOriginalPrincipalIsCheckedWithoutProxyAuthData() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setAuthorizationEnabled(true);
        conf.setProxyRoles(Set.of(ProxiedAuthDataProvider.PROXY_ROLE));
        conf.setAuthorizationProvider(ProxiedAuthDataProvider.class.getName());
        AuthorizationService service = new AuthorizationService(conf, null);
        String proxy = ProxiedAuthDataProvider.PROXY_ROLE;
        String client = "pass.client";
        // the auth data of the proxy request
        AuthenticationDataSource authData = new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromHttp() {
                return true;
            }
        };
        AuthenticationDataSource subscriptionAuthData =
                new AuthenticationDataSubscription(authData, ProxiedAuthDataProvider.SUBSCRIPTION);
        AuthenticationParameters authParams = AuthenticationParameters.builder()
                .clientRole(proxy).originalPrincipal(client).clientAuthenticationDataSource(authData).build();
        NamespaceName namespace = NamespaceName.get("public/default");
        TopicName topic = TopicName.get("persistent://public/default/topic");

        assertTrue(service.isSuperUser(authParams).get());
        assertTrue(service.allowFunctionOpsAsync(namespace, authParams).get());
        assertTrue(service.allowSourceOpsAsync(namespace, authParams).get());
        assertTrue(service.allowSinkOpsAsync(namespace, authParams).get());
        assertTrue(service.allowTopicOperationAsync(topic, TopicOperation.PRODUCE, authParams).get());
        assertTrue(service.allowTenantOperationAsync("public", TenantOperation.CREATE_NAMESPACE, client, proxy,
                authData).get());
        assertTrue(service.allowNamespaceOperationAsync(namespace, NamespaceOperation.GET_TOPICS, client, proxy,
                authData).get());
        assertTrue(service.allowNamespacePolicyOperationAsync(namespace, PolicyName.ALL, PolicyOperation.READ,
                client, proxy, authData).get());
        assertTrue(service.allowTopicPolicyOperationAsync(topic, PolicyName.ALL, PolicyOperation.READ, client,
                proxy, authData).get());
        assertTrue(service.allowTopicOperationAsync(topic, TopicOperation.PRODUCE, client, proxy, authData).get());
        assertTrue(service.allowTopicOperationAsync(topic, TopicOperation.CONSUME, client, proxy,
                subscriptionAuthData).get());
        assertFalse(service.allowTopicOperationAsync(topic, TopicOperation.CONSUME, client, proxy,
                authData).get());
        assertTrue(service.allowBrokerOperationAsync("test", "broker", BrokerOperation.GET_BROKER, client, proxy,
                authData).get());
        assertTrue(service.allowClusterOperationAsync("test", ClusterOperation.GET_CLUSTER, client, proxy,
                authData).get());
        assertTrue(service.allowClusterPolicyOperationAsync("test", PolicyName.ALL, PolicyOperation.READ, client,
                proxy, authData).get());
    }
}
