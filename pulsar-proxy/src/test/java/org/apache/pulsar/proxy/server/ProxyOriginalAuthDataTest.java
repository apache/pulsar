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
package org.apache.pulsar.proxy.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyOriginalAuthDataTest extends MockedPulsarServiceBaseTest {
    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String CLIENT_ROLE = "CLIENT";
    private static final String PROXY_ROLE = "PROXY";
    private static final String PROXY_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, PROXY_ROLE, Optional.empty());
    private static final String BUILTIN_ROLE = "BUILTIN";
    private static final String BUILTIN_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, BUILTIN_ROLE, Optional.empty());

    public static class MyAuthorizationProvider implements AuthorizationProvider {

        public MyAuthorizationProvider() {
        }

        private void assertRoleAndAuthenticationData(String role, AuthenticationDataSource authenticationData) {
            String token;
            if (authenticationData.hasDataFromHttp()) {
                token = authenticationData.getHttpHeader("Authorization").replace("Bearer ", "");
            } else {
                token = authenticationData.getCommandData();
            }
            switch (role) {
                case PROXY_ROLE:
                    assertEquals(token, PROXY_TOKEN);
                    break;
                case BUILTIN_ROLE:
                    assertEquals(token, BUILTIN_TOKEN);
                    break;
                case CLIENT_ROLE:
                    String subject = Jwts.parserBuilder().setSigningKey(SECRET_KEY).build().parseClaimsJws(token)
                            .getBody().getSubject();
                    assertEquals(subject, role);
                    break;
                default:
                    fail("Unknown role");
            }
        }

        @Override
        public CompletableFuture<Boolean> isSuperUser(String role, AuthenticationDataSource authenticationData,
                                                      ServiceConfiguration serviceConfiguration) {
            assertRoleAndAuthenticationData(role, authenticationData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> isTenantAdmin(String tenant, String role, TenantInfo tenantInfo,
                                                        AuthenticationDataSource authenticationData) {
            assertRoleAndAuthenticationData(role, authenticationData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void initialize(ServiceConfiguration conf, ConfigurationCacheService configCache) throws IOException {
            // noop
        }

        @Override
        public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                                                          AuthenticationDataSource authenticationData) {
            assertRoleAndAuthenticationData(role, authenticationData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                                                          AuthenticationDataSource authenticationData,
                                                          String subscription) {
            assertRoleAndAuthenticationData(role, authenticationData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                                                         AuthenticationDataSource authenticationData) {
            assertRoleAndAuthenticationData(role, authenticationData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role,
                                                                AuthenticationDataSource authenticationData) {
            assertRoleAndAuthenticationData(role, authenticationData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role,
                                                              AuthenticationDataSource authenticationData) {
            assertRoleAndAuthenticationData(role, authenticationData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role,
                                                            AuthenticationDataSource authenticationData) {
            assertRoleAndAuthenticationData(role, authenticationData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions,
                                                            String role, String authDataJson) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace,
                                                                        String subscriptionName, Set<String> roles,
                                                                        String authDataJson) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace,
                                                                         String subscriptionName, String role,
                                                                         String authDataJson) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions, String role,
                                                            String authDataJson) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName, String role,
                                                                    TenantOperation operation,
                                                                    AuthenticationDataSource authData) {
            assertRoleAndAuthenticationData(role, authData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName, String role,
                                                                       NamespaceOperation operation,
                                                                       AuthenticationDataSource authData) {
            assertRoleAndAuthenticationData(role, authData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                             PolicyName policy,
                                                                             PolicyOperation operation, String role,
                                                                             AuthenticationDataSource authData) {
            assertRoleAndAuthenticationData(role, authData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic, String role,
                                                                   TopicOperation operation,
                                                                   AuthenticationDataSource authData) {
            assertRoleAndAuthenticationData(role, authData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topic, String role,
                                                                         PolicyName policy, PolicyOperation operation,
                                                                         AuthenticationDataSource authData) {
            assertRoleAndAuthenticationData(role, authData);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void close() throws IOException {
            // noop
        }
    }


    private ProxyService proxyService;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();

        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticateOriginalAuthData(true);
        conf.setAdvertisedAddress(null);
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setAuthorizationProvider(MyAuthorizationProvider.class.getName());
        conf.setAuthenticationRefreshCheckSeconds(5);
        conf.setClusterName(configClusterName);
        conf.setNumExecutorThreadPoolSize(5);
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        conf.setSuperUserRoles(Sets.newHashSet(BUILTIN_TOKEN));
        conf.setProperties(properties);
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + BUILTIN_TOKEN);
    }


    @BeforeClass
    @Override
    protected void setup() throws Exception {
        init();
        admin = newPulsarAdmin(new AuthenticationToken(BUILTIN_TOKEN));
        setupDefaultTenantAndNamespace();

        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setForwardAuthorizationCredentials(true);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerServiceURLTLS(pulsar.getBrokerServiceUrlTls());
        proxyConfig.setAdvertisedAddress(null);
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters("token:" + PROXY_TOKEN);
        proxyConfig.setAuthenticationProviders(Set.of(AuthenticationProviderTls.class.getName(),
                AuthenticationProviderToken.class.getName()));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        proxyConfig.setProperties(properties);

        proxyService = Mockito.spy(new ProxyService(proxyConfig,
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig))));
        proxyService.start();

        pulsarClient = newPulsarClient(new AuthenticationToken(BUILTIN_TOKEN));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        proxyService.close();
    }

    private PulsarClient newPulsarClient(Authentication authentication) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .authentication(authentication)
                .operationTimeout(3, TimeUnit.SECONDS)
                .build();
    }

    protected PulsarAdmin newPulsarAdmin(Authentication authentication) throws PulsarClientException {
        return PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(authentication)
                .requestTimeout(3, TimeUnit.SECONDS)
                .build();
    }

    @Test
    public void testAdmin() throws PulsarAdminException {
        TenantInfo tenantInfo = TenantInfo.builder().allowedClusters(Sets.newHashSet(configClusterName)).build();
        admin.tenants().createTenant("test-tenant-1", tenantInfo);
        admin.namespaces().createNamespace("test-tenant-1/test-namespace-1");
        String partitionedTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(partitionedTopic, 3);
        String nonPartitionedTopic = UUID.randomUUID().toString();
        admin.topics().createNonPartitionedTopic(nonPartitionedTopic);
        admin.lookups().lookupPartitionedTopic(partitionedTopic);
        admin.lookups().lookupTopic(nonPartitionedTopic);
    }

    @Test
    public void testClient() throws PulsarClientException {
        String topic = UUID.randomUUID().toString();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        byte[] msg = "Hello".getBytes(StandardCharsets.UTF_8);
        producer.send(msg);

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test").subscribe();
        Message<byte[]> receive = consumer.receive(3, TimeUnit.SECONDS);
        assertNotNull(receive);
        assertEquals(receive.getData(), msg);
    }

    @Test
    public void testClientTokenRefresh() throws PulsarClientException {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, 10);
        AtomicReference<String> token = new AtomicReference<>(AuthTokenUtils
                .createToken(SECRET_KEY, CLIENT_ROLE, Optional.of(calendar.getTime())));

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrl())
                .operationTimeout(3, TimeUnit.SECONDS)
                .authentication(new AuthenticationToken(token::get))
                .build();

        String topic = UUID.randomUUID().toString();
        Producer<byte[]> producer = client.newProducer().sendTimeout(3, TimeUnit.SECONDS).topic(topic).create();
        byte[] data = "heart beat".getBytes(StandardCharsets.UTF_8);
        // token expired
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThrows(PulsarClientException.TimeoutException.class, () -> producer.send(data));
                    assertTrue(System.currentTimeMillis() > calendar.getTimeInMillis(),
                            "Should be thrown by token expired");
                });

        // refresh token, then send data
        token.set(AuthTokenUtils.createToken(SECRET_KEY, CLIENT_ROLE, Optional.empty()));
        producer.send(data);
    }
}
