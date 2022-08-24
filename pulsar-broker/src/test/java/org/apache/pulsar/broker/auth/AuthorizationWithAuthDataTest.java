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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AuthorizationWithAuthDataTest extends MockedPulsarServiceBaseTest {

    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String ADMIN_ROLE = "admin";
    private static final String ADMIN_TOKEN = Jwts.builder().setSubject(ADMIN_ROLE).signWith(SECRET_KEY).compact();

    public static class MyAuthorizationProvider implements AuthorizationProvider {

        public MyAuthorizationProvider() {
        }

        private void assertRoleAndAuthenticationData(String role, AuthenticationDataSource authenticationData) {
            assertEquals(role, ADMIN_ROLE);
            if (authenticationData.hasDataFromHttp()) {
                String authorization = authenticationData.getHttpHeader("Authorization");
                assertEquals(authorization, "Bearer " + ADMIN_TOKEN);
            } else {
                assertEquals(authenticationData.getCommandData(), ADMIN_TOKEN);
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

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setAuthenticationEnabled(true);
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);
        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);

        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters(ADMIN_TOKEN);

        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationProvider(MyAuthorizationProvider.class.getName());
    }

    @SneakyThrows
    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {
        super.customizeNewPulsarAdminBuilder(pulsarAdminBuilder);
        pulsarAdminBuilder.authentication(AuthenticationToken.class.getName(), ADMIN_TOKEN);
    }

    @SneakyThrows
    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        super.customizeNewPulsarClientBuilder(clientBuilder);
        clientBuilder.authentication(AuthenticationToken.class.getName(), ADMIN_TOKEN);
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void testAdmin() throws PulsarAdminException {
        admin.tenants().createTenant("test-tenant-1",
                TenantInfo.builder().allowedClusters(Collections.singleton(configClusterName)).build());
        admin.namespaces().createNamespace("test-tenant-1/test-namespace-1");
        String partitionedTopic = UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(partitionedTopic,3);
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
}
