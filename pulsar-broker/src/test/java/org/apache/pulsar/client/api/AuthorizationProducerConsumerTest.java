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
package org.apache.pulsar.client.api;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.naming.AuthenticationException;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class AuthorizationProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(AuthorizationProducerConsumerTest.class);

    private final static String clientRole = "plugbleRole";

    protected void setup() throws Exception {

        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superUser");
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<>();
        providers.add(TestAuthenticationProvider.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("test");

        super.init();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * It verifies plugable authorization service
     *
     * <pre>
     * 1. Client passes correct authorization plugin-name + correct auth role: SUCCESS
     * 2. Client passes correct authorization plugin-name + incorrect auth-role: FAIL
     * 3. Client passes incorrect authorization plugin-name + correct auth-role: FAIL
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testProducerAndConsumerAuthorization() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(TestAuthorizationProvider.class.getName());
        setup();

        Authentication adminAuthentication = new ClientAuthentication("superUser");
        admin = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).authentication(adminAuthentication).build());

        String lookupUrl;
        lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT).toString();

        Authentication authentication = new ClientAuthentication(clientRole);
        Authentication authenticationInvalidRole = new ClientAuthentication("test-role");

        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl).authentication(authentication).build();
        PulsarClient pulsarClientInvalidRole = PulsarClient.builder().serviceUrl(lookupUrl)
                .authentication(authenticationInvalidRole).build();

        admin.tenants().createTenant("my-property",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));

        // (1) Valid Producer and consumer creation
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic")
                .subscriptionName("my-subscriber-name").subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic")
                .create();
        consumer.close();
        producer.close();

        // (2) InValid user auth-role will be rejected by authorization service
        try {
            consumer = pulsarClientInvalidRole.newConsumer().topic("persistent://my-property/my-ns/my-topic")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed with authorization error");
        } catch (PulsarClientException.AuthorizationException pa) {
            // Ok
        }
        try {
            producer = pulsarClientInvalidRole.newProducer().topic("persistent://my-property/my-ns/my-topic")
                    .create();
            Assert.fail("should have failed with authorization error");
        } catch (PulsarClientException.AuthorizationException pa) {
            // Ok
        }

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testSubscriptionPrefixAuthorization() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(TestAuthorizationProviderWithSubscriptionPrefix.class.getName());
        setup();

        Authentication adminAuthentication = new ClientAuthentication("superUser");
        admin = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).authentication(adminAuthentication).build());

        String lookupUrl;
        lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT).toString();

        Authentication authentication = new ClientAuthentication(clientRole);

        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl).authentication(authentication).build();

        admin.tenants().createTenant("prop-prefix",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("prop-prefix/ns", Sets.newHashSet("test"));

        // (1) Valid subscription name will be approved by authorization service
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://prop-prefix/ns/t1")
                .subscriptionName(clientRole + "-sub1").subscribe();
        consumer.close();

        // (2) InValid subscription name will be rejected by authorization service
        try {
            consumer = pulsarClient.newConsumer().topic("persistent://prop-prefix/ns/t1").subscriptionName("sub1")
                    .subscribe();
            Assert.fail("should have failed with authorization error");
        } catch (PulsarClientException.AuthorizationException pa) {
            // Ok
        }

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testGrantPermission() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(TestAuthorizationProviderWithGrantPermission.class.getName());
        setup();

        AuthorizationService authorizationService = new AuthorizationService(conf, null);
        TopicName topicName = TopicName.get("persistent://prop/cluster/ns/t1");
        String role = "test-role";
        Assert.assertFalse(authorizationService.canProduce(topicName, role, null));
        Assert.assertFalse(authorizationService.canConsume(topicName, role, null, "sub1"));
        authorizationService.grantPermissionAsync(topicName, null, role, "auth-json").get();
        Assert.assertTrue(authorizationService.canProduce(topicName, role, null));
        Assert.assertTrue(authorizationService.canConsume(topicName, role, null, "sub1"));

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testAuthData() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(TestAuthorizationProviderWithGrantPermission.class.getName());
        setup();

        AuthorizationService authorizationService = new AuthorizationService(conf, null);
        TopicName topicName = TopicName.get("persistent://prop/cluster/ns/t1");
        String role = "test-role";
        authorizationService.grantPermissionAsync(topicName, null, role, "auth-json").get();
        Assert.assertEquals(TestAuthorizationProviderWithGrantPermission.authDataJson, "auth-json");
        Assert.assertTrue(authorizationService.canProduce(topicName, role, new AuthenticationDataCommand("prod-auth")));
        Assert.assertEquals(TestAuthorizationProviderWithGrantPermission.authenticationData.getCommandData(),
                "prod-auth");
        Assert.assertTrue(
                authorizationService.canConsume(topicName, role, new AuthenticationDataCommand("cons-auth"), "sub1"));
        Assert.assertEquals(TestAuthorizationProviderWithGrantPermission.authenticationData.getCommandData(),
                "cons-auth");

        log.info("-- Exiting {} test --", methodName);
    }

    public static class ClientAuthentication implements Authentication {
        String user;

        public ClientAuthentication(String user) {
            this.user = user;
        }

        @Override
        public void close() throws IOException {
            // No-op
        }

        @Override
        public String getAuthMethodName() {
            return "test";
        }

        @Override
        public AuthenticationDataProvider getAuthData() throws PulsarClientException {
            AuthenticationDataProvider provider = new AuthenticationDataProvider() {
                public boolean hasDataForHttp() {
                    return true;
                }

                @SuppressWarnings("unchecked")
                public Set<Map.Entry<String, String>> getHttpHeaders() {
                    return Sets.newHashSet(Maps.immutableEntry("user", user));
                }

                public boolean hasDataFromCommand() {
                    return true;
                }

                public String getCommandData() {
                    return user;
                }
            };
            return provider;
        }

        @Override
        public void configure(Map<String, String> authParams) {
            // No-op
        }

        @Override
        public void start() throws PulsarClientException {
            // No-op
        }

    }

    public static class TestAuthenticationProvider implements AuthenticationProvider {

        @Override
        public void close() throws IOException {
            // no-op
        }

        @Override
        public void initialize(ServiceConfiguration config) throws IOException {
            // No-op
        }

        @Override
        public String getAuthMethodName() {
            return "test";
        }

        @Override
        public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
            return authData.getCommandData() != null ? authData.getCommandData() : authData.getHttpHeader("user");
        }

    }

    public static class TestAuthorizationProvider implements AuthorizationProvider {

        @Override
        public void close() throws IOException {
            // No-op
        }

        @Override
        public void initialize(ServiceConfiguration conf, ConfigurationCacheService configCache) throws IOException {
            // No-op
        }

        @Override
        public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(clientRole.equals(role));
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            return CompletableFuture.completedFuture(clientRole.equals(role));
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(clientRole.equals(role));
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions,
                String role, String authenticationData) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(TopicName topicname, Set<AuthAction> actions, String role,
                String authenticationData) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * This provider always fails authorization on consumer and passes on producer
     *
     */
    public static class TestAuthorizationProvider2 extends TestAuthorizationProvider {

        @Override
        public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(true);
        }
    }

    public static class TestAuthorizationProviderWithSubscriptionPrefix extends TestAuthorizationProvider {

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            if (isNotBlank(subscription)) {
                if (!subscription.startsWith(role)) {
                    future.completeExceptionally(new PulsarServerException(
                            "The subscription name needs to be prefixed by the authentication role"));
                }
            }
            future.complete(clientRole.equals(role));
            return future;
        }

    }

    public static class TestAuthorizationProviderWithGrantPermission extends TestAuthorizationProvider {

        private Set<String> grantRoles = Sets.newHashSet();
        static AuthenticationDataSource authenticationData;
        static String authDataJson;

        @Override
        public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            this.authenticationData = authenticationData;
            return CompletableFuture.completedFuture(grantRoles.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            this.authenticationData = authenticationData;
            return CompletableFuture.completedFuture(grantRoles.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                AuthenticationDataSource authenticationData) {
            this.authenticationData = authenticationData;
            return CompletableFuture.completedFuture(grantRoles.contains(role));
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions,
                String role, String authData) {
            this.authDataJson = authData;
            grantRoles.add(role);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(TopicName topicname, Set<AuthAction> actions, String role,
                String authData) {
            this.authDataJson = authData;
            grantRoles.add(role);
            return CompletableFuture.completedFuture(null);
        }
    }

}
