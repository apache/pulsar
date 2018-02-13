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

import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.naming.AuthenticationException;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
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

        conf.setBrokerClientAuthenticationPlugin(TestAuthenticationProvider.class.getName());

        Set<String> providers = new HashSet<>();
        providers.add(TestAuthenticationProvider.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("use");

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

        ClientConfiguration adminConf = new ClientConfiguration();
        Authentication adminAuthentication = new ClientAuthentication("superUser");
        adminConf.setAuthentication(adminAuthentication);
        admin = spy(new PulsarAdmin(brokerUrl, adminConf));

        String lookupUrl;
        lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT).toString();

        ClientConfiguration clientConfValid = new ClientConfiguration();
        Authentication authentication = new ClientAuthentication(clientRole);
        clientConfValid.setAuthentication(authentication);

        ClientConfiguration clientConfInvalidRole = new ClientConfiguration();
        Authentication authenticationInvalidRole = new ClientAuthentication("test-role");
        clientConfInvalidRole.setAuthentication(authenticationInvalidRole);

        pulsarClient = PulsarClient.create(lookupUrl, clientConfValid);
        PulsarClient pulsarClientInvalidRole = PulsarClient.create(lookupUrl, clientConfInvalidRole);

        admin.properties().createProperty("my-property",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");

        // (1) Valid Producer and consumer creation
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic", "my-subscriber-name");
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic");
        consumer.close();
        producer.close();

        // (2) InValid user auth-role will be rejected by authorization service
        try {
            consumer = pulsarClientInvalidRole.subscribe("persistent://my-property/use/my-ns/my-topic",
                    "my-subscriber-name");
            Assert.fail("should have failed with authorization error");
        } catch (PulsarClientException.AuthorizationException pa) {
            // Ok
        }
        try {
            producer = pulsarClientInvalidRole.createProducer("persistent://my-property/use/my-ns/my-topic");
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
        DestinationName destination = DestinationName.get("persistent://prop/cluster/ns/t1");
        String role = "test-role";
        Assert.assertFalse(authorizationService.canProduce(destination, role, null));
        Assert.assertFalse(authorizationService.canConsume(destination, role, null, "sub1"));
        authorizationService
                .grantPermissionAsync(destination, null, role, "auth-json").get();
        Assert.assertTrue(authorizationService.canProduce(destination, role, null));
        Assert.assertTrue(authorizationService.canConsume(destination, role, null, "sub1"));

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testAuthData() throws Exception {
        log.info("-- Starting {} test --", methodName);

        conf.setAuthorizationProvider(TestAuthorizationProviderWithGrantPermission.class.getName());
        setup();

        AuthorizationService authorizationService = new AuthorizationService(conf, null);
        DestinationName destination = DestinationName.get("persistent://prop/cluster/ns/t1");
        String role = "test-role";
        authorizationService
                .grantPermissionAsync(destination, null, role, "auth-json")
                .get();
        Assert.assertEquals(TestAuthorizationProviderWithGrantPermission.authDataJson, "auth-json");
        Assert.assertTrue(
                authorizationService.canProduce(destination, role, new AuthenticationDataCommand("prod-auth")));
        Assert.assertEquals(TestAuthorizationProviderWithGrantPermission.authenticationData.getCommandData(),
                "prod-auth");
        Assert.assertTrue(authorizationService.canConsume(destination, role, new AuthenticationDataCommand("cons-auth"),
                "sub1"));
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
        public CompletableFuture<Boolean> canProduceAsync(DestinationName destination, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(clientRole.equals(role));
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(DestinationName destination, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            return CompletableFuture.completedFuture(clientRole.equals(role));
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(DestinationName destination, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(clientRole.equals(role));
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions,
                String role, String authenticationData) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> grantPermissionAsync(DestinationName topicname, Set<AuthAction> actions,
                String role, String authenticationData) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * This provider always fails authorization on consumer and passes on producer
     *
     */
    public static class TestAuthorizationProvider2 extends TestAuthorizationProvider {

        @Override
        public CompletableFuture<Boolean> canProduceAsync(DestinationName destination, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(DestinationName destination, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(DestinationName destination, String role,
                AuthenticationDataSource authenticationData) {
            return CompletableFuture.completedFuture(true);
        }
    }

    public static class TestAuthorizationProviderWithGrantPermission extends TestAuthorizationProvider {

        private Set<String> grantRoles = Sets.newHashSet();
        static AuthenticationDataSource authenticationData;
        static String authDataJson;

        @Override
        public CompletableFuture<Boolean> canProduceAsync(DestinationName destination, String role,
                AuthenticationDataSource authenticationData) {
            this.authenticationData = authenticationData;
            return CompletableFuture.completedFuture(grantRoles.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> canConsumeAsync(DestinationName destination, String role,
                AuthenticationDataSource authenticationData, String subscription) {
            this.authenticationData = authenticationData;
            return CompletableFuture.completedFuture(grantRoles.contains(role));
        }

        @Override
        public CompletableFuture<Boolean> canLookupAsync(DestinationName destination, String role,
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
        public CompletableFuture<Void> grantPermissionAsync(DestinationName topicname, Set<AuthAction> actions,
                String role, String authData) {
            this.authDataJson = authData;
            grantRoles.add(role);
            return CompletableFuture.completedFuture(null);
        }
    }

}
