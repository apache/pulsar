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
package org.apache.pulsar.proxy.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import com.google.common.collect.Sets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProxyAuthenticationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyAuthenticationTest.class);
    private static final String CLUSTER_NAME = "test";

    public static class BasicAuthenticationData implements AuthenticationDataProvider {
        private final String authParam;

        public BasicAuthenticationData(String authParam) {
            this.authParam = authParam;
        }

        public boolean hasDataFromCommand() {
            return true;
        }

        public String getCommandData() {
            return authParam;
        }

        public boolean hasDataForHttp() {
            return true;
        }

        @Override
        public Set<Entry<String, String>> getHttpHeaders() {
            Map<String, String> headers = new HashMap<>();
            headers.put("BasicAuthentication", authParam);
            headers.put("X-Pulsar-Auth-Method-Name", "BasicAuthentication");
            return headers.entrySet();
        }
    }

    public static class BasicAuthentication implements Authentication {

        private String authParam;

        @Override
        public void close() throws IOException {
            // noop
        }

        @Override
        public String getAuthMethodName() {
            return "BasicAuthentication";
        }

        @Override
        public AuthenticationDataProvider getAuthData() throws PulsarClientException {
            try {
                return new BasicAuthenticationData(authParam);
            } catch (Exception e) {
                throw new PulsarClientException(e);
            }
        }

        @Override
        public void configure(Map<String, String> authParams) {
            this.authParam = String.format("{\"entityType\": \"%s\", \"expiryTime\": \"%s\"}",
                    authParams.get("entityType"), authParams.get("expiryTime"));
        }

        @Override
        public void start() throws PulsarClientException {
            // noop
        }
    }

    public static class BasicAuthenticationState implements AuthenticationState {
        private final long expiryTimeInMillis;
        private final String authRole;
        private final AuthenticationDataSource authenticationDataSource;

        private static boolean isExpired(long expiryTimeInMillis) {
            return System.currentTimeMillis() > expiryTimeInMillis;
        }

        private static String[] parseAuthData(String commandData) {
            JsonObject element = JsonParser.parseString(commandData).getAsJsonObject();
            long expiryTimeInMillis = Long.parseLong(element.get("expiryTime").getAsString());
            if (isExpired(expiryTimeInMillis)) {
                throw new IllegalArgumentException("Credentials have expired");
            }
            String role = element.get("entityType").getAsString();
            return new String[]{role, String.valueOf(expiryTimeInMillis)};
        }

        public BasicAuthenticationState(AuthenticationDataSource authData) {
            this(authData.hasDataFromCommand() ? authData.getCommandData()
                    : authData.getHttpHeader("BasicAuthentication"));
        }

        public BasicAuthenticationState(AuthData authData) {
            this(new String(authData.getBytes(), StandardCharsets.UTF_8));
        }

        private BasicAuthenticationState(String commandData) {
            String[] parsed = parseAuthData(commandData);
            this.authRole = parsed[0];
            this.expiryTimeInMillis = Long.parseLong(parsed[1]);
            this.authenticationDataSource = new AuthenticationDataCommand(commandData, null, null);
        }

        @Override
        public String getAuthRole() {
            return authRole;
        }

        @Override
        public AuthData authenticate(AuthData authData) throws AuthenticationException {
            return null; // Authentication complete
        }

        @Override
        public CompletableFuture<AuthData> authenticateAsync(AuthData authData) {
            return CompletableFuture.completedFuture(null); // Authentication complete
        }

        @Override
        public AuthenticationDataSource getAuthDataSource() {
            return authenticationDataSource;
        }

        @Override
        public boolean isComplete() {
            return authRole != null;
        }

        @Override
        public boolean isExpired() {
            return isExpired(expiryTimeInMillis);
        }
    }

    public static class BasicAuthenticationProvider implements AuthenticationProvider {

        @Override
        public void close() throws IOException {
        }

        @Override
        public void initialize(ServiceConfiguration config) throws IOException {
        }

        @Override
        public String getAuthMethodName() {
            return "BasicAuthentication";
        }

        @Override
        public AuthenticationState newAuthState(AuthData authData, SocketAddress remoteAddress, SSLSession sslSession) {
            return new BasicAuthenticationState(authData);
        }

        @Override
        public CompletableFuture<String> authenticateAsync(AuthenticationDataSource authData) {
            BasicAuthenticationState basicAuthenticationState = new BasicAuthenticationState(authData);
            return CompletableFuture.supplyAsync(basicAuthenticationState::getAuthRole);
        }
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
        // Expires after an hour
        conf.setBrokerClientAuthenticationParameters(
                "entityType:admin,expiryTime:" + (System.currentTimeMillis() + 3600 * 1000));

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<>();
        providers.add(BasicAuthenticationProvider.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName(CLUSTER_NAME);
        Set<String> proxyRoles = new HashSet<>();
        proxyRoles.add("proxy");
        conf.setProxyRoles(proxyRoles);
        conf.setAuthenticateOriginalAuthData(true);
        super.init();

        updateAdminClient();
        producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    void testAuthentication() throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Step 1: Create Admin Client
        updateAdminClient();
        // create a client which connects to proxy and pass authData
        String namespaceName = "my-property/my-ns";
        String topicName = "persistent://my-property/my-ns/my-topic1";
        String subscriptionName = "my-subscriber-name";
        // expires after 60 seconds
        String clientAuthParams = "entityType:client,expiryTime:" + (System.currentTimeMillis() + 60 * 1000);
        // expires after 60 seconds
        String proxyAuthParams = "entityType:proxy,expiryTime:" + (System.currentTimeMillis() + 60 * 1000);

        admin.namespaces().grantPermissionOnNamespace(namespaceName, "proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(namespaceName, "client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        // Step 2: Try to use proxy Client as a normal Client - expect exception
        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setClusterName(CLUSTER_NAME);

        proxyConfig.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(proxyAuthParams);

        Set<String> providers = new HashSet<>();
        providers.add(BasicAuthenticationProvider.class.getName());
        proxyConfig.setAuthenticationProviders(providers);
        proxyConfig.setForwardAuthorizationCredentials(true);
                AuthenticationService authenticationService = new AuthenticationService(
                        PulsarConfigurationLoader.convertFrom(proxyConfig));
        @Cleanup
        final Authentication proxyClientAuthentication =
                AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();
        @Cleanup
        ProxyService proxyService = new ProxyService(proxyConfig, authenticationService, proxyClientAuthentication);

        proxyService.start();
        final String proxyServiceUrl = proxyService.getServiceUrl();

        // Step 3: Pass correct client params and use multiple connections
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyServiceUrl, clientAuthParams, 3);
        proxyClient.newProducer(Schema.BYTES).topic(topicName).create();
        proxyClient.newProducer(Schema.BYTES).topic(topicName).create();
        proxyClient.newProducer(Schema.BYTES).topic(topicName).create();

        // Step 4: Ensure that all client contexts share the same auth provider
        Assert.assertTrue(proxyService.getClientCnxs().size() >= 3, "expect at least 3 clients");
        proxyService.getClientCnxs().stream().forEach((cnx) -> {
            Assert.assertSame(cnx.authenticationProvider,
                    proxyService.getAuthenticationService().getAuthenticationProvider("BasicAuthentication"));
        });
    }

    private void updateAdminClient() throws PulsarClientException {
        // Expires after an hour
        String adminAuthParams = "entityType:admin,expiryTime:" + (System.currentTimeMillis() + 3600 * 1000);
        closeAdmin();
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(BasicAuthentication.class.getName(), adminAuthParams).build());
    }

    private PulsarClient createPulsarClient(String proxyServiceUrl, String authParams, int numberOfConnections)
            throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(proxyServiceUrl)
                .authentication(BasicAuthentication.class.getName(), authParams)
                .connectionsPerBroker(numberOfConnections).build();
    }

    @Test
    void testClientDisconnectWhenCredentialsExpireWithoutForwardAuth() throws Exception {
        log.info("-- Starting {} test --", methodName);

        String namespaceName = "my-property/my-ns";
        String topicName = "persistent://my-property/my-ns/my-topic1";

        admin.namespaces().grantPermissionOnNamespace(namespaceName, "proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(namespaceName, "client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        // Important: When forwardAuthorizationCredentials=false, broker should not authenticate original auth data
        // because the proxy doesn't forward it. Set authenticateOriginalAuthData=false to match this behavior.
        conf.setAuthenticateOriginalAuthData(false);

        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthenticationRefreshCheckSeconds(2); // Check every 2 seconds
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setClusterName(CLUSTER_NAME);

        // Proxy auth with long expiry
        String proxyAuthParams = "entityType:proxy,expiryTime:" + (System.currentTimeMillis() + 3600 * 1000);
        proxyConfig.setBrokerClientAuthenticationPlugin(BasicAuthentication.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(proxyAuthParams);

        Set<String> providers = new HashSet<>();
        providers.add(BasicAuthenticationProvider.class.getName());
        proxyConfig.setAuthenticationProviders(providers);
        proxyConfig.setForwardAuthorizationCredentials(false);

        @Cleanup
        AuthenticationService authenticationService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));
        @Cleanup
        final Authentication proxyClientAuthentication =
                AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();
        @Cleanup
        ProxyService proxyService = new ProxyService(proxyConfig, authenticationService, proxyClientAuthentication);
        proxyService.start();
        final String proxyServiceUrl = proxyService.getServiceUrl();

        // Create client with credentials that will expire in 3 seconds
        long clientExpireTime = System.currentTimeMillis() + 3 * 1000;
        String clientAuthParams = "entityType:client,expiryTime:" + clientExpireTime;

        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyServiceUrl, clientAuthParams, 1);

        @Cleanup
        var producer =
                proxyClient.newProducer(Schema.BYTES).topic(topicName).sendTimeout(5, TimeUnit.SECONDS).create();
        producer.send("test message".getBytes());

        Awaitility.await().untilAsserted(() -> {
            assertThatThrownBy(() -> producer.send("test message after expiry".getBytes()))
                    .isExactlyInstanceOf(PulsarClientException.TimeoutException.class);
        });

        if (producer instanceof ProducerImpl<byte[]> producerImpl) {
            long lastDisconnectedTimestamp = producerImpl.getLastDisconnectedTimestamp();
            assertThat(lastDisconnectedTimestamp).isGreaterThan(clientExpireTime);
        }
    }
}
