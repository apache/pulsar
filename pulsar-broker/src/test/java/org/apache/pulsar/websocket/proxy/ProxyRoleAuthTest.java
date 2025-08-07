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
package org.apache.pulsar.websocket.proxy;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import java.net.URI;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.ProxyServer;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.service.WebSocketServiceStarter;
import org.awaitility.Awaitility;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration test for WebSocket proxy with JWT token authentication and authorization enabled.
 * This test specifically verifies that the WebSocket dummy principal fix allows
 * WebSocket connections to work properly through a proxy when both authentication
 * and authorization are enabled, using real JWT tokens for different roles.
 *
 * The test uses four different JWT tokens:
 * - ADMIN_TOKEN: Used by admin client and broker's internal client for setup operations
 * - PROXY_TOKEN: Used by WebSocket proxy's internal client to connect to broker
 * - CLIENT_TOKEN: Used by WebSocket clients to authenticate to the proxy
 * - UNAUTHORIZED_TOKEN: Used to test that unauthorized tokens are properly rejected
 *
 * Test coverage:
 * 1. testWebSocketProxyProduceConsumeWithAuthorization: Positive test with authorized tokens
 * 2. testWebSocketProxyWithUnauthorizedToken: Negative test with unauthorized tokens
 */
@Test(groups = "websocket")
public class ProxyRoleAuthTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(ProxyRoleAuthTest.class);

    // JWT token authentication setup with different roles
    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String PROXY_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, "websocket_proxy",
            Optional.empty());
    private static final String CLIENT_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, "client", Optional.empty());
    private static final String ADMIN_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, "admin", Optional.empty());
    private static final String UNAUTHORIZED_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, "unauthorized_user",
            Optional.empty());

    private ProxyServer proxyServer;
    private WebSocketService service;
    private WebSocketClient consumeClient;
    private WebSocketClient produceClient;

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // Enable authentication and authorization in broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        // Configure proxy roles for the broker - this is critical for the WebSocket proxy fix
        conf.setProxyRoles(Sets.newHashSet("websocket_proxy"));

        // Set super user roles for admin operations and proxy role
        conf.setSuperUserRoles(Sets.newHashSet("admin", "websocket_proxy"));

        // Configure broker's internal client to use admin token
        conf.setBrokerClientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationToken");
        conf.setBrokerClientAuthenticationParameters("token:" + ADMIN_TOKEN);

        // Enable JWT token authentication provider
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));

        // Configure JWT token secret key for token validation
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        conf.setProperties(properties);
    }

    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();

        // Configure admin client with ADMIN_TOKEN for namespace and permission setup
        if (admin != null) {
            admin.close();
        }
        admin = PulsarAdmin.builder()
            .serviceHttpUrl(brokerUrl.toString())
            .authentication("org.apache.pulsar.client.impl.auth.AuthenticationToken",
                    "token:" + ADMIN_TOKEN)
            .build();

        // Setup namespace and grant permissions for client role
        setupNamespacePermissions();

        WebSocketProxyConfiguration proxyConfig = getProxyConfig();

        service = spyWithClassAndConstructorArgs(WebSocketService.class, proxyConfig);
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(service)
                .createConfigMetadataStore(anyString(), anyInt(), anyBoolean());

        proxyServer = new ProxyServer(proxyConfig);
        WebSocketServiceStarter.start(proxyServer, service);

        log.info("WebSocket Proxy Server started on port: {}", proxyServer.getListenPortHTTP().get());
    }

    protected WebSocketProxyConfiguration getProxyConfig() {
        // Create WebSocket proxy configuration with authentication and authorization enabled
        WebSocketProxyConfiguration proxyConfig = new WebSocketProxyConfiguration();
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setClusterName("test");
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(true);  // Enable authorization at proxy level
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);

        // Configure WebSocket proxy to use JWT token authentication
        proxyConfig.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));

        // Set up JWT token authentication properties for proxy
        Properties proxyProperties = new Properties();
        proxyProperties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        proxyConfig.setProperties(proxyProperties);

        // Configure proxy's internal client to use PROXY_TOKEN when connecting to broker
        proxyConfig.setBrokerClientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationToken");
        proxyConfig.setBrokerClientAuthenticationParameters("token:" + PROXY_TOKEN);

        // Set broker service URL to connect to our test broker
        proxyConfig.setBrokerServiceUrl(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerServiceUrlTls(pulsar.getBrokerServiceUrlTls());
        return  proxyConfig;
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        try {
            if (consumeClient != null) {
                consumeClient.stop();
            }
            if (produceClient != null) {
                produceClient.stop();
            }
            log.info("WebSocket clients stopped successfully");
        } catch (Exception e) {
            log.error("Error stopping WebSocket clients: {}", e.getMessage());
        }

        try {
            if (service != null) {
                service.close();
            }
            if (proxyServer != null) {
                proxyServer.stop();
            }
        } catch (Exception e) {
            log.error("Error stopping proxy server: {}", e.getMessage());
        }

        super.internalCleanup();
        log.info("Finished cleaning up test setup");
    }

    /**
     * Test WebSocket message produce and consume through proxy with JWT token authentication and authorization enabled.
     * This verifies the fix for WebSocket dummy principal authorization issue using real JWT tokens.
     *
     * The test validates that:
     * 1. Admin client uses ADMIN_TOKEN for setup operations (namespace creation, permission grants)
     * 2. WebSocket proxy uses PROXY_TOKEN for its internal client connection to broker
     * 3. WebSocket clients use CLIENT_TOKEN for authentication to the proxy via Authorization header
     * 4. Topic-level permissions are correctly granted to the "client" role
     * 5. The WebSocket dummy principal fix prevents authorization failures when proxy role is configured
     * 6. The authorization service correctly handles different JWT tokens for different roles
     *    without blocking operations
     */
    @Test(timeOut = 30000)
    public void testWebSocketProxyProduceConsumeWithAuthorization() throws Exception {
        final String namespaceName = "my-property/my-ns";
        final String topic = namespaceName + "/my-websocket-topic";

        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/consumer/persistent/" + topic + "/my-sub";
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/producer/persistent/" + topic;

        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        consumeClient = new WebSocketClient();
        SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
        produceClient = new WebSocketClient();
        SimpleProducerSocket produceSocket = new SimpleProducerSocket();

        try {
            // Connect consumer with CLIENT_TOKEN in Authorization header
            consumeClient.start();
            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            // Add JWT token authentication for WebSocket client
            consumeRequest.setHeader("Authorization", "Bearer " + CLIENT_TOKEN);
            Future<Session> consumerFuture = consumeClient.connect(consumeSocket, consumeUri, consumeRequest);
            log.info("Connecting consumer to: {} with CLIENT_TOKEN", consumeUri);

            // Connect producer with CLIENT_TOKEN in Authorization header
            produceClient.start();
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            // Add JWT token authentication for WebSocket client
            produceRequest.setHeader("Authorization", "Bearer " + CLIENT_TOKEN);
            Future<Session> producerFuture = produceClient.connect(produceSocket, produceUri, produceRequest);
            log.info("Connecting producer to: {} with CLIENT_TOKEN", produceUri);

            // Verify connections are established
            Session consumerSession = consumerFuture.get(10, TimeUnit.SECONDS);
            Session producerSession = producerFuture.get(10, TimeUnit.SECONDS);

            Assert.assertTrue(consumerSession.isOpen(), "Consumer WebSocket session should be open");
            Assert.assertTrue(producerSession.isOpen(), "Producer WebSocket session should be open");

            // Wait for messages to be produced and consumed
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        Assert.assertFalse(produceSocket.getBuffer().isEmpty(),
                                "Producer should have sent messages");
                        Assert.assertEquals(produceSocket.getBuffer(), consumeSocket.getBuffer(),
                                "Consumer should receive all produced messages");
                    });

            log.info("Successfully produced and consumed {} messages through WebSocket proxy "
                    + "with JWT token authorization",
                    produceSocket.getBuffer().size());

            // Verify that we successfully exchanged messages with JWT token authentication
            // This proves that the WebSocket dummy principal fix is working correctly:
            // - Proxy authenticates to broker with PROXY_TOKEN (websocket_proxy role)
            // - Client authenticates to proxy with CLIENT_TOKEN (client role)
            // - Authorization service correctly handles the WebSocket dummy principal scenario
            Assert.assertTrue(produceSocket.getBuffer().size() >= 3,
                    "Should have exchanged at least 3 messages with JWT token authentication");

            log.info("Test passed: WebSocket proxy produce/consume works correctly with JWT token authorization");

        } finally {
            try {
                if (consumeClient != null && consumeClient.isStarted()) {
                    consumeClient.stop();
                }
                if (produceClient != null && produceClient.isStarted()) {
                    produceClient.stop();
                }
            } catch (Exception e) {
                log.warn("Error stopping WebSocket clients in finally block: {}", e.getMessage());
            }
        }
    }

    /**
     * Test WebSocket connections with unauthorized token should fail.
     * This verifies that the authorization system correctly rejects tokens without proper permissions.
     */
    @Test(timeOut = 30000)
    public void testWebSocketProxyWithUnauthorizedToken() throws Exception {
        final String namespaceName = "my-property/my-ns";
        final String topic = namespaceName + "/my-websocket-topic";

        final String consumerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/consumer/persistent/" + topic + "/my-sub-unauthorized";
        final String producerUri = "ws://localhost:" + proxyServer.getListenPortHTTP().get()
                + "/ws/v2/producer/persistent/" + topic;

        URI consumeUri = URI.create(consumerUri);
        URI produceUri = URI.create(producerUri);

        WebSocketClient unauthorizedConsumeClient = null;
        WebSocketClient unauthorizedProduceClient = null;

        try {
            // Test unauthorized consumer connection
            unauthorizedConsumeClient = new WebSocketClient();
            SimpleConsumerSocket consumeSocket = new SimpleConsumerSocket();
            unauthorizedConsumeClient.start();

            ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
            // Use UNAUTHORIZED_TOKEN which doesn't have permissions
            consumeRequest.setHeader("Authorization", "Bearer " + UNAUTHORIZED_TOKEN);
            Future<Session> consumerFuture = unauthorizedConsumeClient.connect(consumeSocket, consumeUri,
                    consumeRequest);

            log.info("Attempting to connect consumer with unauthorized token to: {}", consumeUri);

            try {
                Session consumerSession = consumerFuture.get(10, TimeUnit.SECONDS);
                // If we reach here, the connection succeeded when it should have failed
                if (consumerSession.isOpen()) {
                    Assert.fail("Consumer connection should have been rejected due to lack of permissions");
                }
            } catch (Exception e) {
                // Expected: Connection should fail due to authorization
                log.info("Consumer connection correctly failed with unauthorized token: {}", e.getMessage());
            }

            // Test unauthorized producer connection
            unauthorizedProduceClient = new WebSocketClient();
            SimpleProducerSocket produceSocket = new SimpleProducerSocket();
            unauthorizedProduceClient.start();

            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();
            // Use UNAUTHORIZED_TOKEN which doesn't have permissions
            produceRequest.setHeader("Authorization", "Bearer " + UNAUTHORIZED_TOKEN);
            Future<Session> producerFuture = unauthorizedProduceClient.connect(produceSocket, produceUri,
                    produceRequest);

            log.info("Attempting to connect producer with unauthorized token to: {}", produceUri);

            try {
                Session producerSession = producerFuture.get(10, TimeUnit.SECONDS);
                // If we reach here, the connection succeeded when it should have failed
                if (producerSession.isOpen()) {
                    Assert.fail("Producer connection should have been rejected due to lack of permissions");
                }
            } catch (Exception e) {
                // Expected: Connection should fail due to authorization
                log.info("Producer connection correctly failed with unauthorized token: {}", e.getMessage());
            }

            log.info("Test passed: Unauthorized tokens are correctly rejected by WebSocket proxy");

        } finally {
            // Clean up connections
            try {
                if (unauthorizedConsumeClient != null && unauthorizedConsumeClient.isStarted()) {
                    unauthorizedConsumeClient.stop();
                }
                if (unauthorizedProduceClient != null && unauthorizedProduceClient.isStarted()) {
                    unauthorizedProduceClient.stop();
                }
            } catch (Exception e) {
                log.warn("Error stopping unauthorized WebSocket clients in finally block: {}", e.getMessage());
            }
        }
    }

    /**
     * Setup namespace and permissions for testing authorization scenarios.
     * This method creates the necessary tenant, namespace, and grants permissions.
     */
    private void setupNamespacePermissions() throws Exception {
        String namespaceName = "my-property/my-ns";
        try {
            // Create cluster if not exists
            admin.clusters().createCluster("test",
                org.apache.pulsar.common.policies.data.ClusterData.builder()
                    .serviceUrl(pulsar.getWebServiceAddress())
                    .serviceUrlTls(pulsar.getWebServiceAddressTls())
                    .brokerServiceUrl(pulsar.getBrokerServiceUrl())
                    .brokerServiceUrlTls(pulsar.getBrokerServiceUrlTls())
                    .build());
        } catch (Exception e) {
            // Cluster might already exist, ignore
            log.debug("Cluster creation failed (may already exist): {}", e.getMessage());
        }

        try {
            // Create tenant
            admin.tenants().createTenant("my-property",
                org.apache.pulsar.common.policies.data.TenantInfoImpl.builder()
                    .allowedClusters(Sets.newHashSet("test"))
                    .build());
        } catch (Exception e) {
            // Tenant might already exist, ignore
            log.debug("Tenant creation failed (may already exist): {}", e.getMessage());
        }

        try {
            // Create namespace
            admin.namespaces().createNamespace(namespaceName);
        } catch (Exception e) {
            // Namespace might already exist, ignore
            log.debug("Namespace creation failed (may already exist): {}", e.getMessage());
        }

        // Grant permissions for WebSocket proxy and client
        try {
            // Grant permissions for the proxy role
            admin.namespaces().grantPermissionOnNamespace(namespaceName, "client",
                    Sets.newHashSet(
                        org.apache.pulsar.common.policies.data.AuthAction.consume,
                        org.apache.pulsar.common.policies.data.AuthAction.produce));
            log.info("Granted permissions for namespace: {}", namespaceName);
        } catch (Exception e) {
            log.warn("Failed to grant permissions (may already exist): {}", e.getMessage());
        }
    }
}