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
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.lang.reflect.Method;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.MultiRolesTokenAuthorizationProvider;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.logging.LoggingFeature;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class ProxyWithJwtAuthorizationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyWithJwtAuthorizationTest.class);
    private static final String CLUSTER_NAME = "proxy-authorization";

    private static final String ADMIN_ROLE = "admin";
    private static final String PROXY_ROLE = "proxy";
    private static final String BROKER_ROLE = "broker";
    private static final String CLIENT_ROLE = "client";
    private static final String ANONYMOUS_ROLE = "anonymous";
    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    private final String adminToken;
    private final String proxyToken;
    private final String brokerToken;
    private final String clientToken;
    private final boolean multiRoles;
    private final String roleClaim;

    @Factory(dataProvider = "authorizationModes")
    public ProxyWithJwtAuthorizationTest(boolean multiRoles, String roleClaim) {
        this.multiRoles = multiRoles;
        this.roleClaim = roleClaim;
        adminToken = Jwts.builder().claim(roleClaim, ADMIN_ROLE).signWith(SECRET_KEY).compact();
        proxyToken = Jwts.builder().claim(roleClaim, PROXY_ROLE).signWith(SECRET_KEY).compact();
        brokerToken = Jwts.builder().claim(roleClaim, BROKER_ROLE).signWith(SECRET_KEY).compact();
        clientToken = Jwts.builder().claim(roleClaim, CLIENT_ROLE).signWith(SECRET_KEY).compact();
    }

    @DataProvider
    public static Object[][] authorizationModes() {
        return new Object[][]{{false, "sub"}, {false, "roles"}, {true, "roles"}};
    }

    private ProxyService proxyService;
    private WebServer webServer;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @BeforeMethod
    public void setupForTest(Method method) throws Exception {
        String anonymousRole = method.getName().equals("testAnonymousClientPermissionsWithSuperUserProxy")
                ? ANONYMOUS_ROLE : null;
        conf.setAuthenticateOriginalAuthData(
                !method.getName().equals("testForwardedPrincipalPermissions"));
        proxyConfig.setForwardAuthorizationCredentials(true);
        conf.setAnonymousUserRole(anonymousRole);
        proxyConfig.setAnonymousUserRole(anonymousRole);
        setup();
    }

    @Override
    protected void setup() throws Exception {
        // enable auth&auth and use JWT at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        if (multiRoles) {
            conf.setAuthorizationProvider(MultiRolesTokenAuthorizationProvider.class.getName());
        }
        if (!"sub".equals(roleClaim)) {
            conf.getProperties().setProperty("tokenAuthClaim", roleClaim);
        }
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add(ADMIN_ROLE);
        superUserRoles.add(PROXY_ROLE);
        superUserRoles.add(BROKER_ROLE);
        conf.setSuperUserRoles(superUserRoles);
        conf.setProxyRoles(Collections.singleton(PROXY_ROLE));

        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters(brokerToken);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName(CLUSTER_NAME);
        conf.setNumExecutorThreadPoolSize(5);

        super.init();

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        if (!"sub".equals(roleClaim)) {
            proxyConfig.getProperties().setProperty("tokenAuthClaim", roleClaim);
        }
        proxyConfig.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerWebServiceURL(pulsar.getWebServiceAddress());

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setClusterName(CLUSTER_NAME);

        // enable auth&auth and use JWT at proxy
        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(proxyToken);
        proxyConfig.setAuthenticationProviders(providers);
        proxyConfig.setStatusFilePath("./src/test/resources/vip_status.html");

        AuthenticationService authService =
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig));
        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();
        proxyService = Mockito.spy(new ProxyService(proxyConfig, authService, proxyClientAuthentication));
        webServer = new WebServer(proxyConfig, authService);
    }

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder builder) {
        super.customizeNewPulsarAdminBuilder(builder);
        builder.authentication(AuthenticationFactory.token(adminToken));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        proxyService.close();
        webServer.stop();
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    private void startProxy() throws Exception {
        proxyService.start();
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, proxyService, null, proxyClientAuthentication);
        webServer.start();
    }

    @Test
    public void testAnonymousClientPermissionsWithSuperUserProxy() throws Exception {
        startProxy();
        admin.clusters().createCluster(CLUSTER_NAME,
                ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("anonymous-client",
                new TenantInfoImpl(Set.of(ADMIN_ROLE), Set.of(CLUSTER_NAME)));
        admin.namespaces().createNamespace("anonymous-client/ns");
        String allowedTopic = "persistent://anonymous-client/ns/allowed";
        String consumeOnlyTopic = "persistent://anonymous-client/ns/consume-only";
        String produceOnlyTopic = "persistent://anonymous-client/ns/produce-only";
        String deniedTopic = "persistent://anonymous-client/ns/denied";
        for (String topic : List.of(allowedTopic, consumeOnlyTopic, produceOnlyTopic, deniedTopic)) {
            admin.topics().createNonPartitionedTopic(topic);
        }
        admin.topics().grantPermission(allowedTopic, ANONYMOUS_ROLE, Set.of(AuthAction.produce, AuthAction.consume));
        admin.topics().grantPermission(consumeOnlyTopic, ANONYMOUS_ROLE, Set.of(AuthAction.consume));
        admin.topics().grantPermission(produceOnlyTopic, ANONYMOUS_ROLE, Set.of(AuthAction.produce));

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .operationTimeout(5, TimeUnit.SECONDS).build();
        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer().topic(allowedTopic).subscriptionName("sub").subscribe();
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(allowedTopic).create();
        producer.send(new byte[]{1});
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        assertThat(message.getData()).containsExactly((byte) 1);

        // Each topic permits lookup through the other action, so these exercise produce/consume authorization.
        assertThatThrownBy(() -> {
            try (Producer<byte[]> ignored = client.newProducer().topic(consumeOnlyTopic).create()) {
                // Creation should be rejected.
            }
        }).isInstanceOf(PulsarClientException.AuthorizationException.class);
        assertThatThrownBy(() -> {
            try (Consumer<byte[]> ignored = client.newConsumer().topic(produceOnlyTopic)
                    .subscriptionName("sub").subscribe()) {
                // Subscription should be rejected.
            }
        }).isInstanceOf(PulsarClientException.AuthorizationException.class);

        @Cleanup
        PulsarAdmin anonymousAdmin = PulsarAdmin.builder().serviceHttpUrl(webServer.getServiceUri().toString()).build();
        assertThat(anonymousAdmin.topics().getStats(allowedTopic)).isNotNull();
        assertThatThrownBy(() -> anonymousAdmin.topics().getStats(deniedTopic))
                .isInstanceOf(PulsarAdminException.NotAuthorizedException.class);
    }

    @DataProvider
    public Object[][] forwardedCredentialSettings() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "forwardedCredentialSettings")
    public void testForwardedPrincipalPermissions(boolean forwardCredentials) throws Exception {
        proxyConfig.setForwardAuthorizationCredentials(forwardCredentials);
        startProxy();
        admin.clusters().createCluster(CLUSTER_NAME,
                ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("forwarded-client",
                new TenantInfoImpl(Set.of(ADMIN_ROLE), Set.of(CLUSTER_NAME)));
        String namespace = "forwarded-client/ns";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setSubscriptionAuthMode(namespace, SubscriptionAuthMode.Prefix);
        String allowedTopic = "persistent://" + namespace + "/allowed";
        String consumeOnlyTopic = "persistent://" + namespace + "/consume-only";
        String produceOnlyTopic = "persistent://" + namespace + "/produce-only";
        String additionalRoleTopic = "persistent://" + namespace + "/additional-role";
        for (String topic : List.of(allowedTopic, consumeOnlyTopic, produceOnlyTopic, additionalRoleTopic)) {
            admin.topics().createNonPartitionedTopic(topic);
        }
        admin.topics().grantPermission(allowedTopic, CLIENT_ROLE, Set.of(AuthAction.produce, AuthAction.consume));
        admin.topics().grantPermission(consumeOnlyTopic, CLIENT_ROLE, Set.of(AuthAction.consume));
        admin.topics().grantPermission(produceOnlyTopic, CLIENT_ROLE, Set.of(AuthAction.produce));
        admin.topics().grantPermission(additionalRoleTopic, "additional-role",
                Set.of(AuthAction.produce, AuthAction.consume));

        String token = Jwts.builder()
                .claim(roleClaim, multiRoles ? new String[]{CLIENT_ROLE, "additional-role"} : CLIENT_ROLE)
                .signWith(SECRET_KEY).compact();
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .authentication(AuthenticationFactory.token(token)).operationTimeout(5, TimeUnit.SECONDS).build();
        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer().topic(allowedTopic)
                .subscriptionName(CLIENT_ROLE + "-sub").subscribe();
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(allowedTopic).create();
        producer.send(new byte[]{1});
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        assertThat(message.getData()).containsExactly((byte) 1);

        // Lookup is allowed on these topics; the requested action must still be checked.
        assertThatThrownBy(() -> {
            try (Producer<byte[]> ignored = client.newProducer().topic(consumeOnlyTopic).create()) {
                // Creation should be rejected.
            }
        }).isInstanceOf(PulsarClientException.AuthorizationException.class);
        assertThatThrownBy(() -> {
            try (Consumer<byte[]> ignored = client.newConsumer().topic(produceOnlyTopic)
                    .subscriptionName(CLIENT_ROLE + "-sub").subscribe()) {
                // Subscription should be rejected.
            }
        }).isInstanceOf(PulsarClientException.AuthorizationException.class);
        assertThatThrownBy(() -> {
            try (Consumer<byte[]> ignored = client.newConsumer().topic(allowedTopic)
                    .subscriptionName("other-sub").subscribe()) {
                // Subscription prefix must match the forwarded principal.
            }
        }).isInstanceOf(PulsarClientException.AuthorizationException.class);
        assertThatThrownBy(() -> {
            try (Producer<byte[]> ignored = client.newProducer().topic(additionalRoleTopic).create()) {
                // Additional roles require original-client authentication.
            }
        }).isInstanceOf(PulsarClientException.AuthorizationException.class);
    }

    @Test
    public void testMultiRoleClientPermissionsWithSuperUserProxy() throws Exception {
        assertThat(conf.isAuthenticateOriginalAuthData()).isTrue();
        assertThat(proxyConfig.isForwardAuthorizationCredentials()).isTrue();
        startProxy();
        createAdminClient();
        admin.clusters().createCluster(CLUSTER_NAME,
                ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("multi-role",
                new TenantInfoImpl(Set.of(ADMIN_ROLE), Set.of(CLUSTER_NAME)));
        admin.namespaces().createNamespace("multi-role/ns");
        String allowedTopic = "persistent://multi-role/ns/allowed";
        String deniedTopic = "persistent://multi-role/ns/denied";
        admin.topics().createNonPartitionedTopic(allowedTopic);
        admin.topics().createNonPartitionedTopic(deniedTopic);
        admin.topics().grantPermission(allowedTopic, CLIENT_ROLE, Set.of(AuthAction.produce, AuthAction.consume));
        admin.topics().grantPermission(deniedTopic, "other-client", Set.of(AuthAction.produce, AuthAction.consume));

        String token = Jwts.builder()
                .claim(roleClaim, multiRoles ? new String[]{"unprivileged", CLIENT_ROLE} : CLIENT_ROLE)
                .signWith(SECRET_KEY).compact();
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .authentication(AuthenticationFactory.token(token)).operationTimeout(5, TimeUnit.SECONDS).build();
        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer().topic(allowedTopic).subscriptionName("sub").subscribe();
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(allowedTopic).create();
        producer.send(new byte[]{1});
        Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        assertThat(message.getData()).containsExactly((byte) 1);

        assertThatThrownBy(() -> {
            try (Producer<byte[]> ignored = client.newProducer().topic(deniedTopic).create()) {
                // Creation should be rejected.
            }
        }).isInstanceOf(PulsarClientException.AuthorizationException.class);
        assertThatThrownBy(() -> {
            try (Consumer<byte[]> ignored = client.newConsumer().topic(deniedTopic)
                    .subscriptionName("sub").subscribe()) {
                // Subscription should be rejected.
            }
        }).isInstanceOf(PulsarClientException.AuthorizationException.class);
    }

    /**
     * <pre>
     * It verifies jwt + Authentication + Authorization (client -> proxy -> broker).
     *
     * 1. client connects to proxy over jwt and pass auth-data
     * 2. proxy authenticate client and retrieve client-role
     *    and send it to broker as originalPrincipal over jwt
     * 3. client creates producer/consumer via proxy
     * 4. broker authorize producer/consumer create request using originalPrincipal
     *
     * </pre>
     */
    @Test
    public void testProxyAuthorization() throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        createAdminClient();
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), PulsarClient.builder());

        String namespaceName = "my-property/my-ns";

        admin.clusters().createCluster("proxy-authorization", ClusterData.builder()
                .serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);

        Consumer<byte[]> consumer;
        try {
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed with authorization error");
        } catch (Exception ex) {
            // excepted
            admin.namespaces().grantPermissionOnNamespace(namespaceName, CLIENT_ROLE,
                    Sets.newHashSet(AuthAction.consume));
            log.info("-- Admin permissions {} ---", admin.namespaces().getPermissions(namespaceName));
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
        }

        Producer<byte[]> producer;
        try {
            producer = proxyClient.newProducer(Schema.BYTES)
                    .topic("persistent://my-property/my-ns/my-topic1").create();
            Assert.fail("should have failed with authorization error");
        } catch (Exception ex) {
            // excepted
            admin.namespaces().grantPermissionOnNamespace(namespaceName, CLIENT_ROLE,
                    Sets.newHashSet(AuthAction.produce, AuthAction.consume));
            log.info("-- Admin permissions {} ---", admin.namespaces().getPermissions(namespaceName));
            producer = proxyClient.newProducer(Schema.BYTES)
                    .topic("persistent://my-property/my-ns/my-topic1").create();
        }
        final int msgs = 10;
        for (int i = 0; i < msgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        int count = 0;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            count++;
        }
        // Acknowledge the consumption of all messages at once
        Assert.assertEquals(msgs, count);
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * <pre>
     * 1. Create a 2-partition topic and grant produce/consume permission to client role.
     * 2. Use producer/consumer with client role to process the topic, which is fine.
     * 2. Update the topic partition number to 4.
     * 3. Use new producer/consumer with client role to process the topic.
     * 4. Broker should authorize producer/consumer normally.
     * 5. revoke produce/consumer permission of topic
     * 6. new producer/consumer should not be authorized
     * </pre>
     */
    @Test
    public void testUpdatePartitionNumAndReconnect() throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        createAdminClient();
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), PulsarClient.builder());

        String clusterName = "proxy-authorization";
        String namespaceName = "my-property/my-ns";
        String topicName = "persistent://my-property/my-ns/my-topic1";
        String subscriptionName = "my-subscriber-name";

        admin.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(new HashSet<>(), Sets.newHashSet(clusterName)));
        admin.namespaces().createNamespace(namespaceName);
        admin.topics().createPartitionedTopic(topicName, 2);
        admin.topics().grantPermission(topicName, CLIENT_ROLE,
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        Consumer<byte[]> consumer = proxyClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName).subscribe();

        Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES)
                .topic(topicName).create();
        final int msgNum = 10;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < msgNum; i++) {
            String message = "my-message-" + i;
            messageSet.add(message);
            producer.send(message.getBytes());
        }

        Message<byte[]> msg;
        Set<String> receivedMessageSet = new HashSet<>();
        for (int i = 0; i < msgNum; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            receivedMessageSet.add(expectedMessage);
            consumer.acknowledgeAsync(msg);
        }
        Assert.assertEquals(messageSet, receivedMessageSet);
        consumer.close();
        producer.close();

        // update partition num
        admin.topics().updatePartitionedTopic(topicName, 4);

        // produce/consume the topic again
        consumer = proxyClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName).subscribe();
        producer = proxyClient.newProducer(Schema.BYTES)
                .topic(topicName).create();

        messageSet.clear();
        for (int i = 0; i < msgNum; i++) {
            String message = "my-message-" + i;
            messageSet.add(message);
            producer.send(message.getBytes());
        }

        receivedMessageSet.clear();
        for (int i = 0; i < msgNum; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            receivedMessageSet.add(expectedMessage);
            consumer.acknowledgeAsync(msg);
        }
        Assert.assertEquals(messageSet, receivedMessageSet);
        consumer.close();
        producer.close();

        // revoke produce/consume permission
        admin.topics().revokePermissions(topicName, CLIENT_ROLE);

        // produce/consume the topic should fail
        try {
            consumer = proxyClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subscriptionName).subscribe();
            Assert.fail("Should not pass");
        } catch (PulsarClientException.AuthorizationException ex) {
            // ok
        }
        try {
            producer = proxyClient.newProducer(Schema.BYTES)
                    .topic(topicName).create();
            Assert.fail("Should not pass");
        } catch (PulsarClientException.AuthorizationException ex) {
            // ok
        }
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * <pre>
     * It verifies jwt + Authentication + Authorization (client -> proxy -> broker).
     * It also test `SubscriptionAuthMode.Prefix` mode.
     *
     * 1. client connects to proxy over jwt and pass auth-data
     * 2. proxy authenticate client and retrieve client-role
     *    and send it to broker as originalPrincipal over jwt
     * 3. client creates producer/consumer via proxy
     * 4. broker authorize producer/consumer create request using originalPrincipal
     *
     * </pre>
     */
    @Test
    public void testProxyAuthorizationWithPrefixSubscriptionAuthMode() throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        createAdminClient();
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), PulsarClient.builder());

        String namespaceName = "my-property/my-ns";

        admin.clusters().createCluster("proxy-authorization", ClusterData.builder()
                .serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().grantPermissionOnNamespace(namespaceName, CLIENT_ROLE,
                Sets.newHashSet(AuthAction.produce, AuthAction.consume));
        admin.namespaces().setSubscriptionAuthMode(namespaceName, SubscriptionAuthMode.Prefix);

        Consumer<byte[]> consumer;
        try {
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed with authorization error");
        } catch (Exception ex) {
            // excepted
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/my-ns/my-topic1")
                    .subscriptionName(CLIENT_ROLE + "-sub1").subscribe();
        }

        Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES)
                .topic("persistent://my-property/my-ns/my-topic1").create();
        final int msgs = 10;
        for (int i = 0; i < msgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        int count = 0;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            count++;
        }
        // Acknowledge the consumption of all messages at once
        Assert.assertEquals(msgs, count);
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    void testGetStatus() throws Exception {
        log.info("-- Starting {} test --", methodName);
        final PulsarResources resource = new PulsarResources(registerCloseable(new ZKMetadataStore(mockZooKeeper)),
                registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal)));
        final AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));
        final WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, proxyService,
                registerCloseable(new BrokerDiscoveryProvider(proxyConfig, resource)), proxyClientAuthentication);
        webServer.start();
        @Cleanup
        final Client client = javax.ws.rs.client.ClientBuilder
                .newClient(new ClientConfig().register(LoggingFeature.class));
        try {
            final Response r = client.target(webServer.getServiceUri()).path("/status.html").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
        } finally {
            webServer.stop();
        }
        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    void testGetMetrics() throws Exception {
        log.info("-- Starting {} test --", methodName);
        startProxy();
        PulsarResources resource = new PulsarResources(registerCloseable(new ZKMetadataStore(mockZooKeeper)),
                registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal)));
        AuthenticationService authService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));
        proxyConfig.setAuthenticateMetricsEndpoint(false);
        WebServer webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, proxyService,
                registerCloseable(new BrokerDiscoveryProvider(proxyConfig, resource)), proxyClientAuthentication);
        webServer.start();
        @Cleanup
        Client client = javax.ws.rs.client.ClientBuilder.newClient(new ClientConfig().register(LoggingFeature.class));
        try {
            Response r = client.target(webServer.getServiceUri()).path("/metrics").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.OK.getStatusCode());
        } finally {
            webServer.stop();
        }
        proxyConfig.setAuthenticateMetricsEndpoint(true);
        webServer = new WebServer(proxyConfig, authService);
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, proxyService,
                registerCloseable(new BrokerDiscoveryProvider(proxyConfig, resource)), proxyClientAuthentication);
        webServer.start();
        try {
            Response r = client.target(webServer.getServiceUri()).path("/metrics").request().get();
            Assert.assertEquals(r.getStatus(), Response.Status.UNAUTHORIZED.getStatusCode());
        } finally {
            webServer.stop();
        }
        log.info("-- Exiting {} test --", methodName);
    }

    private void createAdminClient() throws Exception {
        closeAdmin();
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(webServer.getServiceUri().toString())
                .authentication(AuthenticationFactory.token(adminToken)).build());
    }

    private PulsarClient createPulsarClient(String proxyServiceUrl, ClientBuilder clientBuilder)
            throws PulsarClientException {
        return clientBuilder.serviceUrl(proxyServiceUrl).statsInterval(0, TimeUnit.SECONDS)
                .authentication(AuthenticationFactory.token(clientToken))
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
    }
}
