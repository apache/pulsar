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

import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;

import java.util.*;
import java.util.concurrent.TimeUnit;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;

public class ProxyWithJwtAuthorizationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyWithAuthorizationTest.class);

    private final String ADMIN_ROLE = "admin";
    private final String PROXY_ROLE = "proxy";
    private final String BROKER_ROLE = "broker";
    private final String CLIENT_ROLE = "client";
    private final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    private final String ADMIN_TOKEN = Jwts.builder().setSubject(ADMIN_ROLE).signWith(SECRET_KEY).compact();
    private final String PROXY_TOKEN = Jwts.builder().setSubject(PROXY_ROLE).signWith(SECRET_KEY).compact();
    private final String BROKER_TOKEN = Jwts.builder().setSubject(BROKER_ROLE).signWith(SECRET_KEY).compact();
    private final String CLIENT_TOKEN = Jwts.builder().setSubject(CLIENT_ROLE).signWith(SECRET_KEY).compact();

    private ProxyService proxyService;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // enable auth&auth and use JWT at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64," + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add(ADMIN_ROLE);
        superUserRoles.add(PROXY_ROLE);
        superUserRoles.add(BROKER_ROLE);
        conf.setSuperUserRoles(superUserRoles);

        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters(BROKER_TOKEN);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("proxy-authorization");
        conf.setNumExecutorThreadPoolSize(5);

        super.init();

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.getProperties().setProperty("tokenSecretKey", "data:;base64," + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));

        // enable auth&auth and use JWT at proxy
        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(PROXY_TOKEN);
        proxyConfig.setAuthenticationProviders(providers);

        proxyService = Mockito.spy(new ProxyService(proxyConfig,
                new AuthenticationService(
                        PulsarConfigurationLoader.convertFrom(proxyConfig))));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        proxyService.close();
    }

    private void startProxy() throws Exception {
        proxyService.start();
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
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), PulsarClient.builder());

        String namespaceName = "my-property/proxy-authorization/my-ns";

        admin.clusters().createCluster("proxy-authorization", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);

        Consumer<byte[]> consumer;
        try {
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed with authorization error");
        } catch (Exception ex) {
            // excepted
            admin.namespaces().grantPermissionOnNamespace(namespaceName, CLIENT_ROLE,
                    Sets.newHashSet(AuthAction.consume));
            log.info("-- Admin permissions {} ---", admin.namespaces().getPermissions(namespaceName));
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
        }

        Producer<byte[]> producer;
        try {
            producer = proxyClient.newProducer(Schema.BYTES)
                    .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1").create();
            Assert.fail("should have failed with authorization error");
        } catch (Exception ex) {
            // excepted
            admin.namespaces().grantPermissionOnNamespace(namespaceName, CLIENT_ROLE,
                    Sets.newHashSet(AuthAction.produce, AuthAction.consume));
            log.info("-- Admin permissions {} ---", admin.namespaces().getPermissions(namespaceName));
            producer = proxyClient.newProducer(Schema.BYTES)
                    .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1").create();
        }
        final int msgs = 10;
        for (int i = 0; i < msgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
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
     * </pre>
     */
    @Test
    public void testUpdatePartitionNumAndReconnect() throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        createAdminClient();
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), PulsarClient.builder());

        String clusterName = "proxy-authorization";
        String namespaceName = "my-property/my-ns";
        String topicName = "persistent://my-property/my-ns/my-topic1";
        String subscriptionName = "my-subscriber-name";

        admin.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet(clusterName)));
        admin.namespaces().createNamespace(namespaceName);
        admin.topics().createPartitionedTopic(topicName, 2);
        admin.topics().grantPermission(topicName, CLIENT_ROLE,
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        Consumer<byte[]> consumer = proxyClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName).subscribe();

        Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES)
                .topic(topicName).create();
        final int MSG_NUM = 10;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < MSG_NUM; i++) {
            String message = "my-message-" + i;
            messageSet.add(message);
            producer.send(message.getBytes());
        }

        Message<byte[]> msg;
        Set<String> receivedMessageSet = Sets.newHashSet();
        for (int i = 0; i < MSG_NUM; i++) {
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
        for (int i = 0; i < MSG_NUM; i++) {
            String message = "my-message-" + i;
            messageSet.add(message);
            producer.send(message.getBytes());
        }

        receivedMessageSet.clear();
        for (int i = 0; i < MSG_NUM; i++) {
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
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), PulsarClient.builder());

        String namespaceName = "my-property/proxy-authorization/my-ns";

        admin.clusters().createCluster("proxy-authorization", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().grantPermissionOnNamespace(namespaceName, CLIENT_ROLE,
                Sets.newHashSet(AuthAction.produce, AuthAction.consume));
        admin.namespaces().setSubscriptionAuthMode(namespaceName, SubscriptionAuthMode.Prefix);

        Consumer<byte[]> consumer;
        try {
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed with authorization error");
        } catch (Exception ex) {
            // excepted
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                    .subscriptionName(CLIENT_ROLE + "-sub1").subscribe();
        }

        Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES)
                .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1").create();
        final int msgs = 10;
        for (int i = 0; i < msgs; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
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

    private void createAdminClient() throws Exception {
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(AuthenticationFactory.token(ADMIN_TOKEN)).build());
    }

    private PulsarClient createPulsarClient(String proxyServiceUrl, ClientBuilder clientBuilder)
            throws PulsarClientException {
        return clientBuilder.serviceUrl(proxyServiceUrl).statsInterval(0, TimeUnit.SECONDS)
                .authentication(AuthenticationFactory.token(CLIENT_TOKEN))
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
    }
}
