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
package org.apache.pulsar.tests.integration.auth.token;

import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.ProxyContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public abstract class PulsarTokenAuthenticationBaseSuite extends PulsarClusterTestBase {

    protected String superUserAuthToken;
    protected String proxyAuthToken;
    protected String clientAuthToken;

    protected abstract void createKeysAndTokens(PulsarContainer<?> container) throws Exception;
    protected abstract void configureBroker(BrokerContainer brokerContainer) throws Exception;
    protected abstract void configureProxy(ProxyContainer proxyContainer) throws Exception;

    protected abstract String createClientTokenWithExpiry(long expiryTime, TimeUnit unit) throws Exception;

    protected static final String SUPER_USER_ROLE = "super-user";
    protected static final String PROXY_ROLE = "proxy";
    protected static final String REGULAR_USER_ROLE = "client";

    protected ZKContainer<?> cmdContainer;

    @BeforeClass(alwaysRun = true)
    @Override
    public final void setupCluster() throws Exception {
        incrementSetupNumber();
        // Before starting the cluster, generate the secret key and the token
        // Use Zk container to have 1 container available before starting the cluster
        this.cmdContainer = new ZKContainer<>("cli-setup");
        cmdContainer
                .withNetwork(Network.newNetwork())
                .withNetworkAliases(ZKContainer.NAME)
                .withEnv("zkServers", ZKContainer.NAME);
        cmdContainer.start();

        createKeysAndTokens(cmdContainer);

        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(2)
                .numProxies(1)
                .clusterName(clusterName)
                .build();

        log.info("Setting up cluster {} with token authentication  and {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);

        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            configureBroker(brokerContainer);
            brokerContainer.withEnv("authenticationEnabled", "true");
            brokerContainer.withEnv("authenticationProviders",
                    "org.apache.pulsar.broker.authentication.AuthenticationProviderToken");
            brokerContainer.withEnv("authorizationEnabled", "true");
            brokerContainer.withEnv("superUserRoles", SUPER_USER_ROLE + "," + PROXY_ROLE);
            brokerContainer.withEnv("brokerClientAuthenticationPlugin", AuthenticationToken.class.getName());
            brokerContainer.withEnv("brokerClientAuthenticationParameters", "token:" + superUserAuthToken);
            brokerContainer.withEnv("authenticationRefreshCheckSeconds", "1");
            brokerContainer.withEnv("authenticateOriginalAuthData", "true");
        }

        ProxyContainer proxyContainer = pulsarCluster.getProxy();
        configureProxy(proxyContainer);
        proxyContainer.withEnv("authenticationEnabled", "true");
        proxyContainer.withEnv("authenticationProviders",
                "org.apache.pulsar.broker.authentication.AuthenticationProviderToken");
        proxyContainer.withEnv("authorizationEnabled", "true");
        proxyContainer.withEnv("brokerClientAuthenticationPlugin", AuthenticationToken.class.getName());
        proxyContainer.withEnv("brokerClientAuthenticationParameters", "token:" + proxyAuthToken);
        proxyContainer.withEnv("forwardAuthorizationCredentials", "true");

        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    @AfterClass(alwaysRun = true)
    @Override
    public final void tearDownCluster() throws Exception {
        super.tearDownCluster();
        cmdContainer.close();
    }

    @Test
    public void testPublishWithTokenAuth() throws Exception {
        final String tenant = "token-test-tenant" + randomName(4);
        final String namespace = tenant + "/ns-1";
        final String topic = "persistent://" + namespace + "/topic-1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .authentication(AuthenticationFactory.token(superUserAuthToken))
                .build();

        try {
        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Collections.singleton(REGULAR_USER_ROLE),
                        Collections.singleton(pulsarCluster.getClusterName())));

        } catch (Exception e) {
            e.printStackTrace();
        }

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));
        admin.namespaces().grantPermissionOnNamespace(namespace, REGULAR_USER_ROLE, EnumSet.allOf(AuthAction.class));

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .authentication(AuthenticationFactory.token(clientAuthToken))
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("my-sub")
                .subscribe();

        final int numMessages = 10;

        for (int i = 0; i < numMessages; i++) {
            producer.send("hello-" + i);
        }

        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive();
            assertEquals(msg.getValue(), "hello-" + i);

            consumer.acknowledge(msg);
        }

        // Test client with no auth and expect it to fail
        @Cleanup
        PulsarClient clientNoAuth = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        try {
            clientNoAuth.newProducer(Schema.STRING).topic(topic)
                    .create();
            fail("Should have failed to create producer");
        } catch (PulsarClientException e) {
            // Expected
        }
    }

    @Test
    public void testProxyRedirectWithTokenAuth() throws Exception {

        final String tenant = "token-test-tenant" + randomName(4);
        final String namespace = tenant + "/ns-1";
        final String topic = namespace + "/my-topic-1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .authentication(AuthenticationFactory.token(superUserAuthToken))
                .build();

        try {
            admin.tenants().createTenant(tenant,
                    new TenantInfoImpl(Collections.singleton(REGULAR_USER_ROLE),
                            Collections.singleton(pulsarCluster.getClusterName())));

        } catch (Exception e) {
            e.printStackTrace();
        }

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));
        admin.namespaces().grantPermissionOnNamespace(namespace, REGULAR_USER_ROLE,
                EnumSet.allOf(AuthAction.class));

        admin.topics().createPartitionedTopic(topic, 16);

        // Create the partitions
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .authentication(AuthenticationFactory.token(REGULAR_USER_ROLE))
                .build();

        // Force the topics to be created
        client.newProducer()
                .topic(topic)
                .create()
                .close();

        admin.topics().getList(namespace);

        // Test multiple stats request to make sure the proxy will try against all brokers and receive 307
        // responses that it will handle internally.
        for (int i = 0; i < 10; i++) {
            admin.topics().getStats(topic);
        }
    }

    @DataProvider(name = "shouldRefreshToken")
    public static Object[][] shouldRefreshToken() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Test(dataProvider = "shouldRefreshToken")
    public void testExpiringToken(boolean shouldRefreshToken) throws Exception {
        final String tenant = "token-test-tenant" + randomName(4);
        final String namespace = tenant + "/ns-1";
        final String topic = "persistent://" + namespace + "/topic-1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .authentication(AuthenticationFactory.token(superUserAuthToken))
                .build();

        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Collections.singleton(REGULAR_USER_ROLE),
                        Collections.singleton(pulsarCluster.getClusterName())));

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));
        admin.namespaces().grantPermissionOnNamespace(namespace, REGULAR_USER_ROLE, EnumSet.allOf(AuthAction.class));

        String initialToken = this.createClientTokenWithExpiry(5, TimeUnit.SECONDS);
        String refreshedToken = this.createClientTokenWithExpiry(30, TimeUnit.SECONDS);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .authentication(AuthenticationFactory.token(() -> {
                    if (shouldRefreshToken) {
                        try {
                            return refreshedToken;
                        } catch (Exception e) {
                            return null;
                        }
                    } else {
                        return initialToken;
                    }
                }))
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .sendTimeout(3, TimeUnit.SECONDS)
                .create();
        // Initially the token is valid and producer will be able to publish
        producer.send("hello-1");
        long lastDisconnectedTimestamp = producer.getLastDisconnectedTimestamp();

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        if (shouldRefreshToken) {
            // The token will have been refreshed, so the app won't see any error
            producer.send("hello-2");
            long timestamp = producer.getLastDisconnectedTimestamp();
            assertEquals(timestamp, lastDisconnectedTimestamp);
        } else {
            // The token has expired, so this next message will be rejected
            try {
                producer.send("hello-2");
                fail("Publish should have failed");
            } catch (PulsarClientException e) {
                // Expected
            }
        }
    }

    @Test
    public void testExpiringTokenWithRefreshAndProducerRestart() throws Exception {
        final String tenant = "token-expiry-test-tenant" + randomName(4);
        final String namespace = tenant + "/ns-1";
        final String topic = "persistent://" + namespace + "/topic-1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .authentication(AuthenticationFactory.token(superUserAuthToken))
                .build();

        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Collections.singleton(REGULAR_USER_ROLE),
                        Collections.singleton(pulsarCluster.getClusterName())));

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));
        admin.namespaces().grantPermissionOnNamespace(namespace, REGULAR_USER_ROLE, EnumSet.allOf(AuthAction.class));

        final int TokenExpiryTimeSecs = 2;
        String initialToken = this.createClientTokenWithExpiry(TokenExpiryTimeSecs, TimeUnit.SECONDS);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .authentication(AuthenticationFactory.token(() -> {
                    try {
                        return createClientTokenWithExpiry(TokenExpiryTimeSecs, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        return null;
                    }
                }))
                .build();

        Producer<String> producer1 = client.newProducer(Schema.STRING)
                .topic(topic)
                .sendTimeout(1, TimeUnit.SECONDS)
                .create();

        // Initially the token is valid and producer will be able to publish
        producer1.send("hello-1");

        producer1.close();

        Thread.sleep(TimeUnit.SECONDS.toMillis(TokenExpiryTimeSecs));

        @Cleanup
        Producer<String> producer2 = client.newProducer(Schema.STRING)
                    .topic(topic)
                    .sendTimeout(1, TimeUnit.SECONDS)
                    .create();
    }

    @Test
    public void testAuthenticationFailedImmediately() throws PulsarClientException {
        try {
            @Cleanup
            PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .authentication(AuthenticationFactory.token("invalid_token"))
                .build();
            client.newProducer().topic("test_token_topic" + randomName(4));
        } catch (PulsarClientException.AuthenticationException pae) {
            // expected error
        }
    }
}
