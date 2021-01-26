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

import com.google.common.collect.Sets;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test Token authentication with:
 *    client: org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2
 *    broker: org.apache.pulsar.broker.authentication.AuthenticationProviderToken
 */
public class TokenOauth2AuthenticatedProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(TokenOauth2AuthenticatedProducerConsumerTest.class);

    // public key in oauth2 server to verify the client passed in token. get from https://jwt.io/
    private final String TOKEN_TEST_PUBLIC_KEY = "data:;base64,MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2tZd/4gJda3U2Pc3tpgRAN7JPGWx/Gn17v/0IiZlNNRbP/Mmf0Vc6G1qsnaRaWNWOR+t6/a6ekFHJMikQ1N2X6yfz4UjMc8/G2FDPRmWjA+GURzARjVhxc/BBEYGoD0Kwvbq/u9CZm2QjlKrYaLfg3AeB09j0btNrDJ8rBsNzU6AuzChRvXj9IdcE/A/4N/UQ+S9cJ4UXP6NJbToLwajQ5km+CnxdGE6nfB7LWHvOFHjn9C2Rb9e37CFlmeKmIVFkagFM0gbmGOb6bnGI8Bp/VNGV0APef4YaBvBTqwoZ1Z4aDHy5eRxXfAMdtBkBupmBXqL6bpd15XRYUbu/7ck9QIDAQAB";

    private final String ADMIN_ROLE = "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x@clients";

    // Credentials File, which contains "client_id" and "client_secret"
    private final String CREDENTIALS_FILE = "./src/test/resources/authentication/token/credentials_file.json";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationRefreshCheckSeconds(5);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add(ADMIN_ROLE);
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("test");

        // Set provider domain name
        Properties properties = new Properties();
        properties.setProperty("tokenPublicKey", TOKEN_TEST_PUBLIC_KEY);

        conf.setProperties(properties);
        super.init();
    }

    // setup both admin and pulsar client
    protected final void clientSetup() throws Exception {
        Path path = Paths.get(CREDENTIALS_FILE).toAbsolutePath();
        log.info("Credentials File path: {}", path.toString());

        // AuthenticationOAuth2
        Authentication authentication = AuthenticationFactoryOAuth2.clientCredentials(
                new URL("https://dev-kt-aa9ne.us.auth0.com"),
                new URL("file://" + path.toString()),  // key file path
                "https://dev-kt-aa9ne.us.auth0.com/api/v2/"
        );

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(authentication)
                .build());

        pulsarClient = PulsarClient.builder().serviceUrl(new URI(pulsar.getBrokerServiceUrl()).toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .authentication(authentication)
                .build();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "batch")
    public Object[][] codecProvider() {
        return new Object[][] { { 0 }, { 1000 } };
    }

    public void testSyncProducerAndConsumer() throws Exception {
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic")
                .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic");

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
    }

    @Test
    public void testTokenProducerAndConsumer() throws Exception {
        log.info("-- Starting {} test --", methodName);
        clientSetup();

        // test rest by admin
        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        admin.tenants().createTenant("my-property",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));

        // test protocol by producer/consumer
        testSyncProducerAndConsumer();

        log.info("-- Exiting {} test --", methodName);
    }

    @Test
    public void testOAuth2TokenRefreshedWithoutReconnect() throws Exception {
        log.info("-- Starting {} test --", methodName);
        clientSetup();

        // test rest by admin
        admin.clusters().createCluster("test", new ClusterData(brokerUrl.toString()));
        admin.tenants().createTenant("my-property",
            new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic")
            .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic("persistent://my-property/my-ns/my-topic");
        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);

        // get the first connection stats
        ProducerImpl producerImpl = (ProducerImpl) producer;
        String accessTokenOld = producerImpl.getClientCnx().getAuthenticationDataProvider().getCommandData();
        long lastDisconnectTime = producer.getLastDisconnectedTimestamp();

        // the token expire duration is 10 seconds, so we need to wait for the authenticationData refreshed
        Awaitility.await()
            .atLeast(10, TimeUnit.SECONDS)
            .atMost(20, TimeUnit.SECONDS)
            .with()
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> {
                String accessTokenNew = producerImpl.getClientCnx().getAuthenticationDataProvider().getCommandData();
                Assert.assertNotEquals(accessTokenOld, accessTokenNew);
            });

        // get the lastDisconnectTime, it should be same with the before, because the connection shouldn't disconnect
        long lastDisconnectTimeAfterTokenExpired = producer.getLastDisconnectedTimestamp();
        Assert.assertEquals(lastDisconnectTime, lastDisconnectTimeAfterTokenExpired);

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        msg = null;
        messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
    }
}
