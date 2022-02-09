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
package org.apache.pulsar.testclient;

import static org.mockito.Mockito.spy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TokenOauth2AuthenticatedProducerConsumerTest;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class Oauth2PerformanceTransactionTest extends ProducerConsumerBase {
    private final String testTenant = "pulsar";
    private final String testNamespace = "perf";
    private final String myNamespace = testTenant + "/" + testNamespace;
    private final String testTopic = "persistent://" + myNamespace + "/test-";
    private static final Logger log = LoggerFactory.getLogger(TokenOauth2AuthenticatedProducerConsumerTest.class);

    // public key in oauth2 server to verify the client passed in token. get from https://jwt.io/
    private final String TOKEN_TEST_PUBLIC_KEY = "data:;base64,MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2tZd/4gJda3U2Pc3tpgRAN7JPGWx/Gn17v/0IiZlNNRbP/Mmf0Vc6G1qsnaRaWNWOR+t6/a6ekFHJMikQ1N2X6yfz4UjMc8/G2FDPRmWjA+GURzARjVhxc/BBEYGoD0Kwvbq/u9CZm2QjlKrYaLfg3AeB09j0btNrDJ8rBsNzU6AuzChRvXj9IdcE/A/4N/UQ+S9cJ4UXP6NJbToLwajQ5km+CnxdGE6nfB7LWHvOFHjn9C2Rb9e37CFlmeKmIVFkagFM0gbmGOb6bnGI8Bp/VNGV0APef4YaBvBTqwoZ1Z4aDHy5eRxXfAMdtBkBupmBXqL6bpd15XRYUbu/7ck9QIDAQAB";

    private final String ADMIN_ROLE = "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x@clients";

    // Credentials File, which contains "client_id" and "client_secret"
    private final String CREDENTIALS_FILE = "./src/test/resources/authentication/token/credentials_file.json";

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration serviceConfiguration = getDefaultConf();
        serviceConfiguration.setSystemTopicEnabled(true);
        serviceConfiguration.setTransactionCoordinatorEnabled(true);
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

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    // setup both admin and pulsar client
    protected final void clientSetup() throws Exception {
        Path path = Paths.get(CREDENTIALS_FILE).toAbsolutePath();
        log.info("Credentials File path: {}", path.toString());

        // AuthenticationOAuth2
        Authentication authentication = AuthenticationFactoryOAuth2.clientCredentials(
                new URL("https://dev-kt-aa9ne.us.auth0.com"),
                path.toUri().toURL(),  // key file path
                "https://dev-kt-aa9ne.us.auth0.com/api/v2/"
        );

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(authentication)
                .build());

        replacePulsarClient(PulsarClient.builder().serviceUrl(new URI(pulsar.getBrokerServiceUrl()).toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .authentication(authentication));

        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 1);
    }

    @Test
    public void testTransactionPerf() throws Exception {
        Path path = Paths.get(CREDENTIALS_FILE).toAbsolutePath();
        clientSetup();
        String argString = "--topics-c %s --topics-p %s -threads 1 -ntxn 50 -u %s -ss %s -np 1 -au %s"
                + " --auth-plugin org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2"
                + " --auth-params %s";
        HashMap<String, Object> params = new HashMap<>();
        params.put("issuerUrl", new URL("https://dev-kt-aa9ne.us.auth0.com"));
        params.put("privateKey", path.toUri().toURL());
        params.put("audience", "https://dev-kt-aa9ne.us.auth0.com/api/v2/");
        String testConsumeTopic = testTopic + UUID.randomUUID();
        String testProduceTopic = testTopic + UUID.randomUUID();
        String testSub = "testSub";
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        String args = String.format(argString, testConsumeTopic, testProduceTopic,
                pulsar.getBrokerServiceUrl(), testSub, new URL(pulsar.getWebServiceAddress()),
                jsonMapper.writeValueAsString(params));

        Producer<byte[]> produceToConsumeTopic = pulsarClient.newProducer(Schema.BYTES)
                .producerName("perf-transaction-producer")
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(testConsumeTopic)
                .create();
        pulsarClient.newConsumer(Schema.BYTES)
                .consumerName("perf-transaction-consumeVerify")
                .topic(testConsumeTopic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(testSub + "pre")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        CountDownLatch countDownLatch = new CountDownLatch(50);
        for (int i = 0; i < 50
                ; i++) {
            produceToConsumeTopic.newMessage().value(("testConsume " + i).getBytes()).sendAsync().thenRun(
                    countDownLatch::countDown);
        }

        countDownLatch.await();

        Thread thread = new Thread(() -> {
            try {
                PerformanceTransaction.main(args.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        thread.join();
        Consumer<byte[]> consumeFromConsumeTopic = pulsarClient.newConsumer(Schema.BYTES)
                .consumerName("perf-transaction-consumeVerify")
                .topic(testConsumeTopic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(testSub)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        Consumer<byte[]> consumeFromProduceTopic = pulsarClient.newConsumer(Schema.BYTES)
                .consumerName("perf-transaction-produceVerify")
                .topic(testProduceTopic)
                .subscriptionName(testSub)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        for (int i = 0; i < 50; i++) {
            Message<byte[]> message = consumeFromProduceTopic.receive(2, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            consumeFromProduceTopic.acknowledge(message);
        }
        Message<byte[]> message = consumeFromConsumeTopic.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);
        message = consumeFromProduceTopic.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(message);

    }


    private void testSyncProducerAndConsumer() throws Exception {
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
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));

        // test protocol by producer/consumer
        testSyncProducerAndConsumer();

        log.info("-- Exiting {} test --", methodName);
    }
}
