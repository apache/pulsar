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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test Token authentication with:
 *    client: org.apache.pulsar.client.impl.auth.AuthenticationToken
 *    broker: org.apache.pulsar.broker.authentication.AuthenticationProviderToken
 */
public class TokenAuthenticatedProducerConsumerTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(TokenAuthenticatedProducerConsumerTest.class);

    // pre-create a public/private_key pair.  Public key used for broker to verify client passed in token
    private final String TOKEN_PUBLIC_KEY = "data:;base64,MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAhHKgdY6arG7eE75bUPtznN5WjMu0sxLq7pI5Aaiw2Ijerbz33iO/Fdd2yJVuAZNDZPD/AVSaeliEh/BP+s2rN8KNuiywD+SlL1NGf2JDS5BvGT4Q8eHfDDRd/iY5zkK58wYwlke6C8fKCx10MTH9iYAJpzaaxs+Tu1RaatK+691aYSiMkYIfgbqAKmSCpK+48al/PkmENfuhzaTBPhCnEblhNvUhS5MjzBcAcGzecpEuVSxUzDtm8rU8DEQR6kkdXS1QnGHVNis/vgk8QzctkJKbtgDIaGzNUmDvTCyPZ8WLWSWJWb1oPxRZwpfXVP69ijU0Rme4/YkuHt6IEw6ANQIDAQAB";
    // admin token created based on private_key.
    private final String ADMIN_TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImV4cCI6MTYyNTEzNjQyMn0.DAfbUPZwQURgGvor4scO0NoqoyHkCulKZkhP7kksCWFvgx6B22iKuXGX42EFlFSRMWYYgIJXV7UZATCLCjJpn_ijrO6AWBmooib3f94OPoLDdkF3qXnqaLnvJtl8_sCoLCSghR_O3hQFgQW2GRjMDdfJgl2_HXCWuzedtI5cQJdbpfU0NU10nzo7RtrpCmUdgQYQEHegYOawLqQVvr53ZGjrZilBXY9HHz1mSlnwZGNGVNNdvRthBuGtXtfKgtfSDF5jLqABvK8TUpdNJybibeiOspdzuY19-wVt4eVXzNAGsP4V4Zs91MgIUYV5lWKnBUuVWalppkMWhRF4Jf-KWQ";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("test");

        // Set provider domain name
        Properties properties = new Properties();
        properties.setProperty("tokenPublicKey", TOKEN_PUBLIC_KEY);

        conf.setProperties(properties);
        super.init();
    }

    // setup both admin and pulsar client
    protected final void clientSetup() throws Exception {
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(AuthenticationFactory.token(ADMIN_TOKEN))
                .build());

        pulsarClient = PulsarClient.builder().serviceUrl(new URI(pulsar.getBrokerServiceUrl()).toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .authentication(AuthenticationFactory.token(ADMIN_TOKEN))
                .build();
    }

    @AfterMethod
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

}
