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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

// TLS test without authentication and authorization based on KeyStore type config.
@Slf4j
@Test(groups = "broker-impl")
public class KeyStoreTlsProducerConsumerTestWithoutAuthTest extends ProducerConsumerBase {
    protected final String BROKER_KEYSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/broker.keystore.jks";
    protected final String BROKER_TRUSTSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/broker.truststore.jks";
    protected final String BROKER_KEYSTORE_PW = "111111";
    protected final String BROKER_TRUSTSTORE_PW = "111111";

    protected final String CLIENT_KEYSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/client.keystore.jks";
    protected final String CLIENT_TRUSTSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/client.truststore.jks";
    protected final String CLIENT_KEYSTORE_PW = "111111";
    protected final String CLIENT_TRUSTSTORE_PW = "111111";

    protected final String KEYSTORE_TYPE = "JKS";

    private final String clusterName = "use";
    Set<String> tlsProtocols = Sets.newConcurrentHashSet();

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // TLS configuration for Broker
        internalSetUpForBroker();

        // Start Broker
        super.init();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void internalSetUpForBroker() {
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsEnabledWithKeyStore(true);

        conf.setTlsKeyStoreType(KEYSTORE_TYPE);
        conf.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
        conf.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);

        conf.setTlsTrustStoreType(KEYSTORE_TYPE);
        conf.setTlsTrustStore(CLIENT_TRUSTSTORE_FILE_PATH);
        conf.setTlsTrustStorePassword(CLIENT_TRUSTSTORE_PW);

        conf.setClusterName(clusterName);
        conf.setTlsRequireTrustedClientCertOnConnect(true);
        tlsProtocols.add("TLSv1.3");
        tlsProtocols.add("TLSv1.2");
        conf.setTlsProtocols(tlsProtocols);
        conf.setNumExecutorThreadPoolSize(5);
    }

    protected void internalSetUpForClient(boolean addCertificates, String lookupUrl) throws Exception {
        if (pulsarClient != null) {
            pulsarClient.close();
        }

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(lookupUrl)
                .enableTls(true)
                .useKeyStoreTls(true)
                .tlsTrustStorePath(BROKER_TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(BROKER_TRUSTSTORE_PW)
                .allowTlsInsecureConnection(false)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        if (addCertificates) {
            Map<String, String> authParams = new HashMap<>();
            authParams.put(AuthenticationKeyStoreTls.KEYSTORE_TYPE, KEYSTORE_TYPE);
            authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, CLIENT_KEYSTORE_FILE_PATH);
            authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PW, CLIENT_KEYSTORE_PW);
            clientBuilder.authentication(AuthenticationKeyStoreTls.class.getName(), authParams);
        }
        replacePulsarClient(clientBuilder);
    }

    protected void internalSetUpForNamespace() throws Exception {
        Map<String, String> authParams = new HashMap<>();
        authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, CLIENT_KEYSTORE_FILE_PATH);
        authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PW, CLIENT_KEYSTORE_PW);

        if (admin != null) {
            admin.close();
        }

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrlTls.toString())
                .useKeyStoreTls(true)
                .tlsTrustStorePath(BROKER_TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(BROKER_TRUSTSTORE_PW)
                .allowTlsInsecureConnection(true)
                .authentication(AuthenticationKeyStoreTls.class.getName(), authParams).build());
        admin.clusters().createCluster(clusterName, ClusterData.builder()
                .serviceUrl(brokerUrl.toString())
                .serviceUrlTls(brokerUrlTls.toString())
                .brokerServiceUrl(pulsar.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar.getBrokerServiceUrlTls())
                .build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/my-ns");
    }

    /**
     * verifies that messages whose size is larger than 2^14 bytes (max size of single TLS chunk) can be
     * produced/consumed
     *
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testTlsLargeSizeMessage() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int MESSAGE_SIZE = 16 * 1024 + 1;
        log.info("-- message size -- {}", MESSAGE_SIZE);
        String topicName = "persistent://my-property/use/my-ns/testTlsLargeSizeMessage"
                           + System.currentTimeMillis();

        internalSetUpForClient(true, pulsar.getBrokerServiceUrlTls());
        internalSetUpForNamespace();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name").subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .create();
        for (int i = 0; i < 10; i++) {
            byte[] message = new byte[MESSAGE_SIZE];
            Arrays.fill(message, (byte) i);
            producer.send(message);
        }

        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            byte[] expected = new byte[MESSAGE_SIZE];
            Arrays.fill(expected, (byte) i);
            Assert.assertEquals(expected, msg.getData());
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 300000)
    public void testTlsClientAuthOverBinaryProtocol() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int MESSAGE_SIZE = 16 * 1024 + 1;
        log.info("-- message size -- {}", MESSAGE_SIZE);
        String topicName = "persistent://my-property/use/my-ns/testTlsClientAuthOverBinaryProtocol"
                           + System.currentTimeMillis();

        internalSetUpForNamespace();

        // Test 1 - Using TLS on binary protocol without sending certs - expect failure
        internalSetUpForClient(false, pulsar.getBrokerServiceUrlTls());

        try {
            pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            Assert.fail("Server should have failed the TLS handshake since client didn't .");
        } catch (Exception ex) {
            // OK
        }

        // Test 2 - Using TLS on binary protocol - sending certs
        internalSetUpForClient(true, pulsar.getBrokerServiceUrlTls());
        try {
            pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
        } catch (Exception ex) {
            Assert.fail("Should not fail since certs are sent.");
        }
    }

    @Test(timeOut = 30000)
    public void testTlsClientAuthOverHTTPProtocol() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int MESSAGE_SIZE = 16 * 1024 + 1;
        log.info("-- message size -- {}", MESSAGE_SIZE);
        String topicName = "persistent://my-property/use/my-ns/testTlsClientAuthOverHTTPProtocol"
                           + System.currentTimeMillis();

        internalSetUpForNamespace();

        // Test 1 - Using TLS on https without sending certs - expect failure
        internalSetUpForClient(false, pulsar.getWebServiceAddressTls());
        try {
            pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            Assert.fail("Server should have failed the TLS handshake since client didn't .");
        } catch (Exception ex) {
            // OK
        }

        // Test 2 - Using TLS on https - sending certs
        internalSetUpForClient(true, pulsar.getWebServiceAddressTls());
        try {
            pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
        } catch (Exception ex) {
            Assert.fail("Should not fail since certs are sent.");
        }
    }

}
