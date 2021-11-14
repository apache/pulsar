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

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class ProxyWithAuthorizationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyWithAuthorizationTest.class);

    private final String TLS_PROXY_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/proxy-cacert.pem";
    private final String TLS_PROXY_CERT_FILE_PATH = "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/proxy-cert.pem";
    private final String TLS_PROXY_KEY_FILE_PATH = "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/proxy-key.pem";
    private final String TLS_BROKER_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/broker-cacert.pem";
    private final String TLS_BROKER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/broker-cert.pem";
    private final String TLS_BROKER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/broker-key.pem";
    private final String TLS_CLIENT_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/client-cacert.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/ProxyWithAuthorizationTest/client-key.pem";
    private final String TLS_SUPERUSER_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";
    private final String TLS_SUPERUSER_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_SUPERUSER_CLIENT_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";

    private ProxyService proxyService;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @DataProvider(name = "hostnameVerification")
    public Object[][] hostnameVerificationCodecProvider() {
        return new Object[][] {
            { Boolean.TRUE },
            { Boolean.FALSE }
        };
    }

    @DataProvider(name = "protocolsCiphersProvider")
    public Object[][] protocolsCiphersProviderCodecProvider() {
        // Test using defaults
        Set<String> ciphers_1 = new TreeSet<>();
        Set<String> protocols_1 = new TreeSet<>();

        // Test explicitly specifying protocols defaults
        Set<String> ciphers_2 = new TreeSet<>();
        Set<String> protocols_2 = new TreeSet<>();
        protocols_2.add("TLSv1.3");
        protocols_2.add("TLSv1.2");

        // Test for invalid ciphers
        Set<String> ciphers_3 = new TreeSet<>();
        Set<String> protocols_3 = new TreeSet<>();
        ciphers_3.add("INVALID_PROTOCOL");

        // Incorrect Config since TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 was introduced in TLSv1.2
        Set<String> ciphers_4 = new TreeSet<>();
        Set<String> protocols_4 = new TreeSet<>();
        ciphers_4.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        protocols_4.add("TLSv1.1");

        // Incorrect Config since TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 was introduced in TLSv1.2
        Set<String> ciphers_5 = new TreeSet<>();
        Set<String> protocols_5 = new TreeSet<>();
        ciphers_5.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        protocols_5.add("TLSv1");

        // Correct Config
        Set<String> ciphers_6 = new TreeSet<>();
        Set<String> protocols_6 = new TreeSet<>();
        ciphers_6.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        protocols_6.add("TLSv1.2");

        // In correct config - JDK 8 doesn't support TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
        Set<String> ciphers_7 = new TreeSet<>();
        Set<String> protocols_7 = new TreeSet<>();
        protocols_7.add("TLSv1.2");
        ciphers_7.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

        // Correct config - Atlease one of the Cipher Suite is supported
        Set<String> ciphers_8 = new TreeSet<>();
        Set<String> protocols_8 = new TreeSet<>();
        protocols_8.add("TLSv1.2");
        ciphers_8.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        ciphers_8.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

        return new Object[][] {
            { ciphers_1, protocols_1, Boolean.FALSE },
            { ciphers_2, protocols_2, Boolean.FALSE },
            { ciphers_3, protocols_3, Boolean.TRUE },
            { ciphers_4, protocols_4, Boolean.TRUE },
            { ciphers_5, protocols_5, Boolean.TRUE },
            { ciphers_6, protocols_6, Boolean.FALSE },
            { ciphers_7, protocols_7, Boolean.FALSE },
            { ciphers_8, protocols_8, Boolean.FALSE }
        };
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {

        // enable tls and auth&auth at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsTrustCertsFilePath(TLS_PROXY_TRUST_CERT_FILE_PATH);
        conf.setTlsCertificateFilePath(TLS_BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_BROKER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(true);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superUser");
        conf.setSuperUserRoles(superUserRoles);

        conf.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        conf.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + TLS_BROKER_CERT_FILE_PATH + "," + "tlsKeyFile:" + TLS_BROKER_KEY_FILE_PATH);
        conf.setBrokerClientTrustCertsFilePath(TLS_BROKER_TRUST_CERT_FILE_PATH);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("proxy-authorization");
        conf.setNumExecutorThreadPoolSize(5);

        super.init();

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerServiceURLTLS(pulsar.getBrokerServiceUrlTls());

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(true);

        // enable tls and auth&auth at proxy
        proxyConfig.setTlsCertificateFilePath(TLS_PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(TLS_PROXY_KEY_FILE_PATH);
        proxyConfig.setTlsTrustCertsFilePath(TLS_CLIENT_TRUST_CERT_FILE_PATH);
        proxyConfig.setBrokerClientTrustCertsFilePath(TLS_BROKER_TRUST_CERT_FILE_PATH);
        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + TLS_PROXY_CERT_FILE_PATH + "," + "tlsKeyFile:" + TLS_PROXY_KEY_FILE_PATH);
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
     * It verifies e2e tls + Authentication + Authorization (client -> proxy -> broker)
     *
     * 1. client connects to proxy over tls and pass auth-data
     * 2. proxy authenticate client and retrieve client-role
     *    and send it to broker as originalPrincipal over tls
     * 3. client creates producer/consumer via proxy
     * 4. broker authorize producer/consumer create request using originalPrincipal
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testProxyAuthorization() throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        createAdminClient();
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrlTls(), PulsarClient.builder());

        String namespaceName = "my-property/proxy-authorization/my-ns";

        admin.clusters().createCluster("proxy-authorization", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);

        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        Consumer<byte[]> consumer = proxyClient.newConsumer()
                .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES)
                .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1").create();
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

    @Test(dataProvider = "hostnameVerification")
    public void testTlsHostVerificationProxyToClient(boolean hostnameVerificationEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        createAdminClient();
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrlTls(),
                PulsarClient.builder().enableTlsHostnameVerification(hostnameVerificationEnabled));

        String namespaceName = "my-property/proxy-authorization/my-ns";

        admin.clusters().createCluster("proxy-authorization", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);

        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        try {
            proxyClient.newConsumer().topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            if (hostnameVerificationEnabled) {
                Assert.fail("Connection should be failed due to hostnameVerification enabled");
            }
        } catch (PulsarClientException e) {
            if (!hostnameVerificationEnabled) {
                Assert.fail("Consumer should be created because hostnameverification is disabled");
            }
        }

        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies hostname verification at proxy when proxy tries to connect with broker. Proxy performs hostname
     * verification when broker sends its certs over tls .
     *
     * <pre>
     * 1. Broker sends certs back to proxy with CN="Broker" however, proxy tries to connect with hostname=localhost
     * 2. so, client fails to create consumer if proxy is enabled with hostname verification
     * </pre>
     *
     * @param hostnameVerificationEnabled
     * @throws Exception
     */
    @Test(dataProvider = "hostnameVerification")
    public void testTlsHostVerificationProxyToBroker(boolean hostnameVerificationEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        proxyConfig.setTlsHostnameVerificationEnabled(hostnameVerificationEnabled);
        startProxy();
        createAdminClient();
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrlTls(),
                PulsarClient.builder().operationTimeout(1, TimeUnit.SECONDS));

        String namespaceName = "my-property/proxy-authorization/my-ns";

        admin.clusters().createCluster("proxy-authorization", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);

        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        try {
            proxyClient.newConsumer().topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            if (hostnameVerificationEnabled) {
                Assert.fail("Connection should be failed due to hostnameVerification enabled");
            }
        } catch (PulsarClientException e) {
            if (!hostnameVerificationEnabled) {
                Assert.fail("Consumer should be created because hostnameverification is disabled");
            }
        }

        log.info("-- Exiting {} test --", methodName);
    }

    /*
     * This test verifies whether the Client and Proxy honor the protocols and ciphers specified. Details description of
     * test cases can be found in protocolsCiphersProviderCodecProvider
     */
    @Test(dataProvider = "protocolsCiphersProvider", timeOut = 5000)
    public void tlsCiphersAndProtocols(Set<String> tlsCiphers, Set<String> tlsProtocols, boolean expectFailure)
            throws Exception {
        log.info("-- Starting {} test --", methodName);
        String namespaceName = "my-property/proxy-authorization/my-ns";
        createAdminClient();

        admin.clusters().createCluster("proxy-authorization", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        admin.namespaces().createNamespace(namespaceName);

        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerServiceURLTLS(pulsar.getBrokerServiceUrlTls());

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(true);

        // enable tls and auth&auth at proxy
        proxyConfig.setTlsCertificateFilePath(TLS_PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(TLS_PROXY_KEY_FILE_PATH);
        proxyConfig.setTlsTrustCertsFilePath(TLS_CLIENT_TRUST_CERT_FILE_PATH);

        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + TLS_PROXY_CERT_FILE_PATH + "," + "tlsKeyFile:" + TLS_PROXY_KEY_FILE_PATH);
        proxyConfig.setBrokerClientTrustCertsFilePath(TLS_BROKER_TRUST_CERT_FILE_PATH);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        conf.setAuthenticationProviders(providers);
        proxyConfig.setAuthenticationProviders(providers);
        proxyConfig.setTlsProtocols(tlsProtocols);
        proxyConfig.setTlsCiphers(tlsCiphers);

        ProxyService proxyService = Mockito.spy(new ProxyService(proxyConfig,
                                                        new AuthenticationService(
                                                                PulsarConfigurationLoader.convertFrom(proxyConfig))));
        try {
            proxyService.start();
        } catch (Exception ex) {
            if (!expectFailure) {
                Assert.fail("This test case should not fail");
            }
        }
        org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically((test) -> {
            try {
                return admin.namespaces().getPermissions(namespaceName).containsKey("Proxy")
                        && admin.namespaces().getPermissions(namespaceName).containsKey("Client");
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 3, 1000);
        try {
            @Cleanup
            PulsarClient proxyClient = createPulsarClient("pulsar://localhost:" + proxyService.getListenPortTls().get(), PulsarClient.builder());
            Consumer<byte[]> consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/proxy-authorization/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();

            if (expectFailure) {
                Assert.fail("Failure expected for this test case");
            }
            consumer.close();
        } catch (Exception ex) {
            if (!expectFailure) {
                Assert.fail("This test case should not fail");
            }
        }
        admin.close();
        log.info("-- Exiting {} test --", methodName);
    }

    private void createAdminClient() throws Exception {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", TLS_SUPERUSER_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_SUPERUSER_CLIENT_KEY_FILE_PATH);

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrlTls.toString())
                .tlsTrustCertsFilePath(TLS_PROXY_TRUST_CERT_FILE_PATH).allowTlsInsecureConnection(true)
                .authentication(AuthenticationTls.class.getName(), authParams).build());
    }

    @SuppressWarnings("deprecation")
    private PulsarClient createPulsarClient(String proxyServiceUrl, ClientBuilder clientBuilder)
            throws PulsarClientException {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);

        return clientBuilder.serviceUrl(proxyServiceUrl).statsInterval(0, TimeUnit.SECONDS)
                .tlsTrustCertsFilePath(TLS_PROXY_TRUST_CERT_FILE_PATH).allowTlsInsecureConnection(true)
                .authentication(authTls).enableTls(true)
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
    }
}
