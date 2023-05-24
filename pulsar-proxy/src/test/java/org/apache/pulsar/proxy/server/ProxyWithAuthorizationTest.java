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

import static org.mockito.Mockito.spy;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
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
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
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

/**
 * This test verifies authentication as well as several aspects of TLS for client to proxy to broker.
 * That includes several combinations of cipher suites and protocols. Hostname verification is verified in
 * {@link ProxyWithHostnameVerificationTest}.
 */
public class ProxyWithAuthorizationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyWithAuthorizationTest.class);
    private static final String ADVERTISED_ADDRESS = "127.0.0.1";

    private final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private final String CLIENT_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, "Client", Optional.empty());
    private ProxyService proxyService;
    private WebServer webServer;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

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

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // enable tls and auth&auth at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setProxyRoles(Collections.singleton("proxy"));
        conf.setAdvertisedAddress(ADVERTISED_ADDRESS);

        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setBrokerServicePort(Optional.empty());
        conf.setWebServicePortTls(Optional.of(0));
        conf.setWebServicePort(Optional.empty());
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        superUserRoles.add("proxy");
        conf.setSuperUserRoles(superUserRoles);

        conf.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        conf.setBrokerClientAuthenticationParameters(String.format("tlsCertFile:%s,tlsKeyFile:%s",
                        getTlsFileForClient("admin.cert"), getTlsFileForClient("admin.key-pk8")));
        conf.setBrokerClientTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setAuthenticationProviders(Set.of(AuthenticationProviderTls.class.getName(),
                AuthenticationProviderToken.class.getName()));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        conf.setProperties(properties);

        conf.setClusterName("proxy-authorization");
        conf.setNumExecutorThreadPoolSize(5);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.init();

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setForwardAuthorizationCredentials(true);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerServiceURLTLS(pulsar.getBrokerServiceUrlTls());
        proxyConfig.setBrokerWebServiceURLTLS(pulsar.getWebServiceAddressTls());
        proxyConfig.setAdvertisedAddress(ADVERTISED_ADDRESS);

        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(true);

        // enable tls and auth&auth at proxy
        proxyConfig.setTlsCertificateFilePath(PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(PROXY_KEY_FILE_PATH);
        proxyConfig.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        proxyConfig.setBrokerClientTrustCertsFilePath(CA_CERT_FILE_PATH);
        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(String.format("tlsCertFile:%s,tlsKeyFile:%s",
                getTlsFileForClient("proxy.cert"), getTlsFileForClient("proxy.key-pk8")));
        proxyConfig.setAuthenticationProviders(Set.of(AuthenticationProviderTls.class.getName(),
                AuthenticationProviderToken.class.getName()));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        proxyConfig.setProperties(properties);

        AuthenticationService authService =
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig));
        proxyService = Mockito.spy(new ProxyService(proxyConfig, authService));
        webServer = new WebServer(proxyConfig, authService);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        proxyService.close();
        webServer.stop();
    }

    private void startProxy() throws Exception {
        proxyService.start();
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, proxyService, null);
        webServer.start();
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
        createProxyAdminClient();
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createProxyClient(proxyService.getServiceUrlTls(), PulsarClient.builder());

        String namespaceName = "my-tenant/my-ns";

        initializeCluster(admin, namespaceName);

        Consumer<byte[]> consumer = proxyClient.newConsumer()
                .topic("persistent://my-tenant/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES)
                .topic("persistent://my-tenant/my-ns/my-topic1").create();
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

    /*
     * This test verifies whether the Client and Proxy honor the protocols and ciphers specified. Details description of
     * test cases can be found in protocolsCiphersProviderCodecProvider
     */
    @Test(dataProvider = "protocolsCiphersProvider", timeOut = 5000)
    public void tlsCiphersAndProtocols(Set<String> tlsCiphers, Set<String> tlsProtocols, boolean expectFailure)
            throws Exception {
        log.info("-- Starting {} test --", methodName);
        String namespaceName = "my-tenant/my-ns";
        createBrokerAdminClient();

        initializeCluster(admin, namespaceName);

        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setForwardAuthorizationCredentials(true);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerServiceURLTLS(pulsar.getBrokerServiceUrlTls());
        proxyConfig.setAdvertisedAddress(ADVERTISED_ADDRESS);

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(true);

        // enable tls and auth&auth at proxy
        proxyConfig.setTlsCertificateFilePath(PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(PROXY_KEY_FILE_PATH);
        proxyConfig.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);

        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(String.format("tlsCertFile:%s,tlsKeyFile:%s",
            getTlsFileForClient("proxy.cert"), getTlsFileForClient("proxy.key-pk8")));
        proxyConfig.setBrokerClientTrustCertsFilePath(CA_CERT_FILE_PATH);
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
                return admin.namespaces().getPermissions(namespaceName).containsKey("proxy")
                        && admin.namespaces().getPermissions(namespaceName).containsKey("Client")
                        && admin.namespaces().getPermissions(namespaceName).containsKey("user1");
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 3, 1000);
        try {
            @Cleanup
            PulsarClient proxyClient = createProxyClient("pulsar://localhost:" + proxyService.getListenPortTls().get(),
                    PulsarClient.builder());
            Consumer<byte[]> consumer = proxyClient.newConsumer()
                    .topic("persistent://my-tenant/my-ns/my-topic1")
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

    private final Authentication tlsAuth =
            new AuthenticationTls(getTlsFileForClient("user1.cert"), getTlsFileForClient("user1.key-pk8"));
    private final Authentication tokenAuth = new AuthenticationToken(CLIENT_TOKEN);

    @DataProvider
    public Object[] tlsTransportWithAuth() {
        return new Object[]{
                tlsAuth,
                tokenAuth,
        };
    }

    @Test(dataProvider = "tlsTransportWithAuth")
    public void testProxyTlsTransportWithAuth(Authentication auth) throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        createProxyAdminClient();

        @Cleanup
        PulsarClient proxyClient = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrlTls())
                .statsInterval(0, TimeUnit.SECONDS)
                .authentication(auth)
                .tlsKeyFilePath(getTlsFileForClient("user1.key-pk8"))
                .tlsCertificateFilePath(getTlsFileForClient("user1.cert"))
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .enableTlsHostnameVerification(false)
                .build();

        String namespaceName = "my-tenant/my-ns";

        initializeCluster(admin, namespaceName);

        Consumer<byte[]> consumer = proxyClient.newConsumer()
                .topic("persistent://my-tenant/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES)
                .topic("persistent://my-tenant/my-ns/my-topic1").create();
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

    private void initializeCluster(PulsarAdmin adminClient, String namespaceName) throws Exception {
        adminClient.clusters().createCluster("proxy-authorization", ClusterData.builder()
                .serviceUrlTls(brokerUrlTls.toString()).build());

        adminClient.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization")));
        adminClient.namespaces().createNamespace(namespaceName);

        adminClient.namespaces().grantPermissionOnNamespace(namespaceName, "proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        adminClient.namespaces().grantPermissionOnNamespace(namespaceName, "Client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        adminClient.namespaces().grantPermissionOnNamespace(namespaceName, "user1",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
    }

    private void createProxyAdminClient() throws Exception {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", getTlsFileForClient("admin.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("admin.key-pk8"));

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(
                String.format("https://%s:%s", ADVERTISED_ADDRESS, webServer.getListenPortHTTPS().get()))
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .authentication(AuthenticationTls.class.getName(), authParams).build());
    }

    private void createBrokerAdminClient() throws Exception {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", getTlsFileForClient("admin.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("admin.key-pk8"));

        admin = spy(PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrlTls.toString().replace(ADVERTISED_ADDRESS, "localhost"))
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .authentication(AuthenticationTls.class.getName(), authParams).build());
    }

    @SuppressWarnings("deprecation")
    private PulsarClient createProxyClient(String proxyServiceUrl, ClientBuilder clientBuilder)
            throws PulsarClientException {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", getTlsFileForClient("user1.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("user1.key-pk8"));
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);

        return clientBuilder.serviceUrl(proxyServiceUrl).statsInterval(0, TimeUnit.SECONDS)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .authentication(authTls).enableTls(true)
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
    }
}
