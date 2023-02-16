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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
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
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class ProxyWithAuthorizationNegTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyWithAuthorizationNegTest.class);

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

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

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
        conf.setAdvertisedAddress("localhost");

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superUser");
        conf.setSuperUserRoles(superUserRoles);
        conf.setProxyRoles(Collections.singleton("Proxy"));

        conf.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        conf.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + TLS_BROKER_CERT_FILE_PATH + "," + "tlsKeyFile:" + TLS_BROKER_KEY_FILE_PATH);

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setClusterName("proxy-authorization-neg");
        conf.setNumExecutorThreadPoolSize(5);

        super.init();

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setBrokerServiceURLTLS(pulsar.getBrokerServiceUrlTls());

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
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

        proxyConfig.setAuthenticationProviders(providers);

        AuthenticationService authenticationService = new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig));
        proxyService = Mockito.spy(new ProxyService(proxyConfig, authenticationService));

        proxyService.start();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        proxyService.close();
    }

    /**
     * <pre>
     * It verifies e2e tls + Authentication + Authorization (client -> proxy -> broker>
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

        createAdminClient();
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createPulsarClient("pulsar+ssl://localhost:" + proxyService.getListenPortTls().get());

        String namespaceName = "my-property/proxy-authorization-neg/my-ns";

        admin.clusters().createCluster("proxy-authorization-neg", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());

        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("proxy-authorization-neg")));
        admin.namespaces().createNamespace(namespaceName);

        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Proxy", Sets.newHashSet(AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(namespaceName, "Client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        Consumer<byte[]> consumer;
        try {
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/proxy-authorization-neg/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
        } catch (Exception ex) {
            // expected
            admin.namespaces().grantPermissionOnNamespace(namespaceName, "Proxy", Sets.newHashSet(AuthAction.consume));
            log.info("-- Admin permissions {} ---", admin.namespaces().getPermissions(namespaceName));
            consumer = proxyClient.newConsumer()
                    .topic("persistent://my-property/proxy-authorization-neg/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
        }
        Producer<byte[]> producer;
        try {
            producer = proxyClient.newProducer(Schema.BYTES)
                    .topic("persistent://my-property/proxy-authorization-neg/my-ns/my-topic1").create();
        } catch (Exception ex) {
            // expected
            admin.namespaces().grantPermissionOnNamespace(namespaceName, "Proxy",
                    Sets.newHashSet(AuthAction.produce, AuthAction.consume));
            producer = proxyClient.newProducer(Schema.BYTES)
                    .topic("persistent://my-property/proxy-authorization-neg/my-ns/my-topic1").create();
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

    protected final void createAdminClient() throws Exception {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", TLS_SUPERUSER_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_SUPERUSER_CLIENT_KEY_FILE_PATH);

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrlTls.toString())
                .tlsTrustCertsFilePath(TLS_BROKER_TRUST_CERT_FILE_PATH).allowTlsInsecureConnection(true)
                .authentication(AuthenticationTls.class.getName(), authParams).build());
    }

    @SuppressWarnings("deprecation")
    private PulsarClient createPulsarClient(String proxyServiceUrl) throws PulsarClientException {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);
        return PulsarClient.builder().serviceUrl(proxyServiceUrl).statsInterval(0, TimeUnit.SECONDS)
                .tlsTrustCertsFilePath(TLS_PROXY_TRUST_CERT_FILE_PATH).allowTlsInsecureConnection(true)
                .authentication(authTls).enableTls(true).build();
    }
}
