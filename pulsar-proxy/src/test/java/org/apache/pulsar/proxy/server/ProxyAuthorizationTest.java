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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
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
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import javax.crypto.SecretKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static org.mockito.Mockito.spy;

public class ProxyAuthorizationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyAuthorizationTest.class);
    private static final String CLUSTER_NAME = "proxy-authorization";

    private final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private final String CLIENT_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, "Client", Optional.empty());

    final String brokerClientSubject = "broker_client";

    final String brokerClientToken = AuthTokenUtils.createToken(SECRET_KEY, brokerClientSubject, Optional.empty());

    final String proxyToken = AuthTokenUtils.createToken(SECRET_KEY, "superUser", Optional.empty());

    static final ObjectMapper objectMapper = new ObjectMapper();

    private ProxyService proxyService;
    private WebServer webServer;
    private BrokerDiscoveryProvider discoveryProvider;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();


    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setTopicLevelPoliciesEnabled(false);
        Set<String> proxyRoles = new HashSet<>();
        proxyRoles.add("Proxy");
        proxyRoles.add(brokerClientSubject);
        conf.setProxyRoles(proxyRoles);
        conf.setAdvertisedAddress(null);

        conf.setBrokerServicePort(Optional.of(0));
        conf.setWebServicePort(Optional.of(0));

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superUser");
        superUserRoles.add("Proxy");
        conf.setSuperUserRoles(superUserRoles);

        final Map<String, String> brokerClientAuthParams = new HashMap<>();
        brokerClientAuthParams.put("token", brokerClientToken);
        final String brokerClientAuthParam = objectMapper.writeValueAsString(brokerClientAuthParams);

        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters(brokerClientAuthParam);
        conf.setAuthenticationProviders(Set.of(AuthenticationProviderToken.class.getName()));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        conf.setProperties(properties);

        conf.setClusterName(CLUSTER_NAME);
        conf.setNumExecutorThreadPoolSize(5);
    }



    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.init();

        final Map<String, String> brokerClientAuthParams = new HashMap<>();
        brokerClientAuthParams.put("token", brokerClientToken);
        final String brokerClientAuthParam = objectMapper.writeValueAsString(brokerClientAuthParams);

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(false);
        proxyConfig.setForwardAuthorizationCredentials(true);
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setAdvertisedAddress(null);
        proxyConfig.setClusterName(CLUSTER_NAME);

        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setWebServicePort(Optional.of(0));


        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(brokerClientAuthParam);
        proxyConfig.setAuthenticationProviders(Set.of(AuthenticationProviderToken.class.getName()));
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(SECRET_KEY));
        proxyConfig.setProperties(properties);

        AuthenticationService authService =
                new AuthenticationService(PulsarConfigurationLoader.convertFrom(proxyConfig));
        proxyService = Mockito.spy(new ProxyService(proxyConfig, authService));
        proxyService.setGracefulShutdown(false);
        webServer = new WebServer(proxyConfig, authService);

        PulsarResources resource = new PulsarResources(registerCloseable(new ZKMetadataStore(mockZooKeeper)),
                registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal)));
        discoveryProvider = spy(registerCloseable(new BrokerDiscoveryProvider(proxyConfig, resource)));

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
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, proxyService, discoveryProvider);
        webServer.start();
    }

    @Test
    public void testAuthorization() throws Exception {
        log.info("-- Starting {} test --", methodName);

        startProxy();
        // Skip hostname verification because the certs intentionally do not have a hostname
        createProxyAdminClient();

        @Cleanup
        PulsarClient proxyClient = createPulsarClient(proxyService.getServiceUrl(), PulsarClient.builder());

        String namespaceName = "my-tenant/my-ns";

        initializeCluster(admin, namespaceName);

        Consumer<byte[]> consumer = proxyClient.newConsumer()
                .topic("persistent://my-tenant/my-ns/my-topic1")
                .subscriptionName("Client-my-subscriber-name").subscribe();

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
        String clusterName = "proxy-authorization";
        adminClient.clusters().createCluster(clusterName, ClusterData.builder()
                .serviceUrl(brokerUrl.toString()).build());

        adminClient.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(clusterName)));
        adminClient.namespaces().createNamespace(namespaceName);

        adminClient.namespaces().grantPermissionOnNamespace(namespaceName, "Proxy",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        adminClient.namespaces().grantPermissionOnNamespace(namespaceName, "Client",
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        adminClient.namespaces().grantPermissionOnNamespace(namespaceName, brokerClientSubject,
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        adminClient.namespaces().setSubscriptionAuthMode(namespaceName, SubscriptionAuthMode.Prefix);

    }

    private void createProxyAdminClient() throws Exception {
        closeAdmin();
        admin = spy(PulsarAdmin.builder().serviceHttpUrl("http://localhost:" + webServer.getListenPortHTTP().get())
                .authentication(new AuthenticationToken(proxyToken)).build());
    }

    @SuppressWarnings("deprecation")
    private PulsarClient createPulsarClient(String proxyServiceUrl, ClientBuilder clientBuilder)
            throws PulsarClientException {

        return clientBuilder.serviceUrl(proxyServiceUrl).statsInterval(0, TimeUnit.SECONDS)
                .authentication(new AuthenticationToken(CLIENT_TOKEN))
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
    }
}
