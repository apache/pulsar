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
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
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
 * This test verifies tls authentication as well as several aspects of TLS hostname verification for client to proxy to
 * broker. That includes several combinations of hostname verification settings. The hostname verification
 * checks rely on the fact that 127.0.0.2 loops back to the local host. This is not the default on OSX, so you may
 * need to configure that by running the following command: sudo ifconfig lo0 alias 127.0.0.2 up
 */
public class ProxyWithHostnameVerificationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ProxyWithAuthorizationTest.class);
    // An IP that routes correctly, but is not on any of the TLS certificate's SAN list
    // If you are seeing an inability to connect, you may need to configure loop back for this IP.
    // Run: sudo ifconfig lo0 alias 127.0.0.2 up
    private static final String ADVERTISED_ADDRESS = "127.0.0.2";

    private final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private final String CLIENT_TOKEN = AuthTokenUtils.createToken(SECRET_KEY, "Client", Optional.empty());
    private ProxyService proxyService;
    private WebServer webServer;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @DataProvider(name = "hostnameVerification")
    public Object[][] hostnameVerificationCodecProvider() {
        return new Object[][] {
                { Boolean.TRUE },
                { Boolean.FALSE }
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

    private void startProxy(boolean hostnameVerificationEnabled) throws Exception {
        proxyConfig.setTlsHostnameVerificationEnabled(hostnameVerificationEnabled);
        proxyService.start();
        ProxyServiceStarter.addWebServerHandlers(webServer, proxyConfig, proxyService, null);
        webServer.start();
    }

    @Test(dataProvider = "hostnameVerification")
    public void testTlsHostVerificationProxyToClient(boolean hostnameVerificationEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        // Not testing proxy to broker here
        startProxy(false);
        // Testing client to proxy hostname verification, so use the dataProvider's value here
        createProxyAdminClient(hostnameVerificationEnabled);
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createProxyClient(proxyService.getServiceUrlTls(),
                PulsarClient.builder(), hostnameVerificationEnabled);

        String namespaceName = "my-tenant/my-ns";

        try {
            initializeCluster(admin, namespaceName);
            if (hostnameVerificationEnabled) {
                Assert.fail("Connection should be failed due to hostnameVerification enabled");
            }
        } catch (PulsarAdminException e) {
            if (!hostnameVerificationEnabled) {
                Assert.fail("Cluster should initialize because hostnameverification is disabled");
            }
            admin.close();
            // Need new client because the admin client to proxy is failing due to hostname verification, and we still
            // want to test the binary protocol client fails to connect as well
            createProxyAdminClient(false);
            initializeCluster(admin, namespaceName);
        }

        try {
            proxyClient.newConsumer().topic("persistent://my-tenant/my-ns/my-topic1")
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

        startProxy(hostnameVerificationEnabled);
        // This test skips hostname verification for client to proxy in order to test proxy to broker
        createProxyAdminClient(false);
        // create a client which connects to proxy over tls and pass authData
        @Cleanup
        PulsarClient proxyClient = createProxyClient(proxyService.getServiceUrlTls(),
                PulsarClient.builder().operationTimeout(15, TimeUnit.SECONDS),
                hostnameVerificationEnabled);

        String namespaceName = "my-tenant/my-ns";

        try {
            initializeCluster(admin, namespaceName);
            if (hostnameVerificationEnabled) {
                Assert.fail("Connection should be failed due to hostnameVerification enabled for proxy to broker");
            }
        } catch (PulsarAdminException.ServerSideErrorException e) {
            if (!hostnameVerificationEnabled) {
                Assert.fail("Cluster should initialize because hostnameverification is disabled for proxy to broker");
            }
            Assert.assertEquals(e.getStatusCode(), 502, "Should get bad gateway");
            admin.close();
            // Need to use broker's admin client because the proxy to broker is failing, and we still want to test
            // the binary protocol client fails to connect as well
            createBrokerAdminClient();
            initializeCluster(admin, namespaceName);
        }

        try {
            proxyClient.newConsumer().topic("persistent://my-tenant/my-ns/my-topic1")
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
        // reset
        proxyConfig.setTlsHostnameVerificationEnabled(true);
    }

    private final Authentication tlsAuth =
            new AuthenticationTls(getTlsFileForClient("user1.cert"), getTlsFileForClient("user1.key-pk8"));
    private final Authentication tokenAuth = new AuthenticationToken(CLIENT_TOKEN);
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

    private void createProxyAdminClient(boolean enableTlsHostnameVerification) throws Exception {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", getTlsFileForClient("admin.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("admin.key-pk8"));

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(
                        String.format("https://%s:%s", ADVERTISED_ADDRESS, webServer.getListenPortHTTPS().get()))
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .enableTlsHostnameVerification(enableTlsHostnameVerification)
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
    private PulsarClient createProxyClient(String proxyServiceUrl, ClientBuilder clientBuilder, boolean enableTlsHostnameVerification)
            throws PulsarClientException {
        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("tlsCertFile", getTlsFileForClient("user1.cert"));
        authParams.put("tlsKeyFile", getTlsFileForClient("user1.key-pk8"));
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);

        return clientBuilder.serviceUrl(proxyServiceUrl).statsInterval(0, TimeUnit.SECONDS)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .authentication(authTls).enableTls(true)
                .enableTlsHostnameVerification(enableTlsHostnameVerification)
                .operationTimeout(1000, TimeUnit.MILLISECONDS).build();
    }
}
