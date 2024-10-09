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

import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.eclipse.jetty.servlet.ServletHolder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class AdminProxyHandlerKeystoreTLSTest extends MockedPulsarServiceBaseTest {


    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    private Authentication proxyClientAuthentication;

    private WebServer webServer;

    private BrokerDiscoveryProvider discoveryProvider;

    private PulsarResources resource;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {

        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(false);
        conf.setWebServicePortTls(Optional.of(0));
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setTlsEnabledWithKeyStore(true);
        conf.setTlsAllowInsecureConnection(false);
        conf.setTlsKeyStoreType(KEYSTORE_TYPE);
        conf.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
        conf.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);
        conf.setTlsTrustStoreType(KEYSTORE_TYPE);
        conf.setTlsTrustStore(CLIENT_TRUSTSTORE_FILE_PATH);
        conf.setTlsTrustStorePassword(CLIENT_TRUSTSTORE_PW);

        super.internalSetup();

        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(true);
        proxyConfig.setTlsEnabledWithKeyStore(true);

        proxyConfig.setTlsKeyStoreType(KEYSTORE_TYPE);
        proxyConfig.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
        proxyConfig.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);
        proxyConfig.setTlsTrustStoreType(KEYSTORE_TYPE);
        proxyConfig.setTlsTrustStore(CLIENT_TRUSTSTORE_FILE_PATH);
        proxyConfig.setTlsTrustStorePassword(CLIENT_TRUSTSTORE_PW);

        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setBrokerClientTlsEnabledWithKeyStore(true);
        proxyConfig.setBrokerClientTlsKeyStoreType(KEYSTORE_TYPE);
        proxyConfig.setBrokerClientTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
        proxyConfig.setBrokerClientTlsKeyStorePassword(BROKER_KEYSTORE_PW);
        proxyConfig.setBrokerClientTlsTrustStoreType(KEYSTORE_TYPE);
        proxyConfig.setBrokerClientTlsTrustStore(BROKER_TRUSTSTORE_FILE_PATH);
        proxyConfig.setBrokerClientTlsTrustStorePassword(BROKER_TRUSTSTORE_PW);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        proxyConfig.setAuthenticationProviders(providers);
        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationKeyStoreTls.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(String.format("keyStoreType:%s,keyStorePath:%s,keyStorePassword:%s",
                KEYSTORE_TYPE, BROKER_KEYSTORE_FILE_PATH, BROKER_KEYSTORE_PW));
        proxyConfig.setClusterName(configClusterName);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        resource = new PulsarResources(registerCloseable(new ZKMetadataStore(mockZooKeeper)),
                registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal)));
        webServer = new WebServer(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)));
        discoveryProvider = spy(registerCloseable(new BrokerDiscoveryProvider(proxyConfig, resource)));
        LoadManagerReport report = new LoadReport(brokerUrl.toString(), brokerUrlTls.toString(), null, null);
        doReturn(report).when(discoveryProvider).nextBroker();
        ServletHolder servletHolder = new ServletHolder(new AdminProxyHandler(proxyConfig, discoveryProvider, proxyClientAuthentication));
        webServer.addServlet("/admin", servletHolder);
        webServer.addServlet("/lookup", servletHolder);
        webServer.start();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        webServer.stop();
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
        super.internalCleanup();
    }

    PulsarAdmin getAdminClient() throws Exception {
        return PulsarAdmin.builder()
                .serviceHttpUrl("https://localhost:" + webServer.getListenPortHTTPS().get())
                .useKeyStoreTls(true)
                .allowTlsInsecureConnection(false)
                .tlsTrustStorePath(BROKER_TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(BROKER_TRUSTSTORE_PW)
                .authentication(AuthenticationKeyStoreTls.class.getName(),
                        String.format("keyStoreType:%s,keyStorePath:%s,keyStorePassword:%s",
                                KEYSTORE_TYPE, BROKER_KEYSTORE_FILE_PATH, BROKER_KEYSTORE_PW))
                .build();
    }

    @Test
    public void testAdmin() throws Exception {
        @Cleanup
        PulsarAdmin admin = getAdminClient();
        admin.clusters().createCluster(configClusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
    }

}
