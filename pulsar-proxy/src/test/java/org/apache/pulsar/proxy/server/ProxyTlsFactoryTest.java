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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import java.util.Optional;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 2b: verifies the proxy serves TLS on the new {@code PulsarTlsFactory} path
 * (selected by {@code tlsFactoryClassName=default}) for the binary front-end (purpose PROXY) and, by
 * starting the TLS web server, the WEB purpose. The legacy PIP-337 path is exercised by
 * {@link ProxyTlsTest}, which must remain green.
 */
public class ProxyTlsFactoryTest extends MockedPulsarServiceBaseTest {

    private ProxyService proxyService;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();
        setupDefaultTenantAndNamespace();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        // Advertise over loopback so the proxy service URL host (localhost) matches the proxy server
        // certificate's SubjectAltName (proxy.cert.pem carries DNS:localhost, IP:127.0.0.1). TLS hostname
        // verification is on by default (PIP-478, SAN-only), and the default advertised address resolves
        // to the machine's canonical hostname/IP, which is not in the cert SAN. Keeps verification enabled.
        proxyConfig.setAdvertisedAddress("localhost");
        proxyConfig.setTlsEnabledWithBroker(false);
        proxyConfig.setTlsCertificateFilePath(PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(PROXY_KEY_FILE_PATH);
        // Select the new PIP-478 TLS SPI (built-in file-based factory) for the proxy server purposes.
        proxyConfig.setTlsFactoryClassName("default");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(configClusterName);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper)))
                .when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();

        proxyService.start();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        if (proxyService != null) {
            proxyService.close();
        }
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    @Test
    public void testProducerOverProxyTlsFactory() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrlTls())
                .allowTlsInsecureConnection(false)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH).build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("persistent://public/default/tls-factory-topic").create();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }
        assertThat(producer.isConnected()).isTrue();
    }
}
