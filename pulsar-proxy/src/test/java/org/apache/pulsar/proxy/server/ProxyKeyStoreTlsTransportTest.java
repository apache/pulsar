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

import static org.mockito.Mockito.doReturn;
import java.util.Optional;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "proxy")
public class ProxyKeyStoreTlsTransportTest extends MockedPulsarServiceBaseTest {
    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @Override
    @BeforeMethod
    protected void setup() throws Exception {

        // broker with JKS
        conf.setWebServicePortTls(Optional.of(0));
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setTlsEnabledWithKeyStore(true);
        conf.setTlsKeyStoreType(KEYSTORE_TYPE);
        conf.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
        conf.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);
        conf.setTlsTrustStoreType(KEYSTORE_TYPE);
        conf.setTlsTrustStore(CLIENT_TRUSTSTORE_FILE_PATH);
        conf.setTlsTrustStorePassword(CLIENT_TRUSTSTORE_PW);
        conf.setTlsRequireTrustedClientCertOnConnect(true);

        internalSetup();

        // proxy with JKS
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
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
        proxyConfig.setClusterName(configClusterName);

        proxyConfig.setTlsRequireTrustedClientCertOnConnect(false);

        proxyConfig.setBrokerClientTlsEnabledWithKeyStore(true);
        proxyConfig.setBrokerClientTlsKeyStore(CLIENT_KEYSTORE_FILE_PATH);
        proxyConfig.setBrokerClientTlsKeyStorePassword(CLIENT_KEYSTORE_PW);
        proxyConfig.setBrokerClientTlsTrustStore(BROKER_TRUSTSTORE_FILE_PATH);
        proxyConfig.setBrokerClientTlsTrustStorePassword(BROKER_TRUSTSTORE_PW);

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        proxyService = Mockito.spy(new ProxyService(proxyConfig,
                                                    new AuthenticationService(
                                                            PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper))).when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();

        proxyService.start();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    protected PulsarClient newClient() throws Exception {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrlTls())
                .useKeyStoreTls(true)
                .tlsTrustStorePath(BROKER_TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(BROKER_TRUSTSTORE_PW)
                .tlsKeyStorePath(CLIENT_KEYSTORE_FILE_PATH)
                .tlsKeyStorePassword(CLIENT_KEYSTORE_PW)
                .allowTlsInsecureConnection(false);
        return clientBuilder.build();
    }

    @Test
    public void testProducer() throws Exception {
        @Cleanup
        PulsarClient client = newClient();
        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("persistent://sample/test/local/topic" + System.currentTimeMillis())
                .create();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }
    }
}
