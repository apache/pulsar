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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.mockito.Mockito.doReturn;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyTlsTest extends MockedPulsarServiceBaseTest {

    private final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";
    private final String TLS_PROXY_CERT_FILE_PATH = "./src/test/resources/authentication/tls/server-cert.pem";
    private final String TLS_PROXY_KEY_FILE_PATH = "./src/test/resources/authentication/tls/server-key.pem";
    private final String DUMMY_VALUE = "DUMMY_VALUE";

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(false);
        proxyConfig.setTlsCertificateFilePath(TLS_PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(TLS_PROXY_KEY_FILE_PATH);
        proxyConfig.setZookeeperServers(DUMMY_VALUE);
        proxyConfig.setConfigurationStoreServers(DUMMY_VALUE);

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                                                            PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(mockZooKeeperClientFactory).when(proxyService).getZooKeeperClientFactory();

        proxyService.start();
    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
    }

    @Test
    public void testProducer() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrlTls())
                .allowTlsInsecureConnection(false).tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH).build();
        Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic("persistent://sample/test/local/topic").create();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }
        producer.close();
        client.close();
    }

    @Test
    public void testPartitions() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrlTls())
                .allowTlsInsecureConnection(false).tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH).build();
        TenantInfo tenantInfo = createDefaultTenantInfo();
        List<String> tenants = admin.tenants().getTenants();
        if(!tenants.contains("sample")) {
            admin.tenants().createTenant("sample", tenantInfo);
        }
        String topic = "persistent://sample/test/local/" + UUID.randomUUID().toString();
        admin.topics().createPartitionedTopic(topic, 2);

        Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic(topic)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        // Create a consumer directly attached to broker
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
        }

        client.close();
    }

}
