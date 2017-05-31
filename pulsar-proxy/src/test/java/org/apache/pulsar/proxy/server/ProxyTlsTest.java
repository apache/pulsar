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

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyTlsTest extends MockedPulsarServiceBaseTest {

    private static final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/cacert.pem";
    private static final String TLS_PROXY_CERT_FILE_PATH = "./src/test/resources/proxy-cert.pem";
    private static final String TLS_PROXY_KEY_FILE_PATH = "./src/test/resources/proxy-key.pem";

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(PortManager.nextFreePort());
        proxyConfig.setServicePortTls(PortManager.nextFreePort());
        proxyConfig.setWebServicePort(PortManager.nextFreePort());
        proxyConfig.setWebServicePortTls(PortManager.nextFreePort());
        proxyConfig.setTlsEnabledInProxy(true);
        proxyConfig.setTlsEnabledWithBroker(false);
        proxyConfig.setTlsCertificateFilePath(TLS_PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(TLS_PROXY_KEY_FILE_PATH);
        proxyService = Mockito.spy(new ProxyService(proxyConfig));
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
        ClientConfiguration conf = new ClientConfiguration();
        conf.setUseTls(true);
        conf.setTlsAllowInsecureConnection(false);
        conf.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        PulsarClient client = PulsarClient.create("pulsar+ssl://localhost:" + proxyConfig.getServicePortTls(), conf);
        Producer producer = client.createProducer("persistent://sample/test/local/topic");

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        client.close();
    }

    @Test
    public void testPartitions() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setUseTls(true);
        conf.setTlsAllowInsecureConnection(false);
        conf.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);

        PulsarClient client = PulsarClient.create("pulsar://localhost:" + proxyConfig.getServicePortTls(), conf);
        admin.persistentTopics().createPartitionedTopic("persistent://sample/test/local/partitioned-topic", 2);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = client.createProducer("persistent://sample/test/local/partitioned-topic", producerConf);

        // Create a consumer directly attached to broker
        Consumer consumer = pulsarClient.subscribe("persistent://sample/test/local/partitioned-topic", "my-sub");

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
        }

        client.close();
    }

}
