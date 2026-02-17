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

import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.doReturn;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyTlsTest extends MockedPulsarServiceBaseTest {

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(false);
        proxyConfig.setTlsCertificateFilePath(PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(PROXY_KEY_FILE_PATH);
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

        proxyService.close();
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
    }

    private void createNamespaceIfAbsent(TopicName topicName) throws Exception {
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        NamespaceName namespaceName = topicName.getNamespaceObject();
        if (!namespaceName.isV2()) {
            if (!admin.clusters().getClusters().contains(namespaceName.getCluster())) {
                admin.clusters().createCluster(namespaceName.getCluster(), ClusterData.builder()
                        .brokerServiceUrl(pulsar.getBrokerServiceUrl()).build());
            }
            tenantInfo.getAllowedClusters().add(namespaceName.getCluster());
        }
        if (!admin.tenants().getTenants().contains(topicName.getTenant())) {
            admin.tenants().createTenant(topicName.getTenant(), tenantInfo);
        }
        try {
            admin.namespaces().createNamespace(topicName.getNamespace());
        } catch (Exception ex) {
            // Namespace may already exist.
        }
    }

    @Test
    public void testProducer() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrlTls())
                .allowTlsInsecureConnection(false)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH).build();
        admin.topics().createPartitionedTopic("persistent://sample/test/local/topic", 2);
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("persistent://sample/test/local/topic").create();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }
    }

    @Test
    public void testPartitions() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxyService.getServiceUrlTls())
                .allowTlsInsecureConnection(false).tlsTrustCertsFilePath(CA_CERT_FILE_PATH).build();
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("sample", tenantInfo);
        String topicName = "persistent://sample/" + pulsar.getConfig().getClusterName()
                + "/local/partitioned-topic";
        createNamespaceIfAbsent(TopicName.get(topicName));
        admin.topics().createPartitionedTopic(topicName, 2);

        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(topicName)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        // Create a consumer directly attached to broker
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            requireNonNull(msg);
        }
    }

}
