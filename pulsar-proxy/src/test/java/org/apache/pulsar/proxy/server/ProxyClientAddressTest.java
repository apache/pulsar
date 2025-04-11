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
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.Metadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ProxyClientAddressTest extends MockedPulsarServiceBaseTest {

    private ProxyService proxyService;
    private final ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.ofNullable(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");

        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setClusterName(conf.getClusterName());

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)), AuthenticationDisabled.INSTANCE));

        proxyService.start();

        // create default resources.
        admin.clusters().createCluster(conf.getClusterName(),
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfo tenantInfo =
                new TenantInfoImpl(Collections.emptySet(), Collections.singleton(conf.getClusterName()));
        admin.tenants().createTenant("public", tenantInfo);
        admin.namespaces().createNamespace("public/default");
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
    }

    @Test
    public void testProducerConsumerAddress() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();

        String topic = "topic-" + UUID.randomUUID();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) client.newProducer()
                .topic(topic)
                .create();
        String subName = "my-sub";
        @Cleanup
        ConsumerImpl<byte[]> consumer =
                (ConsumerImpl<byte[]>) client.newConsumer().subscriptionName(subName).topic(topic).subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertThat(stats.getPublishers())
                .element(0)
                .satisfies(n -> {
                    assertThat(n.getAddress()).isEqualTo(
                            producer.getClientCnx().ctx().channel().localAddress().toString());
                });

        assertThat(stats.getSubscriptions())
                .containsKey(subName)
                .extractingByKey(subName)
                .satisfies(n -> {
                    assertThat(n.getConsumers())
                            .element(0)
                            .satisfies(c -> {
                                assertThat(c.getAddress()).isEqualTo(
                                        consumer.getClientCnx().ctx().channel().localAddress().toString());
                            });
                });
    }

    @Test
    public void testProducerConsumerWithCustomAddress() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();

        String topic = "topic-" + UUID.randomUUID();
        String producerAddress = "producer-address";
        @Cleanup
        Producer<byte[]> ignoredProducer = client.newProducer()
                .property(Metadata.CLIENT_IP, producerAddress)
                .topic(topic)
                .create();
        String subName = "my-sub";
        String consumerAddress = "consumer-address";
        @Cleanup
        Consumer<byte[]> ignoredConsumer = client.newConsumer().property(Metadata.CLIENT_IP, consumerAddress)
                .subscriptionName(subName).topic(topic).subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertThat(stats.getPublishers())
                .element(0)
                .satisfies(n -> {
                    assertThat(n.getAddress()).isEqualTo(producerAddress);
                });

        assertThat(stats.getSubscriptions())
                .containsKey(subName)
                .extractingByKey(subName)
                .satisfies(n -> {
                    assertThat(n.getConsumers())
                            .element(0)
                            .satisfies(c -> {
                                assertThat(c.getAddress()).isEqualTo(consumerAddress);
                            });
                });
    }
}