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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
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
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CustomLog
public class ProxyParserTest extends MockedPulsarServiceBaseTest {

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();
        setupDefaultTenantAndNamespace();

        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
        proxyConfig.setClusterName(configClusterName);
        //enable full parsing feature
        proxyConfig.setProxyLogLevel(Optional.ofNullable(2));
        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig)), proxyClientAuthentication));
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeper))).when(proxyService).createLocalMetadataStore();
        doReturn(registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal))).when(proxyService)
                .createConfigurationMetadataStore();

        Optional<Integer> proxyLogLevel = Optional.of(2);
        assertEquals(proxyLogLevel, proxyService.getConfiguration().getProxyLogLevel());
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

    @Test
    public void testProducer() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();
        Producer<byte[]> producer =
                client.newProducer(Schema.BYTES).topic("persistent://public/default/producer-topic")
                        .create();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }
    }

    @Test
    public void testProducerConsumer() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("persistent://public/default/producer-consumer-topic")
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        // Create a consumer directly attached to broker
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://public/default/producer-consumer-topic").subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            requireNonNull(msg);
            consumer.acknowledge(msg);
        }

        Message<byte[]> msg = consumer.receive(0, TimeUnit.SECONDS);
        checkArgument(msg == null);

        consumer.close();
    }

    @Test
    public void testPartitions() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();
        admin.topics().createPartitionedTopic("persistent://public/default/partitioned-topic", 2);

        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("persistent://public/default/partitioned-topic")
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        // Create a consumer directly attached to broker
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://public/default/partitioned-topic")
                .subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            requireNonNull(msg);
        }
    }

    @Test
    @SuppressWarnings("try")
    public void testRegexSubscription() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .connectionsPerBroker(5).ioThreads(5).build();

        // create two topics by subscribing to a topic and closing it
        try (Consumer<byte[]> ignored = client.newConsumer()
                .topic("persistent://public/default/topic1")
                .subscriptionName("ignored")
                .subscribe()) {
        }
        try (Consumer<byte[]> ignored = client.newConsumer()
                .topic("persistent://public/default/topic2")
                .subscriptionName("ignored")
                .subscribe()) {
        }
        String subName = "regex-sub-proxy-parser-test-" + System.currentTimeMillis();

        // make sure regex subscription
        String regexSubscriptionPattern = "persistent://public/default/topic.*";
        log.info()
                .attr("regexSubscriptionPattern", regexSubscriptionPattern)
                .log("Regex subscribe to topics");
        try (Consumer<byte[]> consumer = client.newConsumer()
                .topicsPattern(regexSubscriptionPattern)
                .subscriptionName(subName)
                .subscribe()) {
            log.info()
                    .attr("regexSubscriptionPattern", regexSubscriptionPattern)
                    .log("Successfully subscribe to topics using regex");

            final int numMessages = 20;

            try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                    .topic("persistent://public/default/topic1")
                    .create()) {
                for (int i = 0; i < numMessages; i++) {
                    producer.send(("message-" + i).getBytes(UTF_8));
                }
            }

            for (int i = 0; i < numMessages; i++) {
                Message<byte[]> msg = consumer.receive();
                assertEquals("message-" + i, new String(msg.getValue(), UTF_8));
            }
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testProtocolVersionAdvertisement() throws Exception {
        final String topic = "persistent://public/default/protocol-version-advertisement";
        final String sub = "my-sub";

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(proxyService.getServiceUrl());
        @Cleanup("shutdownNow")
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false,
                new DefaultThreadFactory("pulsar-client-io", Thread.currentThread().isDaemon()));
        @Cleanup
        PulsarClient client = getClientActiveConsumerChangeNotSupported(conf,
                eventLoopGroup);

        Producer<byte[]> producer = client.newProducer().topic(topic).create();
        Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName(sub)
                .subscriptionType(SubscriptionType.Failover).subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test-msg".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(10, TimeUnit.SECONDS);
            requireNonNull(msg);
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();
        client.close();
        // shutdown EventLoopGroup created in getClientActiveConsumerChangeNotSupported method
        ((PulsarClientImpl) client).getCnxPool().close();
    }

    private static PulsarClient getClientActiveConsumerChangeNotSupported(ClientConfigurationData conf,
                                                                          final EventLoopGroup eventLoopGroup)
            throws Exception {

        ConnectionPool cnxPool = new ConnectionPool(InstrumentProvider.NOOP, conf, eventLoopGroup, () -> {
            return new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup, ProtocolVersion.v11_VALUE) {
                @Override
                protected void handleActiveConsumerChange(CommandActiveConsumerChange change) {
                    throw new UnsupportedOperationException();
                }
            };
        }, null);

        return new PulsarClientImpl(conf, eventLoopGroup, cnxPool);
    }

}
