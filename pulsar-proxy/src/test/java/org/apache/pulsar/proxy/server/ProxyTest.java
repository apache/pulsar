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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.apache.avro.reflect.Nullable;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyTest extends MockedPulsarServiceBaseTest {

    private static final Logger log = LoggerFactory.getLogger(ProxyTest.class);

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Data
    @ToString
    @EqualsAndHashCode
    public static class Foo {
        @Nullable
        private String field1;
        @Nullable
        private String field2;
        private int field3;
    }

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(Optional.ofNullable(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl(DUMMY_VALUE);
        proxyConfig.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);

        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                                                            PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();

        proxyService.start();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
    }

    @Test
    public void testProducer() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic("persistent://sample/test/local/producer-topic")
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

        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
            .topic("persistent://sample/test/local/producer-consumer-topic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        // Create a consumer directly attached to broker
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://sample/test/local/producer-consumer-topic").subscriptionName("my-sub").subscribe();

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
    }

    @Test
    public void testPartitions() throws Exception {
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("sample", tenantInfo);
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();
        admin.topics().createPartitionedTopic("persistent://sample/test/local/partitioned-topic", 2);

        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
            .topic("persistent://sample/test/local/partitioned-topic")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();

        // Create a consumer directly attached to broker
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://sample/test/local/partitioned-topic")
                .subscriptionName("my-sub").subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            requireNonNull(msg);
        }
    }

    /**
     * test auto create partitioned topic by proxy
     **/
    @Test
    public void testAutoCreateTopic() throws Exception{
        int defaultPartition = 2;
        int defaultNumPartitions = pulsar.getConfiguration().getDefaultNumPartitions();
        pulsar.getConfiguration().setAllowAutoTopicCreationType("partitioned");
        pulsar.getConfiguration().setDefaultNumPartitions(defaultPartition);
        try {
            @Cleanup
            PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
              .build();
            String topic = "persistent://sample/test/local/partitioned-proxy-topic";
            CompletableFuture<List<String>> partitionNamesFuture = client.getPartitionsForTopic(topic);
            List<String> partitionNames = partitionNamesFuture.get(30000, TimeUnit.MILLISECONDS);
            Assert.assertEquals(partitionNames.size(), defaultPartition);
        } finally {
            pulsar.getConfiguration().setAllowAutoTopicCreationType("non-partitioned");
            pulsar.getConfiguration().setDefaultNumPartitions(defaultNumPartitions);
        }
    }

    @Test
    public void testRegexSubscription() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
            .connectionsPerBroker(5).ioThreads(5).build();

        // create two topics by subscribing to a topic and closing it
        try (Consumer<byte[]> ignored = client.newConsumer()
            .topic("persistent://sample/test/local/regex-sub-topic1")
            .subscriptionName("proxy-ignored")
            .subscribe()) {
        }
        try (Consumer<byte[]> ignored = client.newConsumer()
            .topic("persistent://sample/test/local/regex-sub-topic2")
            .subscriptionName("proxy-ignored")
            .subscribe()) {
        }

        String subName = "regex-sub-proxy-test-" + System.currentTimeMillis();

        // make sure regex subscription
        String regexSubscriptionPattern = "persistent://sample/test/local/regex-sub-topic.*";
        log.info("Regex subscribe to topics {}", regexSubscriptionPattern);
        try (Consumer<byte[]> consumer = client.newConsumer()
            .topicsPattern(regexSubscriptionPattern)
            .subscriptionName(subName)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe()) {
            log.info("Successfully subscribe to topics using regex {}", regexSubscriptionPattern);

            final int numMessages = 20;

            try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("persistent://sample/test/local/regex-sub-topic1")
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

    @Test
    public void testGetSchema() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();
        Schema<Foo> schema = Schema.AVRO(Foo.class);
        try {
            try (Producer<Foo> ignored = client.newProducer(schema).topic("persistent://sample/test/local/get-schema")
                .create()) {
            }
        } catch (Exception ex) {
            Assert.fail("Should not have failed since can acquire LookupRequestSemaphore");
        }
        byte[] schemaVersion = new byte[8];
        byte b = Long.valueOf(0L).byteValue();
        for (int i = 0; i < 8; i++){
            schemaVersion[i] = b;
        }
        SchemaInfo schemaInfo = ((PulsarClientImpl) client).getLookup()
                .getSchema(TopicName.get("persistent://sample/test/local/get-schema"), schemaVersion)
                .get().orElse(null);
        Assert.assertEquals(new String(schemaInfo.getSchema()), new String(schema.getSchemaInfo().getSchema()));
    }

    @Test
    public void testProtocolVersionAdvertisement() throws Exception {
        final String topic = "persistent://sample/test/local/protocol-version-advertisement";
        final String sub = "my-sub";

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(proxyService.getServiceUrl());

        @Cleanup
        PulsarClient client = getClientActiveConsumerChangeNotSupported(conf);

        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(topic).create();

        @Cleanup
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
    }

    @Test
    public void testGetClientVersion() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();

        String topic = "persistent://sample/test/local/testGetClientVersion";
        String subName = "test-sub";

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .subscribe();

        consumer.receiveAsync();


        Assert.assertEquals(admin.topics().getStats(topic).getSubscriptions().get(subName).getConsumers()
                .get(0).getClientVersion(), PulsarVersion.getVersion());
    }

    private static PulsarClient getClientActiveConsumerChangeNotSupported(ClientConfigurationData conf)
            throws Exception {
        ThreadFactory threadFactory = new DefaultThreadFactory("pulsar-client-io", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);

        ConnectionPool cnxPool = new ConnectionPool(conf, eventLoopGroup, () -> {
            return new ClientCnx(conf, eventLoopGroup, ProtocolVersion.v11_VALUE) {
                @Override
                protected void handleActiveConsumerChange(CommandActiveConsumerChange change) {
                    throw new UnsupportedOperationException();
                }
            };
        });

        return new PulsarClientImpl(conf, eventLoopGroup, cnxPool);
    }

}
