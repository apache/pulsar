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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.client.util.MathUtils.signSafeMod;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import com.google.api.client.util.Lists;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.customroute.PartialRoundRobinMessageRouterImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.assertj.core.util.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit Tests of {@link PartitionedProducerImpl}.
 */
public class PartitionedProducerImplTest {

    private static final String TOPIC_NAME = "testTopicName";
    private PulsarClientImpl client;
    private ProducerBuilderImpl producerBuilderImpl;
    private Schema schema;
    private ProducerInterceptors producerInterceptors;
    private CompletableFuture<Producer> producerCreatedFuture;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        schema = mock(Schema.class);
        producerInterceptors = mock(ProducerInterceptors.class);
        producerCreatedFuture = new CompletableFuture<>();
        ClientConfigurationData clientConfigurationData = mock(ClientConfigurationData.class);
        Timer timer = mock(Timer.class);

        producerBuilderImpl = new ProducerBuilderImpl(client, Schema.BYTES);

        when(client.getConfiguration()).thenReturn(clientConfigurationData);
        when(client.timer()).thenReturn(timer);
        when(client.newProducer()).thenReturn(producerBuilderImpl);
        when(client.newProducerImpl(anyString(), anyInt(), any(), any(), any(), any(), any()))
                .thenAnswer(invocationOnMock -> {
            return new ProducerImpl<>(client, invocationOnMock.getArgument(0),
                    invocationOnMock.getArgument(2), invocationOnMock.getArgument(5),
                    invocationOnMock.getArgument(1), invocationOnMock.getArgument(3),
                    invocationOnMock.getArgument(4), invocationOnMock.getArgument(6));
        });
    }

    @Test
    public void testSinglePartitionMessageRouterImplInstance() throws NoSuchFieldException, IllegalAccessException {
        ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        producerConfigurationData.setMessageRoutingMode(MessageRoutingMode.SinglePartition);

        MessageRouter messageRouter = getMessageRouter(producerConfigurationData);
        assertTrue(messageRouter instanceof SinglePartitionMessageRouterImpl);
    }

    @Test
    public void testRoundRobinPartitionMessageRouterImplInstance() throws NoSuchFieldException, IllegalAccessException {
        ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        producerConfigurationData.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);

        MessageRouter messageRouter = getMessageRouter(producerConfigurationData);
        assertTrue(messageRouter instanceof RoundRobinPartitionMessageRouterImpl);
    }

    @Test
    public void testCustomMessageRouterInstance() throws NoSuchFieldException, IllegalAccessException {
        ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
        producerConfigurationData.setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        producerConfigurationData.setCustomMessageRouter(new CustomMessageRouter());

        MessageRouter messageRouter = getMessageRouter(producerConfigurationData);
        assertTrue(messageRouter instanceof CustomMessageRouter);
    }

    @Test
    public void testPartialPartition() {
        final MessageRouter router = new PartialRoundRobinMessageRouterImpl(3);
        final Set<Integer> actualSet = Sets.newHashSet();
        final Message<byte[]> msg = MessageImpl
                .create(new MessageMetadata(), ByteBuffer.wrap(new byte[0]), Schema.BYTES, null);

        for (int i = 0; i < 10; i++) {
            final TopicMetadata metadata = new TopicMetadataImpl(10);
            actualSet.add(router.choosePartition(msg, metadata));
        }
        assertEquals(actualSet.size(), 3);

        try {
            new PartialRoundRobinMessageRouterImpl(0);
            fail();
        } catch (Exception e) {
            assertEquals(e.getClass(), IllegalArgumentException.class);
        }
    }

    @Test
    public void testPartialPartitionWithKey() {
        final MessageRouter router = new PartialRoundRobinMessageRouterImpl(3);
        final Hash hash = Murmur3Hash32.getInstance();
        final List<Integer> expectedHashList = Lists.newArrayList();
        final List<Integer> actualHashList = Lists.newArrayList();

        for (int i = 0; i < 10; i++) {
            final String key = String.valueOf(i);
            final Message<byte[]> msg = MessageImpl
                    .create(new MessageMetadata().setPartitionKey(key), ByteBuffer.wrap(new byte[0]), Schema.BYTES, null);
            final TopicMetadata metadata = new TopicMetadataImpl(10);
            expectedHashList.add(signSafeMod(hash.makeHash(key), 10));
            actualHashList.add(router.choosePartition(msg, metadata));
        }
        assertNotEquals(actualHashList, expectedHashList);
    }

    private MessageRouter getMessageRouter(ProducerConfigurationData producerConfigurationData)
            throws NoSuchFieldException, IllegalAccessException {
        PartitionedProducerImpl impl = new PartitionedProducerImpl(
                client, TOPIC_NAME, producerConfigurationData,
                2, producerCreatedFuture, schema, producerInterceptors);

        Field routerPolicy = impl.getClass().getDeclaredField("routerPolicy");
        routerPolicy.setAccessible(true);
        MessageRouter messageRouter = (MessageRouter) routerPolicy.get(impl);
        assertNotNull(messageRouter);
        return messageRouter;
    }

    private class CustomMessageRouter implements MessageRouter {
        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            int partitionIndex = Integer.parseInt(msg.getKey()) % metadata.numPartitions();
            return partitionIndex;
        }
    }

    @Test
    public void testGetStats() throws Exception {
        String topicName = "test-stats";
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setStatsIntervalSeconds(100);

        ThreadFactory threadFactory = new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);

        PulsarClientImpl clientImpl = new PulsarClientImpl(conf, eventLoopGroup);

        ProducerConfigurationData producerConfData = new ProducerConfigurationData();
        producerConfData.setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        producerConfData.setCustomMessageRouter(new CustomMessageRouter());

        assertEquals(Long.parseLong("100"), clientImpl.getConfiguration().getStatsIntervalSeconds());

        PartitionedProducerImpl impl = new PartitionedProducerImpl(
            clientImpl, topicName, producerConfData,
            1, null, null, null);

        impl.getStats();
    }

    @Test
    public void testGetStatsWithoutArriveUpdateInterval() throws Exception {
        String topicName = "test-stats-without-arrive-interval";
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setStatsIntervalSeconds(100);

        ThreadFactory threadFactory =
                new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup = EventLoopUtil
                .newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);

        PulsarClientImpl clientImpl = new PulsarClientImpl(conf, eventLoopGroup);

        ProducerConfigurationData producerConfData = new ProducerConfigurationData();
        producerConfData.setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        producerConfData.setCustomMessageRouter(new CustomMessageRouter());

        assertEquals(Long.parseLong("100"), clientImpl.getConfiguration().getStatsIntervalSeconds());

        PartitionedProducerImpl<byte[]> impl = new PartitionedProducerImpl<>(
                clientImpl, topicName, producerConfData,
                1, null, null, null);

        impl.getProducers().get(0).getStats().incrementSendFailed();
        ProducerStatsRecorderImpl stats = impl.getStats();
        assertEquals(stats.getTotalSendFailed(), 0);
        // When close producer, the ProducerStatsRecorder will update stats immediately
        impl.close();
        stats = impl.getStats();
        assertEquals(stats.getTotalSendFailed(), 1);
    }

    @Test
    public void testGetNumOfPartitions() throws Exception {
        String topicName = "test-get-num-of-partitions";
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setStatsIntervalSeconds(100);

        ThreadFactory threadFactory = new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);

        PulsarClientImpl clientImpl = new PulsarClientImpl(conf, eventLoopGroup);

        ProducerConfigurationData producerConfData = new ProducerConfigurationData();
        producerConfData.setMessageRoutingMode(MessageRoutingMode.CustomPartition);
        producerConfData.setCustomMessageRouter(new CustomMessageRouter());

        PartitionedProducerImpl partitionedProducerImpl = new PartitionedProducerImpl(
                clientImpl, topicName, producerConfData, 1, null, null, null);

        assertEquals(partitionedProducerImpl.getNumOfPartitions(), 1);

        String nonPartitionedTopicName = "test-get-num-of-partitions-for-non-partitioned-topic";
        ProducerConfigurationData producerConfDataNonPartitioned = new ProducerConfigurationData();
        ProducerImpl producerImpl = new ProducerImpl(clientImpl, nonPartitionedTopicName, producerConfDataNonPartitioned,
                null, 0, null, null, Optional.empty());
        assertEquals(producerImpl.getNumOfPartitions(), 0);
    }

}
