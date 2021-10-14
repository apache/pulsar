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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.customroute.PartialRoundRobinMessageRouterImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class PartialPartitionedProducerTest extends ProducerConsumerBase {
    @Override
    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPtWithSinglePartition() throws Throwable {
        final String topic = BrokerTestUtil.newUniqueName("pt-with-single-routing");
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImpl = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        for (int i = 0; i < 10; i++) {
            producerImpl.newMessage().value("msg".getBytes()).send();
        }
        assertEquals(producerImpl.getProducers().size(), 1);
    }

    @Test
    public void testPtWithPartialPartition() throws Throwable {
        final String topic = BrokerTestUtil.newUniqueName("pt-with-partial-routing");
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImpl = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new PartialRoundRobinMessageRouterImpl(3))
                .create();

        for (int i = 0; i < 10; i++) {
            producerImpl.newMessage().value("msg".getBytes()).send();
        }
        assertEquals(producerImpl.getProducers().size(), 3);
    }

    // AddPartitionTest
    @Test
    public void testPtLazyLoading() throws Throwable {
        final String topic = BrokerTestUtil.newUniqueName("pt-lazily");
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImpl = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        final Supplier<Boolean> send = () -> {
            for (int i = 0; i < 10; i++) {
                try {
                    producerImpl.newMessage().value("msg".getBytes()).send();
                } catch (Throwable e) {
                    return false;
                }
            }
            return true;
        };

        // create first producer at initialization step
        assertEquals(producerImpl.getProducers().size(), 1);

        assertTrue(send.get());
        assertEquals(producerImpl.getProducers().size(), 10);
    }

    @Test
    public void testPtLoadingNotSharedMode() throws Throwable {
        final String topic = BrokerTestUtil.newUniqueName("pt-not-shared-mode");
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImplExclusive = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .accessMode(ProducerAccessMode.Exclusive)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        // create first producer at initialization step
        assertEquals(producerImplExclusive.getProducers().size(), 10);

        producerImplExclusive.close();

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImplWaitForExclusive = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        assertEquals(producerImplWaitForExclusive.getProducers().size(), 10);
    }

    // AddPartitionAndLimitTest
    @Test
    public void testPtUpdateWithPartialPartition() throws Throwable {
        final String topic = BrokerTestUtil.newUniqueName("pt-update-with-partial-routing");
        admin.topics().createPartitionedTopic(topic, 2);

        final Field field = PartitionedProducerImpl.class.getDeclaredField("topicMetadata");
        field.setAccessible(true);
        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImpl = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new PartialRoundRobinMessageRouterImpl(3))
                .accessMode(ProducerAccessMode.Shared)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .create();

        final Supplier<Boolean> send = ()-> {
            for (int i = 0; i < 10; i++) {
                try {
                    producerImpl.newMessage().value("msg".getBytes()).send();
                } catch (Throwable e) {
                    return false;
                }
            }
            return true;
        };

        // create first producer at initialization step
        assertEquals(producerImpl.getProducers().size(), 1);

        assertTrue(send.get());
        assertEquals(producerImpl.getProducers().size(), 2);

        admin.topics().updatePartitionedTopic(topic, 3);
        Awaitility.await().untilAsserted(() ->
                assertEquals(((TopicMetadata) field.get(producerImpl)).numPartitions(), 3));
        assertEquals(producerImpl.getProducers().size(), 2);

        assertTrue(send.get());
        assertEquals(producerImpl.getProducers().size(), 3);

        admin.topics().updatePartitionedTopic(topic, 4);
        Awaitility.await().untilAsserted(() ->
                assertEquals(((TopicMetadata) field.get(producerImpl)).numPartitions(), 4));
        assertTrue(send.get());
        assertEquals(producerImpl.getProducers().size(), 3);
    }

    @Test
    public void testPtUpdateNotSharedMode() throws Throwable {
        final String topic = BrokerTestUtil.newUniqueName("pt-update-not-shared");
        admin.topics().createPartitionedTopic(topic, 2);

        final Field field = PartitionedProducerImpl.class.getDeclaredField("topicMetadata");
        field.setAccessible(true);
        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImplExclusive = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .accessMode(ProducerAccessMode.Exclusive)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .create();

        assertEquals(producerImplExclusive.getProducers().size(), 2);

        admin.topics().updatePartitionedTopic(topic, 3);
        Awaitility.await().untilAsserted(() ->
                assertEquals(((TopicMetadata) field.get(producerImplExclusive)).numPartitions(), 3));
        assertEquals(producerImplExclusive.getProducers().size(), 3);

        producerImplExclusive.close();

        @Cleanup
        final PartitionedProducerImpl<byte[]> producerImplWaitForExclusive = (PartitionedProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .autoUpdatePartitions(true)
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS)
                .create();

        assertEquals(producerImplWaitForExclusive.getProducers().size(), 3);

        admin.topics().updatePartitionedTopic(topic, 4);
        Awaitility.await().untilAsserted(() ->
                assertEquals(((TopicMetadata) field.get(producerImplWaitForExclusive)).numPartitions(), 4));
        assertEquals(producerImplWaitForExclusive.getProducers().size(), 4);
    }
}
