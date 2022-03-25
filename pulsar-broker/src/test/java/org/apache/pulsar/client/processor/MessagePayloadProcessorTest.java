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
package org.apache.pulsar.client.processor;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import io.netty.buffer.ByteBuf;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for {@link MessagePayloadProcessor}.
 */
@Slf4j
@Test(groups = "broker-impl")
public class MessagePayloadProcessorTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default", Sets.newHashSet("test"));
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public static Object[][] config() {
        return new Object[][] {
                // numPartitions / enableBatching / batchingMaxMessages
                { 1, true, 1 },
                { 1, true, 4 },
                { 1, false, 1 },
                { 3, false, 1 }
        };
    }

    @DataProvider
    public static Object[][] customBatchConfig() {
        return new Object[][] {
                // numMessages / batchingMaxMessages
                { 10, 1 },
                { 10, 4 }
        };
    }

    @Test(dataProvider = "config")
    public void testDefaultProcessor(int numPartitions, boolean enableBatching, int batchingMaxMessages)
            throws Exception {
        final String topic = "testDefaultProcessor-" + numPartitions + "-" + enableBatching + "-" + batchingMaxMessages;
        final int numMessages = 10;
        final String messagePrefix = "msg-";

        if (numPartitions > 1) {
            admin.topics().createPartitionedTopic(topic, numPartitions);
        }

        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(enableBatching)
                .batchingMaxMessages(batchingMaxMessages)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .messageRouter(new MessageRouter() {
                    int i = 0;

                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return i++ % metadata.numPartitions();
                    }
                })
                .create();
        for (int i = 0; i < numMessages; i++) {
            final String value = messagePrefix + i;
            producer.sendAsync(value).whenComplete((id, e) -> {
                if (e == null) {
                    log.info("Send {} to {} {}", value, topic, id);
                } else {
                    log.error("Failed to send {}: {}", value, e.getMessage());
                }
            });
        }

        final DefaultProcessorWithRefCnt processor = new DefaultProcessorWithRefCnt();
        @Cleanup
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messagePayloadProcessor(processor)
                .subscribe();
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            final Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            values.add(message.getValue());
            consumer.acknowledge(message.getMessageId());
        }

        if (numPartitions > 1) {
            // messages are out of order across multiple partitions
            Collections.sort(values);
        }
        for (int i = 0; i < numMessages; i++) {
            Assert.assertEquals(values.get(i), messagePrefix + i);
        }

        // Each buffer's refCnt is 2 because after retrieving the refCnt, there will be two release for the ByteBuf:
        // 1. ConsumerImpl#processPayloadByProcessor
        // 2. PulsarDecoder#channelRead
        if (enableBatching) {
            int numBatches = numMessages / batchingMaxMessages;
            numBatches += (numMessages % batchingMaxMessages == 0) ? 0 : 1;
            Assert.assertEquals(processor.getTotalRefCnt(), 2 * numBatches);
        } else {
            Assert.assertEquals(processor.getTotalRefCnt(), 2 * numMessages);
        }
    }

    @Test
    public void testCustomBatchFormat() {
        final List<List<String>> inputs = new ArrayList<>();
        inputs.add(Collections.emptyList());
        inputs.add(Collections.singletonList("java"));
        inputs.add(Arrays.asList("hello", "world", "java"));

        for (List<String> input : inputs) {
            final ByteBuf buf = CustomBatchFormat.serialize(input);

            final CustomBatchFormat.Metadata metadata = CustomBatchFormat.readMetadata(buf);
            final List<String> parsedTokens = new ArrayList<>();
            for (int i = 0; i < metadata.getNumMessages(); i++) {
                parsedTokens.add(Schema.STRING.decode(CustomBatchFormat.readMessage(buf)));
            }

            Assert.assertEquals(parsedTokens, input);
            Assert.assertEquals(parsedTokens.size(), input.size());

            Assert.assertEquals(buf.refCnt(), 1);
            buf.release();
        }
    }

    @Test(dataProvider = "customBatchConfig")
    public void testCustomProcessor(final int numMessages, final int batchingMaxMessages) throws Exception {
        final String topic = "persistent://public/default/testCustomProcessor-"
                + numMessages + "-" + batchingMaxMessages;

        @Cleanup
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messagePayloadProcessor(new CustomBatchPayloadProcessor())
                .subscribe();

        final PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().orElse(null);
        Assert.assertNotNull(persistentTopic);

        final String messagePrefix = "msg-";
        final CustomBatchProducer producer = new CustomBatchProducer(persistentTopic, batchingMaxMessages);
        for (int i = 0; i < numMessages; i++) {
            producer.sendAsync(messagePrefix + i);
        }
        producer.flush();

        for (int i = 0; i < numMessages; i++) {
            final Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            Assert.assertEquals(message.getValue(), messagePrefix + i);
            consumer.acknowledge(message.getMessageId());
        }
    }
}
