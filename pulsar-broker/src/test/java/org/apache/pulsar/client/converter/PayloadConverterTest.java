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
package org.apache.pulsar.client.converter;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for {@link org.apache.pulsar.client.api.PayloadConverter}.
 */
@Slf4j
public class PayloadConverterTest extends ProducerConsumerBase {

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

    private static int getNumBatches(final int numMessages, final int batchingMaxMessages) {
        int numBatches = numMessages / batchingMaxMessages;
        numBatches += (numMessages % batchingMaxMessages == 0) ? 0 : 1;
        return numBatches;
    }

    @Test(dataProvider = "config")
    public void testDefaultConverter(int numPartitions, boolean enableBatching, int batchingMaxMessages)
            throws Exception {
        final String topic = "testDefaultConverter-" + numPartitions + "-" + enableBatching + "-" + batchingMaxMessages;
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

        final DefaultPayloadConverter converter = new DefaultPayloadConverter();

        @Cleanup
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .payloadConverter(converter)
                .subscribe();
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            final Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            values.add(message.getValue());
        }

        if (numPartitions > 1) {
            // messages are out of order across multiple partitions
            Collections.sort(values);
        }
        for (int i = 0; i < numMessages; i++) {
            Assert.assertEquals(values.get(i), messagePrefix + i);
        }

        // Each buffer's refCnt is 1 when the iteration is stopped, because it will be released in
        // PulsarDecoder#channelRead() finally.
        if (enableBatching) {
            Assert.assertEquals(converter.getTotalRefCnt(), getNumBatches(numMessages, batchingMaxMessages));
        } else {
            Assert.assertEquals(converter.getTotalRefCnt(), numMessages);
        }
    }

    @Test(dataProvider = "customBatchConfig")
    public void testCustomConverter(final int numMessages, final int batchingMaxMessages) throws Exception {
        final String topic = "persistent://public/default/testCustomConverter-"
                + numMessages + "-" + batchingMaxMessages;
        final String messagePrefix = "msg-";

        final CustomBatchConverter converter = new CustomBatchConverter();
        @Cleanup
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .payloadConverter(converter)
                .subscribe();

        final PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().orElse(null);
        Assert.assertNotNull(persistentTopic);

        final List<String> values = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            final String value = messagePrefix + i;
            values.add(value);
            if (values.size() >= batchingMaxMessages || i == numMessages - 1) {
                final ByteBuf valueBuf = CustomBatchFormat.serialize(values);
                values.clear();
                final ByteBuf headerAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.None,
                        createCustomMetadata(), valueBuf);
                valueBuf.release();
                persistentTopic.publishMessage(headerAndPayload, (e, ledgerId, entryId) -> {
                    if (e == null) {
                        log.info("Send {} to {} ({}, {})", value, topic, ledgerId, entryId);
                    } else {
                        log.error("Failed to send {}: {}", value, e.getMessage());
                    }
                });
            }
        }

        for (int i = 0; i < numMessages; i++) {
            final Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            Assert.assertEquals(message.getValue(), messagePrefix + i);
        }

        Assert.assertEquals(converter.getTotalRefCnt(), getNumBatches(numMessages, batchingMaxMessages));
    }

    private static MessageMetadata createCustomMetadata() {
        final MessageMetadata messageMetadata = new MessageMetadata();
        // Here are required fields
        messageMetadata.setProducerName("producer");
        messageMetadata.setSequenceId(0L);
        messageMetadata.setPublishTime(0L);
        // Add the property to identify the message format
        messageMetadata.addProperty().setKey(CustomBatchFormat.KEY).setValue(CustomBatchFormat.VALUE);
        return messageMetadata;
    }
}
