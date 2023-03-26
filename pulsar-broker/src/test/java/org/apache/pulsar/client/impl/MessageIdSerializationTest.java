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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class MessageIdSerializationTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testAcknowledge() throws Exception {
        String topic = "test-acknowledge-" + System.currentTimeMillis();
        @Cleanup Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .batchingMaxMessages(100)
                .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                .create();
        ConsumerImpl<Integer> consumer = createConsumer(topic);
        final int numMessages = 10;
        for (int i = 0; i < numMessages; i++) {
            producer.sendAsync(i);
        }
        producer.flush();
        final List<MessageId> msgIds = new ArrayList<>();
        final Set<Pair<Long, Long>> positions = new TreeSet<>();
        receiveAtMost(consumer, numMessages, msgIds, positions);
        assertEquals(positions.size(), 1);

        for (int i = 0; i < msgIds.size(); i++) {
            consumer.acknowledge(msgIds.get(i));
            if (i < msgIds.size() - 1) {
                assertEquals(consumer.getBatchMessageToAckerSize(), 1);
            } else {
                assertEquals(consumer.getBatchMessageToAckerSize(), 0);
            }
        }
        consumer.close();

        consumer = createConsumer(topic);
        MessageId newMsgId = producer.send(0);
        MessageId receivedMessageId = consumer.receive().getMessageId();
        assertEquals(newMsgId, receivedMessageId);
        consumer.close();
    }

    @Test(timeOut = 30000)
    public void testAcknowledgeCumulative() throws Exception {
        String topic = "test-acknowledge-cumulative-" + System.currentTimeMillis();
        final int batchingMaxMessages = 10;
        @Cleanup Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .batchingMaxMessages(batchingMaxMessages)
                .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                .create();
        ConsumerImpl<Integer> consumer = createConsumer(topic);
        // send 3 batches
        for (int i = 0; i < batchingMaxMessages * 2; i++) {
            producer.sendAsync(i);
        }
        producer.flush();
        final List<MessageId> msgIds = new ArrayList<>();
        final Set<Pair<Long, Long>> positions = new TreeSet<>();
        receiveAtMost(consumer, batchingMaxMessages * 2, msgIds, positions);
        assertEquals(positions.size(), 2);

        consumer.acknowledgeCumulative(msgIds.get(2));
        assertEquals(consumer.getBatchMessageToAckerSize(), 1);
        for (int i = batchingMaxMessages; i < batchingMaxMessages * 2; i++) {
            consumer.acknowledgeCumulative(msgIds.get(i));
            if (i < batchingMaxMessages * 2 - 1) {
                assertEquals(consumer.getBatchMessageToAckerSize(), 1);
            } else {
                assertEquals(consumer.getBatchMessageToAckerSize(), 0);
            }
        }
        consumer.close();
        consumer = createConsumer(topic);
        MessageId newMsgId = producer.send(0);
        MessageId receivedMessageId = consumer.receive().getMessageId();
        assertEquals(newMsgId, receivedMessageId);
        consumer.close();
    }

    private ConsumerImpl<Integer> createConsumer(String topic) throws PulsarClientException {
        return (ConsumerImpl<Integer>) pulsarClient.newConsumer(Schema.INT32).topic(topic)
                .subscriptionName("sub").isAckReceiptEnabled(true).subscribe();
    }

    private static void receiveAtMost(Consumer<Integer> consumer, int numMessages, List<MessageId> msgIds,
                                      Set<Pair<Long, Long>> positions) throws IOException {
        for (int i = 0; i < numMessages; i++) {
            final MessageIdImpl messageId = (MessageIdImpl) consumer.receive().getMessageId();
            MessageId deserialized = MessageId.fromByteArray(messageId.toByteArray());
            assertTrue(deserialized instanceof BatchMessageIdImpl);
            msgIds.add(deserialized);
            positions.add(Pair.of(messageId.getLedgerId(), messageId.getEntryId()));
        }
    }
}
