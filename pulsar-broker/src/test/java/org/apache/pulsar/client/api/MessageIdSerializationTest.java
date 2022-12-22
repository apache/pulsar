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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
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
    public void testSerialization() throws Exception {
        String topic = "test-serialization-origin";
        @Cleanup Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .batchingMaxMessages(100)
                .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                .create();
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .isAckReceiptEnabled(true)
                .subscribe();

        final int numMessages = 10;
        for (int i = 0; i < numMessages; i++) {
            producer.sendAsync(i);
        }
        producer.flush();
        final List<MessageId> msgIds = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            msgIds.add(consumer.receive().getMessageId());
        }
        final AtomicLong ledgerId = new AtomicLong(-1L);
        final AtomicLong entryId = new AtomicLong(-1L);
        for (int i = 0; i < numMessages; i++) {
            assertTrue(msgIds.get(i) instanceof BatchMessageIdImpl);
            final BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) msgIds.get(i);
            ledgerId.compareAndSet(-1L, batchMessageId.getLedgerId());
            assertEquals(batchMessageId.getLedgerId(), ledgerId.get());
            entryId.compareAndSet(-1L, batchMessageId.getEntryId());
            assertEquals(batchMessageId.getEntryId(), entryId.get());
            assertEquals(batchMessageId.getBatchSize(), numMessages);
        }

        final List<MessageId> deserializedMsgIds = new ArrayList<>();
        for (MessageId msgId : msgIds) {
            MessageId deserialized = MessageId.fromByteArray(msgId.toByteArray());
            assertTrue(deserialized instanceof BatchMessageIdImpl);
            deserializedMsgIds.add(deserialized);
        }
        for (MessageId msgId : deserializedMsgIds) {
            consumer.acknowledge(msgId);
        }
        consumer.close();

        consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .isAckReceiptEnabled(true)
                .subscribe();
        MessageId newMsgId = producer.send(0);
        MessageId receivedMessageId = consumer.receive().getMessageId();
        assertEquals(newMsgId, receivedMessageId);
        consumer.close();
    }
}
