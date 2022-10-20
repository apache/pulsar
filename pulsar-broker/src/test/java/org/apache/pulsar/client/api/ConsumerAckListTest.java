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
package org.apache.pulsar.client.api;

import lombok.Cleanup;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker-api")
public class ConsumerAckListTest extends ProducerConsumerBase {

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

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @Test(timeOut = 30000, dataProvider = "ackReceiptEnabled")
    public void testBatchListAck(boolean ackReceiptEnabled) throws Exception {
        ackListMessage(true,true, ackReceiptEnabled);
        ackListMessage(true,false, ackReceiptEnabled);
        ackListMessage(false,false, ackReceiptEnabled);
        ackListMessage(false,true, ackReceiptEnabled);
    }

    private void ackListMessage(boolean isBatch, boolean isPartitioned, boolean ackReceiptEnabled) throws Exception {
        final String topic = "persistent://my-property/my-ns/batch-ack-" + UUID.randomUUID();
        final String subName = "testBatchAck-sub" + UUID.randomUUID();
        final int messageNum = ThreadLocalRandom.current().nextInt(50, 100);
        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topic, 3);
        }

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(isBatch)
                .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)
                .topic(topic).create();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Shared)
                .topic(topic)
                .negativeAckRedeliveryDelay(1001, TimeUnit.MILLISECONDS)
                .subscriptionName(subName)
                .enableBatchIndexAcknowledgment(ackReceiptEnabled)
                .isAckReceiptEnabled(ackReceiptEnabled)
                .subscribe();

        sendMessagesAsyncAndWait(producer, messageNum);
        List<MessageId> messages = new ArrayList<>();
        for (int i = 0; i < messageNum; i++) {
            messages.add(consumer.receive().getMessageId());
        }
        consumer.acknowledge(messages);
        //Wait ack send.
        Thread.sleep(1000);
        consumer.redeliverUnacknowledgedMessages();
        Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg);
    }

    private void sendMessagesAsyncAndWait(Producer<String> producer, int messages) throws Exception {
        CountDownLatch latch = new CountDownLatch(messages);
        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message).thenAccept(messageId -> {
                if (messageId != null) {
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

}
