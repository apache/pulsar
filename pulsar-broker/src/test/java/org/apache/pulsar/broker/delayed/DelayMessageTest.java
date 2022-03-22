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
package org.apache.pulsar.broker.delayed;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Test(groups = "broker-impl")
public class DelayMessageTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    public void testDelayAtOrderAndTime() throws PulsarClientException {
        final int MESSAGE_NUM = 500;
        pulsarClient = PulsarClient.builder().
                serviceUrl(lookupUrl.toString())
                .build();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("test")
                .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("test")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("test-sub")
                .subscribe();
        final long sendTaskStartTime = System.currentTimeMillis();
        AtomicInteger counter = new AtomicInteger(0);
        Thread sendTask = new Thread(() -> {
            for (int i = 0; i < MESSAGE_NUM; i++) {
                producer.newMessage()
                        .deliverAt(sendTaskStartTime + TimeUnit.SECONDS.toMillis(10))
                        .value(counter.getAndIncrement() + "")
                        .sendAsync();
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        sendTask.start();
        HashSet<MessageId> receivedMessageSet = new HashSet<>();
        List<MessageId> receivedMessageList = new ArrayList<>();
        Integer lastMsg = null;
        while (true) {
            Message<String> msg = consumer.receive(15, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            int msgBody = Integer.parseInt(new String(msg.getData()));
            if (lastMsg != null) {
                // To ensure message order.
                Assert.assertEquals(lastMsg.intValue(), msgBody - 1);
            }
            // To ensure we receive delay message at greater than 10 seconds.
            Assert.assertTrue(System.currentTimeMillis() - sendTaskStartTime
                    > TimeUnit.SECONDS.toMillis(10));
            lastMsg = msgBody;
            consumer.acknowledge(msg);
            receivedMessageSet.add(msg.getMessageId());
            receivedMessageList.add(msg.getMessageId());
        }
        Assert.assertEquals(receivedMessageList.size(), MESSAGE_NUM);
        Assert.assertEquals(receivedMessageSet.size(), MESSAGE_NUM);
        if (sendTask.isAlive()) {
            sendTask.interrupt();
        }
        Assert.assertFalse(sendTask.isAlive());
    }
}
