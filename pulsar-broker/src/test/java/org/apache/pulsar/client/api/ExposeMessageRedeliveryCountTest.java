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

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker-api")
public class ExposeMessageRedeliveryCountTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testRedeliveryCount() throws PulsarClientException {

        final String topic = "persistent://my-property/my-ns/redeliveryCount";

        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .receiverQueueSize(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .create();

        producer.send("Hello Pulsar".getBytes());

        do {
            Message<byte[]> message = consumer.receive();
            message.getProperties();
            final int redeliveryCount = message.getRedeliveryCount();
            if (redeliveryCount > 2) {
                consumer.acknowledge(message);
                Assert.assertEquals(3, redeliveryCount);
                break;
            }
        } while (true);

        producer.close();
        consumer.close();
    }

    @Test(timeOut = 30000)
    public void testRedeliveryCountWithPartitionedTopic() throws PulsarClientException, PulsarAdminException {

        final String topic = "persistent://my-property/my-ns/redeliveryCount.partitioned";

        admin.topics().createPartitionedTopic(topic, 3);

        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .receiverQueueSize(100)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topic)
                .create();

        producer.send("Hello Pulsar".getBytes());

        do {
            Message<byte[]> message = consumer.receive();
            message.getProperties();
            final int redeliveryCount = message.getRedeliveryCount();
            if (redeliveryCount > 2) {
                consumer.acknowledge(message);
                Assert.assertEquals(3, redeliveryCount);
                break;
            }
        } while (true);

        producer.close();
        consumer.close();

        admin.topics().deletePartitionedTopic(topic);
    }

    @Test(timeOut = 30000)
    public void testRedeliveryCountWhenConsumerDisconnected() throws PulsarClientException {

        String topic = "persistent://my-property/my-ns/testRedeliveryCountWhenConsumerDisconnected";

        Consumer<String> consumer0 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("s1")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send("my-message-" + i);
        }

        List<Message<String>> receivedMessagesForConsumer0 = new ArrayList<>();
        List<Message<String>> receivedMessagesForConsumer1 = new ArrayList<>();

        for (int i = 0; i < messages; i++) {
            Message<String> msg = consumer0.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                receivedMessagesForConsumer0.add(msg);
            } else {
                break;
            }
        }

        for (int i = 0; i < messages; i++) {
            Message<String> msg = consumer1.receive(1, TimeUnit.SECONDS);
            if (msg != null) {
                receivedMessagesForConsumer1.add(msg);
            } else {
                break;
            }        }

        Assert.assertEquals(receivedMessagesForConsumer0.size() + receivedMessagesForConsumer1.size(), messages);

        consumer0.close();

        for (int i = 0; i < receivedMessagesForConsumer0.size(); i++) {
            Assert.assertEquals(consumer1.receive().getRedeliveryCount(), 1);
        }

    }
}
