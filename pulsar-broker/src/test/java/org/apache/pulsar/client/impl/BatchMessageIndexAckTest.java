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

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BatchMessageIndexAckTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAcknowledgmentAtBatchIndexLevelEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testBatchMessageIndexAckForSharedSubscription() throws Exception {
        final String topic = "testBatchMessageIndexAckForSharedSubscription";
        final String subscriptionName = "sub";

        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName(subscriptionName)
            .receiverQueueSize(100)
            .subscriptionType(SubscriptionType.Shared)
            .enableBatchIndexAcknowledgment(true)
            .negativeAckRedeliveryDelay(2, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)
            .create();

        final int messages = 100;
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        List<MessageId> acked = new ArrayList<>(50);
        for (int i = 0; i < messages; i++) {
            Message<Integer> msg = consumer.receive();
            if (i % 2 == 0) {
                consumer.acknowledge(msg);
                acked.add(msg.getMessageId());
            } else {
                consumer.negativeAcknowledge(consumer.receive());
            }
        }

        List<MessageId> received = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive().getMessageId());
        }

        Assert.assertEquals(received.size(), 50);
        acked.retainAll(received);
        Assert.assertEquals(acked.size(), 0);

        for (MessageId messageId : received) {
            consumer.acknowledge(messageId);
        }

        Thread.sleep(1000);

        consumer.redeliverUnacknowledgedMessages();

        Message<Integer> moreMessage = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(moreMessage);

        // check the mark delete position was changed
        BatchMessageIdImpl ackedMessageId = (BatchMessageIdImpl) received.get(0);
        PersistentTopicInternalStats stats = admin.topics().getInternalStats(topic);
        String markDeletePosition = stats.cursors.get(subscriptionName).markDeletePosition;
        Assert.assertEquals(ackedMessageId.ledgerId + ":" + ackedMessageId.entryId, markDeletePosition);

        futures.clear();
        for (int i = 0; i < 50; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive().getMessageId());
        }

        // Ensure the flow permit is work well since the client skip the acked batch index,
        // broker also need to handle the available permits.
        Assert.assertEquals(received.size(), 100);
    }

    @Test
    public void testBatchMessageIndexAckForExclusiveSubscription() throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "testBatchMessageIndexAckForExclusiveSubscription";

        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName("sub")
            .receiverQueueSize(100)
            .enableBatchIndexAcknowledgment(true)
            .subscribe();

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
            .topic(topic)
            .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)
            .create();

        final int messages = 100;
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        for (int i = 0; i < messages; i++) {
            if (i == 49) {
                consumer.acknowledgeCumulative(consumer.receive());
            } else {
                consumer.receive();
            }
        }

        //Wait ack send.
        Thread.sleep(1000);
        consumer.close();
        consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName("sub")
            .receiverQueueSize(100)
            .subscribe();

        List<Message<Integer>> received = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive());
        }

        Assert.assertEquals(received.size(), 50);

        Message<Integer> moreMessage = consumer.receive(1, TimeUnit.SECONDS);
        Assert.assertNull(moreMessage);

        futures.clear();
        for (int i = 0; i < 50; i++) {
            futures.add(producer.sendAsync(i));
        }
        FutureUtil.waitForAll(futures).get();

        for (int i = 0; i < 50; i++) {
            received.add(consumer.receive());
        }

        // Ensure the flow permit is work well since the client skip the acked batch index,
        // broker also need to handle the available permits.
        Assert.assertEquals(received.size(), 100);
    }

    @Test
    public void testDoNotRecycleAckSetMultipleTimes() throws Exception  {
        final String topic = "persistent://my-property/my-ns/testSafeAckSetRecycle";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .batchingMaxMessages(10)
                .blockIfQueueFull(true).topic(topic)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .acknowledgmentGroupTime(1, TimeUnit.MILLISECONDS)
                .topic(topic)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionName("test")
                .subscribe();

        final int messages = 100;
        for (int i = 0; i < messages; i++) {
            producer.sendAsync("Hello Pulsar".getBytes());
        }

        // Should not throw an exception.
        for (int i = 0; i < messages; i++) {
            consumer.acknowledgeCumulative(consumer.receive());
            // make sure the group ack flushed.
            Thread.sleep(2);
        }

        producer.close();
        consumer.close();
    }
}
