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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class BatchedProducerConsumerTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
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

    @Override
    protected void doInitConf() {
        conf.setAcknowledgmentAtBatchIndexLevelEnabled(true);
    }

    @Test
    public void testNegativeAckAndLongAckDelayWillNotLeadRepeatConsume() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp_");
        final String subscriptionName = "s1";
        final int redeliveryDelaySeconds = 2;

        // Create producer and consumer.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxMessages(1000)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .create();
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .negativeAckRedeliveryDelay(redeliveryDelaySeconds, TimeUnit.SECONDS)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .acknowledgmentGroupTime(1, TimeUnit.HOURS)
                .subscribe();

        // Send 10 messages in batch.
        ArrayList<String> messagesSent = new ArrayList<>();
        List<CompletableFuture<MessageId>> sendTasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String msg = Integer.valueOf(i).toString();
            sendTasks.add(producer.sendAsync(Integer.valueOf(i).toString()));
            messagesSent.add(msg);
        }
        producer.flush();
        FutureUtil.waitForAll(sendTasks);

        // Receive messages.
        ArrayList<String> messagesReceived = new ArrayList<>();
        // NegativeAck "batchMessageIdIndex1" once.
        boolean index1HasBeenNegativeAcked = false;
        while (true) {
            Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            if (index1HasBeenNegativeAcked) {
                messagesReceived.add(message.getValue());
                consumer.acknowledge(message);
                continue;
            }
            if (message.getMessageId() instanceof BatchMessageIdImpl batchMessageId) {
                if (batchMessageId.getBatchIndex() == 1) {
                    consumer.negativeAcknowledge(message);
                    index1HasBeenNegativeAcked = true;
                    continue;
                }
            }
            messagesReceived.add(message.getValue());
            consumer.acknowledge(message);
        }

        // Wait the message negative acknowledgment finished.
        Thread.sleep(redeliveryDelaySeconds * 3);

        // Receive negative acked messages.
        while (true) {
            Message<String> message = consumer.receive(2, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            messagesReceived.add(message.getValue());
            consumer.acknowledge(message);
        }

        log.info(messagesSent.toString());
        log.info(messagesReceived.toString());
        Assert.assertEquals(messagesReceived.size(), messagesSent.size());
    }
}
