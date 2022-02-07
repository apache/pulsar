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

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Test(groups = "broker-impl")
public class DispatchAccordingPermitsTests extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * The test case is to simulate dispatch batches with different batch size to the consumer.
     * 1. The consumer has 1000 available permits
     * 2. The producer send batches with different batch size
     *
     * According the batch average size dispatching, the broker will dispatch all the batches to the consumer
     */
    @Test
    public void testFlowPermitsWithMultiBatchesDispatch() throws PulsarAdminException, PulsarClientException {
        final String topic = "persistent://public/default/testFlowPermitsWithMultiBatchesDispatch";
        final String subName = "test";
        admin.topics().createSubscription(topic, "test", MessageId.earliest);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .batchingMaxPublishDelay(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                .create();

        for (int i = 0; i < 100; i++) {
            producer.sendAsync("msg - " + i);
        }
        producer.flush();

        for (int i = 0; i < 350; i++) {
            producer.sendAsync("msg - " + i);
        }
        producer.flush();

        for (int i = 0; i < 50; i++) {
            producer.sendAsync("msg - " + i);
            producer.flush();
        }

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        for (int i = 0; i < 500; i++) {
            consumer.acknowledge(consumer.receive());
        }

        ConsumerImpl<String> consumerImpl = (ConsumerImpl<String>) consumer;
        Assert.assertEquals(consumerImpl.incomingMessages.size(), 0);

        TopicStats stats = admin.topics().getStats(topic);
        Assert.assertTrue(stats.getSubscriptions().get(subName).getConsumers().get(0).getAvailablePermits() > 0);
    }
}
