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
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Test(groups = "broker-impl")
public class BatchMessageIndexAckDisableTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAcknowledgmentAtBatchIndexLevelEnabled(false);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "ackReceiptEnabled")
    public Object[][] ackReceiptEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testBatchMessageIndexAckForSharedSubscription(boolean ackReceiptEnabled) throws
            PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "testBatchMessageIndexAckForSharedSubscription";

        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName("sub")
            .receiverQueueSize(100)
            .subscriptionType(SubscriptionType.Shared)
            .isAckReceiptEnabled(ackReceiptEnabled)
            .ackTimeout(1, TimeUnit.SECONDS)
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
            if (i % 2 == 0) {
                consumer.acknowledge(consumer.receive());
            }
        }

        List<Message<Integer>> received = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            received.add(consumer.receive());
        }

        Assert.assertEquals(received.size(), 100);
    }

    @Test(dataProvider = "ackReceiptEnabled")
    public void testBatchMessageIndexAckForExclusiveSubscription(boolean ackReceiptEnabled) throws
            PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "testBatchMessageIndexAckForExclusiveSubscription";

        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
            .topic(topic)
            .subscriptionName("sub")
            .receiverQueueSize(100)
            .isAckReceiptEnabled(ackReceiptEnabled)
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

        List<Message<Integer>> received = new ArrayList<>(100);
        for (int i = 0; i < messages; i++) {
            received.add(consumer.receive());
        }

        Assert.assertEquals(received.size(), 100);
    }
}
