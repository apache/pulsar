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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests batch message behavior across broker restarts.
 * This test requires stopping and starting the broker, so it cannot use SharedPulsarCluster.
 */
@Test(groups = "broker")
public class BatchMessageBrokerRestartTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "containerBuilder")
    public Object[][] containerBuilderProvider() {
        return new Object[][] {
                { BatcherBuilder.DEFAULT },
                { BatcherBuilder.KEY_BASED }
        };
    }

    @Test(dataProvider = "containerBuilder")
    public void testSimpleBatchProducerWithStoppingAndStartingBroker(BatcherBuilder builder) throws Exception {
        // Send enough messages to trigger one batch by size and then have a remaining message in the batch container
        int numMsgs = 3;
        int numMsgsInBatch = 2;
        final String topicName = "persistent://prop/ns-abc/testBatchBrokerRestart-" + System.nanoTime();
        final String subscriptionName = "syncsub-1";

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .subscribe();
        consumer.close();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(numMsgsInBatch)
                .enableBatching(true)
                .batcherBuilder(builder)
                .create();

        stopBroker();

        List<CompletableFuture<MessageId>> messages = new ArrayList<>();
        for (int i = 0; i < numMsgs; i++) {
            byte[] message = ("my-message-" + i).getBytes();
            messages.add(producer.sendAsync(message));
        }

        startBroker();

        // Fail if any one message fails to get acknowledged
        FutureUtil.waitForAll(messages).get(30, TimeUnit.SECONDS);

        Awaitility.await().timeout(30, TimeUnit.SECONDS)
                .until(() -> pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        rolloverPerIntervalStats();
        assertEquals(topic.getSubscription(subscriptionName).getNumberOfEntriesInBacklog(false), 2);
        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();

        for (int i = 0; i < numMsgs; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            Assert.assertEquals(receivedMessage, expectedMessage,
                    "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);
        }
        consumer.close();
        producer.close();
    }
}
