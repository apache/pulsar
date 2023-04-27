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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.stats.AnalyzeSubscriptionBacklogResult;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class AnalyzeBacklogSubscriptionTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void simpleAnalyzeBacklogTest() throws Exception {
        simpleAnalyzeBacklogTest(false);
    }

    @Test
    public void simpleAnalyzeBacklogTestWithBatching() throws Exception {
        simpleAnalyzeBacklogTest(true);
    }

    private void simpleAnalyzeBacklogTest(boolean batching) throws Exception {
        int numMessages = 20;
        int batchSize = batching ? 5 : 1;
        int numEntries = numMessages / batchSize;

        String topic = "persistent://my-property/my-ns/my-topic-" + batching;
        String subName = "sub-1";
        admin.topics().createSubscription(topic, subName, MessageId.latest);

        assertEquals(admin.topics().getSubscriptions(topic), List.of("sub-1"));

        verifyBacklog(topic, subName, 0, 0);

        @Cleanup
        Producer<byte[]> p = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(batching)
                .batchingMaxMessages(batchSize)
                .batchingMaxPublishDelay(Integer.MAX_VALUE, TimeUnit.SECONDS)
                .create();

        List<CompletableFuture<MessageId>> handles = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            CompletableFuture<MessageId> handle
                    = p.sendAsync(("test-" + i).getBytes());
            handles.add(handle);
        }
        FutureUtil.waitForAll(handles).get();

        MessageId middleMessageId = handles.get(numMessages / 2).get();

        verifyBacklog(topic, subName, numEntries, numMessages);

        // create a second subscription
        admin.topics().createSubscription(topic, "from-middle", middleMessageId);

        verifyBacklog(topic, "from-middle", numEntries / 2, numMessages / 2);


        try (Consumer consumer = pulsarClient
                .newConsumer()
                .topic(topic)
                // we want to wait for the server to process acks, in order to not have a flaky test
                .isAckReceiptEnabled(true)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe()) {
            Message receive1 = consumer.receive();
            Message receive2 = consumer.receive();
            Message receive3 = consumer.receive();
            Message receive4 = consumer.receive();
            Message receive5 = consumer.receive();

            verifyBacklog(topic, subName, numEntries, numMessages);

            consumer.acknowledge(receive2);

            // one individually deleted message
            if (batching) {
                // acknowledging a single message in a entry is not enough
                // to count -1 for the backlog
                verifyBacklog(topic, subName, numEntries, numMessages);
            } else {
                verifyBacklog(topic, subName, numEntries - 1, numMessages - 1);
            }

            consumer.acknowledge(receive1);
            consumer.acknowledge(receive3);
            consumer.acknowledge(receive4);
            consumer.acknowledge(receive5);

            verifyBacklog(topic, subName, numEntries - (5 / batchSize), numMessages - 5);

            int count = numMessages - 5;
            while (count -- > 0) {
                Message m = consumer.receive();
                consumer.acknowledge(m);
            }

            verifyBacklog(topic, subName, 0,0);
        }

    }

    private void verifyBacklog(String topic, String subscription, int numEntries, int numMessages) throws Exception {
        AnalyzeSubscriptionBacklogResult analyzeSubscriptionBacklogResult
                = admin.topics().analyzeSubscriptionBacklog(topic, subscription, Optional.empty());

        assertEquals(numEntries, analyzeSubscriptionBacklogResult.getEntries());
        assertEquals(numEntries, analyzeSubscriptionBacklogResult.getFilterAcceptedEntries());
        assertEquals(0, analyzeSubscriptionBacklogResult.getFilterRejectedEntries());
        assertEquals(0, analyzeSubscriptionBacklogResult.getFilterRescheduledEntries());
        assertEquals(0, analyzeSubscriptionBacklogResult.getFilterRescheduledEntries());

        assertEquals(numMessages, analyzeSubscriptionBacklogResult.getMessages());
        assertEquals(numMessages, analyzeSubscriptionBacklogResult.getFilterAcceptedMessages());
        assertEquals(0, analyzeSubscriptionBacklogResult.getFilterRejectedMessages());

        assertEquals(0, analyzeSubscriptionBacklogResult.getFilterRescheduledMessages());
        assertFalse(analyzeSubscriptionBacklogResult.isAborted());
    }


    @Test
    public void partitionedTopicNotAllowed() throws Exception {
        String topic = "persistent://my-property/my-ns/my-partitioned-topic";
        String subName = "sub-1";
        admin.topics().createPartitionedTopic(topic, 2);
        admin.topics().createSubscription(topic, subName, MessageId.latest);
        assertEquals(admin.topics().getSubscriptions(topic), List.of("sub-1"));

        // you cannot use this feature on a partitioned topic
        assertThrows(PulsarAdminException.NotAllowedException.class, () -> {
            admin.topics().analyzeSubscriptionBacklog(topic, "sub-1", Optional.empty());
        });

        // you can access single partitions
        AnalyzeSubscriptionBacklogResult analyzeSubscriptionBacklogResult
                = admin.topics().analyzeSubscriptionBacklog(topic + "-partition-0", "sub-1", Optional.empty());
        assertEquals(0, analyzeSubscriptionBacklogResult.getEntries());
    }

}
