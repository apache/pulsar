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
import org.apache.commons.lang3.RandomUtils;
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
            CompletableFuture<MessageId> handle =
                    p.sendAsync(("test-" + i).getBytes());
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
            while (count-- > 0) {
                Message m = consumer.receive();
                consumer.acknowledge(m);
            }

            verifyBacklog(topic, subName, 0, 0);
        }

    }

    private void verifyBacklog(String topic, String subscription, int numEntries, int numMessages) throws Exception {
        AnalyzeSubscriptionBacklogResult analyzeSubscriptionBacklogResult =
                admin.topics().analyzeSubscriptionBacklog(topic, subscription, Optional.empty());

        assertEquals(analyzeSubscriptionBacklogResult.getEntries(), numEntries);
        assertEquals(analyzeSubscriptionBacklogResult.getFilterAcceptedEntries(), numEntries);
        assertEquals(analyzeSubscriptionBacklogResult.getFilterRejectedEntries(), 0);
        assertEquals(analyzeSubscriptionBacklogResult.getFilterRescheduledEntries(), 0);
        assertEquals(analyzeSubscriptionBacklogResult.getFilterRescheduledEntries(), 0);

        assertEquals(analyzeSubscriptionBacklogResult.getMessages(), numMessages);
        assertEquals(analyzeSubscriptionBacklogResult.getMarkerMessages(), 0);
        assertEquals(analyzeSubscriptionBacklogResult.getFilterAcceptedMessages(), numMessages);
        assertEquals(analyzeSubscriptionBacklogResult.getFilterRejectedMessages(), 0);

        assertEquals(analyzeSubscriptionBacklogResult.getFilterRescheduledMessages(), 0);
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
        AnalyzeSubscriptionBacklogResult analyzeSubscriptionBacklogResult =
                admin.topics().analyzeSubscriptionBacklog(topic + "-partition-0", "sub-1", Optional.empty());
        assertEquals(0, analyzeSubscriptionBacklogResult.getEntries());
    }

    @Test
    public void analyzeBacklogUsingClientSideLooping() throws Exception {
        int serverSubscriptionBacklogScanMaxEntries = 10;
        conf.setSubscriptionBacklogScanMaxEntries(serverSubscriptionBacklogScanMaxEntries);

        String topic = "persistent://my-property/my-ns/analyze-backlog-using-client-side-looping-topic";
        String subName = "sub-1";
        admin.topics().createSubscription(topic, subName, MessageId.latest);

        assertEquals(admin.topics().getSubscriptions(topic), List.of("sub-1"));
        verifyBacklog(topic, subName, 0, 0);

        @Cleanup Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();

        // Test server returns false aborted flag.
        int firstSendMessages = 5;
        List<CompletableFuture<MessageId>> firstSendFutures = new ArrayList<>();
        for (int i = 0; i < firstSendMessages; i++) {
            CompletableFuture<MessageId> future = producer.sendAsync(("first-" + i).getBytes());
            firstSendFutures.add(future);
        }
        FutureUtil.waitForAll(firstSendFutures).get();

        AnalyzeSubscriptionBacklogResult backlogResult =
                admin.topics().analyzeSubscriptionBacklog(topic, subName, Optional.empty(), 15);

        assertEquals(backlogResult.getEntries(), firstSendMessages);
        assertEquals(backlogResult.getMessages(), firstSendMessages);

        // Test backlogScanMaxEntries(client side) <= subscriptionBacklogScanMaxEntries(server side), but server returns
        // true aborted flag.
        int secondSendMessages = 10;
        List<CompletableFuture<MessageId>> secondSendFutures = new ArrayList<>();
        for (int i = 0; i < secondSendMessages; i++) {
            CompletableFuture<MessageId> future = producer.sendAsync(("second-" + i).getBytes());
            secondSendFutures.add(future);
        }
        FutureUtil.waitForAll(secondSendFutures).get();

        backlogResult = admin.topics().analyzeSubscriptionBacklog(topic, subName, Optional.empty(),
                serverSubscriptionBacklogScanMaxEntries - 1);

        assertEquals(backlogResult.getEntries(), serverSubscriptionBacklogScanMaxEntries);
        assertEquals(backlogResult.getMessages(), serverSubscriptionBacklogScanMaxEntries);

        // Test client side loop: backlogScanMaxEntries > subscriptionBacklogScanMaxEntries, the loop termination
        // condition is that server returns false aborted flag.
        int thirdSendMessages = 10;
        List<CompletableFuture<MessageId>> thirdFutures = new ArrayList<>();
        for (int i = 0; i < thirdSendMessages; i++) {
            CompletableFuture<MessageId> future = producer.sendAsync(("third-" + i).getBytes());
            thirdFutures.add(future);
        }
        FutureUtil.waitForAll(thirdFutures).get();

        int thirdTotalMessages = firstSendMessages + secondSendMessages + thirdSendMessages;
        backlogResult =
                admin.topics().analyzeSubscriptionBacklog(topic, subName, Optional.empty(), thirdTotalMessages + 1);

        assertEquals(backlogResult.getEntries(), thirdTotalMessages);
        assertEquals(backlogResult.getMessages(), thirdTotalMessages);

        // Test client side loop: backlogScanMaxEntries > subscriptionBacklogScanMaxEntries, the loop termination
        // condition is that total entries exceeds backlogScanMaxEntries.
        int fourthSendMessages = 10;
        List<CompletableFuture<MessageId>> fourthFutures = new ArrayList<>();
        for (int i = 0; i < fourthSendMessages; i++) {
            CompletableFuture<MessageId> future = producer.sendAsync(("fourth-" + i).getBytes());
            fourthFutures.add(future);
        }
        FutureUtil.waitForAll(fourthFutures).get();

        backlogResult = admin.topics().analyzeSubscriptionBacklog(topic, subName, Optional.empty(), thirdTotalMessages);

        int fourthExpectedMessages = (thirdTotalMessages / serverSubscriptionBacklogScanMaxEntries)
                + serverSubscriptionBacklogScanMaxEntries;
        assertEquals(backlogResult.getEntries(), fourthExpectedMessages);
        assertEquals(backlogResult.getMessages(), fourthExpectedMessages);

        // Test client side loop with topic unload.
        admin.topics().unload(topic);
        int fifthSendMessages = 10;
        List<CompletableFuture<MessageId>> fifthFutures = new ArrayList<>();
        for (int i = 0; i < fifthSendMessages; i++) {
            CompletableFuture<MessageId> future = producer.sendAsync(("fifth-" + i).getBytes());
            fifthFutures.add(future);
            if (RandomUtils.secure().randomBoolean()) {
                admin.topics().unload(topic);
            }
        }
        FutureUtil.waitForAll(fifthFutures).get();

        int fifthTotalMessages = thirdTotalMessages + fourthSendMessages + fifthSendMessages;
        backlogResult = admin.topics().analyzeSubscriptionBacklog(topic, subName, Optional.empty(), fifthTotalMessages);

        assertEquals(backlogResult.getEntries(), fifthTotalMessages);
        assertEquals(backlogResult.getMessages(), fifthTotalMessages);
    }
}
