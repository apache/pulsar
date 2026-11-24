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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.testinterceptor.BrokerTestInterceptor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumersReadLimitsTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // start at max batch size to reproduce the issue more easily
        conf.setDispatcherMinReadBatchSize(conf.getDispatcherMaxReadBatchSize());
        BrokerTestInterceptor.INSTANCE.configure(conf);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    protected void resetInterceptors() throws Exception {
        BrokerTestInterceptor.INSTANCE.reset();
    }

    @DataProvider
    public Object[][] subscriptionTypesAndFlowControl() {
        return new Object[][] {
                {SubscriptionType.Shared, false},
                {SubscriptionType.Shared, true},
                {SubscriptionType.Key_Shared, false},
                {SubscriptionType.Key_Shared, true},
                {SubscriptionType.Exclusive, false},
                {SubscriptionType.Exclusive, true},
                {SubscriptionType.Failover, false},
                {SubscriptionType.Failover, true}
        };
    }

    @Test(dataProvider = "subscriptionTypesAndFlowControl", timeOut = 60_000)
    public void testDynamicMaxReadBatchSizeDecrease(SubscriptionType subscriptionType, boolean preciseFlowControl)
            throws Exception {
        String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/dynamic-read-batch-size");
        String subscription = "sub";
        int initialBatchSize = 500;
        int reducedBatchSize = 100;
        boolean originalPreciseFlowControl = conf.isPreciseDispatcherFlowControl();
        BlockingQueue<Integer> requestedReadSizes = new LinkedBlockingQueue<>();
        BrokerTestInterceptor.INSTANCE.applyCursorSpyDecorator(cursor -> {
            if (subscription.equals(cursor.getName())) {
                doAnswer(invocation -> {
                    requestedReadSizes.add(invocation.getArgument(0));
                    return invocation.callRealMethod();
                }).when(cursor).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());
            }
        });
        conf.setPreciseDispatcherFlowControl(preciseFlowControl);
        try {
            admin.brokers().updateDynamicConfiguration("dispatcherMaxReadBatchSize", "500");
            Awaitility.await().untilAsserted(() ->
                    assertThat(conf.getDispatcherMaxReadBatchSize()).isEqualTo(initialBatchSize));
            // The broker snapshots precise flow control when the connection is created.
            @Cleanup
            PulsarClient client = newPulsarClient(pulsar.getBrokerServiceUrl(), 0);
            @Cleanup
            Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subscription)
                    .subscriptionType(subscriptionType)
                    .startPaused(true)
                    .receiverQueueSize(initialBatchSize)
                    .subscribe();
            int numberOfMessages = initialBatchSize * 3;
            @Cleanup
            Producer<byte[]> producer = client.newProducer().topic(topicName).enableBatching(false)
                    .maxPendingMessages(numberOfMessages).create();
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int i = 0; i < numberOfMessages; i++) {
                sends.add(producer.newMessage().key("key").value(new byte[] {1}).sendAsync());
            }
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName)
                    .orElseThrow();
            Dispatcher dispatcher = topic.getSubscription(subscription).getDispatcher();
            assertThat(dispatcher.getConsumers().get(0).isPreciseDispatcherFlowControl())
                    .isEqualTo(preciseFlowControl);
            consumer.resume();
            assertThat(requestedReadSizes.poll(10, TimeUnit.SECONDS)).isEqualTo(initialBatchSize);
            // Exhaust the initial permits without consuming, so no read is in flight when the config changes.
            Awaitility.await().untilAsserted(() -> {
                assertThat(((ConsumerImpl<?>) consumer).numMessagesInQueue()).isEqualTo(initialBatchSize);
                assertThat(dispatcher.getConsumers().get(0).getAvailablePermits()).isZero();
            });
            assertThat(requestedReadSizes).isEmpty();

            admin.brokers().updateDynamicConfiguration("dispatcherMaxReadBatchSize", "100");
            Awaitility.await().untilAsserted(() ->
                    assertThat(conf.getDispatcherMaxReadBatchSize()).isEqualTo(reducedBatchSize));
            for (int i = 0; i < numberOfMessages; i++) {
                Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                consumer.acknowledge(message);
            }
            assertThat(topic.getSubscription(subscription).getDispatcher()).isSameAs(dispatcher);
            assertThat(requestedReadSizes).isNotEmpty().allSatisfy(size ->
                    assertThat(size).as("cursor read after lowering the dynamic maximum")
                            .isLessThanOrEqualTo(reducedBatchSize));
        } finally {
            conf.setPreciseDispatcherFlowControl(originalPreciseFlowControl);
            admin.brokers().deleteDynamicConfiguration("dispatcherMaxReadBatchSize");
            Awaitility.await().untilAsserted(() ->
                    assertThat(conf.getDispatcherMaxReadBatchSize()).isEqualTo(initialBatchSize));
        }
    }

    @SuppressWarnings("deprecation")
    @Test(timeOut = 30 * 1000)
    public void testDispatcherMaxReadSizeBytes() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName(
                "persistent://public/default/testDispatcherMaxReadSizeBytes");
        final String subscription = "sub";

        AtomicInteger entriesReadMax = new AtomicInteger(0);
        BrokerTestInterceptor.INSTANCE.applyDispatcherSpyDecorator(PersistentDispatcherMultipleConsumers.class,
                spy -> {
                    doAnswer(invocation -> {
                        List<Entry> entries = invocation.getArgument(0);
                        PersistentDispatcherMultipleConsumers.ReadType readType = invocation.getArgument(1);
                        int numberOfEntries = entries.size();
                        log.info().attr("readentriescompleteWith", numberOfEntries).attr("readType", readType)
                                .log("intercepted readEntriesComplete with entries, read type");
                        entriesReadMax.updateAndGet(current -> Math.max(current, numberOfEntries));
                        return invocation.callRealMethod();
                    }).when(spy).readEntriesComplete(any(), any());
                }
        );

        // Create two consumers on a shared subscription
        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .consumerName("c1")
                .topic(topicName)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(10000)
                .subscribe();

        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .consumerName("c2")
                .topic(topicName)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared)
                .startPaused(true)
                .receiverQueueSize(10000)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer =
                pulsarClient.newProducer().enableBatching(false).topic(topicName).create();
        int numberOfMessages = 200;
        int payLoadSizeBytes = 1025 * 1024; // 1025kB
        byte[] payload = RandomUtils.nextBytes(payLoadSizeBytes);
        for (int i = 0; i < numberOfMessages; i++) {
            producer.send(payload);
        }

        // Consume messages with consumer1 but don't ack
        for (int i = 0; i < numberOfMessages; i++) {
            consumer1.receive();
        }

        // Close consumer1 and resume consumer2 to replay the messages
        consumer1.close();
        consumer2.resume();

        // Verify that consumer2 can receive the messages
        for (int i = 0; i < numberOfMessages; i++) {
            Message<byte[]> msg = consumer2.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg, "Consumer2 should receive the message");
            consumer2.acknowledge(msg);
        }

        int expectedMaxEntriesInRead = conf.getDispatcherMaxReadSizeBytes() / payLoadSizeBytes;
        assertThat(entriesReadMax.get()).isLessThanOrEqualTo(expectedMaxEntriesInRead);
    }
}
