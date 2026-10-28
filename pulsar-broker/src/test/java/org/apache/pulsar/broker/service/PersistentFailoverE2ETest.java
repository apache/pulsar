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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentFailoverE2ETest extends BrokerTestBase {

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

    private static final int CONSUMER_ADD_OR_REMOVE_WAIT_TIME = 100;

    private static class TestConsumerStateEventListener implements ConsumerEventListener {

        final LinkedBlockingQueue<Integer> activeQueue = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<Integer> inActiveQueue = new LinkedBlockingQueue<>();
        String name = "";

        public TestConsumerStateEventListener(String name) {
            this.name = name;
        }

        @Override
        public void becameActive(Consumer<?> consumer, int partitionId) {
            try {
                activeQueue.put(partitionId);
            } catch (InterruptedException e) {
            }
        }

        @Override
        public void becameInactive(Consumer<?> consumer, int partitionId) {
            try {
                inActiveQueue.put(partitionId);
            } catch (InterruptedException e) {
            }
        }
    }

    private void verifyConsumerNotReceiveAnyStateChanges(TestConsumerStateEventListener listener) {
        assertNull(listener.activeQueue.poll());
        assertNull(listener.inActiveQueue.poll());
    }

    private void verifyConsumerActive(TestConsumerStateEventListener listener, int partitionId) throws Exception {
        Integer pid = listener.activeQueue.take();
        assertNotNull(pid);
        assertEquals(partitionId, pid.intValue());
    }

    private void verifyConsumerInactive(TestConsumerStateEventListener listener, int partitionId) throws Exception {
        Integer pid = listener.inActiveQueue.take();
        assertNotNull(pid);
        assertEquals(partitionId, pid.intValue());
    }

    private void clearEventQueue(TestConsumerStateEventListener listener) {
        listener.inActiveQueue.clear();
        listener.activeQueue.clear();
    }

    private static class ActiveInactiveListenerEvent implements ConsumerEventListener {

        private final Set<Integer> activePtns = new HashSet<>();
        private final Set<Integer> inactivePtns = new HashSet<>();

        @Override
        public synchronized void becameActive(Consumer<?> consumer, int partitionId) {
            activePtns.add(partitionId);
            inactivePtns.remove(partitionId);
        }

        @Override
        public synchronized void becameInactive(Consumer<?> consumer, int partitionId) {
            activePtns.remove(partitionId);
            inactivePtns.add(partitionId);
        }
    }

    @AllArgsConstructor
    static class FailoverConsumer {
        private String consumerName;
        private Consumer<byte[]> consumer;
        private TestConsumerStateEventListener listener;
        private PersistentDispatcherSingleActiveConsumer dispatcher;
        private boolean isActiveConsumer(){
            return dispatcher.getActiveConsumer().consumerName().equals(consumerName);
        }
    }

    FailoverConsumer createConsumer(String topicName, String subName, String listenerName, String consumerName)
            throws Exception {
        TestConsumerStateEventListener listener = new TestConsumerStateEventListener(listenerName);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Failover)
                .consumerName(consumerName)
                .consumerEventListener(listener)
                .subscribe();
        PersistentDispatcherSingleActiveConsumer dispatcher =
                (PersistentDispatcherSingleActiveConsumer) pulsar.getBrokerService()
                .getTopic(topicName, false).get().get()
                .getSubscription(subName)
                .getDispatcher();
        return new FailoverConsumer(consumerName, consumer, listener, dispatcher);
    }

    @Test
    public void testSimpleConsumerEventsWithoutPartition() throws Exception {
        final String topicName = "persistent://prop/ns-abc/failover-topic1-" + System.currentTimeMillis();
        final String subName = "sub1";
        final int numMsgs = 100;

        // 1. Registry two consumers.
        FailoverConsumer failoverConsumer1 = createConsumer(topicName, subName, "l1", "c1");
        FailoverConsumer failoverConsumer2 = createConsumer(topicName, subName, "l2", "c2");
        FailoverConsumer firstConsumer;
        FailoverConsumer secondConsumer;
        if (failoverConsumer1.isActiveConsumer()){
            firstConsumer = failoverConsumer1;
            secondConsumer = failoverConsumer2;
        } else {
            firstConsumer = failoverConsumer2;
            secondConsumer = failoverConsumer1;
        }

        verifyConsumerActive(firstConsumer.listener, -1);
        verifyConsumerInactive(secondConsumer.listener, -1);
        clearEventQueue(firstConsumer.listener);
        clearEventQueue(secondConsumer.listener);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        assertNotNull(topicRef);
        assertNotNull(subRef);

        // 2. validate basic dispatcher state
        assertTrue(subRef.getDispatcher().isConsumerConnected());
        assertEquals(subRef.getDispatcher().getType(), SubType.Failover);

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(numMsgs);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        rolloverPerIntervalStats();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(subRef.getNumberOfEntriesInBacklog(false), numMsgs);
        });

        // 3. firstConsumer should have all the messages while secondConsumer should have no messages
        Message<byte[]> msg = null;
        Assert.assertNull(secondConsumer.consumer.receive(100, TimeUnit.MILLISECONDS));
        for (int i = 0; i < numMsgs; i++) {
            msg = firstConsumer.consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            firstConsumer.consumer.acknowledge(msg);
        }

        rolloverPerIntervalStats();

        // 4. messages deleted on individual acks
        Awaitility.await().untilAsserted(() -> {
            assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);
        });

        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        // 5. master consumer failure should resend unacked messages and new messages to another consumer
        for (int i = 0; i < 5; i++) {
            msg = firstConsumer.consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            firstConsumer.consumer.acknowledge(msg);
        }
        for (int i = 5; i < 10; i++) {
            msg = firstConsumer.consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            // do not ack
        }
        firstConsumer.consumer.close();

        Awaitility.await().untilAsserted(() -> {
            verifyConsumerActive(secondConsumer.listener, -1);
            verifyConsumerNotReceiveAnyStateChanges(firstConsumer.listener);
            clearEventQueue(firstConsumer.listener);
            clearEventQueue(secondConsumer.listener);
        });

        for (int i = 5; i < numMsgs; i++) {
            msg = secondConsumer.consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            secondConsumer.consumer.acknowledge(msg);
        }
        Assert.assertNull(secondConsumer.consumer.receive(100, TimeUnit.MILLISECONDS));

        rolloverPerIntervalStats();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);
        });

        // 8. unsubscribe not allowed if multiple consumers connected
        try {
            firstConsumer.consumer.unsubscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            // ok
        }

        // 9. unsubscribe allowed if there is alone consumer
        firstConsumer.consumer.close();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        try {
            secondConsumer.consumer.unsubscribe();
        } catch (PulsarClientException e) {
            fail("Should not fail", e);
        }

        Awaitility.await().untilAsserted(() -> {
            assertNull(topicRef.getSubscription(subName));
        });

        producer.close();
        secondConsumer.consumer.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testSimpleConsumerEventsWithPartition() throws Exception {
        // Resetting ActiveConsumerFailoverDelayTimeMillis else if testActiveConsumerFailoverWithDelay get executed
        // first could cause this test to fail.
        conf.setActiveConsumerFailoverDelayTimeMillis(0);
        restartBroker();

        int numPartitions = 4;

        final String topicName = BrokerTestUtil.newUniqueName(
                "persistent://prop/ns-abc/testSimpleConsumerEventsWithPartition");
        final TopicName destName = TopicName.get(topicName);
        final String subName = "sub1";
        final int numMsgs = 100;
        Set<String> uniqueMessages = new HashSet<>();
        admin.topics().createPartitionedTopic(topicName, numPartitions);

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover);

        // 1. two consumers on the same subscription
        ActiveInactiveListenerEvent listener1 = new ActiveInactiveListenerEvent();
        ActiveInactiveListenerEvent listener2 = new ActiveInactiveListenerEvent();

        Consumer<byte[]> consumer1 = consumerBuilder.clone()
                .consumerName("1").consumerEventListener(listener1)
                .receiverQueueSize(1)
                .subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.clone()
                .consumerName("2").consumerEventListener(listener2)
                .receiverQueueSize(1)
                .subscribe();

        PersistentTopic topicRef;
        topicRef = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(destName.getPartition(0).toString()).get();
        PersistentDispatcherSingleActiveConsumer disp0 = (PersistentDispatcherSingleActiveConsumer) topicRef
                .getSubscription(subName).getDispatcher();
        topicRef = (PersistentTopic) pulsar.getBrokerService().
                getTopicReference(destName.getPartition(1).toString()).get();
        PersistentDispatcherSingleActiveConsumer disp1 = (PersistentDispatcherSingleActiveConsumer) topicRef
                .getSubscription(subName).getDispatcher();
        topicRef = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(destName.getPartition(2).toString()).get();
        PersistentDispatcherSingleActiveConsumer disp2 = (PersistentDispatcherSingleActiveConsumer) topicRef
                .getSubscription(subName).getDispatcher();
        topicRef = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(destName.getPartition(3).toString()).get();
        PersistentDispatcherSingleActiveConsumer disp3 = (PersistentDispatcherSingleActiveConsumer) topicRef
                .getSubscription(subName).getDispatcher();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition).create();
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }
        producer.flush();

        // equal distribution between both consumers
        int totalMessages = 0;
        Message<byte[]> msg = null;
        Set<Integer> receivedPtns = new HashSet<>();
        while (true) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            totalMessages++;
            consumer1.acknowledge(msg);
            receivedPtns.add(((MessageIdAdv) msg.getMessageId()).getPartitionIndex());
        }

        assertTrue(Sets.difference(listener1.activePtns, receivedPtns).isEmpty());
        assertTrue(Sets.difference(listener2.inactivePtns, receivedPtns).isEmpty());

        Assert.assertEquals(totalMessages, numMsgs / 2);

        receivedPtns = new HashSet<>();
        while (true) {
            msg = consumer2.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            totalMessages++;
            consumer2.acknowledge(msg);
            receivedPtns.add(((MessageIdAdv) msg.getMessageId()).getPartitionIndex());
        }
        assertTrue(Sets.difference(listener1.inactivePtns, receivedPtns).isEmpty());
        assertTrue(Sets.difference(listener2.activePtns, receivedPtns).isEmpty());

        Assert.assertEquals(totalMessages, numMsgs);
        Assert.assertEquals(disp0.getActiveConsumer().consumerName(), "1");
        Assert.assertEquals(disp1.getActiveConsumer().consumerName(), "2");
        Assert.assertEquals(disp2.getActiveConsumer().consumerName(), "1");
        Assert.assertEquals(disp3.getActiveConsumer().consumerName(), "2");
        totalMessages = 0;

        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }
        producer.flush();

        // add a consumer
        for (int i = 0; i < 20; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            uniqueMessages.add(new String(msg.getData()));
            consumer1.acknowledge(msg);
        }
        Consumer<byte[]> consumer3 = consumerBuilder.clone().consumerName("3")
                .receiverQueueSize(1)
                .subscribe();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        while (true) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            uniqueMessages.add(new String(msg.getData()));
            consumer1.acknowledge(msg);
        }
        while (true) {
            msg = consumer2.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            uniqueMessages.add(new String(msg.getData()));
            consumer2.acknowledge(msg);
        }
        while (true) {
            msg = consumer3.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            uniqueMessages.add(new String(msg.getData()));
            consumer3.acknowledge(msg);
        }

        Assert.assertEquals(uniqueMessages.size(), numMsgs);
        Assert.assertEquals(disp0.getActiveConsumer().consumerName(), "1");
        Assert.assertEquals(disp1.getActiveConsumer().consumerName(), "2");
        Assert.assertEquals(disp2.getActiveConsumer().consumerName(), "3");
        Assert.assertEquals(disp3.getActiveConsumer().consumerName(), "1");
        uniqueMessages.clear();

        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }
        producer.flush();

        // remove a consumer
        for (int i = 0; i < 10; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            uniqueMessages.add(new String(msg.getData()));
            consumer1.acknowledge(msg);
        }
        consumer1.close();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        while (true) {
            msg = consumer2.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            uniqueMessages.add(new String(msg.getData()));
            consumer2.acknowledge(msg);
        }
        while (true) {
            msg = consumer3.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            uniqueMessages.add(new String(msg.getData()));
            consumer3.acknowledge(msg);
        }

        Assert.assertEquals(uniqueMessages.size(), numMsgs);
        Assert.assertEquals(disp0.getActiveConsumer().consumerName(), "2");
        Assert.assertEquals(disp1.getActiveConsumer().consumerName(), "3");
        Assert.assertEquals(disp2.getActiveConsumer().consumerName(), "2");
        Assert.assertEquals(disp3.getActiveConsumer().consumerName(), "3");

        producer.close();
        consumer2.close();
        consumer3.unsubscribe();

        admin.topics().deletePartitionedTopic(topicName);
    }

    @Test
    public void testActiveConsumerFailoverWithDelay() throws Exception {
        final String topicName = "persistent://prop/ns-abc/failover-topic3";
        final String subName = "sub1";
        final int numMsgs = 100;
        List<Message<byte[]>> receivedMessages = new ArrayList<>();

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover).messageListener((consumer, msg) -> {
                    try {
                        synchronized (receivedMessages) {
                            receivedMessages.add(msg);
                        }
                        consumer.acknowledge(msg);
                    } catch (Exception e) {
                        fail("Should not fail");
                    }
                });

        ConsumerBuilder<byte[]> consumerBuilder1 = consumerBuilder.clone().consumerName("1");
        ConsumerBuilder<byte[]> consumerBuilder2 = consumerBuilder.clone().consumerName("2");

        conf.setActiveConsumerFailoverDelayTimeMillis(500);
        restartBroker();

        // create subscription
        Consumer<byte[]> consumer = consumerBuilder1.subscribe();
        consumer.close();
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription subRef = topicRef.getSubscription(subName);

        // enqueue messages
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(numMsgs);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();
        producer.close();

        // two consumers subscribe at almost the same time
        CompletableFuture<Consumer<byte[]>> subscribeFuture2 = consumerBuilder2.subscribeAsync();
        CompletableFuture<Consumer<byte[]>> subscribeFuture1 = consumerBuilder1.subscribeAsync();

        // wait for all messages to be dequeued
        int retry = 20;
        for (int i = 0; i < retry; i++) {
            if (receivedMessages.size() >= numMsgs && subRef.getNumberOfEntriesInBacklog(false) == 0) {
                break;
            } else if (i != retry - 1) {
                Thread.sleep(100);
            }
        }

        // check if message duplication has occurred
        assertEquals(receivedMessages.size(), numMsgs);
        assertEquals(subRef.getNumberOfEntriesInBacklog(false), 0);
        for (int i = 0; i < receivedMessages.size(); i++) {
            Assert.assertNotNull(receivedMessages.get(i));
            Assert.assertEquals(new String(receivedMessages.get(i).getData()), "my-message-" + i);
        }

        subscribeFuture1.get().close();
        subscribeFuture2.get().unsubscribe();
        admin.topics().delete(topicName);
    }

    @DataProvider(name = "failoverMetadataBudgets")
    public Object[][] failoverMetadataBudgets() {
        return new Object[][]{{0, 0, true}, {32, 32, true}, {1024, 1000, true},
                {1024, 1024, true}, {1024, 1025, false}};
    }

    @Test(dataProvider = "failoverMetadataBudgets")
    public void testFailoverApplicationMetadataBudget(int limit, int applicationSize, boolean accepted)
            throws Exception {
        int originalLimit = conf.getMaxConsumerMetadataSize();
        String topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/failover-metadata-budget");
        String subName = "sub1";
        Map<String, String> properties = applicationSize == 0 ? Map.of() : Map.of("k", "x".repeat(applicationSize - 1));
        admin.topics().createPartitionedTopic(topicName, 2);
        conf.setMaxConsumerMetadataSize(limit);
        try {
            ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Failover).consumerName("worker").properties(properties);
            if (accepted) {
                try (Consumer<byte[]> consumer = builder.subscribe()) {
                    assertThat(consumer.isConnected()).isTrue();
                    for (int i = 0; i < 2; i++) {
                        var brokerConsumer = failoverDispatcher(topicName, subName, i).getConsumers().get(0);
                        assertThat(brokerConsumer.consumerInstanceId()).hasSize(36);
                        assertThat(brokerConsumer.getStats().getMetadata()).isEqualTo(properties);
                    }
                }
            } else {
                assertThatThrownBy(builder::subscribe).isInstanceOf(PulsarClientException.BrokerMetadataException.class)
                        .hasMessageContaining("metadata has a max size");
            }
        } finally {
            conf.setMaxConsumerMetadataSize(originalLimit);
            admin.topics().deletePartitionedTopic(topicName, true);
        }
    }

    @DataProvider(name = "failoverReconnection")
    public Object[][] failoverReconnection() {
        return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "failoverReconnection")
    public void testFailoverAssignmentStaysBalancedAfterPartitionReconnect(boolean duplicateNames,
                                                                          boolean consistentHashing) throws Exception {
        conf.setActiveConsumerFailoverConsistentHashing(consistentHashing);
        String topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/failover-reconnect");
        String subName = "sub1";
        admin.topics().createPartitionedTopic(topicName, 2);
        try (Consumer<byte[]> first = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                     .subscriptionType(SubscriptionType.Failover)
                     .consumerName(duplicateNames ? "worker" : "worker-first")
                     .property("instance", "first").subscribe();
             Consumer<byte[]> second = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                     .subscriptionType(SubscriptionType.Failover)
                     .consumerName(duplicateNames ? "worker" : "worker-second")
                     .property("instance", "second").subscribe()) {
            List<AbstractDispatcherSingleActiveConsumer> dispatchers = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                dispatchers.add(failoverDispatcher(topicName, subName, i));
            }
            List<String> instanceIds = dispatchers.get(0).getConsumers().stream()
                    .map(c -> c.consumerInstanceId()).toList();
            assertThat(instanceIds).hasSize(2).doesNotContainNull().doesNotHaveDuplicates();
            for (var dispatcher : dispatchers) {
                assertThat(dispatcher.getConsumers().stream().map(c -> c.consumerInstanceId()).toList())
                        .containsExactlyElementsOf(instanceIds);
                for (var consumer : dispatcher.getConsumers()) {
                    assertThat(consumer.getMetadata()).hasSize(1).containsKey("instance");
                    assertThat(consumer.getStats().getMetadata()).isEqualTo(consumer.getMetadata());
                }
            }
            assertThat(dispatchers.stream().map(d -> d.getActiveConsumer().getMetadata().get("instance")).toList())
                    .containsExactlyInAnyOrder("first", "second");
            var original = dispatchers.get(0).getConsumers().stream()
                    .filter(c -> "first".equals(c.getMetadata().get("instance"))).findFirst().orElseThrow();
            String originalInstanceId = original.consumerInstanceId();
            // The broker closes only this partition's consumer; the real client subscribes it again.
            original.disconnect();
            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                // Selection completes under this monitor after the new consumer is added to the list.
                synchronized (dispatchers.get(0)) {
                    assertThat(dispatchers.get(0).getConsumers()).hasSize(2);
                    assertThat(dispatchers.get(0).getConsumers().stream().noneMatch(c -> c == original)).isTrue();
                }
            });
            assertThat(dispatchers.get(0).getConsumers().stream()
                    .filter(c -> "first".equals(c.getMetadata().get("instance")))
                    .findFirst().orElseThrow().consumerInstanceId()).isEqualTo(originalInstanceId);
            assertThat(dispatchers.stream().map(d -> d.getActiveConsumer().getMetadata().get("instance")).toList())
                    .containsExactlyInAnyOrder("first", "second");
        } finally {
            conf.setActiveConsumerFailoverConsistentHashing(false);
            admin.topics().deletePartitionedTopic(topicName, true);
        }
    }

    @Test
    public void testSameNameInstanceIdsSurviveBuilderReuseAndPartitionExpansion() throws Exception {
        conf.setActiveConsumerFailoverConsistentHashing(true);
        String topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/failover-instance-expansion");
        String subName = "sub1";
        admin.topics().createPartitionedTopic(topicName, 2);
        ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Failover).consumerName("worker")
                .property("application", "same-name").autoUpdatePartitionsInterval(1, TimeUnit.SECONDS);
        try (Consumer<byte[]> first = builder.subscribe(); Consumer<byte[]> second = builder.subscribe()) {
            assertThat(first.getConsumerName()).isEqualTo("worker");
            assertThat(second.getConsumerName()).isEqualTo("worker");
            assertThat(((ConsumerBase<?>) first).getConf().getProperties())
                    .isEqualTo(Map.of("application", "same-name"));
            assertThat(((ConsumerBase<?>) second).getConf().getProperties())
                    .isEqualTo(Map.of("application", "same-name"));
            List<String> instanceIds = failoverDispatcher(topicName, subName, 0).getConsumers().stream()
                    .map(c -> c.consumerInstanceId()).toList();
            assertThat(instanceIds).hasSize(2).doesNotContainNull().doesNotHaveDuplicates();
            admin.topics().updatePartitionedTopic(topicName, 4);
            Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                List<String> owners = new ArrayList<>();
                for (int i = 0; i < 4; i++) {
                    var dispatcher = failoverDispatcher(topicName, subName, i);
                    synchronized (dispatcher) {
                        assertThat(dispatcher.getConsumers().stream().map(c -> c.consumerInstanceId()).toList())
                                .containsExactlyElementsOf(instanceIds);
                        for (var consumer : dispatcher.getConsumers()) {
                            assertThat(consumer.consumerName()).isEqualTo("worker");
                            assertThat(consumer.getMetadata()).isEqualTo(Map.of("application", "same-name"));
                            assertThat(consumer.getStats().getMetadata()).isEqualTo(consumer.getMetadata());
                        }
                        owners.add(dispatcher.getActiveConsumer().consumerInstanceId());
                    }
                }
                for (String instanceId : instanceIds) {
                    assertThat(owners.stream().filter(instanceId::equals).count()).isEqualTo(2);
                }
            });
        } finally {
            conf.setActiveConsumerFailoverConsistentHashing(false);
            admin.topics().deletePartitionedTopic(topicName, true);
        }
    }

    private AbstractDispatcherSingleActiveConsumer failoverDispatcher(String topicName, String subName,
                                                                     int partition) throws Exception {
        String partitionName = TopicName.get(topicName).getPartition(partition).toString();
        return (AbstractDispatcherSingleActiveConsumer) pulsar.getBrokerService()
                .getTopic(partitionName, false).get().orElseThrow().getSubscription(subName).getDispatcher();
    }

    @Test
    public void testConsistentHashingAssignmentIsBalanced() throws Exception {
        conf.setActiveConsumerFailoverConsistentHashing(true);
        final String topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/failover-balanced");
        final String subName = "sub1";
        final int numPartitions = 10;
        admin.topics().createPartitionedTopic(topicName, numPartitions);
        List<String> names = List.of("consumer-0", "consumer-1", "consumer-2", "consumer-3");
        List<Consumer<byte[]>> consumers = new ArrayList<>();
        try {
            for (String name : names) {
                consumers.add(pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                        .subscriptionType(SubscriptionType.Failover).consumerName(name).subscribe());
            }
            assertBalancedAssignment(topicName, subName, numPartitions, names);

            consumers.remove(0).close();
            assertBalancedAssignment(topicName, subName, numPartitions, names.subList(1, names.size()));
        } finally {
            conf.setActiveConsumerFailoverConsistentHashing(false);
            for (Consumer<byte[]> consumer : consumers) {
                consumer.close();
            }
            admin.topics().deletePartitionedTopic(topicName);
        }
    }

    @Test
    public void testConsistentHashingRebalancesWhenPartitionsAreAdded() throws Exception {
        conf.setActiveConsumerFailoverConsistentHashing(true);
        List<String> names = List.of("consumer-0", "consumer-1");
        // A topic whose partition 0 has to move to another consumer when the topic grows from 2 to 4 partitions
        String prefix = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/failover-expansion");
        String topicName = null;
        for (int i = 0; topicName == null; i++) {
            String candidate = prefix + "-" + i;
            if (AbstractDispatcherSingleActiveConsumer.balancedAssignment(candidate, 2, names)[0]
                    != AbstractDispatcherSingleActiveConsumer.balancedAssignment(candidate, 4, names)[0]) {
                topicName = candidate;
            }
        }
        final String subName = "sub1";
        admin.topics().createPartitionedTopic(topicName, 2);
        List<Consumer<byte[]>> consumers = new ArrayList<>();
        try {
            for (String name : names) {
                consumers.add(pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                        .subscriptionType(SubscriptionType.Failover).consumerName(name)
                        .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS).subscribe());
            }
            assertBalancedAssignment(topicName, subName, 2, names);

            // The consumers stay connected and subscribe to the new partitions on their own
            admin.topics().updatePartitionedTopic(topicName, 4);
            assertBalancedAssignment(topicName, subName, 4, names);
        } finally {
            conf.setActiveConsumerFailoverConsistentHashing(false);
            for (Consumer<byte[]> consumer : consumers) {
                consumer.close();
            }
            admin.topics().deletePartitionedTopic(topicName);
        }
    }

    // The broker's active consumer of every partition is the one picked by the balanced assignment
    private void assertBalancedAssignment(String topicName, String subName, int numPartitions, List<String> names) {
        int[] expected = AbstractDispatcherSingleActiveConsumer.balancedAssignment(topicName, numPartitions, names);
        // New partitions have no stats until the consumers subscribe to them
        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            for (int i = 0; i < numPartitions; i++) {
                String partition = TopicName.get(topicName).getPartition(i).toString();
                assertEquals(admin.topics().getStats(partition).getSubscriptions().get(subName)
                        .getActiveConsumerName(), names.get(expected[i]), partition);
            }
        });
    }
}
