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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.DestinationName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class PersistentFailoverE2ETest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static final int CONSUMER_ADD_OR_REMOVE_WAIT_TIME = 2000;

    @Test
    public void testSimpleConsumerEventsWithoutPartition() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/failover-topic1";
        final String subName = "sub1";
        final int numMsgs = 100;

        ConsumerConfiguration consumerConf1 = new ConsumerConfiguration();
        consumerConf1.setSubscriptionType(SubscriptionType.Failover);
        consumerConf1.setConsumerName("1");
        ConsumerConfiguration consumerConf2 = new ConsumerConfiguration();
        consumerConf2.setSubscriptionType(SubscriptionType.Failover);
        consumerConf2.setConsumerName("2");

        // 1. two consumers on the same subscription
        Consumer consumer1 = pulsarClient.subscribe(topicName, subName, consumerConf1);
        Consumer consumer2 = pulsarClient.subscribe(topicName, subName, consumerConf2);

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName);
        PersistentSubscription subRef = topicRef.getPersistentSubscription(subName);

        assertNotNull(topicRef);
        assertNotNull(subRef);

        // 2. validate basic dispatcher state
        assertTrue(subRef.getDispatcher().isConsumerConnected());
        assertEquals(subRef.getDispatcher().getType(), SubType.Failover);

        List<CompletableFuture<MessageId>> futures = Lists.newArrayListWithCapacity(numMsgs);
        Producer producer = pulsarClient.createProducer(topicName);
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        rolloverPerIntervalStats();

        assertEquals(subRef.getNumberOfEntriesInBacklog(), numMsgs);
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);

        // 3. consumer1 should have all the messages while consumer2 should have no messages
        Message msg = null;
        Assert.assertNull(consumer2.receive(1, TimeUnit.SECONDS));
        for (int i = 0; i < numMsgs; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer1.acknowledge(msg);
        }

        rolloverPerIntervalStats();

        // 4. messages deleted on individual acks
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(), 0);

        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        // 5. master consumer failure should resend unacked messages and new messages to another consumer
        for (int i = 0; i < 5; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer1.acknowledge(msg);
        }
        for (int i = 5; i < 10; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            // do not ack
        }
        consumer1.close();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        for (int i = 5; i < numMsgs; i++) {
            msg = consumer2.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer2.acknowledge(msg);
        }
        Assert.assertNull(consumer2.receive(1, TimeUnit.SECONDS));

        rolloverPerIntervalStats();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(), 0);

        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        // 6. consumer subscription should send messages to the new consumer if its name is highest in the list
        for (int i = 0; i < 5; i++) {
            msg = consumer2.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer2.acknowledge(msg);
        }
        consumer1 = pulsarClient.subscribe(topicName, subName, consumerConf1);
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        for (int i = 5; i < numMsgs; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer1.acknowledge(msg);
        }
        Assert.assertNull(consumer1.receive(1, TimeUnit.SECONDS));

        rolloverPerIntervalStats();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(), 0);

        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        // 7. consumer subscription should not send messages to the new consumer if its name is not highest in the list
        ConsumerConfiguration consumerConf3 = new ConsumerConfiguration();
        consumerConf3.setSubscriptionType(SubscriptionType.Failover);
        consumerConf3.setConsumerName("3");
        for (int i = 0; i < 5; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer1.acknowledge(msg);
        }
        Consumer consumer3 = pulsarClient.subscribe(topicName, subName, consumerConf3);
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        Assert.assertNull(consumer3.receive(1, TimeUnit.SECONDS));
        for (int i = 5; i < numMsgs; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(new String(msg.getData()), "my-message-" + i);
            consumer1.acknowledge(msg);
        }

        rolloverPerIntervalStats();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        assertEquals(subRef.getNumberOfEntriesInBacklog(), 0);

        // 8. unsubscribe not allowed if multiple consumers connected
        try {
            consumer1.unsubscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            // ok
        }

        // 9. unsubscribe allowed if there is a lone consumer
        consumer1.close();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        consumer2.close();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        try {
            consumer3.unsubscribe();
        } catch (PulsarClientException e) {
            fail("Should not fail", e);
        }

        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        subRef = topicRef.getPersistentSubscription(subName);
        assertNull(subRef);

        producer.close();
        consumer3.close();

        admin.persistentTopics().delete(topicName);
    }

    @Test(enabled = false)
    public void testSimpleConsumerEventsWithPartition() throws Exception {
        int numPartitions = 4;

        final String topicName = "persistent://prop/use/ns-abc/failover-topic2";
        final DestinationName destName = DestinationName.get(topicName);
        final String subName = "sub1";
        final int numMsgs = 100;
        Set<String> uniqueMessages = new HashSet<>();

        admin.persistentTopics().createPartitionedTopic(topicName, numPartitions);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        ConsumerConfiguration consumerConf1 = new ConsumerConfiguration();
        consumerConf1.setSubscriptionType(SubscriptionType.Failover);
        consumerConf1.setConsumerName("1");
        ConsumerConfiguration consumerConf2 = new ConsumerConfiguration();
        consumerConf2.setSubscriptionType(SubscriptionType.Failover);
        consumerConf2.setConsumerName("2");

        // 1. two consumers on the same subscription
        Consumer consumer1 = pulsarClient.subscribe(topicName, subName, consumerConf1);
        Consumer consumer2 = pulsarClient.subscribe(topicName, subName, consumerConf2);

        PersistentTopic topicRef;
        topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(destName.getPartition(0).toString());
        PersistentDispatcherSingleActiveConsumer disp0 = (PersistentDispatcherSingleActiveConsumer) topicRef
                .getPersistentSubscription(subName).getDispatcher();
        topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(destName.getPartition(1).toString());
        PersistentDispatcherSingleActiveConsumer disp1 = (PersistentDispatcherSingleActiveConsumer) topicRef
                .getPersistentSubscription(subName).getDispatcher();
        topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(destName.getPartition(2).toString());
        PersistentDispatcherSingleActiveConsumer disp2 = (PersistentDispatcherSingleActiveConsumer) topicRef
                .getPersistentSubscription(subName).getDispatcher();
        topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(destName.getPartition(3).toString());
        PersistentDispatcherSingleActiveConsumer disp3 = (PersistentDispatcherSingleActiveConsumer) topicRef
                .getPersistentSubscription(subName).getDispatcher();

        List<CompletableFuture<MessageId>> futures = Lists.newArrayListWithCapacity(numMsgs);
        Producer producer = pulsarClient.createProducer(topicName, producerConf);
        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        // equal distribution between both consumers
        int totalMessages = 0;
        Message msg = null;
        while (true) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            totalMessages++;
            consumer1.acknowledge(msg);
        }
        Assert.assertEquals(totalMessages, numMsgs / 2);
        while (true) {
            msg = consumer2.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            totalMessages++;
            consumer2.acknowledge(msg);
        }
        Assert.assertEquals(totalMessages, numMsgs);
        Assert.assertEquals(disp0.getActiveConsumer().consumerName(), consumerConf1.getConsumerName());
        Assert.assertEquals(disp1.getActiveConsumer().consumerName(), consumerConf2.getConsumerName());
        Assert.assertEquals(disp2.getActiveConsumer().consumerName(), consumerConf1.getConsumerName());
        Assert.assertEquals(disp3.getActiveConsumer().consumerName(), consumerConf2.getConsumerName());
        totalMessages = 0;

        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        // add a consumer
        ConsumerConfiguration consumerConf3 = new ConsumerConfiguration();
        consumerConf3.setSubscriptionType(SubscriptionType.Failover);
        consumerConf3.setConsumerName("3");
        for (int i = 0; i < 20; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            uniqueMessages.add(new String(msg.getData()));
            consumer1.acknowledge(msg);
        }
        Consumer consumer3 = pulsarClient.subscribe(topicName, subName, consumerConf3);
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        int consumer1Messages = 0;
        while (true) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                Assert.assertEquals(consumer1Messages, 55);
                break;
            }
            consumer1Messages++;
            uniqueMessages.add(new String(msg.getData()));
            consumer1.acknowledge(msg);
        }
        int consumer2Messages = 0;
        while (true) {
            msg = consumer2.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                Assert.assertEquals(consumer2Messages, 50);
                break;
            }
            consumer2Messages++;
            uniqueMessages.add(new String(msg.getData()));
            consumer2.acknowledge(msg);
        }
        int consumer3Messages = 0;
        while (true) {
            msg = consumer3.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                Assert.assertEquals(consumer3Messages, 15, 10);
                break;
            }
            consumer3Messages++;
            uniqueMessages.add(new String(msg.getData()));
            consumer3.acknowledge(msg);
        }

        Assert.assertEquals(uniqueMessages.size(), numMsgs);
        Assert.assertEquals(disp0.getActiveConsumer().consumerName(), consumerConf1.getConsumerName());
        Assert.assertEquals(disp1.getActiveConsumer().consumerName(), consumerConf2.getConsumerName());
        Assert.assertEquals(disp2.getActiveConsumer().consumerName(), consumerConf3.getConsumerName());
        Assert.assertEquals(disp3.getActiveConsumer().consumerName(), consumerConf1.getConsumerName());
        uniqueMessages.clear();

        for (int i = 0; i < numMsgs; i++) {
            String message = "my-message-" + i;
            futures.add(producer.sendAsync(message.getBytes()));
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        // remove a consumer
        for (int i = 0; i < 10; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            uniqueMessages.add(new String(msg.getData()));
            consumer1.acknowledge(msg);
        }
        consumer1.close();
        Thread.sleep(CONSUMER_ADD_OR_REMOVE_WAIT_TIME);
        consumer2Messages = 0;
        while (true) {
            msg = consumer2.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                Assert.assertEquals(consumer2Messages, 70, 5);
                break;
            }
            consumer2Messages++;
            uniqueMessages.add(new String(msg.getData()));
            consumer2.acknowledge(msg);
        }
        consumer3Messages = 0;
        while (true) {
            msg = consumer3.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                Assert.assertEquals(consumer3Messages, 70, 5);
                break;
            }
            consumer3Messages++;
            uniqueMessages.add(new String(msg.getData()));
            consumer3.acknowledge(msg);
        }

        Assert.assertEquals(uniqueMessages.size(), numMsgs);
        Assert.assertEquals(disp0.getActiveConsumer().consumerName(), consumerConf2.getConsumerName());
        Assert.assertEquals(disp1.getActiveConsumer().consumerName(), consumerConf3.getConsumerName());
        Assert.assertEquals(disp2.getActiveConsumer().consumerName(), consumerConf2.getConsumerName());
        Assert.assertEquals(disp3.getActiveConsumer().consumerName(), consumerConf3.getConsumerName());

        producer.close();
        consumer2.close();
        consumer3.unsubscribe();

        admin.persistentTopics().deletePartitionedTopic(topicName);
    }
}
