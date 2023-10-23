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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.SystemTopicBasedTopicPoliciesService;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class SubscriptionPauseOnAckStatPersistTest extends ProducerConsumerBase {

    private static final int MAX_UNACKED_RANGES_TO_PERSIST = 50;

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

    protected void doInitConf() throws Exception {
        conf.setManagedLedgerMaxUnackedRangesToPersist(MAX_UNACKED_RANGES_TO_PERSIST);
    }

    private void enablePolicyDispatcherPauseOnAckStatePersistent(String tpName) {
        TopicPolicies policies = new TopicPolicies();
        policies.setDispatcherPauseOnAckStatePersistentEnabled(true);
        policies.setIsGlobal(false);
        SystemTopicBasedTopicPoliciesService policiesService =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        Map<TopicName, TopicPolicies> policiesCache = WhiteboxImpl.getInternalState(policiesService, "policiesCache");
        policiesCache.put(TopicName.get(tpName), policies);
    }

    private void cancelPendingRead(String tpName, String cursorName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        Dispatcher dispatcher = persistentTopic.getSubscription(cursorName).getDispatcher();
        if (dispatcher instanceof PersistentDispatcherMultipleConsumers) {
            Method cancelPendingRead = PersistentDispatcherMultipleConsumers.class
                    .getDeclaredMethod("cancelPendingRead", new Class[]{});
            cancelPendingRead.setAccessible(true);
            cancelPendingRead.invoke(dispatcher, new Object[]{});
        } else if (dispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            Method cancelPendingRead = PersistentDispatcherSingleActiveConsumer.class
                    .getDeclaredMethod("cancelPendingRead", new Class[]{});
            cancelPendingRead.setAccessible(true);
            cancelPendingRead.invoke(dispatcher, new Object[]{});
        }
    }

    @DataProvider(name = "subscriptionTypes")
    private Object[][] subscriptionTypes() {
        return new Object[][]{
                {SubscriptionType.Key_Shared},
                {SubscriptionType.Shared},
                {SubscriptionType.Failover}
        };
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testPauseOnAckStatPersist(SubscriptionType subscriptionType) throws Exception {
        final String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        final int msgSendCount = MAX_UNACKED_RANGES_TO_PERSIST * 4;
        final int incomingQueueSize = MAX_UNACKED_RANGES_TO_PERSIST * 10;

        enablePolicyDispatcherPauseOnAckStatePersistent(tpName);
        admin.topics().createNonPartitionedTopic(tpName);
        admin.topics().createSubscription(tpName, subscription, MessageId.earliest);

        // Send double MAX_UNACKED_RANGES_TO_PERSIST messages.
        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING).topic(tpName).enableBatching(false).create();
        ArrayList<MessageId> messageIdsSent = new ArrayList<>();
        for (int i = 0; i < msgSendCount; i++) {
            MessageIdImpl messageId = (MessageIdImpl) p1.send(Integer.valueOf(i).toString());
            messageIdsSent.add(messageId);
        }
        // Make ack holes.
        ArrayList<String> messagesReceived = new ArrayList<>();
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true).subscriptionType(subscriptionType)
                .subscribe();
        while (true) {
            Message<String> msg = c1.receive(2, TimeUnit.SECONDS);
            if (msg != null) {
                messagesReceived.add(msg.getValue());
                if (Integer.valueOf(msg.getValue()) % 2 == 1) {
                    c1.acknowledge(msg);
                }
            } else {
                break;
            }
        }

        cancelPendingRead(tpName, subscription);

        // Verify: the dispatcher has been paused.
        final String specifiedMessage = "9876543210";
        p1.send(specifiedMessage);
        Message<String> msg1 = c1.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg1);

        // Verify: after ack messages, will unpause the dispatcher.
        c1.acknowledge(messageIdsSent);
        LinkedHashSet<String> messagesReceivedAfterAck = new LinkedHashSet<>();
        while (true) {
            Message<String> msg = c1.receive(2, TimeUnit.SECONDS);
            if (msg != null) {
                messagesReceivedAfterAck.add(msg.getValue());
            } else {
                break;
            }
        }
        Assert.assertTrue(messagesReceivedAfterAck.contains(specifiedMessage));

        // cleanup.
        p1.close();
        c1.close();
        admin.topics().delete(tpName, false);
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testPauseOnAckStatPersistNotAffectReplayRead(SubscriptionType subscriptionType) throws Exception {
        final String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        final int msgSendCount = MAX_UNACKED_RANGES_TO_PERSIST * 4;
        final int incomingQueueSize = MAX_UNACKED_RANGES_TO_PERSIST * 10;

        enablePolicyDispatcherPauseOnAckStatePersistent(tpName);
        admin.topics().createNonPartitionedTopic(tpName);
        admin.topics().createSubscription(tpName, subscription, MessageId.earliest);

        // Send double MAX_UNACKED_RANGES_TO_PERSIST messages.
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true).subscriptionType(subscriptionType)
                .subscribe();
        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING).topic(tpName).enableBatching(false).create();
        ArrayList<MessageId> messageIdsSent = new ArrayList<>();
        for (int i = 0; i < msgSendCount; i++) {
            MessageIdImpl messageId = (MessageIdImpl) p1.send(Integer.valueOf(i).toString());
            messageIdsSent.add(messageId);
        }
        // Make ack holes.
        ArrayList<String> messagesReceived = new ArrayList<>();
        int messageCountAckedByC1 = 0;
        while (true) {
            Message<String> msg = c1.receive(2, TimeUnit.SECONDS);
            if (msg != null) {
                messagesReceived.add(msg.getValue());
                if (Integer.valueOf(msg.getValue()) % 2 == 1) {
                    c1.acknowledge(msg);
                    messageCountAckedByC1++;
                }
            } else {
                break;
            }
        }

        cancelPendingRead(tpName, subscription);

        // Verify: the dispatcher has been paused.
        final String specifiedMessage = "9876543210";
        final int specifiedMessageCount = 1;
        p1.send(specifiedMessage);
        Message<String> msg1 = c1.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg1);

        // Verify: close the previous consumer, the new one could receive all messages.
        Consumer<String> c2 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true).subscriptionType(subscriptionType)
                .subscribe();
        c1.close();
        LinkedHashSet<String> messagesReceived2 = new LinkedHashSet<>();
        while (true) {
            Message<String> msg = c2.receive(10, TimeUnit.SECONDS);
            if (msg != null) {
                messagesReceived2.add(msg.getValue());
                c2.acknowledge(msg);
            } else {
                break;
            }
        }
        Assert.assertEquals(messagesReceived2.size(), msgSendCount - messageCountAckedByC1 + specifiedMessageCount);

        // cleanup, c1 has been closed before.
        p1.close();
        c2.close();
        admin.topics().delete(tpName, false);
    }
}
