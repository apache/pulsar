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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.SystemTopicBasedTopicPoliciesService;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.GetStatsOptions;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.awaitility.Awaitility;
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
        Map<TopicName, TopicPolicies> policiesCache =
                WhiteboxImpl.getInternalState(policiesService, "policiesCache");
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

    private void triggerNewReadMoreEntries(String tpName, String cursorName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        Dispatcher dispatcher = persistentTopic.getSubscription(cursorName).getDispatcher();
        if (dispatcher instanceof PersistentDispatcherMultipleConsumers) {
            ((PersistentDispatcherMultipleConsumers) dispatcher).readMoreEntries();
        } else if (dispatcher instanceof PersistentDispatcherSingleActiveConsumer) {
            PersistentDispatcherSingleActiveConsumer persistentDispatcherSingleActiveConsumer =
                    ((PersistentDispatcherSingleActiveConsumer) dispatcher);
            Method readMoreEntries = PersistentDispatcherSingleActiveConsumer.class.getDeclaredMethod(
                    "readMoreEntries", new Class[]{org.apache.pulsar.broker.service.Consumer.class});
            readMoreEntries.setAccessible(true);
            readMoreEntries.invoke(dispatcher,
                    new Object[]{persistentDispatcherSingleActiveConsumer.getActiveConsumer()});
        }
    }

    @DataProvider(name = "multiConsumerSubscriptionTypes")
    private Object[][] multiConsumerSubscriptionTypes() {
        return new Object[][]{
                {SubscriptionType.Key_Shared},
                {SubscriptionType.Shared}
        };
    }

    @DataProvider(name = "singleConsumerSubscriptionTypes")
    private Object[][] singleConsumerSubscriptionTypes() {
        return new Object[][]{
                {SubscriptionType.Failover},
                {SubscriptionType.Exclusive}
        };
    }

    @DataProvider(name = "skipTypes")
    private Object[][] skipTypes() {
        return new Object[][]{
                {SkipType.SKIP_ENTRIES},
                {SkipType.CLEAR_BACKLOG},
                {SkipType.SEEK},
                {SkipType.RESET_CURSOR}
        };
    }

    private enum SkipType{
        SKIP_ENTRIES,
        CLEAR_BACKLOG,
        SEEK,
        RESET_CURSOR;
    }

    private ReceivedMessages receiveAndAckMessages(BiFunction<MessageId, String, Boolean> ackPredicate,
                                                Consumer<String>...consumers) throws Exception {
        ReceivedMessages receivedMessages = new ReceivedMessages();
        while (true) {
            int receivedMsgCount = 0;
            for (int i = 0; i < consumers.length; i++) {
                Consumer<String> consumer = consumers[i];
                while (true) {
                    Message<String> msg = consumer.receive(2, TimeUnit.SECONDS);
                    if (msg != null) {
                        receivedMsgCount++;
                        String v = msg.getValue();
                        MessageId messageId = msg.getMessageId();
                        receivedMessages.messagesReceived.add(Pair.of(msg.getMessageId(), v));
                        if (ackPredicate.apply(messageId, v)) {
                            consumer.acknowledge(msg);
                            receivedMessages.messagesAcked.add(Pair.of(msg.getMessageId(), v));
                        }
                    } else {
                        break;
                    }
                }
            }
            // Because of the possibility of consumers getting stuck with each other, only jump out of the loop if all
            // consumers could not receive messages.
            if (receivedMsgCount == 0) {
                break;
            }
        }
        return receivedMessages;
    }

    private ReceivedMessages ackAllMessages(Consumer<String>...consumers) throws Exception {
        return receiveAndAckMessages((msgId, msgV) -> true, consumers);
    }

    private ReceivedMessages ackOddMessagesOnly(Consumer<String>...consumers) throws Exception {
        return receiveAndAckMessages((msgId, msgV) -> Integer.valueOf(msgV) % 2 == 1, consumers);
    }

    private static class ReceivedMessages {

        List<Pair<MessageId,String>> messagesReceived = new ArrayList<>();

        List<Pair<MessageId,String>> messagesAcked = new ArrayList<>();

        public boolean hasReceivedMessage(String v) {
            for (Pair<MessageId,String> pair : messagesReceived) {
                if (pair.getRight().equals(v)) {
                    return true;
                }
            }
            return false;
        }

        public boolean hasAckedMessage(String v) {
            for (Pair<MessageId,String> pair : messagesAcked) {
                if (pair.getRight().equals(v)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Test
    public void testBrokerDynamicConfig() throws Exception {
        final String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        final int msgSendCount = MAX_UNACKED_RANGES_TO_PERSIST * 4;
        final int incomingQueueSize = MAX_UNACKED_RANGES_TO_PERSIST * 10;

        // Enable "dispatcherPauseOnAckStatePersistentEnabled".
        admin.brokers().updateDynamicConfiguration("dispatcherPauseOnAckStatePersistentEnabled", "true");
        admin.topics().createNonPartitionedTopic(tpName);
        admin.topics().createSubscription(tpName, subscription, MessageId.earliest);

        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        Awaitility.await().untilAsserted(() -> {
            Assert.assertTrue(pulsar.getConfig().isDispatcherPauseOnAckStatePersistentEnabled());
            HierarchyTopicPolicies policies = WhiteboxImpl.getInternalState(persistentTopic, "topicPolicies");
            Boolean v = policies.getDispatcherPauseOnAckStatePersistentEnabled().get();
            Assert.assertNotNull(v);
            Assert.assertTrue(v.booleanValue());
        });

        // Send double MAX_UNACKED_RANGES_TO_PERSIST messages.
        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING).topic(tpName).enableBatching(false).create();
        ArrayList<MessageId> messageIdsSent = new ArrayList<>();
        for (int i = 0; i < msgSendCount; i++) {
            MessageIdImpl messageId = (MessageIdImpl) p1.send(Integer.valueOf(i).toString());
            messageIdsSent.add(messageId);
        }
        // Make ack holes.
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        ackOddMessagesOnly(c1);
        verifyAckHolesIsMuchThanLimit(tpName, subscription);

        cancelPendingRead(tpName, subscription);
        triggerNewReadMoreEntries(tpName, subscription);

        // Verify: the dispatcher has been paused.
        final String specifiedMessage = "9876543210";
        p1.send(specifiedMessage);
        Message<String> msg1 = c1.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg1, msg1 == null ? "null" : msg1.getValue());

        // Disable "dispatcherPauseOnAckStatePersistentEnabled".
        admin.brokers().updateDynamicConfiguration("dispatcherPauseOnAckStatePersistentEnabled", "false");
        Awaitility.await().untilAsserted(() -> {
            Assert.assertFalse(pulsar.getConfig().isDispatcherPauseOnAckStatePersistentEnabled());
            HierarchyTopicPolicies policies = WhiteboxImpl.getInternalState(persistentTopic, "topicPolicies");
            Boolean v = policies.getDispatcherPauseOnAckStatePersistentEnabled().get();
            Assert.assertTrue(v == null || !v.booleanValue());
        });

        // Verify the new message can be received.
        Message<String> msg2 = c1.receive(2, TimeUnit.SECONDS);
        Assert.assertNotNull(msg2);
        Assert.assertEquals(msg2.getValue(), specifiedMessage);
        // cleanup.
        p1.close();
        c1.close();
        admin.topics().delete(tpName, false);
    }

    private void verifyAckHolesIsMuchThanLimit(String tpName, String subscription) {
        Awaitility.await().untilAsserted(() -> {
            Assert.assertTrue(MAX_UNACKED_RANGES_TO_PERSIST < admin.topics()
                    .getInternalStats(tpName).cursors.get(subscription).totalNonContiguousDeletedMessagesRange);
        });
    }

    @Test(dataProvider = "multiConsumerSubscriptionTypes")
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
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true).subscriptionType(subscriptionType)
                .subscribe();
        ackOddMessagesOnly(c1);
        verifyAckHolesIsMuchThanLimit(tpName, subscription);

        cancelPendingRead(tpName, subscription);
        triggerNewReadMoreEntries(tpName, subscription);

        // Verify: the dispatcher has been paused.
        final String specifiedMessage = "9876543210";
        p1.send(specifiedMessage);
        Message<String> msg1 = c1.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg1);

        // Verify: after ack messages, will unpause the dispatcher.
        c1.acknowledge(messageIdsSent);
        ReceivedMessages receivedMessagesAfterPause = ackAllMessages(c1);
        Assert.assertTrue(receivedMessagesAfterPause.hasReceivedMessage(specifiedMessage));
        Assert.assertTrue(receivedMessagesAfterPause.hasAckedMessage(specifiedMessage));

        // cleanup.
        p1.close();
        c1.close();
        admin.topics().delete(tpName, false);
    }

    @Test(dataProvider = "skipTypes")
    public void testUnPauseOnSkipEntries(SkipType skipType) throws Exception {
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
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        ackOddMessagesOnly(c1);
        verifyAckHolesIsMuchThanLimit(tpName, subscription);

        cancelPendingRead(tpName, subscription);
        triggerNewReadMoreEntries(tpName, subscription);

        // Verify: the dispatcher has been paused.
        final String specifiedMessage1 = "9876543210";
        p1.send(specifiedMessage1);
        Message<String> msg1 = c1.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg1);

        // Verify: after enough messages have been skipped, will unpause the dispatcher.
        skipMessages(tpName, subscription, skipType, c1);
        // Since the message "specifiedMessage1" might be skipped, we send a new message to verify the result.
        final String specifiedMessage2 = "9876543211";
        p1.send(specifiedMessage2);

        ReceivedMessages receivedMessagesAfterPause = ackAllMessages(c1);
        Assert.assertTrue(receivedMessagesAfterPause.hasReceivedMessage(specifiedMessage2));
        Assert.assertTrue(receivedMessagesAfterPause.hasAckedMessage(specifiedMessage2));

        // cleanup.
        p1.close();
        c1.close();
        admin.topics().delete(tpName, false);
    }

    private void skipMessages(String tpName, String subscription, SkipType skipType, Consumer c) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        Position LAC = persistentTopic.getManagedLedger().getLastConfirmedEntry();
        MessageIdImpl LACMessageId = new MessageIdImpl(LAC.getLedgerId(), LAC.getEntryId(), -1);
        if (skipType == SkipType.SKIP_ENTRIES) {
            while (true) {
                GetStatsOptions getStatsOptions = new GetStatsOptions(
                        true, /* getPreciseBacklog */
                        false, /* subscriptionBacklogSize */
                        false, /* getEarliestTimeInBacklog */
                        true, /* excludePublishers */
                        true /* excludeConsumers */);
                org.apache.pulsar.common.policies.data.SubscriptionStats subscriptionStats =
                        admin.topics().getStats(tpName, getStatsOptions).getSubscriptions().get(subscription);
                if (subscriptionStats.getMsgBacklog() < MAX_UNACKED_RANGES_TO_PERSIST) {
                    break;
                }
                admin.topics().skipMessages(tpName, subscription, 100);
            }
        } else if (skipType == SkipType.CLEAR_BACKLOG){
            admin.topics().skipAllMessages(tpName, subscription);
        } else if (skipType == SkipType.SEEK) {
            c.seek(LACMessageId);
        } else if (skipType == SkipType.RESET_CURSOR) {
            admin.topics().resetCursor(tpName, subscription, LACMessageId, false);
        }
    }

    @Test(dataProvider = "singleConsumerSubscriptionTypes")
    public void testSingleConsumerDispatcherWillNotPause(SubscriptionType subscriptionType) throws Exception {
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
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true)
                .subscriptionType(subscriptionType)
                .subscribe();
        ackOddMessagesOnly(c1);
        verifyAckHolesIsMuchThanLimit(tpName, subscription);

        cancelPendingRead(tpName, subscription);
        triggerNewReadMoreEntries(tpName, subscription);

        // Verify: the dispatcher has been paused.
        final String specifiedMessage = "9876543210";
        p1.send(specifiedMessage);
        Message<String> msg1 = c1.receive(2, TimeUnit.SECONDS);
        Assert.assertNotNull(msg1);
        Assert.assertEquals(msg1.getValue(), specifiedMessage);

        // cleanup.
        p1.close();
        c1.close();
        admin.topics().delete(tpName, false);
    }

    @Test(dataProvider = "multiConsumerSubscriptionTypes")
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
        ReceivedMessages receivedMessagesC1 = ackOddMessagesOnly(c1);
        verifyAckHolesIsMuchThanLimit(tpName, subscription);

        cancelPendingRead(tpName, subscription);
        triggerNewReadMoreEntries(tpName, subscription);

        // Verify: the dispatcher has been paused.
        Consumer<String> c2 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true).subscriptionType(subscriptionType)
                .subscribe();
        final String specifiedMessage = "9876543210";
        final int specifiedMessageCount = 1;
        p1.send(specifiedMessage);
        Message<String> msg1 = c1.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg1);
        Message<String> msg2 = c2.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg2);

        // Verify: close the previous consumer, the new one could receive all messages.
        c1.close();
        ReceivedMessages receivedMessagesC2 = ackAllMessages(c2);
        int messageCountAckedByC1 = receivedMessagesC1.messagesAcked.size();
        int messageCountAckedByC2 = receivedMessagesC2.messagesAcked.size();
        Assert.assertEquals(messageCountAckedByC2, msgSendCount - messageCountAckedByC1 + specifiedMessageCount);

        // cleanup, c1 has been closed before.
        p1.close();
        c2.close();
        admin.topics().delete(tpName, false);
    }

    @Test(dataProvider = "multiConsumerSubscriptionTypes")
    public void testMultiConsumersPauseOnAckStatPersistNotAffectReplayRead(SubscriptionType subscriptionType)
            throws Exception {
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
        Consumer<String> c2 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true).subscriptionType(subscriptionType)
                .subscribe();
        Producer<String> p1 = pulsarClient.newProducer(Schema.STRING).topic(tpName).enableBatching(false).create();
        ArrayList<MessageId> messageIdsSent = new ArrayList<>();
        for (int i = 0; i < msgSendCount; i++) {
            MessageIdImpl messageId = (MessageIdImpl) p1.send(Integer.valueOf(i).toString());
            messageIdsSent.add(messageId);
        }
        // Make ack holes.
        ReceivedMessages receivedMessagesC1AndC2 = ackOddMessagesOnly(c1, c2);
        verifyAckHolesIsMuchThanLimit(tpName, subscription);

        cancelPendingRead(tpName, subscription);
        triggerNewReadMoreEntries(tpName, subscription);

        // Verify: the dispatcher has been paused.
        Consumer<String> c3 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true).subscriptionType(subscriptionType)
                .subscribe();
        Consumer<String> c4 = pulsarClient.newConsumer(Schema.STRING).topic(tpName).subscriptionName(subscription)
                .receiverQueueSize(incomingQueueSize).isAckReceiptEnabled(true).subscriptionType(subscriptionType)
                .subscribe();
        final String specifiedMessage = "9876543210";
        final int specifiedMessageCount = 1;
        p1.send(specifiedMessage);
        for (Consumer c : Arrays.asList(c1, c2, c3, c4)) {
            Message<String> m = c.receive(2, TimeUnit.SECONDS);
            Assert.assertNull(m);
        }

        // Verify: close the previous consumer, the new one could receive all messages.
        c1.close();
        c2.close();
        ReceivedMessages receivedMessagesC3AndC4 = ackAllMessages(c3, c4);
        int messageCountAckedByC1AndC2 = receivedMessagesC1AndC2.messagesAcked.size();
        int messageCountAckedByC3AndC4 = receivedMessagesC3AndC4.messagesAcked.size();
        Assert.assertEquals(messageCountAckedByC3AndC4,
                msgSendCount - messageCountAckedByC1AndC2 + specifiedMessageCount);

        // cleanup, c1 has been closed before.
        p1.close();
        c3.close();
        c4.close();
        admin.topics().delete(tpName, false);
    }
}
