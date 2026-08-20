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

import static java.util.Collections.emptyMap;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Exclusive;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Failover;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Key_Shared;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Shared;
import static org.apache.pulsar.common.api.proto.KeySharedMode.AUTO_SPLIT;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class SharedDispatcherPermitAccountingTest extends SharedPulsarBaseTest {

    @DataProvider(name = "dispatcherImplementations")
    public Object[][] dispatcherImplementations() {
        return new Object[][] {{false}, {true}};
    }

    @DataProvider(name = "flowRaceDispatcherVariants")
    public Object[][] flowRaceDispatcherVariants() {
        return new Object[][] {
                {false, Shared},
                {true, Shared},
                {false, Key_Shared},
                {true, Key_Shared}
        };
    }

    @DataProvider(name = "untrackedSubscriptionVariants")
    public Object[][] untrackedSubscriptionVariants() {
        return new Object[][] {
                {true, Exclusive},
                {true, Failover},
                {false, Shared},
                {false, Key_Shared}
        };
    }

    @Test(dataProvider = "untrackedSubscriptionVariants", timeOut = 30_000)
    public void testUnrelatedSubscriptionDoesNotAccumulatePendingDispatcherPermits(
            boolean persistent, SubType subType) throws Exception {
        String topicName = newTopicName();
        String subscriptionName = "sub";
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic persistentTopic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        Topic consumerTopic = persistent ? persistentTopic : mock(Topic.class);
        if (!persistent) {
            when(consumerTopic.getBrokerService()).thenReturn(persistentTopic.getBrokerService());
            when(consumerTopic.getHierarchyTopicPolicies())
                    .thenReturn(persistentTopic.getHierarchyTopicPolicies());
        }
        Subscription subscription = mock(Subscription.class);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscription.getTopic()).thenReturn(consumerTopic);
        Consumer consumer = createConsumer(subscription, subType, topicName, 1);

        consumer.flowPermits(100);

        assertThat(consumer.getAvailablePermits()).isEqualTo(100);
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isEqualTo(100);
    }

    @Test(dataProvider = "flowRaceDispatcherVariants", timeOut = 30_000)
    public void testFlowCommandRaceWithConsumerRemovalDoesNotLosePermits(boolean classic, SubType subType)
            throws Exception {
        TestContext context = createTestContext(classic, subType);
        Consumer remainingConsumer = context.remainingConsumer();
        Consumer removedConsumer = context.removedConsumer();

        remainingConsumer.flowPermits(10);
        removedConsumer.flowPermits(20);
        drainBrokerWorkerGroup(context.topic());
        assertThat(totalAvailablePermits(context.dispatcher())).isEqualTo(30);

        synchronized (context.dispatcher()) {
            // Keep the asynchronous Flow tasks queued until removal completes.
            removedConsumer.flowPermits(400);
            removedConsumer.flowPermits(600);
            assertThat(totalAvailablePermits(context.dispatcher())).isEqualTo(30);
            context.dispatcher().removeConsumer(removedConsumer);
        }
        drainBrokerWorkerGroup(context.topic());

        assertThat(totalAvailablePermits(context.dispatcher()))
                .isEqualTo(remainingConsumer.getAvailablePermits());
    }

    @Test(timeOut = 30_000)
    public void testPendingPermitAccountingSurvivesSignedIntegerWrap() throws Exception {
        String topicName = newTopicName();
        String subscriptionName = "shared-sub";
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
        Subscription subscription = mock(PersistentSubscription.class);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscription.getTopic()).thenReturn(topic);
        Consumer consumer = createConsumer(subscription, Shared, topicName, 1);

        consumer.flowPermits(Integer.MAX_VALUE);
        consumer.flowPermits(Integer.MAX_VALUE);
        consumer.flowPermits(Integer.MAX_VALUE);

        assertThat(consumer.getAvailablePermits()).isEqualTo(Integer.MAX_VALUE - 2);
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isZero();

        consumer.completePendingDispatcherFlow(Integer.MAX_VALUE);
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isEqualTo(Integer.MAX_VALUE);
        consumer.completePendingDispatcherFlow(Integer.MAX_VALUE);
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isEqualTo(-2);
        consumer.completePendingDispatcherFlow(Integer.MAX_VALUE);
        assertThat(consumer.getAvailablePermitsForDispatcherRemoval()).isEqualTo(Integer.MAX_VALUE - 2);
    }

    @Test(dataProvider = "dispatcherImplementations", timeOut = 30_000)
    public void testRemovalAppliesNegativeAccountedPermitBalance(boolean classic) throws Exception {
        TestContext context = createTestContext(classic);
        Consumer remainingConsumer = context.remainingConsumer();
        Consumer removedConsumer = context.removedConsumer();
        Consumer secondRemainingConsumer = createConsumer(
                remainingConsumer.getSubscription(), Shared, context.topic().getName(), 3);
        context.dispatcher().addConsumer(secondRemainingConsumer).join();

        remainingConsumer.flowPermits(10);
        secondRemainingConsumer.flowPermits(15);
        removedConsumer.flowPermits(20);
        drainBrokerWorkerGroup(context.topic());
        assertThat(totalAvailablePermits(context.dispatcher())).isEqualTo(45);

        synchronized (context.dispatcher()) {
            // Keep the Flow task queued, then dispatch more than the removed consumer's 20 accounted permits.
            // Its removal balance becomes 70 available - 100 pending = -30 and must be applied as-is.
            removedConsumer.flowPermits(100);
            simulateDispatch(context.dispatcher(), removedConsumer, 50);

            assertThat(removedConsumer.getAvailablePermits()).isEqualTo(70);
            assertThat(removedConsumer.getAvailablePermitsForDispatcherRemoval()).isEqualTo(-30);
            assertThat(totalAvailablePermits(context.dispatcher())).isEqualTo(-5);

            context.dispatcher().removeConsumer(removedConsumer);
            assertThat(totalAvailablePermits(context.dispatcher())).isEqualTo(25);
        }
        drainBrokerWorkerGroup(context.topic());

        // Keep two consumers after removal so a single-consumer read floor cannot mask dispatcher permit drift.
        assertThat(context.dispatcher().getConsumers())
                .containsExactlyInAnyOrder(remainingConsumer, secondRemainingConsumer);
        assertThat(totalAvailablePermits(context.dispatcher()))
                .isEqualTo(remainingConsumer.getAvailablePermits()
                        + secondRemainingConsumer.getAvailablePermits());
    }

    @Test(dataProvider = "dispatcherImplementations", timeOut = 30_000)
    public void testBlockedFlowCommandRaceWithConsumerRemovalDoesNotLosePermits(boolean classic) throws Exception {
        TestContext context = createTestContext(classic);
        Consumer remainingConsumer = context.remainingConsumer();
        Consumer removedConsumer = context.removedConsumer();

        remainingConsumer.flowPermits(10);
        drainBrokerWorkerGroup(context.topic());
        assertThat(totalAvailablePermits(context.dispatcher())).isEqualTo(10);

        ConsumerStatsImpl blockedStats = new ConsumerStatsImpl();
        blockedStats.blockedConsumerOnUnackedMsgs = true;
        removedConsumer.updateStats(blockedStats);
        assertThat(removedConsumer.isBlocked()).isTrue();
        assertThat(removedConsumer.getMaxUnackedMessages()).isPositive();

        removedConsumer.flowPermits(1_000);
        assertThat(removedConsumer.getAvailablePermits()).isZero();

        synchronized (context.dispatcher()) {
            // Keep the asynchronous Flow task queued until removal completes.
            removedConsumer.updateBlockedConsumerOnUnackedMsgs(removedConsumer);
            assertThat(totalAvailablePermits(context.dispatcher())).isEqualTo(10);
            context.dispatcher().removeConsumer(removedConsumer);
        }
        drainBrokerWorkerGroup(context.topic());

        assertThat(totalAvailablePermits(context.dispatcher()))
                .isEqualTo(remainingConsumer.getAvailablePermits());
    }

    @Test(timeOut = 30_000)
    public void testRemainingConsumerCanContinueAfterFlowAndCloseRace() throws Exception {
        int receiverQueueSize = 10;
        int messagesToConsume = receiverQueueSize * 2;
        int pendingFlowPermits = 1_000;
        String topicName = newTopicName();
        String subscriptionName = "shared-sub";
        admin.topics().createNonPartitionedTopic(topicName);

        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
             org.apache.pulsar.client.api.Consumer<byte[]> remainingClient = pulsarClient.newConsumer()
                     .topic(topicName)
                     .subscriptionName(subscriptionName)
                     .subscriptionType(SubscriptionType.Shared)
                     .consumerName("remaining-consumer")
                     .receiverQueueSize(receiverQueueSize)
                     .subscribe();
             org.apache.pulsar.client.api.Consumer<byte[]> removedClient = pulsarClient.newConsumer()
                     .topic(topicName)
                     .subscriptionName(subscriptionName)
                     .subscriptionType(SubscriptionType.Shared)
                     .consumerName("removed-consumer")
                     .receiverQueueSize(receiverQueueSize)
                     .subscribe()) {
            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
            PersistentSubscription subscription =
                    (PersistentSubscription) topic.getSubscription(subscriptionName);
            PersistentDispatcherMultipleConsumers dispatcher =
                    (PersistentDispatcherMultipleConsumers) subscription.getDispatcher();
            Consumer remainingBrokerConsumer = findConsumer(dispatcher, "remaining-consumer");
            Consumer removedBrokerConsumer = findConsumer(dispatcher, "removed-consumer");
            ServerCnx removedConsumerCnx = (ServerCnx) removedBrokerConsumer.cnx();

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(remainingBrokerConsumer.getAvailablePermits()).isEqualTo(receiverQueueSize);
                assertThat(removedBrokerConsumer.getAvailablePermits()).isEqualTo(receiverQueueSize);
            });
            drainBrokerWorkerGroup(topic);
            assertThat(totalAvailablePermits(dispatcher)).isEqualTo(receiverQueueSize * 2);

            // Follow the production subscription -> dispatcher lock order and hold the dispatcher monitor so the
            // asynchronous Flow task cannot run before Consumer.close removes the consumer.
            synchronized (subscription) {
                synchronized (dispatcher) {
                    removedBrokerConsumer.flowPermits(pendingFlowPermits);
                    assertThat(removedBrokerConsumer.getAvailablePermits())
                            .isEqualTo(receiverQueueSize + pendingFlowPermits);
                    assertThat(totalAvailablePermits(dispatcher)).isEqualTo(receiverQueueSize * 2);
                    removedBrokerConsumer.close();
                }
            }
            // Wait for the broker-side close to remove the consumer from the connection map. The client close is then
            // handled idempotently instead of attempting a second dispatcher removal.
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(removedConsumerCnx.getConsumers()
                            .containsKey(removedBrokerConsumer.consumerId())).isFalse());
            removedClient.close();
            drainBrokerWorkerGroup(topic);

            assertThat(dispatcher.getConsumers()).containsExactly(remainingBrokerConsumer);
            assertThat(totalAvailablePermits(dispatcher))
                    .isEqualTo(remainingBrokerConsumer.getAvailablePermits());

            for (int i = 0; i < messagesToConsume; i++) {
                producer.send(new byte[] {(byte) i});
            }
            for (int i = 0; i < messagesToConsume; i++) {
                Message<byte[]> message = remainingClient.receive(1, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                remainingClient.acknowledge(message);
            }
        }
    }

    private TestContext createTestContext(boolean classic) throws Exception {
        return createTestContext(classic, Shared);
    }

    private TestContext createTestContext(boolean classic, SubType subType) throws Exception {
        String topicName = newTopicName();
        String subscriptionName = "shared-sub";
        admin.topics().createNonPartitionedTopic(topicName);

        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().get();
        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        when(cursor.getName()).thenReturn(subscriptionName);
        when(cursor.isClosed()).thenReturn(true);
        Subscription subscription = mock(PersistentSubscription.class);
        when(subscription.getName()).thenReturn(subscriptionName);
        when(subscription.getTopic()).thenReturn(topic);

        Dispatcher dispatcher;
        if (subType == Key_Shared) {
            dispatcher = classic
                    ? new PersistentStickyKeyDispatcherMultipleConsumersClassic(
                            topic, cursor, subscription, getConfig(), new KeySharedMeta().setKeySharedMode(AUTO_SPLIT))
                    : new PersistentStickyKeyDispatcherMultipleConsumers(
                            topic, cursor, subscription, getConfig(), new KeySharedMeta().setKeySharedMode(AUTO_SPLIT));
        } else {
            dispatcher = classic
                    ? new PersistentDispatcherMultipleConsumersClassic(topic, cursor, subscription)
                    : new PersistentDispatcherMultipleConsumers(topic, cursor, subscription);
        }
        doAnswer(invocation -> {
            dispatcher.consumerFlow(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(subscription).consumerFlow(any(), anyInt());

        Consumer remainingConsumer = createConsumer(subscription, subType, topicName, 1);
        Consumer removedConsumer = createConsumer(subscription, subType, topicName, 2);
        dispatcher.addConsumer(remainingConsumer).join();
        dispatcher.addConsumer(removedConsumer).join();

        return new TestContext(topic, dispatcher, remainingConsumer, removedConsumer);
    }

    private Consumer createConsumer(Subscription subscription, SubType subType, String topicName, long consumerId) {
        TransportCnx cnx = mock(TransportCnx.class);
        PulsarCommandSender commandSender = mock(PulsarCommandSender.class);
        when(cnx.isActive()).thenReturn(true);
        when(cnx.isWritable()).thenReturn(true);
        when(cnx.getCommandSender()).thenReturn(commandSender);
        when(commandSender.sendMessagesToConsumer(anyLong(), anyString(), any(), anyInt(), anyList(),
                any(EntryBatchSizes.class), any(EntryBatchIndexesAcks.class), any(RedeliveryTracker.class), anyLong()))
                .thenReturn(ImmediateEventExecutor.INSTANCE.newSucceededFuture(null));
        return new Consumer(subscription, subType, topicName, consumerId, 0, "consumer-" + consumerId,
                true, cnx, "role", emptyMap(), false, new KeySharedMeta().setKeySharedMode(AUTO_SPLIT),
                MessageId.latest, DEFAULT_CONSUMER_EPOCH);
    }

    private static void simulateDispatch(Dispatcher dispatcher, Consumer consumer, int permits) {
        Entry entry = mock(Entry.class);
        when(entry.getLedgerId()).thenReturn(1L);
        when(entry.getEntryId()).thenReturn(1L);
        EntryBatchSizes batchSizes = EntryBatchSizes.get(1);
        batchSizes.setBatchSize(0, permits);
        EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(1);
        RedeliveryTracker redeliveryTracker = mock(RedeliveryTracker.class);

        consumer.sendMessages(new ArrayList<>(List.of(entry)), batchSizes, batchIndexesAcks,
                permits, 0, 0, redeliveryTracker).syncUninterruptibly();
        decrementTotalAvailablePermits(dispatcher, permits);

        batchSizes.recyle();
        batchIndexesAcks.recycle();
    }

    private static void decrementTotalAvailablePermits(Dispatcher dispatcher, int permits) {
        if (dispatcher instanceof PersistentDispatcherMultipleConsumers pip379Dispatcher) {
            pip379Dispatcher.totalAvailablePermits -= permits;
        } else {
            ((PersistentDispatcherMultipleConsumersClassic) dispatcher).totalAvailablePermits -= permits;
        }
    }

    private static void drainBrokerWorkerGroup(PersistentTopic topic) throws Exception {
        for (EventExecutor eventExecutor : topic.getBrokerService().executor()) {
            eventExecutor.submit(() -> { }).sync();
        }
    }

    private static int totalAvailablePermits(Dispatcher dispatcher) {
        if (dispatcher instanceof PersistentDispatcherMultipleConsumers pip379Dispatcher) {
            return pip379Dispatcher.totalAvailablePermits;
        }
        return ((PersistentDispatcherMultipleConsumersClassic) dispatcher).totalAvailablePermits;
    }

    private static Consumer findConsumer(Dispatcher dispatcher, String consumerName) {
        return dispatcher.getConsumers().stream()
                .filter(consumer -> consumerName.equals(consumer.consumerName()))
                .findFirst()
                .orElseThrow();
    }

    private record TestContext(PersistentTopic topic, Dispatcher dispatcher,
                               Consumer remainingConsumer, Consumer removedConsumer) {
    }
}
