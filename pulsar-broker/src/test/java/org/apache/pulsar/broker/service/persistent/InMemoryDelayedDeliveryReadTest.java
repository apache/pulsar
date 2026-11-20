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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.util.Timer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.delayed.InMemoryDelayedDeliveryTracker;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InMemoryDelayedDeliveryReadTest extends SharedPulsarBaseTest {

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @DataProvider
    public Object[][] subscriptionTypes() {
        return new Object[][]{{SubscriptionType.Shared}, {SubscriptionType.Key_Shared}};
    }

    @Test(dataProvider = "subscriptionTypes", timeOut = 60000)
    public void testDueWhileDisconnectedIsDeliveredOnce(SubscriptionType subscriptionType) throws Exception {
        AtomicLong now = new AtomicLong(100_000);
        String topic = newTopicName();
        try (PulsarTestContext context = newControlledContext(now)) {
            PulsarService broker = context.getPulsarService();
            PulsarClient client = broker.getClient();
            try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topic).enableBatching(false).create()) {
                PersistentDispatcherMultipleConsumers dispatcher;
                try (Consumer<String> consumer = subscribe(client, topic, subscriptionType)) {
                    producer.send("unacked");
                    assertThat(receive(consumer).getValue()).isEqualTo("unacked");
                    producer.newMessage().value("delayed").deliverAt(200_000).send();
                    producer.send("initial-marker");
                    consumer.acknowledge(receive(consumer));
                    dispatcher = dispatcher(broker, topic);
                    await().untilAsserted(() -> assertThat(dispatcher.getNumberOfDelayedMessages()).isEqualTo(1));
                }
                await().untilAsserted(() -> assertThat(dispatcher.getConsumers()).isEmpty());
                now.set(300_000);
                try (Consumer<String> consumer = subscribe(client, topic, subscriptionType)) {
                    producer.send("reconnect-marker");
                    assertThat(receiveThroughMarker(consumer, "reconnect-marker"))
                            .containsExactlyInAnyOrder("unacked", "delayed", "reconnect-marker");
                    assertThat(consumer.receive(100, TimeUnit.MILLISECONDS)).isNull();
                    assertThat(dispatcher.getConsumers().get(0).getUnackedMessages()).isEqualTo(3);
                    // Leave both messages unacknowledged: the next rewind must allow their redelivery.
                    await().untilAsserted(() -> assertThat(dispatcher.getDelayedTrackerMemoryUsage()).isZero());
                }
                try (Consumer<String> consumer = subscribe(client, topic, subscriptionType)) {
                    producer.send("second-marker");
                    assertThat(receiveThroughMarker(consumer, "second-marker"))
                            .containsExactlyInAnyOrder("unacked", "delayed", "reconnect-marker", "second-marker");
                    assertThat(consumer.receive(100, TimeUnit.MILLISECONDS)).isNull();
                    broker.getAdminClient().topics().skipAllMessages(topic, "sub");
                }
            }
        }
    }

    @DataProvider
    public Object[][] disableAfterReconnectVariants() {
        return new Object[][]{{SubscriptionType.Shared, false}, {SubscriptionType.Shared, true},
                {SubscriptionType.Key_Shared, false}, {SubscriptionType.Key_Shared, true}};
    }

    @Test(dataProvider = "disableAfterReconnectVariants", timeOut = 60000)
    public void testDisableDelayedDeliveryAfterReconnect(SubscriptionType subscriptionType, boolean cursorCaughtUp)
            throws Exception {
        AtomicLong now = new AtomicLong(100_000);
        String topic = newTopicName();
        try (PulsarTestContext context = newControlledContext(now)) {
            PulsarService broker = context.getPulsarService();
            PulsarClient client = broker.getClient();
            try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topic).enableBatching(false).create()) {
                PersistentDispatcherMultipleConsumers dispatcher;
                Position beforeRewind;
                try (Consumer<String> consumer = subscribe(client, topic, subscriptionType)) {
                    producer.newMessage().value("delayed").deliverAt(200_000).send();
                    producer.send("initial-marker");
                    consumer.acknowledge(receive(consumer));
                    dispatcher = dispatcher(broker, topic);
                    await().untilAsserted(() -> assertThat(dispatcher.getNumberOfDelayedMessages()).isEqualTo(1));
                    beforeRewind = dispatcher.getCursor().getReadPosition();
                }
                // A zero-sized receiver queue registers the replacement but grants no permits until receive().
                try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
                        .subscriptionName("sub").subscriptionType(subscriptionType)
                        .receiverQueueSize(cursorCaughtUp ? 1000 : 0)
                        .subscribe()) {
                    assertThat(dispatcher.getConsumers()).hasSize(1);
                    if (cursorCaughtUp) {
                        await().untilAsserted(() -> assertThat(dispatcher.getCursor().getReadPosition())
                                .isGreaterThanOrEqualTo(beforeRewind));
                    } else {
                        assertThat(dispatcher.getCursor().getReadPosition()).isLessThan(beforeRewind);
                    }
                    broker.getAdminClient().namespaces().setDelayedDeliveryMessages(getNamespace(),
                            DelayedDeliveryPolicies.builder().active(false).tickTime(1).build());
                    await().untilAsserted(() -> assertThat(dispatcher.getTopic().isDelayedDeliveryEnabled()).isFalse());
                    dispatcher.readMoreEntriesAsync();
                    assertThat(receive(consumer).getValue()).isEqualTo("delayed");
                    // Keep the message unacknowledged while its former deadline passes.
                    now.set(300_000);
                    producer.send("after-deadline");
                    assertThat(receive(consumer).getValue()).isEqualTo("after-deadline");
                    await().untilAsserted(() -> assertThat(dispatcher.getNumberOfDelayedMessages()).isZero());
                    await().untilAsserted(() -> assertThat(dispatcher.getDelayedTrackerMemoryUsage()).isZero());
                    broker.getAdminClient().topics().skipAllMessages(topic, "sub");
                }
            }
        }
    }

    private PulsarTestContext newControlledContext(AtomicLong now) throws Exception {
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenAnswer(invocation -> now.get());
        Timer timer = mock(Timer.class);
        DelayedDeliveryTrackerFactory factory = mock(DelayedDeliveryTrackerFactory.class);
        when(factory.newTrackerAsync(any())).thenAnswer(invocation -> CompletableFuture.completedFuture(
                new InMemoryDelayedDeliveryTracker(
                        invocation.getArgument(0), timer, 1, clock, true, 0)));
        PulsarTestContext context = PulsarTestContext.builder().configCustomizer(config -> {
            config.setDispatcherDispatchMessagesInSubscriptionThread(true);
            config.setDelayedDeliveryTickTimeMillis(1);
        }).brokerServiceCustomizer(service -> {
            var spy = BrokerTestUtil.spyWithoutRecordingInvocations(service);
            doReturn(factory).when(spy).getDelayedDeliveryTrackerFactory();
            return spy;
        }).build();
        try {
            PulsarService broker = context.getPulsarService();
            String cluster = broker.getConfiguration().getClusterName();
            broker.getAdminClient().clusters().createCluster(cluster, ClusterData.builder().build());
            broker.getAdminClient().tenants().createTenant(NamespaceName.get(getNamespace()).getTenant(),
                    new TenantInfoImpl(Set.of(), Set.of(cluster)));
            broker.getAdminClient().namespaces().createNamespace(getNamespace(), 1);
            return context;
        } catch (Exception e) {
            context.close();
            throw e;
        }
    }

    private static PersistentDispatcherMultipleConsumers dispatcher(PulsarService broker, String topic) {
        return (PersistentDispatcherMultipleConsumers) broker.getBrokerService().getTopicReference(topic)
                .orElseThrow().getSubscription("sub").getDispatcher();
    }

    private static List<String> receiveThroughMarker(Consumer<String> consumer, String marker) throws Exception {
        List<String> received = new ArrayList<>();
        do {
            received.add(receive(consumer).getValue());
        } while (!received.getLast().equals(marker));
        return received;
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testReconnectSkipsIndexedDelayedMessages(SubscriptionType subscriptionType) throws Exception {
        String topic = newTopicName();
        int delayedMessages = 8;
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).enableBatching(false).create()) {
            PersistentTopic persistentTopic;
            PersistentDispatcherMultipleConsumers dispatcher;
            try (Consumer<String> consumer = subscribe(topic, subscriptionType)) {
                producer.send("unacked");
                for (int i = 0; i < delayedMessages; i++) {
                    producer.newMessage().value("delayed-" + i).deliverAfter(1, TimeUnit.HOURS).send();
                }
                producer.send("initial-marker");
                assertThat(receive(consumer).getValue()).isEqualTo("unacked");
                Message<String> marker = receive(consumer);
                assertThat(marker.getValue()).isEqualTo("initial-marker");
                consumer.acknowledge(marker);
                persistentTopic = (PersistentTopic) getTopicReference(topic).orElseThrow();
                dispatcher = (PersistentDispatcherMultipleConsumers)
                        persistentTopic.getSubscription("sub").getDispatcher();
                await().untilAsserted(() -> assertThat(dispatcher.getNumberOfDelayedMessages())
                        .isEqualTo(delayedMessages));
            }
            await().untilAsserted(() -> assertThat(dispatcher.getConsumers()).isEmpty());
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            // Evict payloads so repeated reads reach BookKeeper; keep the broker's delayed-message index intact.
            ledger.getConfig().setReadAheadEnabled(false);
            ledger.getFactory().getEntryCacheManager().getEntryCache(ledger).clear();
            long readsBeforeReconnect = ledger.getStats().getReadEntriesSucceededTotal();

            try (Consumer<String> consumer = subscribe(topic, subscriptionType)) {
                producer.send("reconnect-marker");
                Message<String> unacked = receive(consumer);
                assertThat(unacked.getValue()).isEqualTo("unacked");
                consumer.acknowledge(unacked);
                Message<String> marker = receive(consumer);
                assertThat(marker.getValue()).isEqualTo("reconnect-marker");
                consumer.acknowledge(marker);

                assertThat(ledger.getStats().getReadEntriesSucceededTotal() - readsBeforeReconnect)
                        .as("only the unacked immediate message needs a storage read; the new marker is cached")
                        .isEqualTo(1);
                assertThat(dispatcher.getNumberOfDelayedMessages()).isEqualTo(delayedMessages);

                admin.topics().skipAllMessages(topic, "sub");
                assertThat(dispatcher.getNumberOfDelayedMessages()).isZero();
                assertThat(dispatcher.getDelayedTrackerMemoryUsage()).isZero();
            }
        }
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testReconnectDoesNotPauseUnackedMessagesBehindFixedDelayLookahead(
            SubscriptionType subscriptionType) throws Exception {
        String topic = newTopicName();
        // The factory captures lookahead at broker startup; isolate the opt-in ordered-delivery configuration.
        try (PulsarTestContext context = PulsarTestContext.builder().configCustomizer(config -> {
            config.setDelayedDeliveryAssumeOrderedDeliveryTimes(true);
            config.setDelayedDeliveryFixedDelayDetectionLookahead(4);
        }).build()) {
            PulsarService broker = context.getPulsarService();
            String cluster = broker.getConfiguration().getClusterName();
            broker.getAdminClient().clusters().createCluster(cluster, ClusterData.builder().build());
            broker.getAdminClient().tenants().createTenant(NamespaceName.get(getNamespace()).getTenant(),
                    new TenantInfoImpl(Set.of(), Set.of(cluster)));
            broker.getAdminClient().namespaces().createNamespace(getNamespace(), 1);
            PulsarClient client = broker.getClient();
            try (Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topic).enableBatching(false).create()) {
                PersistentDispatcherMultipleConsumers dispatcher;
                try (Consumer<String> consumer = subscribe(client, topic, subscriptionType)) {
                    // The immediate message precedes creation of the tracker and remains unacknowledged.
                    producer.send("unacked");
                    assertThat(receive(consumer).getValue()).isEqualTo("unacked");
                    for (int i = 0; i < 4; i++) {
                        producer.newMessage().value("delayed-" + i).deliverAfter(1, TimeUnit.HOURS).send();
                    }
                    dispatcher = (PersistentDispatcherMultipleConsumers) broker.getBrokerService()
                            .getTopicReference(topic).orElseThrow().getSubscription("sub").getDispatcher();
                    await().untilAsserted(() -> {
                        assertThat(dispatcher.getNumberOfDelayedMessages()).isEqualTo(4);
                        assertThat(dispatcher.shouldPauseDeliveryForDelayTracker()).isTrue();
                    });
                }
                await().untilAsserted(() -> assertThat(dispatcher.getConsumers()).isEmpty());
                try (Consumer<String> consumer = subscribe(client, topic, subscriptionType)) {
                    Message<String> unacked = receive(consumer);
                    assertThat(unacked.getValue()).isEqualTo("unacked");
                    consumer.acknowledge(unacked);
                    assertThat(dispatcher.getNumberOfDelayedMessages()).isEqualTo(4);
                    broker.getAdminClient().topics().skipAllMessages(topic, "sub");
                }
            }
        }
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testSeekAndUnloadReleaseRetainedIndex(SubscriptionType subscriptionType) throws Exception {
        String topic = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).enableBatching(false).create()) {
            PersistentDispatcherMultipleConsumers dispatcher;
            try (Consumer<String> consumer = subscribe(topic, subscriptionType)) {
                producer.newMessage().value("before-seek").deliverAfter(1, TimeUnit.HOURS).send();
                producer.send("marker");
                consumer.acknowledge(receive(consumer));
                dispatcher = dispatcher(topic);
                await().untilAsserted(() -> assertThat(dispatcher.getNumberOfDelayedMessages()).isEqualTo(1));
            }
            admin.topics().resetCursor(topic, "sub", MessageId.latest);
            assertThat(dispatcher.getNumberOfDelayedMessages()).isZero();
            assertThat(dispatcher.getDelayedTrackerMemoryUsage()).isZero();

            try (Consumer<String> consumer = subscribe(topic, subscriptionType)) {
                producer.newMessage().value("after-seek").deliverAfter(1, TimeUnit.HOURS).send();
                producer.send("after-seek-marker");
                Message<String> marker = receive(consumer);
                assertThat(marker.getValue()).isEqualTo("after-seek-marker");
                consumer.acknowledge(marker);
                await().untilAsserted(() -> assertThat(dispatcher.getNumberOfDelayedMessages()).isEqualTo(1));
            }
            admin.topics().unload(topic);
            assertThat(dispatcher.getNumberOfDelayedMessages()).isZero();
            assertThat(dispatcher.getDelayedTrackerMemoryUsage()).isZero();
        }
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testDisableDelayedDeliveryBeforeReconnect(SubscriptionType subscriptionType) throws Exception {
        String topic = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).enableBatching(false).create()) {
            PersistentDispatcherMultipleConsumers dispatcher;
            try (Consumer<String> consumer = subscribe(topic, subscriptionType)) {
                producer.newMessage().value("delayed").deliverAfter(1, TimeUnit.HOURS).send();
                producer.send("marker");
                consumer.acknowledge(receive(consumer));
                dispatcher = dispatcher(topic);
                await().untilAsserted(() -> assertThat(dispatcher.getNumberOfDelayedMessages()).isEqualTo(1));
            }
            admin.namespaces().setDelayedDeliveryMessages(getNamespace(),
                    DelayedDeliveryPolicies.builder().active(false).tickTime(1000).build());
            await().untilAsserted(() -> assertThat(dispatcher.getTopic().isDelayedDeliveryEnabled()).isFalse());
            try (Consumer<String> consumer = subscribe(topic, subscriptionType)) {
                Message<String> message = receive(consumer);
                assertThat(message.getValue()).isEqualTo("delayed");
                consumer.acknowledge(message);
                assertThat(dispatcher.getNumberOfDelayedMessages()).isZero();
                assertThat(dispatcher.getDelayedTrackerMemoryUsage()).isZero();
            }
        }
    }

    @Test
    public void testReconnectDropsExpiredPositions() throws Exception {
        String topic = newTopicName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).enableBatching(false).create()) {
            PersistentDispatcherMultipleConsumers dispatcher;
            try (Consumer<String> consumer = subscribe(topic, SubscriptionType.Shared)) {
                producer.newMessage().value("expired").deliverAfter(1, TimeUnit.HOURS).send();
                producer.send("marker");
                consumer.acknowledge(receive(consumer));
                dispatcher = dispatcher(topic);
                await().untilAsserted(() -> assertThat(dispatcher.getNumberOfDelayedMessages()).isEqualTo(1));
            }
            admin.topics().expireMessages(topic, "sub", 0);
            await().untilAsserted(() -> assertThat(dispatcher.getCursor().getNumberOfEntriesInBacklog(false)).isZero());
            try (Consumer<String> consumer = subscribe(topic, SubscriptionType.Shared)) {
                producer.send("after-expiry");
                Message<String> message = receive(consumer);
                assertThat(message.getValue()).isEqualTo("after-expiry");
                consumer.acknowledge(message);
                assertThat(dispatcher.getNumberOfDelayedMessages()).isZero();
                assertThat(dispatcher.getDelayedTrackerMemoryUsage()).isZero();
            }
        }
    }

    private PersistentDispatcherMultipleConsumers dispatcher(String topic) {
        PersistentTopic persistentTopic = (PersistentTopic) getTopicReference(topic).orElseThrow();
        return (PersistentDispatcherMultipleConsumers) persistentTopic.getSubscription("sub").getDispatcher();
    }

    private Consumer<String> subscribe(String topic, SubscriptionType subscriptionType) throws Exception {
        return subscribe(pulsarClient, topic, subscriptionType);
    }

    private static Consumer<String> subscribe(PulsarClient client, String topic, SubscriptionType subscriptionType)
            throws Exception {
        return client.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub")
                .subscriptionType(subscriptionType).acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .isAckReceiptEnabled(true).subscribe();
    }

    private static Message<String> receive(Consumer<String> consumer) throws Exception {
        Message<String> message = consumer.receiveAsync().get(10, TimeUnit.SECONDS);
        assertThat(message).as("a non-delayed message should be dispatched").isNotNull();
        return message;
    }
}
