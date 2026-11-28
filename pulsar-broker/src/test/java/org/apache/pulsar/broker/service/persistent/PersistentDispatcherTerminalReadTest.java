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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers.ReadType;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class PersistentDispatcherTerminalReadTest extends SharedPulsarBaseTest {

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @Test(timeOut = 30000)
    public void testLateFailoverStandbyReceivesEndOfTopic() throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().terminateTopicAsync(topicName).get(10, TimeUnit.SECONDS);
        int originalBackoff = getConfig().getDispatcherReadFailureBackoffInitialTimeInMs();
        getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(100);
        AtomicInteger activeNotifications = new AtomicInteger();
        AtomicInteger standbyNotifications = new AtomicInteger();
        ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("subscription").subscriptionType(SubscriptionType.Failover);
        try (org.apache.pulsar.client.api.Consumer<byte[]> active = builder.clone().consumerName("active")
                .priorityLevel(0).messageListener(terminalListener(activeNotifications)).subscribe()) {
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(activeNotifications).hasValue(1));
            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).get().orElseThrow();
            PersistentDispatcherSingleActiveConsumer dispatcher =
                    (PersistentDispatcherSingleActiveConsumer) topic.getSubscription("subscription").getDispatcher();
            Consumer brokerActive = dispatcher.getActiveConsumer();
            // Keep the active consumer connected while adding, closing and reconnecting the standby.
            for (int expected = 1; expected <= 2; expected++) {
                int expectedNotifications = expected;
                try (org.apache.pulsar.client.api.Consumer<byte[]> standby = builder.clone().consumerName("standby")
                        .priorityLevel(1).messageListener(terminalListener(standbyNotifications)).subscribe()) {
                    assertThat(dispatcher.getActiveConsumer()).isSameAs(brokerActive);
                    Awaitility.await().atMost(5, TimeUnit.SECONDS)
                            .untilAsserted(() -> assertThat(standbyNotifications).hasValue(expectedNotifications));
                    assertThat(standby.hasReachedEndOfTopic()).isTrue();
                    assertThat(active.hasReachedEndOfTopic()).isTrue();
                    Awaitility.await().during(1, TimeUnit.SECONDS).atMost(2, TimeUnit.SECONDS)
                            .untilAsserted(() -> {
                                assertThat(activeNotifications).hasValue(1);
                                assertThat(standbyNotifications).hasValue(expectedNotifications);
                            });
                }
            }
        } finally {
            getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(originalBackoff);
        }
    }

    @Test(timeOut = 30000)
    public void testStandbyReceivesEndOfTopicAfterCursorReset() throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().terminateTopicAsync(topicName).get(10, TimeUnit.SECONDS);
        AtomicInteger activeNotifications = new AtomicInteger();
        AtomicInteger standbyNotifications = new AtomicInteger();
        ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("subscription").subscriptionType(SubscriptionType.Failover);
        try (org.apache.pulsar.client.api.Consumer<byte[]> active = builder.clone().consumerName("active")
                .priorityLevel(0).messageListener(terminalListener(activeNotifications)).subscribe()) {
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(activeNotifications).hasValue(1));
            try (org.apache.pulsar.client.api.Consumer<byte[]> standby = builder.clone().consumerName("standby")
                    .priorityLevel(1).messageListener(terminalListener(standbyNotifications)).subscribe()) {
                Awaitility.await().atMost(5, TimeUnit.SECONDS)
                        .untilAsserted(() -> assertThat(standbyNotifications).hasValue(1));
                PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).get().orElseThrow();
                PersistentSubscription subscription = topic.getSubscription("subscription");
                Consumer brokerStandby = subscription.getConsumers().stream()
                        .filter(consumer -> consumer.consumerName().equals("standby")).findFirst().orElseThrow();

                active.seek(MessageId.earliest);

                Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertThat(activeNotifications).hasValue(2);
                    assertThat(standbyNotifications).hasValue(2);
                });
                assertThat(subscription.getConsumers()).anyMatch(consumer -> consumer == brokerStandby);
                assertThat(standby.isConnected()).isTrue();
            }
        }
    }

    @DataProvider
    public Object[][] standbyJoinOrders() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "standbyJoinOrders")
    public void testStandbyJoinAndTerminalCallback(boolean standbyJoinsBeforeEof) throws Exception {
        PersistentSubscription subscription = createTerminatedSubscription(false, false, false);
        PersistentDispatcherSingleActiveConsumer dispatcher = newFailoverDispatcher(subscription);
        Consumer active = newConsumer("active", 0);
        Consumer standby = newConsumer("standby", 1);
        try {
            dispatcher.addConsumer(active).get(10, TimeUnit.SECONDS);
            if (standbyJoinsBeforeEof) {
                dispatcher.addConsumer(standby).get(10, TimeUnit.SECONDS);
            }
            dispatcher.readEntriesFailed(new NoMoreEntriesToReadException("Topic was terminated"), active, 0);
            if (!standbyJoinsBeforeEof) {
                dispatcher.addConsumer(standby).get(10, TimeUnit.SECONDS);
            }
            // Adding before the queued EOF callback and adding after it must both notify each consumer once.
            verify(active).reachedEndOfTopic();
            verify(standby).reachedEndOfTopic();

            // Removal clears notification state, even when the same Consumer object is reattached.
            dispatcher.removeConsumer(standby);
            dispatcher.addConsumer(standby).get(10, TimeUnit.SECONDS);
            verify(standby, times(2)).reachedEndOfTopic();
            verify(active).reachedEndOfTopic();

            // A reset starts another read of the topic; a standby can survive a Failover cursor reset.
            dispatcher.cursorIsReset();
            dispatcher.readEntriesFailed(new NoMoreEntriesToReadException("Topic was terminated"), active, 0);
            verify(active, times(2)).reachedEndOfTopic();
            verify(standby, times(3)).reachedEndOfTopic();

            // An already-notified standby becoming active does not need another notification for the same EOF.
            dispatcher.removeConsumer(active);
            assertThat(dispatcher.getActiveConsumer()).isSameAs(standby);
            dispatcher.readEntriesFailed(new NoMoreEntriesToReadException("Topic was terminated"), standby, 0);
            verify(standby, times(3)).reachedEndOfTopic();
        } finally {
            dispatcher.close(false, Optional.empty()).get(10, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "terminalReadStates")
    public void testStandbyNotificationRequiresPreciselyDrainedNonMigratedTopic(
            boolean hasBacklog, boolean migrated, boolean underestimatedBacklog) throws Exception {
        PersistentSubscription subscription =
                createTerminatedSubscription(hasBacklog, migrated, underestimatedBacklog);
        PersistentDispatcherSingleActiveConsumer dispatcher = newFailoverDispatcher(subscription);
        Consumer active = newConsumer("active", 0);
        Consumer standby = newConsumer("standby", 1);
        try {
            dispatcher.addConsumer(active).get(10, TimeUnit.SECONDS);
            dispatcher.addConsumer(standby).get(10, TimeUnit.SECONDS);
            verify(active, never()).reachedEndOfTopic();
            verify(standby, times(hasBacklog || migrated ? 0 : 1)).reachedEndOfTopic();
            if (migrated) {
                verify((PersistentTopic) subscription.getTopic(), never()).getMigratedClusterUrlAsync();
            }
        } finally {
            dispatcher.close(false, Optional.empty()).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStandbyOnLiveEmptyTopicDoesNotReceiveEndOfTopic() throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "subscription", MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).get().orElseThrow();
        PersistentDispatcherSingleActiveConsumer dispatcher =
                newFailoverDispatcher(topic.getSubscription("subscription"));
        Consumer active = newConsumer("active", 0);
        Consumer standby = newConsumer("standby", 1);
        try {
            dispatcher.addConsumer(active).get(10, TimeUnit.SECONDS);
            dispatcher.addConsumer(standby).get(10, TimeUnit.SECONDS);
            verify(active, never()).reachedEndOfTopic();
            verify(standby, never()).reachedEndOfTopic();
        } finally {
            dispatcher.close(false, Optional.empty()).get(10, TimeUnit.SECONDS);
        }
    }

    private PersistentDispatcherSingleActiveConsumer newFailoverDispatcher(PersistentSubscription subscription) {
        return new PersistentDispatcherSingleActiveConsumer(subscription.getCursor(), SubType.Failover, 0,
                (PersistentTopic) subscription.getTopic(), subscription) {
            @Override
            protected synchronized void scheduleReadOnActiveConsumer() {
                // Deliver EOF callbacks explicitly to exercise both orders without timing dependencies.
            }
        };
    }

    private Consumer newConsumer(String name, int priority) {
        Consumer consumer = mock(Consumer.class);
        when(consumer.consumerName()).thenReturn(name);
        when(consumer.getPriorityLevel()).thenReturn(priority);
        return consumer;
    }

    private MessageListener<byte[]> terminalListener(AtomicInteger notifications) {
        return new MessageListener<>() {
            @Override
            public void reachedEndOfTopic(org.apache.pulsar.client.api.Consumer<byte[]> consumer) {
                notifications.incrementAndGet();
            }

            @Override
            public void received(org.apache.pulsar.client.api.Consumer<byte[]> consumer, Message<byte[]> message) {
                // The topic is empty.
            }
        };
    }

    @DataProvider
    public Object[][] terminalReadStates() {
        return new Object[][] {
                {false, false, false},
                {true, false, false},
                {false, true, false},
                {true, false, true}
        };
    }

    @Test(dataProvider = "terminalReadStates")
    public void testSingleActiveConsumerTerminalReadRetry(boolean hasBacklog, boolean migrated,
                                                         boolean underestimatedBacklog) throws Exception {
        PersistentSubscription subscription =
                createTerminatedSubscription(hasBacklog, migrated, underestimatedBacklog);
        PersistentTopic topic = (PersistentTopic) subscription.getTopic();
        AtomicInteger scheduledReads = new AtomicInteger();
        AtomicInteger notificationAttempts = new AtomicInteger();
        PersistentDispatcherSingleActiveConsumer dispatcher = new PersistentDispatcherSingleActiveConsumer(
                subscription.getCursor(), SubType.Exclusive, 0, topic, subscription) {
            @Override
            void scheduleReadEntriesWithDelay(Consumer consumer, long delay) {
                scheduledReads.incrementAndGet();
            }

            @Override
            protected void checkAndApplyReachedEndOfTopicOrTopicMigration(List<Consumer> consumers) {
                notificationAttempts.incrementAndGet();
                super.checkAndApplyReachedEndOfTopicOrTopicMigration(consumers);
            }
        };
        try {
            dispatcher.readEntriesFailed(new NoMoreEntriesToReadException("Topic was terminated"), null, 0);

            assertThat(scheduledReads).hasValue(hasBacklog || migrated ? 1 : 0);
            assertThat(notificationAttempts).hasValue(hasBacklog && !underestimatedBacklog ? 0 : 1);
            if (migrated) {
                verify(topic).getMigratedClusterUrlAsync();
            }
        } finally {
            dispatcher.close().get(10, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "terminalReadStates")
    public void testMultipleConsumersTerminalReadRetry(boolean hasBacklog, boolean migrated,
                                                      boolean underestimatedBacklog) throws Exception {
        PersistentSubscription subscription =
                createTerminatedSubscription(hasBacklog, migrated, underestimatedBacklog);
        PersistentTopic topic = (PersistentTopic) subscription.getTopic();
        AtomicInteger scheduledReads = new AtomicInteger();
        AtomicInteger notificationAttempts = new AtomicInteger();
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(
                topic, subscription.getCursor(), subscription) {
            @Override
            void scheduleReadEntriesWithDelay(Exception exception, ReadType readType, long delay) {
                scheduledReads.incrementAndGet();
            }

            @Override
            protected void checkAndApplyReachedEndOfTopicOrTopicMigration(List<Consumer> consumers) {
                notificationAttempts.incrementAndGet();
                super.checkAndApplyReachedEndOfTopicOrTopicMigration(consumers);
            }
        };
        try {
            dispatcher.readEntriesFailed(new NoMoreEntriesToReadException("Topic was terminated"), ReadType.Normal);

            assertThat(scheduledReads).hasValue(hasBacklog || migrated ? 1 : 0);
            assertThat(notificationAttempts).hasValue(hasBacklog && !underestimatedBacklog ? 0 : 1);
            if (migrated) {
                verify(topic).getMigratedClusterUrlAsync();
            }
        } finally {
            dispatcher.close().get(10, TimeUnit.SECONDS);
        }
    }

    private PersistentSubscription createTerminatedSubscription(boolean hasBacklog, boolean migrated,
                                                               boolean underestimatedBacklog) throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "subscription", MessageId.earliest);
        PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).get().orElseThrow();
        PersistentSubscription subscription = topic.getSubscription("subscription");
        ManagedCursor cursor = subscription.getCursor();
        if (hasBacklog) {
            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                    .enableBatching(false).create()) {
                producer.send("unacknowledged-message");
            }
            // Reach the read position's end while leaving the message unacknowledged.
            List<Entry> entries = cursor.readEntries(1);
            try {
                assertThat(entries).hasSize(1);
            } finally {
                entries.forEach(Entry::release);
            }
        }
        if (migrated) {
            topic.getManagedLedger().asyncMigrate().get(10, TimeUnit.SECONDS);
            topic = spy(topic);
            doReturn(CompletableFuture.failedFuture(new IllegalStateException("Migration lookup failed")))
                    .when(topic).getMigratedClusterUrlAsync();
            subscription = spy(subscription);
            doReturn(topic).when(subscription).getTopic();
        } else {
            admin.topics().terminateTopicAsync(topicName).get(10, TimeUnit.SECONDS);
        }
        assertThat(cursor.hasMoreEntries()).isFalse();
        assertThat(cursor.hasBacklog(false)).isEqualTo(hasBacklog);
        if (underestimatedBacklog) {
            // Only underestimate the cheap count; the precise check still sees the real unacknowledged message.
            cursor = spy(cursor);
            doReturn(false).when(cursor).hasBacklog(false);
            subscription = spy(subscription);
            doReturn(cursor).when(subscription).getCursor();
        }
        assertThat(cursor.hasBacklog(true)).isEqualTo(hasBacklog);
        return subscription;
    }
}
