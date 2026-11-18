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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers.ReadType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class PersistentDispatcherTerminalReadTest extends SharedPulsarBaseTest {

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
