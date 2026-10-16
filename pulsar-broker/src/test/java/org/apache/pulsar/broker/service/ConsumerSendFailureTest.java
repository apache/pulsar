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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.CollectorRegistry;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.SharedDispatcherPermitAccountingTest.PermitAssertions;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ConsumerSendFailureTest extends SharedPulsarBaseTest {
    @DataProvider(name = "cleanupRaceModes")
    public Object[][] cleanupRaceModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "cleanupRaceModes", timeOut = 30_000)
    public void testRejectedSendCleanupAfterOrdinaryConsumerRemoval(boolean subscriptionThread) throws Exception {
        boolean oldSubscriptionThread = getConfig().isDispatcherDispatchMessagesInSubscriptionThread();
        int oldInitialBackoff = getConfig().getDispatcherReadFailureBackoffInitialTimeInMs();
        int oldMaxBackoff = getConfig().getDispatcherReadFailureBackoffMaxTimeInMs();
        getConfig().setDispatcherDispatchMessagesInSubscriptionThread(subscriptionThread);
        getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(50);
        getConfig().setDispatcherReadFailureBackoffMaxTimeInMs(200);
        try {
            String topicName = newTopicName();
            var builder = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName("sub")
                    .subscriptionType(SubscriptionType.Shared).receiverQueueSize(1);
            try (var primary = builder.clone().consumerName("primary").priorityLevel(0).subscribe();
                 var secondary = builder.clone().consumerName("secondary").priorityLevel(1).subscribe();
                 Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                         .enableBatching(false).create()) {
                PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
                PersistentSubscription subscription = topic.getSubscription("sub");
                PersistentDispatcherMultipleConsumers dispatcher =
                        (PersistentDispatcherMultipleConsumers) subscription.getDispatcher();
                Consumer brokerConsumer = dispatcher.getConsumers().stream()
                        .filter(consumer -> consumer.consumerName().equals("primary")).findFirst().orElseThrow();
                Awaitility.await().untilAsserted(() -> assertThat(brokerConsumer.getAvailablePermits()).isOne());
                producer.send("before failure");
                Message<String> first = primary.receive(5, TimeUnit.SECONDS);
                assertThat(first).isNotNull();
                primary.acknowledge(first);
                Awaitility.await().untilAsserted(() -> {
                    assertThat(brokerConsumer.getAvailablePermits()).isOne();
                    assertThat(brokerConsumer.getUnackedMessages()).isZero();
                    assertThat(brokerConsumer.getMsgOutCounter()).isOne();
                });
                long expectedUsage = topic.currentUsageCount() - 1;
                long expectedRemovedMessages = brokerConsumer.getMsgOutCounter();
                long expectedRemovedBytes = brokerConsumer.getBytesOutCounter();
                assertThat(expectedRemovedBytes).isPositive();
                ServerCnx cnx = (ServerCnx) brokerConsumer.cnx();
                PulsarCommandSender originalSender = cnx.getCommandSender();
                PulsarCommandSender sender = Mockito.spy(originalSender);
                AtomicBoolean rejected = new AtomicBoolean();
                doAnswer(invocation -> {
                    if (rejected.compareAndSet(false, true)) {
                        assertThat((long) invocation.getArgument(0)).isEqualTo(brokerConsumer.consumerId());
                        throw new RejectedExecutionException("send event loop rejected the task");
                    }
                    return invocation.callRealMethod();
                }).when(sender).sendMessagesToConsumer(anyLong(), anyString(), any(), anyInt(),
                        any(), any(), any(), any(), anyLong());
                try {
                    cnx.setCommandSender(sender);
                    CompletableFuture<Void> primaryClosed;
                    synchronized (subscription) {
                        producer.send("after failure");
                        Awaitility.await().untilTrue(rejected);
                        synchronized (dispatcher) {
                            // Dispatch has finished queuing cleanup, which cannot close the consumer while this
                            // thread holds the subscription lock. Ordinary close must therefore win the race.
                            assertThat(dispatcher.getConsumers()).contains(brokerConsumer);
                        }
                        brokerConsumer.close();
                        // The connection no longer contains this consumer. Closing its client now prevents the
                        // delayed broker notification from reconnecting it and obscuring the usage-count assertion.
                        primaryClosed = primary.closeAsync();
                        assertThat(topic.currentUsageCount()).isEqualTo(expectedUsage);
                    }
                    primaryClosed.get(5, TimeUnit.SECONDS);
                    Message<String> replayed = secondary.receive(5, TimeUnit.SECONDS);
                    assertThat(replayed).as("delivery after deferred cleanup completes").isNotNull();
                    assertThat(replayed.getValue()).isEqualTo("after failure");
                    secondary.acknowledge(replayed);
                    Awaitility.await().untilAsserted(() -> {
                        assertThat(subscription.getCursor().getNumberOfEntriesInBacklog(false)).isZero();
                        assertThat(dispatcher.getConsumers()).hasSize(1);
                        assertThat(dispatcher.getConsumers().get(0).getMsgOutCounter()).isOne();
                    });
                    long connectedMessages = dispatcher.getConsumers().stream()
                            .mapToLong(Consumer::getMsgOutCounter).sum();
                    long connectedBytes = dispatcher.getConsumers().stream()
                            .mapToLong(Consumer::getBytesOutCounter).sum();
                    SoftAssertions.assertSoftly(softly -> {
                        softly.assertThat(topic.currentUsageCount()).as("usage is decremented only once")
                                .isEqualTo(expectedUsage);
                        softly.assertThat(subscription.getMsgOutCounter() - connectedMessages)
                                .as("removed-consumer message statistics are preserved once")
                                .isEqualTo(expectedRemovedMessages);
                        softly.assertThat(subscription.getBytesOutCounter() - connectedBytes)
                                .as("removed-consumer byte statistics are preserved once")
                                .isEqualTo(expectedRemovedBytes);
                    });
                } finally {
                    cnx.setCommandSender(originalSender);
                    Mockito.reset(sender);
                    Mockito.framework().clearInlineMock(sender);
                }
            }
        } finally {
            getConfig().setDispatcherDispatchMessagesInSubscriptionThread(oldSubscriptionThread);
            getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(oldInitialBackoff);
            getConfig().setDispatcherReadFailureBackoffMaxTimeInMs(oldMaxBackoff);
        }
    }

    @DataProvider(name = "sendFailureModes")
    public Object[][] sendFailureModes() {
        return new Object[][] {
                {SubscriptionType.Shared, false, false, false}, {SubscriptionType.Shared, true, false, false},
                {SubscriptionType.Key_Shared, false, false, false}, {SubscriptionType.Key_Shared, true, false, false},
                {SubscriptionType.Shared, false, true, false}, {SubscriptionType.Shared, true, true, false},
                {SubscriptionType.Shared, false, false, true}, {SubscriptionType.Shared, true, false, true},
                {SubscriptionType.Key_Shared, false, false, true}, {SubscriptionType.Key_Shared, true, false, true},
                {SubscriptionType.Shared, false, true, true}, {SubscriptionType.Shared, true, true, true}
        };
    }

    @Test(dataProvider = "sendFailureModes", timeOut = 30_000)
    public void testRejectedSendReleasesBatchAndRecovers(
            SubscriptionType type, boolean subscriptionThread, boolean chunked, boolean additionalConsumer)
            throws Exception {
        boolean oldSubscriptionThread = getConfig().isDispatcherDispatchMessagesInSubscriptionThread();
        int oldReadBatchSize = getConfig().getDispatcherMaxReadBatchSize();
        int oldInitialBackoff = getConfig().getDispatcherReadFailureBackoffInitialTimeInMs();
        int oldMaxBackoff = getConfig().getDispatcherReadFailureBackoffMaxTimeInMs();
        getConfig().setDispatcherDispatchMessagesInSubscriptionThread(subscriptionThread);
        getConfig().setDispatcherMaxReadBatchSize(1);
        getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(50);
        getConfig().setDispatcherReadFailureBackoffMaxTimeInMs(200);
        try {
            String topicName = newTopicName();
            admin.topics().createNonPartitionedTopic(topicName);
            LinkedBlockingQueue<Message<String>> received = new LinkedBlockingQueue<>();
            var consumerBuilder = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .subscriptionName("sub").subscriptionType(type).receiverQueueSize(1)
                    .messageListener((client, message) -> {
                        received.add(message);
                        client.acknowledgeAsync(message);
                    });
            try (var clientConsumer = consumerBuilder.clone().consumerName("primary").subscribe();
                 var otherClient = additionalConsumer
                         ? consumerBuilder.clone().consumerName("secondary").subscribe() : null) {
                PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
                PersistentSubscription subscription = topic.getSubscription("sub");
                PersistentDispatcherMultipleConsumers dispatcher =
                        (PersistentDispatcherMultipleConsumers) subscription.getDispatcher();
                Awaitility.await().untilAsserted(() -> dispatcher.getConsumers()
                        .forEach(consumer -> assertThat(consumer.getAvailablePermits()).isOne()));
                ServerCnx cnx = (ServerCnx) dispatcher.getConsumers().get(0).cnx();
                dispatcher.getConsumers().forEach(consumer -> assertThat(consumer.cnx()).isSameAs(cnx));
                PulsarCommandSender originalSender = cnx.getCommandSender();
                PulsarCommandSender sender = Mockito.spy(originalSender);
                AtomicInteger sends = new AtomicInteger();
                AtomicReference<Consumer> failedConsumer = new AtomicReference<>();
                List<Entry> failedEntries = new CopyOnWriteArrayList<>();
                List<ByteBuf> failedBuffers = new CopyOnWriteArrayList<>();
                doAnswer(invocation -> {
                    if (sends.incrementAndGet() != 1) {
                        return invocation.callRealMethod();
                    }
                    long consumerId = invocation.getArgument(0);
                    failedConsumer.set(dispatcher.getConsumers().stream()
                            .filter(consumer -> consumer.consumerId() == consumerId).findFirst().orElseThrow());
                    List<? extends Entry> entries = invocation.getArgument(4);
                    failedEntries.addAll(entries);
                    entries.forEach(entry -> failedBuffers.add(entry.getDataBuffer()));
                    // Only the external send fails; Consumer has already registered its pending ACK and charged
                    // its permit. Dispatcher construction, managed-ledger reads and callbacks remain real.
                    throw new RejectedExecutionException("send event loop rejected the task");
                }).when(sender).sendMessagesToConsumer(anyLong(), anyString(), any(), anyInt(),
                        any(), any(), any(), any(), anyLong());
                Double sample = CollectorRegistry.defaultRegistry
                        .getSampleValue("pulsar_broker_pending_bytes_to_dispatch");
                double pendingBytes = sample == null ? 0 : sample;
                try {
                    synchronized (dispatcher) {
                        cnx.setCommandSender(sender);
                    }
                    var producerBuilder = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                            .enableBatching(false);
                    if (chunked) {
                        producerBuilder.enableChunking(true).chunkMaxMessageSize(1);
                    }
                    try (Producer<String> producer = producerBuilder.create()) {
                        producer.newMessage().key("key").value("m0").send();
                    }
                    Awaitility.await().untilAsserted(() -> {
                        assertThat(sends.get()).isPositive();
                        assertThat(CollectorRegistry.defaultRegistry
                                .getSampleValue("pulsar_broker_pending_bytes_to_dispatch")).isEqualTo(pendingBytes);
                    });
                    synchronized (dispatcher) {
                        assertThat(failedEntries).hasSize(1);
                        assertThat(((EntryAndMetadata) failedEntries.get(0)).isOwnedByDispatcher()).isFalse();
                        // A surviving consumer prevents the last-consumer reset from hiding a missed permit debit.
                        PermitAssertions.assertAvailablePermitsConsistent(dispatcher);
                    }
                    assertThat(((EntryAndMetadata) failedEntries.get(0)).getMetadata().hasChunkId())
                            .isEqualTo(chunked);
                    Message<String> message = received.poll(10, TimeUnit.SECONDS);
                    assertThat(message).as("delivery after the rejected send").isNotNull();
                    assertThat(message.getValue()).isEqualTo("m0");
                    Awaitility.await().untilAsserted(() -> {
                        assertThat(subscription.getCursor().getNumberOfEntriesInBacklog(false)).isZero();
                        assertThat(dispatcher.getTotalUnackedMessages()).isZero();
                        assertThat(dispatcher.getConsumers().stream()
                                .noneMatch(consumer -> consumer == failedConsumer.get())).isTrue();
                        assertThat(CollectorRegistry.defaultRegistry
                                .getSampleValue("pulsar_broker_pending_bytes_to_dispatch")).isEqualTo(pendingBytes);
                    });
                    assertThat(received).isEmpty();
                } finally {
                    synchronized (dispatcher) {
                        cnx.setCommandSender(originalSender);
                    }
                    Mockito.reset(sender);
                    Mockito.framework().clearInlineMock(sender);
                }
                // Only production cleanup releases the rejected read; invalidating the cache must free its buffers.
                admin.topics().unload(topicName);
                Awaitility.await().untilAsserted(() ->
                        failedBuffers.forEach(buffer -> assertThat(buffer.refCnt()).isZero()));
            }
        } finally {
            getConfig().setDispatcherDispatchMessagesInSubscriptionThread(oldSubscriptionThread);
            getConfig().setDispatcherMaxReadBatchSize(oldReadBatchSize);
            getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(oldInitialBackoff);
            getConfig().setDispatcherReadFailureBackoffMaxTimeInMs(oldMaxBackoff);
        }
    }
}
