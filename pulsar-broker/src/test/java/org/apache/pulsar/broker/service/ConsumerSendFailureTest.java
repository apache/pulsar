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
import io.prometheus.client.CollectorRegistry;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ConsumerSendFailureTest extends SharedPulsarBaseTest {
    @DataProvider(name = "sendFailureModes")
    public Object[][] sendFailureModes() {
        return new Object[][] {
                {SubscriptionType.Shared, false, false}, {SubscriptionType.Shared, true, false},
                {SubscriptionType.Key_Shared, false, false}, {SubscriptionType.Key_Shared, true, false},
                {SubscriptionType.Shared, false, true}, {SubscriptionType.Shared, true, true}
        };
    }

    @Test(dataProvider = "sendFailureModes", timeOut = 30_000)
    public void testSynchronousSendFailureDoesNotReplayConsumerPendingAcks(
            SubscriptionType type, boolean subscriptionThread, boolean chunked) throws Exception {
        boolean oldSubscriptionThread = getConfig().isDispatcherDispatchMessagesInSubscriptionThread();
        int oldReadBatchSize = getConfig().getDispatcherMaxReadBatchSize();
        getConfig().setDispatcherDispatchMessagesInSubscriptionThread(subscriptionThread);
        getConfig().setDispatcherMaxReadBatchSize(1);
        try {
            String topicName = newTopicName();
            admin.topics().createNonPartitionedTopic(topicName);
            try (var clientConsumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .subscriptionName("sub").subscriptionType(type).receiverQueueSize(1).subscribe()) {
                PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
                PersistentSubscription subscription = topic.getSubscription("sub");
                PersistentDispatcherMultipleConsumers dispatcher =
                        (PersistentDispatcherMultipleConsumers) subscription.getDispatcher();
                Consumer consumer = dispatcher.getConsumers().get(0);
                Awaitility.await().untilAsserted(() -> assertThat(consumer.getAvailablePermits()).isOne());
                ServerCnx cnx = (ServerCnx) consumer.cnx();
                PulsarCommandSender originalSender = cnx.getCommandSender();
                PulsarCommandSender sender = Mockito.spy(originalSender);
                AtomicInteger sends = new AtomicInteger();
                List<Entry> failedEntries = new CopyOnWriteArrayList<>();
                List<EntryBatchSizes> failedBatchSizes = new CopyOnWriteArrayList<>();
                List<EntryBatchIndexesAcks> failedBatchIndexes = new CopyOnWriteArrayList<>();
                doAnswer(invocation -> {
                    List<? extends Entry> entries = invocation.getArgument(4);
                    failedEntries.addAll(entries);
                    failedBatchSizes.add(invocation.getArgument(5));
                    failedBatchIndexes.add(invocation.getArgument(6));
                    sends.incrementAndGet();
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
                        assertThat(sends.get()).isOne();
                        assertThat(CollectorRegistry.defaultRegistry
                                .getSampleValue("pulsar_broker_pending_bytes_to_dispatch")).isEqualTo(pendingBytes);
                    });
                    synchronized (dispatcher) {
                        // The failed batch is still registered on this Consumer. Replaying it here would charge
                        // its bookkeeping again or stall on its exhausted permit instead of handling a send failure.
                        assertThat(dispatcher.getNumberOfMessagesInReplay()).isZero();
                        assertThat(consumer.getAvailablePermits()).isZero();
                        assertThat(consumer.getPendingAcks().size()).isOne();
                        assertThat(consumer.getStats().unackedMessages).isOne();
                    }
                    assertThat(failedEntries).hasSize(1);
                    assertThat(((EntryAndMetadata) failedEntries.get(0)).getMetadata().hasChunkId())
                            .isEqualTo(chunked);
                } finally {
                    synchronized (dispatcher) {
                        cnx.setCommandSender(originalSender);
                        // The injected sender deliberately throws without taking ownership. Release the test's
                        // captured batch; on the old recovery path it has already been released by the dispatcher.
                        for (Entry entry : failedEntries) {
                            if (((EntryAndMetadata) entry).isOwnedByDispatcher()) {
                                entry.release();
                            }
                        }
                        failedBatchSizes.forEach(EntryBatchSizes::recyle);
                        failedBatchIndexes.stream().filter(indexes -> indexes != null)
                                .forEach(EntryBatchIndexesAcks::recycle);
                    }
                    Mockito.reset(sender);
                    Mockito.framework().clearInlineMock(sender);
                }
            }
        } finally {
            getConfig().setDispatcherDispatchMessagesInSubscriptionThread(oldSubscriptionThread);
            getConfig().setDispatcherMaxReadBatchSize(oldReadBatchSize);
        }
    }
}
