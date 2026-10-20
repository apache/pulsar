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
package org.apache.pulsar.broker.service.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.CollectorRegistry;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class EntryFilterDispatchFailureTest extends SharedPulsarBaseTest {
    @DataProvider(name = "dispatchFailureModes")
    public Object[][] dispatchFailureModes() {
        return new Object[][] {
                {SubscriptionType.Shared, false, false, false, false},
                {SubscriptionType.Shared, true, false, false, false},
                {SubscriptionType.Key_Shared, false, false, false, false},
                {SubscriptionType.Key_Shared, true, false, false, false},
                {SubscriptionType.Shared, false, true, false, false},
                {SubscriptionType.Shared, true, true, false, false},
                {SubscriptionType.Key_Shared, false, true, false, false},
                {SubscriptionType.Key_Shared, true, true, false, false},
                {SubscriptionType.Shared, false, false, true, false},
                {SubscriptionType.Shared, true, false, true, false},
                {SubscriptionType.Shared, false, false, true, true},
                {SubscriptionType.Shared, true, false, true, true}
        };
    }

    @Test(dataProvider = "dispatchFailureModes", timeOut = 30_000)
    public void testDispatchResumesAfterEntryFilterException(SubscriptionType type, boolean subscriptionThread,
                                                            boolean rejectFirst, boolean chunked,
                                                            boolean failFinalChunk) throws Exception {
        boolean oldSubscriptionThread = getConfig().isDispatcherDispatchMessagesInSubscriptionThread();
        int oldBatchSize = getConfig().getDispatcherMaxRoundRobinBatchSize();
        int oldReadBatchSize = getConfig().getDispatcherMaxReadBatchSize();
        int oldInitialBackoff = getConfig().getDispatcherReadFailureBackoffInitialTimeInMs();
        int oldMaxBackoff = getConfig().getDispatcherReadFailureBackoffMaxTimeInMs();
        EntryFilterProvider provider = getPulsar().getBrokerService().getEntryFilterProvider();
        List<EntryFilter> oldFilters = provider.brokerEntryFilters;
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger chunksSeen = new AtomicInteger();
        AtomicInteger firstChunkAttempts = new AtomicInteger();
        int failureCount = failFinalChunk ? 1 : 2;
        List<ByteBuf> failedBuffers = new CopyOnWriteArrayList<>();
        EntryFilter filter = new EntryFilter() {
            @Override
            public FilterResult filterEntry(Entry entry, FilterContext context) {
                long sequenceId = context.getMsgMetadata().getSequenceId();
                boolean isChunk = context.getMsgMetadata().hasChunkId();
                if (isChunk) {
                    chunksSeen.incrementAndGet();
                    if (sequenceId == 1 && context.getMsgMetadata().getChunkId() == 0) {
                        firstChunkAttempts.incrementAndGet();
                    }
                }
                if (rejectFirst && sequenceId == 0) {
                    return FilterResult.REJECT;
                }
                int failingChunkId = failFinalChunk ? context.getMsgMetadata().getNumChunksFromMsg() - 1 : 0;
                if (sequenceId == 1 && (!isChunk || context.getMsgMetadata().getChunkId() == failingChunkId)
                        && attempts.getAndIncrement() < failureCount) {
                    failedBuffers.add(entry.getDataBuffer());
                    throw new IllegalStateException("entry filter failure");
                }
                return FilterResult.ACCEPT;
            }

            @Override
            public void close() {
            }
        };
        getConfig().setDispatcherDispatchMessagesInSubscriptionThread(subscriptionThread);
        // With one-entry consumer batches, Shared sends m0 before m1 fails. Larger batches exercise
        // a REJECT followed by a failure in the same filtering invocation.
        getConfig().setDispatcherMaxRoundRobinBatchSize(rejectFirst ? 100 : 1);
        if (failFinalChunk) {
            // The first chunk must be handed off in an earlier read, outside the failed batch's replay ownership.
            getConfig().setDispatcherMaxReadBatchSize(1);
        }
        getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(50);
        getConfig().setDispatcherReadFailureBackoffMaxTimeInMs(200);
        // Use the provider's protected test seam; dispatcher construction and callbacks run production code.
        provider.brokerEntryFilters = List.of(filter);
        Double sample = CollectorRegistry.defaultRegistry.getSampleValue("pulsar_broker_pending_bytes_to_dispatch");
        double pendingBytes = sample == null ? 0 : sample;
        try {
            String topicName = newTopicName();
            admin.topics().createNonPartitionedTopic(topicName);
            admin.topics().createSubscription(topicName, "sub", MessageId.earliest);
            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).join().orElseThrow();
            PersistentSubscription subscription = topic.getSubscription("sub");
            var producerBuilder = pulsarClient.newProducer(Schema.STRING).topic(topicName).enableBatching(false);
            if (chunked) {
                producerBuilder.enableChunking(true).chunkMaxMessageSize(1);
            }
            try (Producer<String> producer = producerBuilder.create()) {
                for (int i = 0; i < 3; i++) {
                    producer.newMessage().key("key").sequenceId(i).value("m" + i).send();
                }
            }
            try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .subscriptionName("sub").subscriptionType(type).receiverQueueSize(10).subscribe()) {
                for (int i = rejectFirst ? 1 : 0; i < 3; i++) {
                    Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
                    assertThat(message).as("message %s after filter failure", i).isNotNull();
                    assertThat(message.getValue()).isEqualTo("m" + i);
                    consumer.acknowledge(message);
                }
                Awaitility.await().untilAsserted(() -> {
                    assertThat(subscription.getCursor().getNumberOfEntriesInBacklog(false)).isZero();
                    assertThat(CollectorRegistry.defaultRegistry
                            .getSampleValue("pulsar_broker_pending_bytes_to_dispatch")).isEqualTo(pendingBytes);
                });
                assertThat(attempts.get()).isEqualTo(failureCount + 1);
                assertThat(chunksSeen.get() > 0).isEqualTo(chunked);
                if (failFinalChunk) {
                    assertThat(firstChunkAttempts.get()).as("the transferred prefix is not replayed").isOne();
                }
            }
            // Unloading invalidates the cache; neither failed read may retain its buffers.
            admin.topics().unload(topicName);
            assertThat(failedBuffers).hasSize(failureCount);
            Awaitility.await().untilAsserted(() ->
                    failedBuffers.forEach(buffer -> assertThat(buffer.refCnt()).isZero()));
        } finally {
            provider.brokerEntryFilters = oldFilters;
            getConfig().setDispatcherDispatchMessagesInSubscriptionThread(oldSubscriptionThread);
            getConfig().setDispatcherMaxRoundRobinBatchSize(oldBatchSize);
            getConfig().setDispatcherMaxReadBatchSize(oldReadBatchSize);
            getConfig().setDispatcherReadFailureBackoffInitialTimeInMs(oldInitialBackoff);
            getConfig().setDispatcherReadFailureBackoffMaxTimeInMs(oldMaxBackoff);
        }
    }
}
