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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.CollectorRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.cache.InflightReadsLimiter;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheManagerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.mockito.AdditionalAnswers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Verifies callback isolation using a real broker, bookie and network clients. */
@Test(groups = "broker-api", singleThreaded = true)
public class EntryCacheCallbackIsolationBrokerTest extends SharedPulsarBaseTest {
    private static final String PAYLOAD = "valid-message-through-real-bookie";

    @DataProvider
    public Object[][] scenarios() {
        // Use a long timeout so the test verifies prompt delivery without relying on timeout recovery.
        return new Object[][] {{false, 0L}, {true, 0L}, {true, 60L}};
    }

    @Test(dataProvider = "scenarios", timeOut = 90000)
    public void callbackFailureDoesNotStopSubscriptionsOrRetainReadPermits(boolean callbackFault,
                                                                          long readTimeoutSeconds) throws Exception {
        String topicName = newTopicName();
        CompletableFuture<Void> storageGate = new CompletableFuture<>();
        CompletableFuture<Void> storageReached = new CompletableFuture<>();
        CompletableFuture<Void> localCallbackDone = new CompletableFuture<>();
        BufferProbes originalBuffers = new BufferProbes();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .enableBatching(false).create();
             Consumer<String> first = newConsumer(topicName, "first");
             Consumer<String> second = newConsumer(topicName, "second")) {
            PersistentTopic topic = (PersistentTopic) getTopicIfExists(topicName).get(10, TimeUnit.SECONDS)
                    .orElseThrow();
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
            ledger.getConfig().setReadEntryTimeoutSeconds(readTimeoutSeconds);
            ledger.getConfig().setReadAheadEnabled(false);
            ManagedCursorImpl firstCursor = (ManagedCursorImpl) topic.getSubscription("first").getCursor();
            ManagedCursorImpl secondCursor = (ManagedCursorImpl) topic.getSubscription("second").getCursor();
            producer.send(PAYLOAD);
            Position position = ledger.getLastConfirmedEntry();
            RangeEntryCacheManagerImpl manager = (RangeEntryCacheManagerImpl)
                    getPulsar().getDefaultManagedLedgerFactory().getEntryCacheManager();
            RangeEntryCacheImpl cache = (RangeEntryCacheImpl) manager.getEntryCache(ledger);
            cache.clear();
            InflightReadsLimiter limiter = cache.getPendingReadsLimiter();
            assertThat(limiter.isDisabled()).as("The actual broker read limiter is enabled").isFalse();
            long baseline = limiter.getRemainingBytes();
            ReadHandle actualHandle = ledger.getCurrentLedger();
            long estimatedReadBytes = cache.getEstimatedEntrySize(actualHandle);
            AtomicInteger gatedStorageReads = new AtomicInteger();

            // Delegate to the real current ledger without reopening or fencing it. Only completion timing changes.
            ReadHandle gatedHandle = mock(ReadHandle.class, AdditionalAnswers.delegatesTo(actualHandle));
            doAnswer(invocation -> {
                long from = invocation.getArgument(0);
                long to = invocation.getArgument(1);
                gatedStorageReads.incrementAndGet();
                return actualHandle.readUnconfirmedAsync(from, to).thenCompose(entries -> {
                    for (LedgerEntry entry : entries) {
                        originalBuffers.retain(entry.getEntryBuffer());
                    }
                    storageReached.complete(null);
                    return storageGate.thenApply(ignored -> entries);
                });
            }).when(gatedHandle).readUnconfirmedAsync(anyLong(), anyLong());

            cache.asyncReadEntry(gatedHandle, position.getEntryId(), position.getEntryId(), 0,
                    () -> EntryImpl.NO_EXPECTED_READ_COUNT, new ReadEntriesCallback() {
                        @Override
                        public void readEntriesComplete(List<Entry> entries, Object context) {
                            entries.forEach(Entry::release);
                            localCallbackDone.complete(null);
                            if (callbackFault) {
                                throw new IllegalStateException("Test-local callback failure after release");
                            }
                        }

                        @Override
                        public void readEntriesFailed(ManagedLedgerException exception, Object context) {
                            localCallbackDone.completeExceptionally(exception);
                        }
                    }, null);
            storageReached.get(10, TimeUnit.SECONDS);

            double matchedBefore = matchedReads();
            CompletableFuture<Message<String>> firstReceive = first.receiveAsync();
            CompletableFuture<Message<String>> secondReceive = second.receiveAsync();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(matchedReads() - matchedBefore).as("Both dispatcher reads join the seeded read")
                        .isGreaterThanOrEqualTo(2);
                assertThat(firstCursor.getPendingReadOpsCount()).isEqualTo(1);
                assertThat(secondCursor.getPendingReadOpsCount()).isEqualTo(1);
                assertThat(firstReceive).isNotDone();
                assertThat(secondReceive).isNotDone();
            });
            // Exclusive dispatcher registration holds its monitor until the listener has been installed.
            synchronized (topic.getSubscription("first").getDispatcher()) {
                assertThat(firstCursor.getPendingReadOpsCount()).isEqualTo(1);
            }
            synchronized (topic.getSubscription("second").getDispatcher()) {
                assertThat(secondCursor.getPendingReadOpsCount()).isEqualTo(1);
            }
            assertThat(limiter.getRemainingBytes()).as("Three reads acquired the real limiter's permits")
                    .isEqualTo(baseline - 3 * estimatedReadBytes);
            assertThat(gatedStorageReads.get()).as("Both subscriptions share the original BookKeeper read")
                    .isEqualTo(1);

            storageGate.complete(null);
            localCallbackDone.get(10, TimeUnit.SECONDS);
            // Completion must not wait for the enabled 60-second timeout to recover progress.
            CompletableFuture.allOf(firstReceive, secondReceive).get(10, TimeUnit.SECONDS);
            acknowledgeBoth(first, second, firstReceive, secondReceive, PAYLOAD);
            CompletableFuture<Message<String>> firstNext = first.receiveAsync();
            CompletableFuture<Message<String>> secondNext = second.receiveAsync();
            producer.send("follow-up-valid-message");
            CompletableFuture.allOf(firstNext, secondNext).get(10, TimeUnit.SECONDS);
            acknowledgeBoth(first, second, firstNext, secondNext, "follow-up-valid-message");
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(firstCursor.getNumberOfEntriesInBacklog(false)).isZero();
                assertThat(secondCursor.getNumberOfEntriesInBacklog(false)).isZero();
            });
            first.close();
            second.close();
            ledger.getExecutor().submit(() -> { }).get(10, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(limiter.getRemainingBytes()).as("All real broker read permits return after consumers close")
                        .isEqualTo(baseline);
                assertThat(originalBuffers.snapshot()).hasSize(1).allSatisfy(buffer ->
                        assertThat(buffer.refCnt()).as("Only the test's retained buffer probe remains").isEqualTo(1));
            });
        } finally {
            storageGate.complete(null);
            originalBuffers.close();
        }
    }

    private Consumer<String> newConsumer(String topic, String subscription) throws Exception {
        return pulsarClient.newConsumer(Schema.STRING).topic(topic).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(0).acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
    }

    private static void acknowledgeBoth(Consumer<String> first, Consumer<String> second,
                                        CompletableFuture<Message<String>> firstReceive,
                                        CompletableFuture<Message<String>> secondReceive, String expected)
            throws Exception {
        Message<String> firstMessage = firstReceive.getNow(null);
        Message<String> secondMessage = secondReceive.getNow(null);
        assertThat(firstMessage.getValue()).isEqualTo(expected);
        assertThat(secondMessage.getValue()).isEqualTo(expected);
        first.acknowledge(firstMessage);
        second.acknowledge(secondMessage);
    }

    private static double matchedReads() {
        Double value = CollectorRegistry.defaultRegistry.getSampleValue("pulsar_ml_cache_pendingreads_matched_total");
        return value == null ? 0 : value;
    }

    private static final class BufferProbes implements AutoCloseable {
        private final List<ByteBuf> buffers = new ArrayList<>();
        private boolean closed;

        synchronized void retain(ByteBuf buffer) {
            buffer.retain();
            if (closed) {
                // A storage result can arrive after the test has timed out and closed its probes.
                buffer.release();
            } else {
                buffers.add(buffer);
            }
        }

        synchronized List<ByteBuf> snapshot() {
            return List.copyOf(buffers);
        }

        @Override
        public synchronized void close() {
            closed = true;
            // Release exactly the test's retain, including when the product still owns a reference.
            buffers.forEach(ByteBuf::release);
            buffers.clear();
        }
    }

}
