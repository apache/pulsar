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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.PulsarTopicCompactionService;
import org.apache.pulsar.compaction.StrategicTwoPhaseCompactor;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class CompactionConcurrencyTest extends SharedPulsarBaseTest {

    @Test(timeOut = 60000)
    public void testStrategicCompactionCloseFailurePreservesPublishedLedger() throws Exception {
        String topicName = newTopicName();
        BookKeeper bookKeeper = getPulsar().getBookKeeperClient();
        var strategy = new TopicCompactionStrategy<String>() {
            @Override
            public Schema<String> getSchema() {
                return Schema.STRING;
            }

            @Override
            public boolean shouldKeepLeft(String previous, String current) {
                return false;
            }
        };
        var compactor = new StrategicTwoPhaseCompactor(getConfig(), pulsarClient, bookKeeper,
                getPulsar().getCompactorExecutor());
        try (var producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).enableBatching(false).create()) {
            producer.newMessage().key("key").value("original").send();
            var originalCompaction = compactor.compact(topicName, strategy);
            assertThat(originalCompaction).succeedsWithin(15, TimeUnit.SECONDS);
            long originalLedgerId = originalCompaction.join();
            var topicFuture = getTopic(topicName, false);
            assertThat(topicFuture).succeedsWithin(10, TimeUnit.SECONDS);
            PersistentTopic topic = (PersistentTopic) topicFuture.join().orElseThrow();
            var cursor = topic.getSubscription(Compactor.COMPACTION_SUBSCRIPTION).getCursor();
            Awaitility.await().untilAsserted(() -> assertThat(cursor.getProperties())
                    .containsEntry(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, originalLedgerId));
            Position originalPosition = cursor.getMarkDeletedPosition();
            producer.newMessage().key("key").value("updated").send();

            AtomicLong failedLedgerId = new AtomicLong(-1);
            AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            var failingCompactor = new StrategicTwoPhaseCompactor(getConfig(), pulsarClient, bookKeeper,
                    getPulsar().getCompactorExecutor()) {
                @Override
                protected CompletableFuture<Void> closeLedger(LedgerHandle ledger) {
                    failedLedgerId.set(ledger.getId());
                    // Persist a conflicting close through the real metadata manager. BookKeeper's own
                    // close path must detect the inconsistent length and report MetadataVersionException.
                    var ledgerManager = bookKeeper.getLedgerManager();
                    return ledgerManager.readLedgerMetadata(ledger.getId()).thenCompose(metadata ->
                            ledgerManager.writeLedgerMetadata(ledger.getId(),
                                    LedgerMetadataBuilder.from(metadata.getValue()).withClosedState()
                                            .withLastEntryId(ledger.getLastAddConfirmed())
                                            .withLength(ledger.getLength() + 1).build(), metadata.getVersion()))
                            .thenCompose(__ -> super.closeLedger(ledger)
                                    .whenComplete((ignored, error) -> closeFailure.set(error)));
                }
            };
            assertThat(failingCompactor.compact(topicName, strategy))
                    .failsWithin(15, TimeUnit.SECONDS)
                    .withThrowableThat()
                    .withRootCauseInstanceOf(BKException.BKMetadataVersionException.class);
            assertThat(closeFailure.get()).isInstanceOf(BKException.BKMetadataVersionException.class);

            assertThat(cursor.getProperties())
                    .containsEntry(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, originalLedgerId);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(originalPosition);
            var compactedTopic = ((PulsarTopicCompactionService) topic.getTopicCompactionService()).getCompactedTopic();
            assertThat(compactedTopic.getCompactedTopicContextFuture())
                    .succeedsWithin(10, TimeUnit.SECONDS)
                    .extracting(context -> context.getLedger().getId())
                    .isEqualTo(originalLedgerId);
            assertThat(bookKeeper.getLedgerManager().readLedgerMetadata(originalLedgerId))
                    .succeedsWithin(10, TimeUnit.SECONDS)
                    .satisfies(metadata -> assertThat(metadata.getValue().isClosed()).isTrue());
            assertThat(bookKeeper.getLedgerManager().readLedgerMetadata(failedLedgerId.get()))
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableThat()
                    .withRootCauseInstanceOf(BKException.BKNoSuchLedgerExistsOnMetadataServerException.class);

            // A normal retry must publish a closed ledger containing the updated value.
            var retryCompaction = compactor.compact(topicName, strategy);
            assertThat(retryCompaction).succeedsWithin(15, TimeUnit.SECONDS);
            long retryLedgerId = retryCompaction.join();
            Awaitility.await().untilAsserted(() -> assertThat(cursor.getProperties())
                    .containsEntry(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, retryLedgerId));
            assertThat(bookKeeper.getLedgerManager().readLedgerMetadata(retryLedgerId))
                    .succeedsWithin(10, TimeUnit.SECONDS)
                    .satisfies(metadata -> assertThat(metadata.getValue().isClosed()).isTrue());
            try (var reader = pulsarClient.newReader(Schema.STRING).topic(topicName)
                    .startMessageId(MessageId.earliest).readCompacted(true).create()) {
                var message = reader.readNext(10, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                assertThat(message.getKey()).isEqualTo("key");
                assertThat(message.getValue()).isEqualTo("updated");
            }
        }
    }

    private void triggerCompactionAndWait(String topicName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopic(topicName, false).get().get();
        persistentTopic.triggerCompaction();
        Awaitility.await().untilAsserted(() -> {
            Position lastConfirmPos = persistentTopic.getManagedLedger().getLastConfirmedEntry();
            Position markDeletePos = persistentTopic
                    .getSubscription(Compactor.COMPACTION_SUBSCRIPTION).getCursor().getMarkDeletedPosition();
            assertEquals(markDeletePos.getLedgerId(), lastConfirmPos.getLedgerId());
            assertEquals(markDeletePos.getEntryId(), lastConfirmPos.getEntryId());
        });
    }

    @Test
    public void testDisableCompactionConcurrently() throws Exception {
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topicPolicies().setCompactionThreshold(topicName, 1);
        admin.topics().createSubscription(topicName, "s1", MessageId.earliest);
        var producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).enableBatching(false).create();
        producer.newMessage().key("k0").value("v0").send();
        triggerCompactionAndWait(topicName);
        admin.topics().deleteSubscription(topicName, "s1");

        // Disable compaction.
        admin.topicPolicies().removeCompactionThreshold(topicName);
        AtomicReference<CompletableFuture<Void>> f1 = new AtomicReference<CompletableFuture<Void>>();
        AtomicReference<CompletableFuture<Void>> f2 = new AtomicReference<CompletableFuture<Void>>();
        new Thread(() -> {
            f1.set(admin.topics().deleteSubscriptionAsync(topicName, "__compaction"));
        }).start();
        new Thread(() -> {
            f2.set(admin.topics().deleteSubscriptionAsync(topicName, "__compaction"));
        }).start();

        // Verify: at least one of the requests should fail (the other may succeed or also fail
        // with "not found" if the in-memory metadata store processes them sequentially).
        Awaitility.await().untilAsserted(() -> {
            assertTrue(f1.get() != null);
            assertTrue(f2.get() != null);
            assertTrue(f1.get().isDone());
            assertTrue(f2.get().isDone());
            assertTrue(f1.get().isCompletedExceptionally() || f2.get().isCompletedExceptionally());
        });

        // cleanup.
        producer.close();
    }
}
