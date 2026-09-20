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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
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
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.PulsarTopicCompactionService;
import org.apache.pulsar.compaction.StrategicTwoPhaseCompactor;
import org.apache.pulsar.compaction.TopicCompactionStrategyTest;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class CompactionConcurrencyTest extends SharedPulsarBaseTest {

    @Test(timeOut = 60000)
    public void testStrategicCompactionCloseFailurePreservesPublishedLedger() throws Exception {
        String topicName = newTopicName();
        BookKeeper bookKeeper = getPulsar().getBookKeeperClient();
        var strategy = new TopicCompactionStrategyTest.DummyTopicCompactionStrategy();
        var compactor = new StrategicTwoPhaseCompactor(getConfig(), pulsarClient, bookKeeper,
                getPulsar().getCompactorExecutor());
        try (var producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).enableBatching(false).create()) {
            producer.newMessage().key("key").value("original").send();
            long originalLedgerId = compactor.compact(topicName, strategy).get(15, TimeUnit.SECONDS);
            PersistentTopic topic = (PersistentTopic) getTopic(topicName, false).get().orElseThrow();
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
            assertThatThrownBy(() -> failingCompactor.compact(topicName, strategy).get(15, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(BKException.BKMetadataVersionException.class);
            assertThat(closeFailure.get()).isInstanceOf(BKException.BKMetadataVersionException.class);

            assertThat(cursor.getProperties())
                    .containsEntry(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, originalLedgerId);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(originalPosition);
            var compactedTopic = ((PulsarTopicCompactionService) topic.getTopicCompactionService()).getCompactedTopic();
            assertThat(compactedTopic.getCompactedTopicContextFuture().get(10, TimeUnit.SECONDS).getLedger().getId())
                    .isEqualTo(originalLedgerId);
            assertThat(bookKeeper.getLedgerManager().readLedgerMetadata(originalLedgerId)
                    .get(10, TimeUnit.SECONDS).getValue().isClosed()).isTrue();
            assertThatThrownBy(() -> bookKeeper.getLedgerManager().readLedgerMetadata(failedLedgerId.get())
                    .get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(BKException.BKNoSuchLedgerExistsOnMetadataServerException.class);

            // A normal retry must publish a closed ledger containing the updated value.
            long retryLedgerId = compactor.compact(topicName, strategy).get(15, TimeUnit.SECONDS);
            Awaitility.await().untilAsserted(() -> assertThat(cursor.getProperties())
                    .containsEntry(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, retryLedgerId));
            assertThat(bookKeeper.getLedgerManager().readLedgerMetadata(retryLedgerId)
                    .get(10, TimeUnit.SECONDS).getValue().isClosed()).isTrue();
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
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopic(topicName, false).get().get();

        // Disable compaction.
        admin.topicPolicies().removeCompactionThreshold(topicName);
        CompletableFuture<Long> originalCompaction = persistentTopic.currentCompaction;
        CompletableFuture<Long> blockedCompaction = new CompletableFuture<>();
        persistentTopic.currentCompaction = blockedCompaction;
        try {
            CompletableFuture<Void> firstDelete =
                    admin.topics().deleteSubscriptionAsync(topicName, Compactor.COMPACTION_SUBSCRIPTION);
            Awaitility.await().untilAsserted(() -> assertTrue(persistentTopic.disablingCompaction.get()));

            CompletableFuture<Void> secondDelete =
                    admin.topics().deleteSubscriptionAsync(topicName, Compactor.COMPACTION_SUBSCRIPTION);
            Awaitility.await().untilAsserted(() -> assertTrue(secondDelete.isCompletedExceptionally()));
            try {
                secondDelete.join();
                fail("The second concurrent compaction subscription delete should fail");
            } catch (Exception ex) {
                Throwable actEx = FutureUtil.unwrapCompletionException(ex);
                assertTrue(actEx instanceof PulsarAdminException.PreconditionFailedException);
            }

            blockedCompaction.complete(0L);
            Awaitility.await().untilAsserted(() -> {
                assertTrue(firstDelete.isDone());
                assertFalse(firstDelete.isCompletedExceptionally());
                assertFalse(persistentTopic.disablingCompaction.get());
            });
            firstDelete.join();
        } finally {
            blockedCompaction.complete(0L);
            persistentTopic.currentCompaction = originalCompaction;
        }

        // cleanup.
        producer.close();
    }
}
