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
package org.apache.pulsar.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException.BrokerMetadataException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker", timeOut = 60000)
public class MissingCompactedLedgerTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] missingLedgerCases() {
        return new Object[][] {{false, false}, {false, true}, {true, false}};
    }

    @Test(dataProvider = "missingLedgerCases")
    public void testMissingLedgerDoesNotBecomeAnUncompactedTopic(boolean retainSource, boolean appendTail)
            throws Exception {
        String topic = createTopicWithMissingLedger(retainSource, appendTail);
        try (Reader<String> reader = reader(topic, true)) {
            assertThatThrownBy(() -> reader.getLastMessageIdsAsync().get(10, TimeUnit.SECONDS))
                    .as("missing compacted data must not be reported as an empty or partial topic")
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(BrokerMetadataException.class);
        }
        assertThatThrownBy(() -> compactor().compact(topic).get(20, TimeUnit.SECONDS))
                .as("another compaction must not publish an incomplete replacement")
                .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(BrokerMetadataException.class);

        // Ordinary readers can still consume the retained source entries. Only compacted reads
        // promise the old key that may no longer exist in those entries.
        try (Reader<String> reader = reader(topic, false)) {
            Set<String> keys = new HashSet<>();
            while (reader.hasMessageAvailable()) {
                var message = reader.readNext(10, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                keys.add(message.getKey());
            }
            if (retainSource) {
                assertThat(keys).contains("old-key");
            } else {
                assertThat(keys).doesNotContain("old-key");
            }
            if (appendTail) {
                assertThat(keys).contains("tail-key");
            }
        }
    }

    @Test
    public void testOrdinaryReaderCanSeekWithMissingCompactedLedger() throws Exception {
        String topic = createTopicWithMissingLedger(true, false);
        try (Reader<String> reader = reader(topic, false)) {
            assertThat(reader.readNext(10, TimeUnit.SECONDS)).isNotNull()
                    .satisfies(message -> assertThat(message.getKey()).isEqualTo("old-key"));
            reader.seekAsync(MessageId.earliest).get(10, TimeUnit.SECONDS);
            assertThat(reader.readNext(10, TimeUnit.SECONDS)).isNotNull()
                    .satisfies(message -> assertThat(message.getKey()).isEqualTo("old-key"));
        }
        // Seeking an ordinary reader must not clear the failure for compacted readers.
        try (Reader<String> reader = reader(topic, true)) {
            assertThatThrownBy(() -> reader.getLastMessageIdsAsync().get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(BrokerMetadataException.class);
        }
    }

    @Test
    public void testDeleteCompactionSubscriptionClearsMissingLedger() throws Exception {
        String topic = createTopicWithMissingLedger(true, false);
        PersistentTopic persistentTopic = topic(topic);
        admin.topics().deleteSubscription(topic, Compactor.COMPACTION_SUBSCRIPTION);
        // Unsubscribe completes before the in-memory compaction cleanup callback runs.
        Awaitility.await().until(() -> persistentTopic.getTopicCompactionService().getLastCompactedPosition()
                .handle((position, error) -> error == null && position == null).get(5, TimeUnit.SECONDS));
        assertThat(persistentTopic.getSubscription(Compactor.COMPACTION_SUBSCRIPTION)).isNull();
        assertThat(topic(topic)).isSameAs(persistentTopic);
        try (Reader<String> reader = reader(topic, true)) {
            assertThat(reader.readNext(10, TimeUnit.SECONDS)).isNotNull()
                    .satisfies(message -> assertThat(message.getKey()).isEqualTo("old-key"));
        }
        // Explicitly discarding compaction allows a fresh compaction from the retained originals.
        assertThat(compactor().compact(topic).get(20, TimeUnit.SECONDS)).isGreaterThanOrEqualTo(0);
        try (Reader<String> reader = reader(topic, true)) {
            assertThat(reader.readNext(10, TimeUnit.SECONDS)).isNotNull()
                    .satisfies(message -> assertThat(message.getKey()).isEqualTo("old-key"));
            assertThat(reader.readNext(10, TimeUnit.SECONDS)).isNotNull()
                    .satisfies(message -> assertThat(message.getKey()).isEqualTo("new-key"));
        }
    }

    @Test
    public void testNeverCompactedTopicStillReadsOriginalEntries() throws Exception {
        String topic = newTopicName();
        admin.namespaces().setRetention(getNamespace(), new RetentionPolicies(-1, -1));
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create()) {
            producer.newMessage().key("key").value("value").send();
        }
        try (Reader<String> reader = reader(topic, true)) {
            assertThat(reader.hasMessageAvailable()).isTrue();
            assertThat(reader.readNext(10, TimeUnit.SECONDS)).isNotNull()
                    .satisfies(message -> assertThat(message.getValue()).isEqualTo("value"));
        }
    }

    private String createTopicWithMissingLedger(boolean retainSource, boolean appendTail) throws Exception {
        String topic = newTopicName();
        admin.namespaces().setRetention(getNamespace(), new RetentionPolicies(-1, -1));
        admin.namespaces().setDeduplicationStatus(getNamespace(), false);
        PersistentTopic persistentTopic = topic(topic);
        ManagedLedger ledger = persistentTopic.getManagedLedger();
        ledger.getConfig().setMaxEntriesPerLedger(1);
        ledger.getConfig().setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ledger.getConfig().setThrottleMarkDelete(0);

        MessageId oldMessage;
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).enableBatching(false).create()) {
            oldMessage = producer.newMessage().key("old-key").value("only-value").send();
            producer.newMessage().key("new-key").value("new-value").send();
        }

        BookKeeper bookKeeper = bookKeeper();
        Compactor compactor = compactor();
        long compactedLedger = compactor.compact(topic).get(20, TimeUnit.SECONDS);
        assertThat(compactedLedger).isGreaterThanOrEqualTo(0);

        if (!retainSource) {
            // Use the normal retention/trim path. The old key is now only available through compaction.
            ledger.getConfig().setRetentionTime(0, TimeUnit.SECONDS).setRetentionSizeInMB(0);
            CompletableFuture<Void> trimmed = new CompletableFuture<>();
            ledger.trimConsumedLedgersInBackground(trimmed);
            trimmed.get(10, TimeUnit.SECONDS);
            Awaitility.await().untilAsserted(() -> assertThat(ledger.getLedgersInfo())
                    .doesNotContainKey(((MessageIdImpl) oldMessage).getLedgerId()));
        }

        // Prove that real compaction preserved the key even when its source ledger was removed.
        try (Reader<String> reader = reader(topic, true)) {
            assertThat(reader.readNext(10, TimeUnit.SECONDS)).isNotNull()
                    .satisfies(message -> assertThat(message.getKey()).isEqualTo("old-key"));
        }
        if (appendTail) {
            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                    .topic(topic).enableBatching(false).create()) {
                producer.newMessage().key("tail-key").value("tail-value").send();
            }
        }

        // Persist and reload the real cursor. Delete through BookKeeper, without fabricating cursor
        // metadata, replacing futures, or stubbing the recovery/read path.
        admin.topics().unload(topic);
        bookKeeper.deleteLedger(compactedLedger);
        PersistentTopic recovered = topic(topic);
        assertThat(recovered).isNotSameAs(persistentTopic);
        assertThat(recovered.getSubscription(Compactor.COMPACTION_SUBSCRIPTION).getCursor().getProperties())
                .containsEntry(Compactor.COMPACTED_TOPIC_LEDGER_PROPERTY, compactedLedger);

        return topic;
    }

    private BookKeeper bookKeeper() throws Exception {
        return ((ManagedLedgerFactoryImpl) getPulsar().getDefaultManagedLedgerFactory())
                .getBookKeeper().get(10, TimeUnit.SECONDS);
    }

    private Compactor compactor() throws Exception {
        return new PublishingOrderCompactor(getConfig(), pulsarClient, bookKeeper(),
                getPulsar().getCompactorExecutor());
    }

    private PersistentTopic topic(String topic) throws Exception {
        return (PersistentTopic) getPulsar().getBrokerService().getTopic(topic, true)
                .get(10, TimeUnit.SECONDS).orElseThrow();
    }

    private Reader<String> reader(String topic, boolean readCompacted) throws Exception {
        return pulsarClient.newReader(Schema.STRING).topic(topic).readCompacted(readCompacted)
                .startMessageId(MessageId.earliest).create();
    }
}
