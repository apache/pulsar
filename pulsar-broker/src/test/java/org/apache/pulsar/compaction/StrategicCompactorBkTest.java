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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BkEnsemblesTestBase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.impl.RawBatchMessageContainerImpl;
import org.apache.pulsar.client.impl.RawMessageImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class StrategicCompactorBkTest extends BkEnsemblesTestBase {

    private static final String NAMESPACE = "prop/ns-compaction";

    private static String newTopicName() {
        return BrokerTestUtil.newUniqueName("persistent://" + NAMESPACE + "/topic");
    }

    @Test(timeOut = 60000)
    public void testFailedCompactionDoesNotRestoreDeletedKeyOnRetry() throws Exception {
        admin.namespaces().createNamespace(NAMESPACE, Set.of("usc"));
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        String topic = newTopicName();
        BookKeeper bookKeeper = pulsar.getBookKeeperClient();
        AtomicReference<LedgerHandle> failedLedger = new AtomicReference<>();
        AtomicReference<RawBatchMessageContainerImpl> failedBatch = new AtomicReference<>();
        StrategicTwoPhaseCompactor compactor = new StrategicTwoPhaseCompactor(
                config, pulsarClient, bookKeeper, pulsar.getCompactorExecutor()) {
            @Override
            protected CompletableFuture<LedgerHandle> createLedger(BookKeeper bk, Map<String, byte[]> metadata) {
                return super.createLedger(bk, metadata).thenCompose(ledger -> {
                    if (failedLedger.compareAndSet(null, ledger)) {
                        // Close a real ledger so BookKeeper itself reports LedgerClosedException on the first write.
                        return closeLedger(ledger).thenApply(__ -> ledger);
                    }
                    return CompletableFuture.completedFuture(ledger);
                });
            }

            @Override
            <T> CompletableFuture<Boolean> addToCompactedLedger(
                    LedgerHandle ledger, Message<T> message, String topic, Semaphore outstanding,
                    RawBatchMessageContainerImpl batchMessageContainer) {
                CompletableFuture<Boolean> write =
                        super.addToCompactedLedger(ledger, message, topic, outstanding, batchMessageContainer);
                if (ledger == failedLedger.get()) {
                    failedBatch.set(batchMessageContainer);
                    // Observe the real BK callback before advancing the loop, leaving the next batch buffered.
                    // This wait only controls the failure interleaving in this test.
                    write.handle((__, error) -> null).orTimeout(10, TimeUnit.SECONDS).join();
                }
                return write;
            }
        };
        var strategy = new TopicCompactionStrategyTest.DummyTopicCompactionStrategy();
        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create()) {
            producer.newMessage().key("keep").value("retained".getBytes(StandardCharsets.UTF_8)).send();
            producer.newMessage().key("deleted").value("stale".getBytes(StandardCharsets.UTF_8)).send();

            assertThatThrownBy(() -> compactor.compact(topic, strategy).get(15, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(BKException.BKLedgerClosedException.class);
            assertThat(failedBatch.get()).as("batch container from the failed compaction").isNotNull();
            assertThat(failedBatch.get().isEmpty()).as("failed compaction must discard its buffered batch").isTrue();

            producer.newMessage().key("deleted").value(null).send();
            long ledgerId = compactor.compact(topic, strategy).get(15, TimeUnit.SECONDS);
            try (LedgerHandle ledger = bookKeeper.openLedger(ledgerId, Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                    Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD)) {
                // Check persisted output: phase one removed "deleted", so the retry must not flush its stale batch.
                assertThat(ledger.getLastAddConfirmed()).isZero();
                var entry = ledger.readEntries(0, 0).nextElement();
                try (RawMessage message = RawMessageImpl.deserializeFrom(entry.getEntryBuffer())) {
                    var payload = message.getHeadersAndPayload();
                    assertThat(Commands.parseMessageMetadata(payload).getPartitionKey()).isEqualTo("keep");
                    assertThat(payload.toString(StandardCharsets.UTF_8)).isEqualTo("retained");
                } finally {
                    entry.getEntryBuffer().release();
                }
            }
        }
    }
}
