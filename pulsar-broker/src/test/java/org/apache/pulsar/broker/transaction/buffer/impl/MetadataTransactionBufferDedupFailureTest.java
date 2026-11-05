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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.broker.transaction.metadata.TxnHeader;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnOpKind;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/** Exercises the real topic, managed ledger and producer deduplication around a pre-ledger failure. */
@Test(groups = "broker")
public class MetadataTransactionBufferDedupFailureTest extends SharedPulsarBaseTest {
    private static final String PRODUCER = "ordered-producer";

    @Test(timeOut = 60_000)
    public void failedLowerMetadataAppendCannotBecomeAFalseDuplicateAfterHigherPublish() throws Exception {
        String name = newTopicName();
        admin.namespaces().setDeduplicationStatus(getNamespace(), true);
        TxnID txn = new TxnID(3, 1);
        MetadataStore metadata = MetadataStoreFactory.create("memory:dedup-failure",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        ControlledStore store = new ControlledStore(metadata, TxnIds.toKey(txn));
        store.createHeader(TxnIds.toKey(txn),
                new TxnHeader(TxnState.OPEN, Duration.ofMinutes(1), Instant.now(), null, null))
                .get(10, TimeUnit.SECONDS);
        TransactionBufferProvider originalProvider = getPulsar().getTransactionBufferProvider();
        AtomicInteger generations = new AtomicInteger();
        AtomicInteger firstCloseCalls = new AtomicInteger();
        CompletableFuture<Void> firstBufferClosed = new CompletableFuture<>();
        List<ByteBuf> buffers = new ArrayList<>();
        getPulsar().setTransactionBufferProvider(origin -> {
            if (!origin.getName().equals(name)) {
                return originalProvider.newTransactionBuffer(origin);
            }
            boolean first = generations.getAndIncrement() == 0;
            return new MetadataTransactionBuffer((PersistentTopic) origin, store) {
                @Override
                public CompletableFuture<Void> closeAsync() {
                    CompletableFuture<Void> closing = super.closeAsync();
                    if (first) {
                        firstCloseCalls.incrementAndGet();
                        closing.whenComplete((ignored, error) -> {
                            if (error == null) {
                                firstBufferClosed.complete(null);
                            } else {
                                firstBufferClosed.completeExceptionally(error);
                            }
                        });
                    }
                    return closing;
                }
            };
        });
        try {
            PersistentTopic original = (PersistentTopic) getTopic(name, true).get(10, TimeUnit.SECONDS).orElseThrow();
            original.getTransactionBuffer().checkIfTBRecoverCompletely().get(10, TimeUnit.SECONDS);
            original.checkDeduplicationStatus().get(10, TimeUnit.SECONDS);
            assertThat(original.getMessageDeduplication().isEnabled()).isTrue();
            assertStored(original, publish(original, null, 119, "seed-119", buffers).await(),
                    null, 119, "seed-119");

            Send low = publish(original, txn, 120, "low-120", buffers);
            assertThat(store.lowEntered.await(10, TimeUnit.SECONDS)).isTrue();
            Send high = publish(original, txn, 121, "high-121", buffers);
            assertThat(store.highMetadataWritten.await(10, TimeUnit.SECONDS)).isTrue();
            Send plain = publish(original, null, 122, "plain-122", buffers);
            assertThat(high.result().isDone()).isFalse();
            assertThat(plain.result().isDone()).isFalse();

            store.lowMetadata.completeExceptionally(new MetadataStoreException("injected pre-ledger failure"));
            Publication lowResult = low.await();
            Publication highResult = high.await();
            Publication plainResult = plain.await();
            assertThat(lowResult.failure()).isNotNull();

            if (highResult.failure() == null) {
                // On the broken source,121 lands while120 never reaches the ledger. Its high
                // watermark then acknowledges the exact120 resend with the NULL position.
                assertStored(original, highResult, txn, 121, "high-121");
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(original.getPendingWriteOps().get()).isZero());
                Publication retry = publish(original, txn, 120, "low-120", buffers).await();
                assertStored(original, retry, txn, 120, "low-120");
            }
            assertThat(highResult.failure()).as("the queued higher transaction must fail with the old generation")
                    .isNotNull();
            assertThat(plainResult.failure()).as("a queued plain publish must not leap across the failed sequence")
                    .isNotNull();
            firstBufferClosed.get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(getTopicReference(name).orElse(null)).isNotSameAs(original));

            PersistentTopic fresh = (PersistentTopic) getTopicIfExists(name).get(10, TimeUnit.SECONDS).orElseThrow();
            assertThat(fresh).isNotSameAs(original);
            fresh.getTransactionBuffer().checkIfTBRecoverCompletely().get(10, TimeUnit.SECONDS);
            fresh.checkDeduplicationStatus().get(10, TimeUnit.SECONDS);
            assertThat(fresh.getMessageDeduplication().isEnabled()).isTrue();
            Publication retryLow = publish(fresh, txn, 120, "low-120", buffers).await();
            Publication retryHigh = publish(fresh, txn, 121, "high-121", buffers).await();
            Publication retryPlain = publish(fresh, null, 122, "plain-122", buffers).await();
            assertStored(fresh, retryLow, txn, 120, "low-120");
            assertStored(fresh, retryHigh, txn, 121, "high-121");
            assertStored(fresh, retryPlain, null, 122, "plain-122");
            assertThat(retryLow.position()).isLessThan(retryHigh.position());
            assertThat(retryHigh.position()).isLessThan(retryPlain.position());
            assertThat(firstCloseCalls.get()).isOne();
            assertThat(generations.get()).isEqualTo(2);
            for (Send send : List.of(low, high, plain)) {
                verify(send.context(), times(1)).completed(any(), anyLong(), anyLong());
            }
        } finally {
            store.lowMetadata.completeExceptionally(new MetadataStoreException("test cleanup"));
            getPulsar().setTransactionBufferProvider(originalProvider);
            try {
                Topic remaining = getTopicReference(name).orElse(null);
                if (remaining != null) {
                    remaining.close(true).get(10, TimeUnit.SECONDS);
                }
            } finally {
                metadata.close();
                buffers.forEach(ByteBuf::release);
            }
        }
    }

    private Send publish(PersistentTopic topic, TxnID txn, long sequence, String value, List<ByteBuf> buffers) {
        MessageMetadata message = new MessageMetadata().setProducerName(PRODUCER).setSequenceId(sequence)
                .setPublishTime(System.currentTimeMillis());
        if (txn != null) {
            message.setTxnidMostBits(txn.getMostSigBits()).setTxnidLeastBits(txn.getLeastSigBits());
        }
        ByteBuf content = Unpooled.copiedBuffer(value, StandardCharsets.UTF_8);
        ByteBuf data;
        try {
            data = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, message, content);
        } finally {
            content.release();
        }
        buffers.add(data);
        Topic.PublishContext context = mock(Topic.PublishContext.class);
        when(context.getProducerName()).thenReturn(PRODUCER);
        when(context.getSequenceId()).thenReturn(sequence);
        when(context.getHighestSequenceId()).thenReturn(sequence);
        when(context.getNumberOfMessages()).thenReturn(1L);
        CompletableFuture<Publication> result = new CompletableFuture<>();
        doAnswer(invocation -> {
            Exception failure = invocation.getArgument(0);
            long ledger = invocation.getArgument(1);
            long entry = invocation.getArgument(2);
            result.complete(new Publication(failure, PositionFactory.create(ledger, entry)));
            return null;
        }).when(context).completed(any(), anyLong(), anyLong());
        if (txn == null) {
            topic.publishMessage(data, context);
        } else {
            topic.publishTxnMessage(txn, data, context);
        }
        return new Send(context, result);
    }

    private void assertStored(PersistentTopic topic, Publication publication, TxnID txn,
                              long sequence, String value) throws Exception {
        assertThat(publication.failure()).as("a successful resend must correspond to its stored payload").isNull();
        assertThat(publication.position().getLedgerId()).as("NULL success must not conceal a missing lower sequence")
                .isGreaterThanOrEqualTo(0);
        assertThat(publication.position().getEntryId()).isGreaterThanOrEqualTo(0);
        CompletableFuture<Entry> read = new CompletableFuture<>();
        topic.getManagedLedger().asyncReadEntry(publication.position(), new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                read.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException error, Object ctx) {
                read.completeExceptionally(error);
            }
        }, null);
        Entry entry = read.get(10, TimeUnit.SECONDS);
        try {
            ByteBuf data = entry.getDataBuffer().duplicate();
            MessageMetadata message = Commands.parseMessageMetadata(data);
            assertThat(message.getProducerName()).isEqualTo(PRODUCER);
            assertThat(message.getSequenceId()).isEqualTo(sequence);
            assertThat(data.toString(StandardCharsets.UTF_8)).isEqualTo(value);
            assertThat(message.hasTxnidMostBits()).isEqualTo(txn != null);
            if (txn != null) {
                assertThat(message.getTxnidMostBits()).isEqualTo(txn.getMostSigBits());
                assertThat(message.getTxnidLeastBits()).isEqualTo(txn.getLeastSigBits());
            }
        } finally {
            entry.release();
        }
    }

    private record Publication(Exception failure, Position position) {}

    private record Send(Topic.PublishContext context, CompletableFuture<Publication> result) {
        Publication await() throws Exception {
            return result.get(10, TimeUnit.SECONDS);
        }
    }

    private static final class ControlledStore extends TxnMetadataStore {
        private final String txn;
        private final AtomicInteger calls = new AtomicInteger();
        private final CompletableFuture<Stat> lowMetadata = new CompletableFuture<>();
        private final CountDownLatch lowEntered = new CountDownLatch(1);
        private final CountDownLatch highMetadataWritten = new CountDownLatch(1);

        ControlledStore(MetadataStore store, String txn) {
            super(store);
            this.txn = txn;
        }

        @Override
        public CompletableFuture<Stat> appendOp(String key, TxnOp op) {
            if (key.equals(txn) && op.getKind() == TxnOpKind.WRITE) {
                int call = calls.incrementAndGet();
                if (call == 1) {
                    lowEntered.countDown();
                    return lowMetadata;
                }
                CompletableFuture<Stat> written = super.appendOp(key, op);
                if (call == 2) {
                    written.thenRun(highMetadataWritten::countDown);
                }
                return written;
            }
            return super.appendOp(key, op);
        }
    }
}
