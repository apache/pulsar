/**
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

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.RawBatchConverter;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compaction will go through the topic in two passes. The first pass
 * selects latest offset for each key in the topic. Then the second pass
 * writes these values to a ledger.
 *
 * <p>The two passes are required to avoid holding the payloads of each of
 * the latest values in memory, as the payload can be many orders of
 * magnitude larger than a message id.
*/
public class TwoPhaseCompactor extends Compactor {
    private static final Logger log = LoggerFactory.getLogger(TwoPhaseCompactor.class);
    private static final int MAX_OUTSTANDING = 500;
    private static final String COMPACTED_TOPIC_LEDGER_PROPERTY = "CompactedTopicLedger";

    public TwoPhaseCompactor(ServiceConfiguration conf,
                             PulsarClient pulsar,
                             BookKeeper bk,
                             ScheduledExecutorService scheduler) {
        super(conf, pulsar, bk, scheduler);
    }

    @Override
    protected CompletableFuture<Long> doCompaction(RawReader reader, BookKeeper bk) {
        return reader.hasMessageAvailableAsync()
                .thenCompose(available -> {
                    if (available) {
                        return phaseOne(reader).thenCompose(
                                (r) -> phaseTwo(reader, r.from, r.to, r.lastReadId, r.latestForKey, bk));
                    } else {
                        log.info("Skip compaction of the empty topic {}", reader.getTopic());
                        return CompletableFuture.completedFuture(-1L);
                    }
                });
    }

    private CompletableFuture<PhaseOneResult> phaseOne(RawReader reader) {
        Map<String,MessageId> latestForKey = new HashMap<>();
        CompletableFuture<PhaseOneResult> loopPromise = new CompletableFuture<>();

        reader.getLastMessageIdAsync().whenComplete(
                (lastMessageId, exception) -> {
                    if (exception != null) {
                        loopPromise.completeExceptionally(exception);
                    } else {
                        log.info("Commencing phase one of compaction for {}, reading to {}",
                                 reader.getTopic(), lastMessageId);
                        // Each entry is processed as a whole, discard the batchIndex part deliberately.
                        MessageIdImpl lastImpl = (MessageIdImpl) lastMessageId;
                        MessageIdImpl lastEntryMessageId = new MessageIdImpl(lastImpl.getLedgerId(), lastImpl.getEntryId(), lastImpl.getPartitionIndex());
                        phaseOneLoop(reader, Optional.empty(), Optional.empty(), lastEntryMessageId, latestForKey,
                                loopPromise);
                    }
                });
        return loopPromise;
    }

    private void phaseOneLoop(RawReader reader,
                              Optional<MessageId> firstMessageId,
                              Optional<MessageId> toMessageId,
                              MessageId lastMessageId,
                              Map<String,MessageId> latestForKey,
                              CompletableFuture<PhaseOneResult> loopPromise) {
        if (loopPromise.isDone()) {
            return;
        }
        CompletableFuture<RawMessage> future = reader.readNextAsync();
        scheduleTimeout(future);
        future.whenCompleteAsync(
                (m, exception) -> {
                    try {
                        if (exception != null) {
                            loopPromise.completeExceptionally(exception);
                            return;
                        }
                        MessageId id = m.getMessageId();
                        boolean deletedMessage = false;
                        if (RawBatchConverter.isReadableBatch(m)) {
                            try {
                                RawBatchConverter.extractIdsAndKeys(m)
                                    .forEach(e -> latestForKey.put(e.getRight(), e.getLeft()));
                            } catch (IOException ioe) {
                                log.info("Error decoding batch for message {}. Whole batch will be included in output",
                                         id, ioe);
                            }
                        } else {
                            Pair<String,Integer> keyAndSize = extractKeyAndSize(m);
                            if (keyAndSize != null) {
                                if(keyAndSize.getRight() > 0) {
                                    latestForKey.put(keyAndSize.getLeft(), id);
                                } else {
                                    deletedMessage = true;
                                    latestForKey.remove(keyAndSize.getLeft());
                                }
                            }
                        }

                        MessageId first = firstMessageId.orElse(deletedMessage ? null : id);
                        MessageId to = deletedMessage ? toMessageId.orElse(null) : id;
                        if (id.compareTo(lastMessageId) == 0) {
                            loopPromise.complete(new PhaseOneResult(first, to, lastMessageId, latestForKey));
                        } else {
                            phaseOneLoop(reader,
                                         Optional.ofNullable(first),
                                         Optional.ofNullable(to),
                                         lastMessageId,
                                         latestForKey, loopPromise);
                        }
                    } finally {
                        m.close();
                    }
                }, scheduler);
    }

    private void scheduleTimeout(CompletableFuture<RawMessage> future) {
        Future<?> timeout = scheduler.schedule(() -> {
            future.completeExceptionally(new TimeoutException("Timeout"));
        }, 10, TimeUnit.SECONDS);
        future.whenComplete((res, exception) -> {
            timeout.cancel(true);
        });
    }

    private CompletableFuture<Long> phaseTwo(RawReader reader, MessageId from, MessageId to, MessageId lastReadId,
            Map<String, MessageId> latestForKey, BookKeeper bk) {
        Map<String, byte[]> metadata = LedgerMetadataUtils.buildMetadataForCompactedLedger(reader.getTopic(), to.toByteArray());
        return createLedger(bk, metadata).thenCompose((ledger) -> {
            log.info("Commencing phase two of compaction for {}, from {} to {}, compacting {} keys to ledger {}",
                    reader.getTopic(), from, to, latestForKey.size(), ledger.getId());
            return phaseTwoSeekThenLoop(reader, from, to, lastReadId, latestForKey, bk, ledger);
        });
    }

    private CompletableFuture<Long> phaseTwoSeekThenLoop(RawReader reader, MessageId from, MessageId to,
            MessageId lastReadId, Map<String, MessageId> latestForKey, BookKeeper bk, LedgerHandle ledger) {
        CompletableFuture<Long> promise = new CompletableFuture<>();

        reader.seekAsync(from).thenCompose((v) -> {
            Semaphore outstanding = new Semaphore(MAX_OUTSTANDING);
            CompletableFuture<Void> loopPromise = new CompletableFuture<Void>();
            phaseTwoLoop(reader, to, latestForKey, ledger, outstanding, loopPromise);
            return loopPromise;
        }).thenCompose((v) -> closeLedger(ledger))
                .thenCompose((v) -> reader.acknowledgeCumulativeAsync(lastReadId,
                        ImmutableMap.of(COMPACTED_TOPIC_LEDGER_PROPERTY, ledger.getId())))
                .whenComplete((res, exception) -> {
                    if (exception != null) {
                        deleteLedger(bk, ledger).whenComplete((res2, exception2) -> {
                            if (exception2 != null) {
                                log.warn("Cleanup of ledger {} for failed", ledger, exception2);
                            }
                            // complete with original exception
                            promise.completeExceptionally(exception);
                        });
                    } else {
                        promise.complete(ledger.getId());
                    }
                });
        return promise;
    }

    private void phaseTwoLoop(RawReader reader, MessageId to, Map<String, MessageId> latestForKey,
                              LedgerHandle lh, Semaphore outstanding, CompletableFuture<Void> promise) {
        if (promise.isDone()) {
            return;
        }
        reader.readNextAsync().whenCompleteAsync(
                (m, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                        return;
                    } else if (promise.isDone()) {
                        m.close();
                        return;
                    }
                    try {
                        MessageId id = m.getMessageId();
                        Optional<RawMessage> messageToAdd = Optional.empty();
                        if (RawBatchConverter.isReadableBatch(m)) {
                            try {
                                messageToAdd = RawBatchConverter.rebatchMessage(
                                        m, (key, subid) -> latestForKey.get(key).equals(subid));
                            } catch (IOException ioe) {
                                log.info("Error decoding batch for message {}. Whole batch will be included in output",
                                        id, ioe);
                                messageToAdd = Optional.of(m);
                            }
                        } else {
                            Pair<String,Integer> keyAndSize = extractKeyAndSize(m);
                            MessageId msg;
                            if (keyAndSize == null) { // pass through messages without a key
                                messageToAdd = Optional.of(m);
                            } else if ((msg = latestForKey.get(keyAndSize.getLeft())) != null
                                    && msg.equals(id)) { // consider message only if present into latestForKey map
                                if (keyAndSize.getRight() <= 0) {
                                    promise.completeExceptionally(new IllegalArgumentException(
                                            "Compaction phase found empty record from sorted key-map"));
                                }
                                messageToAdd = Optional.of(m);
                            }
                        }

                        if (messageToAdd.isPresent()) {
                            RawMessage message = messageToAdd.get();
                            try {
                                outstanding.acquire();
                                CompletableFuture<Void> addFuture = addToCompactedLedger(lh, message)
                                        .whenComplete((res, exception2) -> {
                                            outstanding.release();
                                            if (exception2 != null) {
                                                promise.completeExceptionally(exception2);
                                            }
                                        });
                                if (to.equals(id)) {
                                    addFuture.whenComplete((res, exception2) -> {
                                        if (exception2 == null) {
                                            promise.complete(null);
                                        }
                                    });
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                promise.completeExceptionally(ie);
                            } finally {
                                if (message != m) {
                                    message.close();
                                }
                            }
                        } else if (to.equals(id)) {
                            // Reached to last-id and phase-one found it deleted-message while iterating on ledger so,
                            // not present under latestForKey. Complete the compaction.
                            try {
                                // make sure all inflight writes have finished
                                outstanding.acquire(MAX_OUTSTANDING);
                                promise.complete(null);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                promise.completeExceptionally(e);
                            }
                            return;
                        }
                        phaseTwoLoop(reader, to, latestForKey, lh, outstanding, promise);
                    } finally {
                        m.close();
                    }
                }, scheduler);
    }

    private CompletableFuture<LedgerHandle> createLedger(BookKeeper bk, Map<String,byte[]> metadata) {
        CompletableFuture<LedgerHandle> bkf = new CompletableFuture<>();
        bk.asyncCreateLedger(conf.getManagedLedgerDefaultEnsembleSize(),
                             conf.getManagedLedgerDefaultWriteQuorum(),
                             conf.getManagedLedgerDefaultAckQuorum(),
                             Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                             Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD,
                             (rc, ledger, ctx) -> {
                                 if (rc != BKException.Code.OK) {
                                     bkf.completeExceptionally(BKException.create(rc));
                                 } else {
                                     bkf.complete(ledger);
                                 }
                             }, null, metadata);
        return bkf;
    }

    private CompletableFuture<Void> deleteLedger(BookKeeper bk, LedgerHandle lh) {
        CompletableFuture<Void> bkf = new CompletableFuture<>();
        bk.asyncDeleteLedger(lh.getId(),
                             (rc, ctx) -> {
                                 if (rc != BKException.Code.OK) {
                                     bkf.completeExceptionally(BKException.create(rc));
                                 } else {
                                     bkf.complete(null);
                                 }
                             }, null);
        return bkf;
    }

    private CompletableFuture<Void> closeLedger(LedgerHandle lh) {
        CompletableFuture<Void> bkf = new CompletableFuture<>();
        lh.asyncClose((rc, ledger, ctx) -> {
                if (rc != BKException.Code.OK) {
                    bkf.completeExceptionally(BKException.create(rc));
                } else {
                    bkf.complete(null);
                }
            }, null);
        return bkf;
    }

    private CompletableFuture<Void> addToCompactedLedger(LedgerHandle lh, RawMessage m) {
        CompletableFuture<Void> bkf = new CompletableFuture<>();
        ByteBuf serialized = m.serialize();
        lh.asyncAddEntry(serialized,
                         (rc, ledger, eid, ctx) -> {
                             if (rc != BKException.Code.OK) {
                                 bkf.completeExceptionally(BKException.create(rc));
                             } else {
                                 bkf.complete(null);
                             }
                         }, null);
        return bkf;
    }

    private static Pair<String,Integer> extractKeyAndSize(RawMessage m) {
        ByteBuf headersAndPayload = m.getHeadersAndPayload();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        try {
            if (msgMetadata.hasPartitionKey()) {
                int size = headersAndPayload.readableBytes();
                if (msgMetadata.hasUncompressedSize()) {
                    size = msgMetadata.getUncompressedSize();
                }
                return Pair.of(msgMetadata.getPartitionKey(), size);
            } else {
                return null;
            }
        } finally {
            msgMetadata.recycle();
        }
    }

    private static class PhaseOneResult {
        final MessageId from;
        final MessageId to; // last undeleted messageId
        final MessageId lastReadId; // last read messageId
        final Map<String,MessageId> latestForKey;

        PhaseOneResult(MessageId from, MessageId to, MessageId lastReadId, Map<String,MessageId> latestForKey) {
            this.from = from;
            this.to = to;
            this.lastReadId = lastReadId;
            this.latestForKey = latestForKey;
        }
    }
}
