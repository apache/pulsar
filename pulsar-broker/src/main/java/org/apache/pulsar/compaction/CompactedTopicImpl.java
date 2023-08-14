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

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ComparisonChain;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer.ReadEntriesCtx;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.impl.RawMessageImpl;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactedTopicImpl implements CompactedTopic {
    static final long NEWER_THAN_COMPACTED = -0xfeed0fbaL;
    static final long COMPACT_LEDGER_EMPTY = -0xfeed0fbbL;
    static final int DEFAULT_STARTPOINT_CACHE_SIZE = 100;

    private final BookKeeper bk;

    private PositionImpl compactionHorizon = null;
    private CompletableFuture<CompactedTopicContext> compactedTopicContext = null;

    public CompactedTopicImpl(BookKeeper bk) {
        this.bk = bk;
    }

    @Override
    public CompletableFuture<CompactedTopicContext> newCompactedLedger(Position p, long compactedLedgerId) {
        synchronized (this) {
            compactionHorizon = (PositionImpl) p;

            CompletableFuture<CompactedTopicContext> previousContext = compactedTopicContext;
            compactedTopicContext = openCompactedLedger(bk, compactedLedgerId);

            // delete the ledger from the old context once the new one is open
            return compactedTopicContext.thenCompose(__ ->
                    previousContext != null ? previousContext : CompletableFuture.completedFuture(null));
        }
    }

    @Override
    public CompletableFuture<Void> deleteCompactedLedger(long compactedLedgerId) {
        return tryDeleteCompactedLedger(bk, compactedLedgerId);
    }

    @Override
    public void asyncReadEntriesOrWait(ManagedCursor cursor,
                                       int numberOfEntriesToRead,
                                       boolean isFirstRead,
                                       ReadEntriesCallback callback, Consumer consumer) {
        synchronized (this) {
            PositionImpl cursorPosition;
            if (isFirstRead && MessageId.earliest.equals(consumer.getStartMessageId())){
                cursorPosition = PositionImpl.EARLIEST;
            } else {
                cursorPosition = (PositionImpl) cursor.getReadPosition();
            }

            // TODO: redeliver epoch link https://github.com/apache/pulsar/issues/13690
            ReadEntriesCtx readEntriesCtx = ReadEntriesCtx.create(consumer, DEFAULT_CONSUMER_EPOCH);
            if (compactionHorizon == null
                || compactionHorizon.compareTo(cursorPosition) < 0) {
                cursor.asyncReadEntriesOrWait(numberOfEntriesToRead, callback, readEntriesCtx, PositionImpl.LATEST);
            } else {
                compactedTopicContext.thenCompose(
                    (context) -> findStartPoint(cursorPosition, context.ledger.getLastAddConfirmed(), context.cache)
                        .thenCompose((startPoint) -> {
                            // do not need to read the compaction ledger if it is empty.
                            // the cursor just needs to be set to the compaction horizon
                            if (startPoint == COMPACT_LEDGER_EMPTY) {
                                cursor.seek(compactionHorizon.getNext());
                                callback.readEntriesComplete(Collections.emptyList(), readEntriesCtx);
                                return CompletableFuture.completedFuture(null);
                            }
                            if (startPoint == NEWER_THAN_COMPACTED && compactionHorizon.compareTo(cursorPosition) < 0) {
                                cursor.asyncReadEntriesOrWait(numberOfEntriesToRead, callback, readEntriesCtx,
                                        PositionImpl.LATEST);
                                return CompletableFuture.completedFuture(null);
                            } else {
                                long endPoint = Math.min(context.ledger.getLastAddConfirmed(),
                                                         startPoint + (numberOfEntriesToRead - 1));
                                if (startPoint == NEWER_THAN_COMPACTED) {
                                    cursor.seek(compactionHorizon.getNext());
                                    callback.readEntriesComplete(Collections.emptyList(), readEntriesCtx);
                                    return CompletableFuture.completedFuture(null);
                                }
                                return readEntries(context.ledger, startPoint, endPoint)
                                    .thenAccept((entries) -> {
                                        Entry lastEntry = entries.get(entries.size() - 1);
                                        // The compaction task depends on the last snapshot and the incremental
                                        // entries to build the new snapshot. So for the compaction cursor, we
                                        // need to force seek the read position to ensure the compactor can read
                                        // the complete last snapshot because of the compactor will read the data
                                        // before the compaction cursor mark delete position
                                        cursor.seek(lastEntry.getPosition().getNext(), true);
                                        callback.readEntriesComplete(entries, readEntriesCtx);
                                    });
                            }
                        }))
                    .exceptionally((exception) -> {
                        if (exception.getCause() instanceof NoSuchElementException) {
                            cursor.seek(compactionHorizon.getNext());
                            callback.readEntriesComplete(Collections.emptyList(), readEntriesCtx);
                        } else {
                            callback.readEntriesFailed(new ManagedLedgerException(exception), readEntriesCtx);
                        }
                        return null;
                    });
            }
        }
    }

    static CompletableFuture<Long> findStartPoint(PositionImpl p,
                                                  long lastEntryId,
                                                  AsyncLoadingCache<Long, MessageIdData> cache) {
        CompletableFuture<Long> promise = new CompletableFuture<>();
        // if lastEntryId is less than zero it means there are no entries in the compact ledger
        if (lastEntryId < 0) {
            promise.complete(COMPACT_LEDGER_EMPTY);
        } else {
            findStartPointLoop(p, 0, lastEntryId, promise, cache);
        }
        return promise;
    }

    private static void findStartPointLoop(PositionImpl p, long start, long end,
                                           CompletableFuture<Long> promise,
                                           AsyncLoadingCache<Long, MessageIdData> cache) {
        long midpoint = start + ((end - start) / 2);

        CompletableFuture<MessageIdData> startEntry = cache.get(start);
        CompletableFuture<MessageIdData> middleEntry = cache.get(midpoint);
        CompletableFuture<MessageIdData> endEntry = cache.get(end);

        CompletableFuture.allOf(startEntry, middleEntry, endEntry).thenRun(
                () -> {
                    if (comparePositionAndMessageId(p, startEntry.join()) <= 0) {
                        promise.complete(start);
                    } else if (comparePositionAndMessageId(p, middleEntry.join()) <= 0) {
                        findStartPointLoop(p, start, midpoint, promise, cache);
                    } else if (comparePositionAndMessageId(p, endEntry.join()) <= 0) {
                        findStartPointLoop(p, midpoint + 1, end, promise, cache);
                    } else {
                        promise.complete(NEWER_THAN_COMPACTED);
                    }
                }).exceptionally((exception) -> {
                        promise.completeExceptionally(exception);
                        return null;
                    });
    }

    static AsyncLoadingCache<Long, MessageIdData> createCache(LedgerHandle lh,
                                                              long maxSize) {
        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .buildAsync((entryId, executor) -> readOneMessageId(lh, entryId));
    }


    private static CompletableFuture<MessageIdData> readOneMessageId(LedgerHandle lh, long entryId) {
        CompletableFuture<MessageIdData> promise = new CompletableFuture<>();

        lh.asyncReadEntries(entryId, entryId,
                            (rc, _lh, seq, ctx) -> {
                                if (rc != BKException.Code.OK) {
                                    promise.completeExceptionally(BKException.create(rc));
                                } else {
                                    // Need to release buffers for all entries in the sequence
                                    if (seq.hasMoreElements()) {
                                        LedgerEntry entry = seq.nextElement();
                                        try (RawMessage m = RawMessageImpl.deserializeFrom(entry.getEntryBuffer())) {
                                            entry.getEntryBuffer().release();
                                            while (seq.hasMoreElements()) {
                                                seq.nextElement().getEntryBuffer().release();
                                            }
                                            promise.complete(m.getMessageIdData());
                                        }
                                    } else {
                                        promise.completeExceptionally(new NoSuchElementException(
                                                String.format("No such entry %d in ledger %d", entryId, lh.getId())));
                                    }
                                }
                            }, null);
        return promise;
    }

    private static CompletableFuture<CompactedTopicContext> openCompactedLedger(BookKeeper bk, long id) {
        CompletableFuture<LedgerHandle> promise = new CompletableFuture<>();
        bk.asyncOpenLedger(id,
                           Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                           Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD,
                           (rc, ledger, ctx) -> {
                               if (rc != BKException.Code.OK) {
                                   promise.completeExceptionally(BKException.create(rc));
                               } else {
                                   promise.complete(ledger);
                               }
                           }, null);
        return promise.thenApply((ledger) -> new CompactedTopicContext(
                                         ledger, createCache(ledger, DEFAULT_STARTPOINT_CACHE_SIZE)));
    }

    private static CompletableFuture<Void> tryDeleteCompactedLedger(BookKeeper bk, long id) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        bk.asyncDeleteLedger(id,
                             (rc, ctx) -> {
                                 if (rc != BKException.Code.OK) {
                                     log.warn("Error deleting compacted topic ledger {}",
                                              id, BKException.create(rc));
                                 } else {
                                     log.debug("Compacted topic ledger deleted successfully");
                                 }
                                 promise.complete(null); // don't propagate any error
                             }, null);
        return promise;
    }

    private static CompletableFuture<List<Entry>> readEntries(LedgerHandle lh, long from, long to) {
        CompletableFuture<Enumeration<LedgerEntry>> promise = new CompletableFuture<>();

        lh.asyncReadEntries(from, to,
                            (rc, _lh, seq, ctx) -> {
                                if (rc != BKException.Code.OK) {
                                    promise.completeExceptionally(BKException.create(rc));
                                } else {
                                    promise.complete(seq);
                                }
                            }, null);
        return promise.thenApply(
                (seq) -> {
                    List<Entry> entries = new ArrayList<Entry>();
                    while (seq.hasMoreElements()) {
                        ByteBuf buf = seq.nextElement().getEntryBuffer();
                        try (RawMessage m = RawMessageImpl.deserializeFrom(buf)) {
                            entries.add(EntryImpl.create(m.getMessageIdData().getLedgerId(),
                                                         m.getMessageIdData().getEntryId(),
                                                         m.getHeadersAndPayload()));
                        } finally {
                            buf.release();
                        }
                    }
                    return entries;
                });
    }

    /**
     * Getter for CompactedTopicContext.
     * @return CompactedTopicContext
     */
    public Optional<CompactedTopicContext> getCompactedTopicContext() throws ExecutionException, InterruptedException {
        return compactedTopicContext == null ? Optional.empty() : Optional.of(compactedTopicContext.get());
    }

    @Override
    public CompletableFuture<Entry> readLastEntryOfCompactedLedger() {
        if (compactionHorizon == null) {
            return CompletableFuture.completedFuture(null);
        }
        return compactedTopicContext.thenCompose(context -> {
            if (context.ledger.getLastAddConfirmed() == -1) {
                return CompletableFuture.completedFuture(null);
            }
            return readEntries(
                    context.ledger, context.ledger.getLastAddConfirmed(), context.ledger.getLastAddConfirmed())
                    .thenCompose(entries -> entries.size() > 0
                            ? CompletableFuture.completedFuture(entries.get(0))
                            : CompletableFuture.completedFuture(null));
        });
    }

    private static int comparePositionAndMessageId(PositionImpl p, MessageIdData m) {
        return ComparisonChain.start()
            .compare(p.getLedgerId(), m.getLedgerId())
            .compare(p.getEntryId(), m.getEntryId()).result();
    }

    public synchronized Optional<Position> getCompactionHorizon() {
        return Optional.ofNullable(this.compactionHorizon);
    }
    private static final Logger log = LoggerFactory.getLogger(CompactedTopicImpl.class);
}

