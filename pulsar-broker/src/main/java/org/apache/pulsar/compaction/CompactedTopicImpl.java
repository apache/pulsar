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

import com.google.common.cache.Cache;
import com.google.common.collect.ComparisonChain;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.impl.RawMessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactedTopicImpl implements CompactedTopic {
    final static long NEWER_THAN_COMPACTED = -0xfeed0fbaL;

    @Override
    public void newCompactedLedger(Position p, long compactedLedgerId) {}

    static CompletableFuture<Long> findStartPoint(LedgerHandle lh, PositionImpl p,
                                                  Cache<LongPair,MessageIdData> cache) {
        CompletableFuture<Long> promise = new CompletableFuture<>();
        findStartPointLoop(lh, p, 0, lh.getLastAddConfirmed(), promise, cache);
        return promise;
    }

    private static void findStartPointLoop(LedgerHandle lh, PositionImpl p, long start, long end,
                                           CompletableFuture<Long> promise,
                                           Cache<LongPair,MessageIdData> cache) {
        long midpoint = start + ((end - start) / 2);

        CompletableFuture<MessageIdData> startEntry = readOneMessageId(lh, start, cache);
        CompletableFuture<MessageIdData> middleEntry = readOneMessageId(lh, midpoint, cache);
        CompletableFuture<MessageIdData> endEntry = readOneMessageId(lh, end, cache);

        CompletableFuture.allOf(startEntry, middleEntry, endEntry).whenComplete(
                (v, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    }
                    try {
                        if (comparePositionAndMessageId(p, startEntry.get()) < 0) {
                            promise.complete(start);
                        } else if (comparePositionAndMessageId(p, middleEntry.get()) < 0) {
                            findStartPointLoop(lh, p, start, midpoint, promise, cache);
                        } else if (comparePositionAndMessageId(p, endEntry.get()) < 0) {
                            findStartPointLoop(lh, p, midpoint + 1, end, promise, cache);
                        } else {
                            promise.complete(NEWER_THAN_COMPACTED);
                        }
                    } catch (InterruptedException ie) {
                        // should never happen as all should have been completed
                        Thread.currentThread().interrupt();
                        log.error("Interrupted waiting on futures which should have completed", ie);
                    } catch (ExecutionException e) {
                        // shouldn't happen, allOf should have given us the exception
                        promise.completeExceptionally(e);
                    }
                });
    }

    private static CompletableFuture<MessageIdData> readOneMessageId(LedgerHandle lh, long entryId,
                                                                     Cache<LongPair,MessageIdData> cache) {
        CompletableFuture<MessageIdData> promise = new CompletableFuture<>();

        LongPair cacheKey = new LongPair(lh.getId(), entryId);
        MessageIdData cached = cache.getIfPresent(cacheKey);

        if (cached == null) {
            lh.asyncReadEntries(entryId, entryId,
                    (rc, _lh, seq, ctx) -> {
                        if (rc != BKException.Code.OK) {
                            promise.completeExceptionally(BKException.create(rc));
                        } else {
                            try (RawMessage m = RawMessageImpl.deserializeFrom(
                                         seq.nextElement().getEntryBuffer())) {
                                promise.complete(m.getMessageIdData());
                            } catch (NoSuchElementException e) {
                                log.error("No such entry {} in ledger {}", entryId, lh.getId());
                                promise.completeExceptionally(e);
                            }
                        }
                    }, null);
            promise.thenAccept((v) -> {
                    cache.put(cacheKey, v);
                });
        } else {
            promise.complete(cached);
        }
        return promise;
    }

    private static int comparePositionAndMessageId(PositionImpl p, MessageIdData m) {
        return ComparisonChain.start()
            .compare(p.getLedgerId(), m.getLedgerId())
            .compare(p.getEntryId(), m.getEntryId()).result();
    }
    private static final Logger log = LoggerFactory.getLogger(CompactedTopicImpl.class);
}

