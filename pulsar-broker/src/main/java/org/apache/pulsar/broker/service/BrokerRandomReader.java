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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.RandomReadInvisibleReason;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;

public class BrokerRandomReader implements AutoCloseable {
    private final long randomReaderId;
    private final String readerName;
    private final Map<String, String> metadata;
    private final PersistentTopic topic;
    private final ServerCnx owner;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong inFlightRequestId = new AtomicLong(-1L);

    @Getter
    private final boolean readCommitted;

    public BrokerRandomReader(long randomReaderId, String readerName, Map<String, String> metadata,
                              PersistentTopic topic, ServerCnx owner, boolean readCommitted) {
        this.randomReaderId = randomReaderId;
        this.readerName = readerName;
        this.metadata = Map.copyOf(metadata);
        this.topic = topic;
        this.owner = owner;
        this.readCommitted = readCommitted;
    }

    public boolean beginRead(long requestId) {
        return !closed.get() && inFlightRequestId.compareAndSet(-1L, requestId);
    }

    public void endRead(long requestId) {
        inFlightRequestId.compareAndSet(requestId, -1L);
    }

    public PersistentTopic topic() {
        return topic;
    }

    public ServerCnx owner() {
        return owner;
    }

    public long randomReaderId() {
        return randomReaderId;
    }

    public String readerName() {
        return readerName;
    }

    public Map<String, String> metadata() {
        return metadata;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public static CompletableFuture<PersistentTopic> validatePersistentTopic(Topic topic) {
        if (topic instanceof PersistentTopic) {
            return CompletableFuture.completedFuture((PersistentTopic) topic);
        }
        return FutureUtil.failedFuture(new BrokerServiceException.NotAllowedException(
                "RandomReader only supports persistent topics"));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    public CompletableFuture<List<EntryResult>> readEntries(Position start, int numberOfEntries,
                                                            Position maxVisible) {
        if (numberOfEntries <= 0) {
            return CompletableFuture.completedFuture(List.of());
        }
        return topic.getManagedLedger().readEntries(start, numberOfEntries)
                .thenApply(entries -> toEntryResults(entries, maxVisible));
    }

    public CompletableFuture<Void> disconnect(String brokerServiceUrl, String brokerServiceUrlTls) {
        close();
        owner.disconnectRandomReader(randomReaderId, brokerServiceUrl, brokerServiceUrlTls);
        return CompletableFuture.completedFuture(null);
    }

    private List<EntryResult> toEntryResults(List<Entry> entries, Position maxVisiblePosition) {
        List<EntryResult> result = new ArrayList<>(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            if (entry == null) {
                continue;
            }
            try {
                Position position = entry.getPosition();
                if (position.compareTo(maxVisiblePosition) > 0) {
                    result.add(EntryResult.invisible(entry, RandomReadInvisibleReason.EXCEEDED_MAX_VISIBLE_POSITION));
                    continue;
                }
                ByteBuf metadataAndPayload = entry.getDataBuffer();
                int readerIndex = metadataAndPayload.readerIndex();
                MessageMetadata metadata = Commands.peekAndCopyMessageMetadata(
                        metadataAndPayload, topic.getName(), -1);
                metadataAndPayload.readerIndex(readerIndex);
                if (metadata == null) {
                    throw new BrokerServiceException.PersistenceException(
                            "Failed to parse message metadata at " + position);
                }
                if (Markers.isTxnMarker(metadata)) {
                    result.add(EntryResult.invisible(entry, RandomReadInvisibleReason.TRANSACTION_MARKER));
                    continue;
                }
                if (Markers.isServerOnlyMarker(metadata)) {
                    result.add(EntryResult.invisible(entry, RandomReadInvisibleReason.SERVER_ONLY_MARKER));
                    continue;
                }
                if (readCommitted && metadata.hasTxnidMostBits() && metadata.hasTxnidLeastBits()
                        && topic.isTxnAborted(new TxnID(metadata.getTxnidMostBits(),
                        metadata.getTxnidLeastBits()), position)) {
                    result.add(EntryResult.invisible(entry, RandomReadInvisibleReason.ABORTED_TRANSACTION));
                    continue;
                }
                if (topic.isDelayedDeliveryEnabled()
                        && metadata.hasDeliverAtTime()
                        && metadata.getDeliverAtTime() > System.currentTimeMillis()) {
                    result.add(EntryResult.invisible(entry, RandomReadInvisibleReason.DELAYED_DELIVERY));
                    continue;
                }
                result.add(EntryResult.visible(entry));
            } catch (Throwable t) {
                entry.release();
                releaseResults(result);
                releaseEntries(entries.subList(i + 1, entries.size()));
                throw FutureUtil.wrapToCompletionException(t);
            }
        }
        return result;
    }

    private static void releaseResults(List<EntryResult> results) {
        for (EntryResult result : results) {
            result.release();
        }
    }

    private static void releaseEntries(List<Entry> entries) {
        for (Entry entry : entries) {
            if (entry != null) {
                entry.release();
            }
        }
    }

    public static ServerError toServerError(Throwable cause) {
        if (cause instanceof ManagedLedgerException) {
            if (cause instanceof ManagedLedgerException.ManagedLedgerFencedException
                    || cause instanceof ManagedLedgerException.ManagedLedgerAlreadyClosedException
                    || cause instanceof ManagedLedgerException.OffloadInProgressException) {
                return ServerError.ServiceNotReady;
            }
            if (cause instanceof ManagedLedgerException.ManagedLedgerNotFoundException) {
                return ServerError.TopicNotFound;
            }
            if (cause instanceof ManagedLedgerException.ManagedLedgerTerminatedException) {
                return ServerError.TopicTerminatedError;
            }
            if (cause instanceof ManagedLedgerException.NonRecoverableLedgerException) {
                return ServerError.PersistenceError;
            }
            // Generic fallback: conservative mapping to PersistenceError
            return ServerError.PersistenceError;
        }
        return BrokerServiceException.getClientErrorCode(cause);
    }

    public static final class EntryResult {
        private final Entry entry;
        private final RandomReadInvisibleReason invisibleReason;

        private EntryResult(Entry entry, RandomReadInvisibleReason invisibleReason) {
            this.entry = entry;
            this.invisibleReason = invisibleReason;
        }

        public static EntryResult visible(Entry entry) {
            return new EntryResult(entry, null);
        }

        public static EntryResult invisible(Entry entry, RandomReadInvisibleReason invisibleReason) {
            return new EntryResult(entry, invisibleReason);
        }

        public Entry entry() {
            return entry;
        }

        public boolean isVisible() {
            return invisibleReason == null;
        }

        public RandomReadInvisibleReason invisibleReason() {
            return invisibleReason;
        }

        public void release() {
            entry.release();
        }
    }
}
