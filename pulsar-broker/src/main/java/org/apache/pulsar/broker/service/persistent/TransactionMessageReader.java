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
package org.apache.pulsar.broker.service.persistent;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Queues;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 * Transaction message reader.
 */
@Slf4j
public class TransactionMessageReader {

    private ManagedLedgerImpl managedLedger;
    private Subscription subscription;
    private Executor executor;

    private final ConcurrentLinkedQueue<Entry> commitMarkerQueue;
    private final Set<PositionImpl> pendingReadPosition;
    private final ArrayList<Entry> abortMarkerList;

    public TransactionMessageReader(ManagedLedgerImpl managedLedger, Subscription subscription, Executor executor) {
        this.managedLedger = managedLedger;
        this.subscription = subscription;
        this.executor = executor;
        this.commitMarkerQueue = Queues.newConcurrentLinkedQueue();
        this.pendingReadPosition = Sets.newHashSet();
        this.abortMarkerList = new ArrayList<>();
    }

    public boolean havePendingTxnToRead() {
        return commitMarkerQueue.size() > 0;
    }

    /**
     * Read transaction messages.
     *
     * @param readMessageNum messages num to read
     * @param ctx context object
     * @param readEntriesCallback ReadEntriesCallback
     */
    public void read(int readMessageNum, Object ctx, AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        Entry commitEntry = commitMarkerQueue.peek();
        if (commitEntry == null) {
            log.warn("Commit entry is null.");
            readEntriesCallback.readEntriesComplete(Collections.emptyList(), ctx);
            return;
        }
        ByteBuf byteBuf = commitEntry.getDataBuffer();
        Commands.parseMessageMetadata(byteBuf);
        PulsarMarkers.TxnCommitMarker commitMarker = null;
        try {
            commitMarker = Markers.parseCommitMarker(byteBuf);
        } catch (IOException e) {
            log.error("Failed to parse commit marker.", e);
            readEntriesCallback.readEntriesFailed(
                    ManagedLedgerException.getManagedLedgerException(
                            new Exception("Failed to parse commit marker.")), ctx);
            return;
        }

        final List<Entry> entryList = new ArrayList<>();
        List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
        for (PulsarMarkers.MessageIdData messageIdData : commitMarker.getMessageIdListList()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            completableFutureList.add(future);
            managedLedger.asyncReadEntry(PositionImpl.get(messageIdData.getLedgerId(), messageIdData.getEntryId()),
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            entryList.add(entry);
                            future.complete(null);
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            future.completeExceptionally(exception);
                        }
                    }, null);
        }
        FutureUtil.waitForAll(completableFutureList).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                log.error("Failed to read transaction messages.", throwable);
                readEntriesCallback.readEntriesFailed(
                        ManagedLedgerException.getManagedLedgerException(
                                new Exception("Failed to read transaction messages.")), ctx);
                return;
            }
            for (Entry entry : entryList) {
                pendingReadPosition.add(PositionImpl.get(entry.getLedgerId(), entry.getEntryId()));
            }
            commitMarkerQueue.remove(commitEntry);
            commitEntry.release();
            readEntriesCallback.readEntriesComplete(entryList, ctx);
        });
    }

    public boolean shouldSendToConsumer(PulsarApi.MessageMetadata msgMetadata, Entry entry,
                                            List<Entry> entries, int entryIndex) {
        if (pendingReadPosition.remove(
                PositionImpl.get(entries.get(entryIndex).getLedgerId(), entries.get(entryIndex).getEntryId()))) {
            return true;
        }
        if (Markers.isTxnCommitMarker(msgMetadata)) {
            commitMarkerQueue.add(entry);
        } else if (Markers.isTxnAbortMarker(msgMetadata)) {
            abortMarkerList.add(entry);
            handleAbort();
        }
        entries.set(entryIndex, null);
        return false;
    }

    private void handleAbort() {
        executor.execute(() -> {
            List<Position> positionList = new ArrayList<>();
            for (Entry abortEntry : abortMarkerList) {
                ByteBuf byteBuf = abortEntry.getDataBuffer();
                Commands.parseMessageMetadata(byteBuf);
                PulsarMarkers.TxnCommitMarker abortMarker = null;
                try {
                    abortMarker = Markers.parseCommitMarker(byteBuf);
                    for (PulsarMarkers.MessageIdData messageIdData : abortMarker.getMessageIdListList()) {
                        positionList.add(PositionImpl.get(messageIdData.getLedgerId(), messageIdData.getEntryId()));
                    }
                    positionList.add(PositionImpl.get(abortEntry.getLedgerId(), abortEntry.getEntryId()));
                } catch (IOException e) {
                    log.error("Failed to parse abort marker.", e);
                }
            }
            subscription.acknowledgeMessage(positionList, PulsarApi.CommandAck.AckType.Individual, Collections.emptyMap());
        });
    }

}
