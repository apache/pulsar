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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import io.netty.buffer.ByteBuf;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckMetadata;
import org.apache.pulsar.broker.transaction.pendingack.proto.PendingAckMetadataEntry;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;

/**
 * Pending ack timer task.
 */
@Slf4j
public class MLPendingAckStoreTimerTask implements TimerTask {

    private int intervalTime;

    private final int maxIntervalTime;

    private final int minIntervalTime;

    private final ManagedCursorImpl subManagedCursor;

    private final ManagedLedgerImpl storeManagedLedger;

    private final ManagedCursorImpl managedCursor;

    private final Timer timer;

    private volatile PositionImpl markDeletePosition;

    public MLPendingAckStoreTimerTask(ManagedCursor managedCursor, ManagedLedger storeManagedLedger,
                                      int maxIntervalTime, int minIntervalTime,
                                      ManagedCursor subManageCursor, Timer timer) {
        this.intervalTime = minIntervalTime;
        this.maxIntervalTime = maxIntervalTime;
        this.minIntervalTime = minIntervalTime;
        this.managedCursor = (ManagedCursorImpl) managedCursor;
        this.subManagedCursor = (ManagedCursorImpl) subManageCursor;
        this.storeManagedLedger = (ManagedLedgerImpl) storeManagedLedger;
        this.markDeletePosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        this.timer = timer;
    }

    @Override
    public void run(Timeout timeout) {
        if (this.managedCursor.getState().equals("Closed")) {
            return;
        }
        try {
            // when no transaction ack operation in this pending ack store, it will increase the interval time
            if (markDeletePosition.compareTo((PositionImpl) storeManagedLedger.getLastConfirmedEntry()) == 0) {
                int time = intervalTime + minIntervalTime;
                intervalTime = Math.min(time, maxIntervalTime);
                managedCursor.markDelete(markDeletePosition);
                timer.newTimeout(MLPendingAckStoreTimerTask.this, intervalTime, TimeUnit.SECONDS);
                return;
            } else {
                int time = intervalTime - minIntervalTime;
                intervalTime = Math.max(time, minIntervalTime);
            }
            // this while in order to find the last position witch can mark delete
            while (true) {
                PositionImpl nextPosition = storeManagedLedger.getNextValidPosition(markDeletePosition);
                if (nextPosition.compareTo((PositionImpl) storeManagedLedger.getLastConfirmedEntry()) > 0) {
                    timer.newTimeout(MLPendingAckStoreTimerTask.this, intervalTime, TimeUnit.SECONDS);
                    return;
                }
                Entry entry = getEntry(nextPosition).get();
                ByteBuf buffer = entry.getDataBuffer();
                PendingAckMetadataEntry pendingAckMetadataEntry = new PendingAckMetadataEntry();
                pendingAckMetadataEntry.parseFrom(buffer, buffer.readableBytes());

                try {
                    switch (pendingAckMetadataEntry.getPendingAckOp()) {
                        case ACK:
                            if (pendingAckMetadataEntry.getAckType() == AckType.Cumulative) {
                                PendingAckMetadata pendingAckMetadata =
                                        pendingAckMetadataEntry.getPendingAckMetadatasList().get(0);
                                handleAckMarkDeletePosition(PositionImpl.get(pendingAckMetadata.getLedgerId(),
                                        pendingAckMetadata.getEntryId()), nextPosition);
                            } else {
                                //this judge the pendingAckMetadataEntry is can delete
                                PositionImpl largestPosition = null;
                                List<PendingAckMetadata> metadataList =
                                        pendingAckMetadataEntry.getPendingAckMetadatasList();
                                for (int i = 0; i < metadataList.size(); i++) {
                                    PendingAckMetadata pendingAckMetadata = metadataList.get(0);
                                    if (largestPosition == null) {
                                        largestPosition = PositionImpl.get(pendingAckMetadata.getLedgerId(),
                                                pendingAckMetadata.getEntryId());
                                    } else {
                                        PositionImpl comparePosition = PositionImpl
                                                .get(pendingAckMetadata.getLedgerId(),
                                                        pendingAckMetadata.getEntryId());
                                        if (largestPosition.compareTo(comparePosition) <  0) {
                                            largestPosition = comparePosition;
                                        }

                                    }
                                }
                                if (largestPosition != null) {
                                    handleAckMarkDeletePosition(largestPosition, nextPosition);
                                }
                            }
                            break;
                        case ABORT:
                        case COMMIT:
                            markDeletePosition = nextPosition;
                            break;
                        default:
                            log.error("PendingAck timer task read illegal metadata state! {}",
                                    pendingAckMetadataEntry.getPendingAckOp());
                    }
                } finally {
                    entry.release();
                }
                // when markDeletePosition is not nextPosition, before markDeletePosition can delete
                if (markDeletePosition != nextPosition) {
                    this.managedCursor.markDelete(markDeletePosition);
                    break;
                }
            }
            this.timer.newTimeout(this, intervalTime, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("PendingAck timer task error!", e);
            }
            if (e instanceof ManagedLedgerException.CursorAlreadyClosedException) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}]MLPendingAckStoreTimerTask will close!", this.storeManagedLedger.getName(), e);
                }
                return;
            }
            this.timer.newTimeout(this, intervalTime, TimeUnit.MILLISECONDS);
        }
    }

    private CompletableFuture<Entry> getEntry(PositionImpl position) {
        CompletableFuture<Entry> completableFuture = new CompletableFuture<>();
        this.storeManagedLedger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                completableFuture.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                completableFuture.completeExceptionally(exception);
            }
        }, null);
        return completableFuture;
    }

    private void handleAckMarkDeletePosition(PositionImpl readPosition, PositionImpl storePosition) {
        if (readPosition.compareTo((PositionImpl) subManagedCursor.getMarkDeletedPosition()) <= 0) {
            markDeletePosition = storePosition;
        }
    }
}
