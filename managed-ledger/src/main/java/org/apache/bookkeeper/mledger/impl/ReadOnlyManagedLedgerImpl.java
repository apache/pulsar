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
package org.apache.bookkeeper.mledger.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.google.common.collect.Range;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetadataNotFoundException;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.metadata.api.Stat;

@Slf4j
public class ReadOnlyManagedLedgerImpl extends ManagedLedgerImpl {

    public ReadOnlyManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, MetaStore store,
            ManagedLedgerConfig config, OrderedScheduler scheduledExecutor,
            String name) {
        super(factory, bookKeeper, store, config, scheduledExecutor, name);
    }

    CompletableFuture<ReadOnlyCursor> initializeAndCreateCursor(PositionImpl startPosition) {
        CompletableFuture<ReadOnlyCursor> future = new CompletableFuture<>();

        // Fetch the list of existing ledgers in the managed ledger
        store.getManagedLedgerInfo(name, false, new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                state = State.LedgerOpened;

                for (LedgerInfo ls : mlInfo.getLedgerInfoList()) {
                    ledgers.put(ls.getLedgerId(), ls);
                }

                // Last ledger stat may be zeroed, we must update it
                if (ledgers.size() > 0 && ledgers.lastEntry().getValue().getEntries() == 0) {
                    long lastLedgerId = ledgers.lastKey();

                    // Fetch last add confirmed for last ledger
                    bookKeeper.newOpenLedgerOp().withRecovery(false).withLedgerId(lastLedgerId)
                            .withDigestType(config.getDigestType()).withPassword(config.getPassword()).execute()
                            .thenAccept(readHandle -> {
                                readHandle.readLastAddConfirmedAsync().thenAccept(lastAddConfirmed -> {
                                    LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(lastLedgerId)
                                            .setEntries(lastAddConfirmed + 1).setSize(readHandle.getLength())
                                            .setTimestamp(clock.millis()).build();
                                    ledgers.put(lastLedgerId, info);

                                    future.complete(createReadOnlyCursor(startPosition));
                                }).exceptionally(ex -> {
                                    if (ex instanceof CompletionException
                                            && ex.getCause() instanceof IllegalArgumentException) {
                                        // The last ledger was empty, so we cannot read the last add confirmed.
                                        LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(lastLedgerId)
                                                .setEntries(0).setSize(0).setTimestamp(clock.millis()).build();
                                        ledgers.put(lastLedgerId, info);
                                        future.complete(createReadOnlyCursor(startPosition));
                                    } else {
                                        future.completeExceptionally(new ManagedLedgerException(ex));
                                    }
                                    return null;
                                });
                            }).exceptionally(ex -> {
                                if (ex instanceof CompletionException
                                        && ex.getCause() instanceof ArrayIndexOutOfBoundsException) {
                                    // The last ledger was empty, so we cannot read the last add confirmed.
                                    LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(lastLedgerId).setEntries(0)
                                            .setSize(0).setTimestamp(clock.millis()).build();
                                    ledgers.put(lastLedgerId, info);
                                    future.complete(createReadOnlyCursor(startPosition));
                                } else {
                                    future.completeExceptionally(new ManagedLedgerException(ex));
                                }
                                return null;
                            });
                } else {
                    // The read-only managed ledger is ready to use
                    future.complete(createReadOnlyCursor(startPosition));
                }
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                if (e instanceof MetadataNotFoundException) {
                    future.completeExceptionally(new ManagedLedgerNotFoundException(e));
                } else {
                    future.completeExceptionally(new ManagedLedgerException(e));
                }
            }
        });

        return future;
    }

    private ReadOnlyCursor createReadOnlyCursor(PositionImpl startPosition) {
        if (ledgers.isEmpty()) {
            lastConfirmedEntry = PositionImpl.earliest;
        } else if (ledgers.lastEntry().getValue().getEntries() > 0) {
            // Last ledger has some of the entries
            lastConfirmedEntry = new PositionImpl(ledgers.lastKey(), ledgers.lastEntry().getValue().getEntries() - 1);
        } else {
            // Last ledger is empty. If there is a previous ledger, position on the last entry of that ledger
            if (ledgers.size() > 1) {
                long lastLedgerId = ledgers.lastKey();
                LedgerInfo li = ledgers.headMap(lastLedgerId, false).lastEntry().getValue();
                lastConfirmedEntry = new PositionImpl(li.getLedgerId(), li.getEntries() - 1);
            } else {
                lastConfirmedEntry = PositionImpl.earliest;
            }
        }

        return new ReadOnlyCursorImpl(bookKeeper, config, this, startPosition, "read-only-cursor");
    }

    @Override
    public void asyncReadEntry(PositionImpl position, AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
            this.getLedgerHandle(position.getLedgerId())
                    .thenAccept((ledger) -> asyncReadEntry(ledger, position, callback, ctx))
                    .exceptionally((ex) -> {
                        log.error("[{}] Error opening ledger for reading at position {} - {}", this.name, position, ex.getMessage());
                        callback.readEntryFailed(ManagedLedgerException.getManagedLedgerException(ex.getCause()), ctx);
                        return null;
                    });
    }

    @Override
    public long getNumberOfEntries() {
        return getNumberOfEntries(Range.openClosed(PositionImpl.earliest, getLastPosition()));
    }

    @Override
    protected boolean isReadOnly() {
        return true;
    }
}
