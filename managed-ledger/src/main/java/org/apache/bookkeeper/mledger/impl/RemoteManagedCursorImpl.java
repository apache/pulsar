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


import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.client.api.CursorClient;
import org.apache.pulsar.client.api.CursorData;
import org.apache.pulsar.client.cursor.CursorDataImpl;
import org.apache.pulsar.common.api.proto.CursorPosition;
import org.apache.pulsar.common.api.proto.LongProperty;

/**
 * {@link RemoteManagedCursorImpl} is used with {@link RemoteManagedLedgerImpl} for readonly topic.
 * RemoteManagedCursorImpl ues a cursorClient to handle persistent state,which is eventually processed by {@link
 * RemoteManagedLedgerImpl} in writer broker.
 */
@Slf4j
public class RemoteManagedCursorImpl extends ManagedCursorImpl {
    private final CursorClient cursorClient;
    private final RemoteManagedLedgerImpl remoteLedger;

    RemoteManagedCursorImpl(BookKeeper bookkeeper,
                            ManagedLedgerConfig config,
                            ManagedLedgerImpl ledger,
                            String cursorName,
                            CursorClient cursorClient) {
        super(bookkeeper, config, ledger, cursorName);
        this.cursorClient = cursorClient;
        this.remoteLedger = (RemoteManagedLedgerImpl) ledger;
    }

    /**
     * Initialize newly created cursor by calling {@link CursorClient#createCursorAsync(String, CursorData)}
     */
    @Override
    void initialize(PositionImpl position, Map<String, Long> properties, VoidCallback callback) {
        CursorPosition cursorPosition = new CursorPosition();
        cursorPosition.setLedgerId(position.ledgerId).setEntryId(position.entryId);
        properties.forEach((k, v) -> cursorPosition.addProperty().setName(k).setValue(v));
        cursorClient.createCursorAsync(name, new CursorDataImpl(cursorPosition))
                .thenCompose(cursorData -> {
                    CursorDataImpl data = (CursorDataImpl) cursorData;
                    log.info("[{}][{}] Create cursor returned {}", remoteLedger.getReaderTopic(), name, data);
                    return ((RemoteManagedLedgerImpl) ledger)
                            .waitSync(data.getVersion(), data.getLastConfirmedEntry())
                            .thenApply(v -> data.getPosition());
                }).thenAccept(pos -> {
            recoverFromCursorPosition(pos);
            callback.operationComplete();
        }).exceptionally(throwable -> {
            String errMsg = String.format("[%s] initialize cursor %s failed.", ledger.getName(), name);
            log.error(errMsg, throwable);
            callback.operationFailed(new ManagedLedgerException(errMsg, throwable));
            return null;
        });
    }

    private void recoverFromCursorPosition(CursorPosition positionInfo) {
        //从 topic writer 读取 PositionInfo
        Map<String, Long> recoveredProperties = Collections.emptyMap();
        if (positionInfo.getPropertiesCount() > 0) {
            // Recover properties map
            recoveredProperties = Maps.newHashMap();
            for (int i = 0; i < positionInfo.getPropertiesCount(); i++) {
                LongProperty property = positionInfo.getPropertyAt(i);
                recoveredProperties.put(property.getName(), property.getValue());
            }
        }

        PositionImpl position = new PositionImpl(positionInfo);

        recoverIndividualDeletedMessagesFromCursorPosition(positionInfo);

        recoverBatchDeletedIndexesFromCursorPosition(positionInfo);

        recoveredCursor(position, recoveredProperties, null);
    }

    /**
     * Recover already existing cursor by calling {@link CursorClient#getCursorAsync(String)}
     *
     * @param callback Recover result callback.
     */
    @Override
    void recover(VoidCallback callback) {
        //recover from remote writer topic
        recoverFromWriteOwnerBroker().thenAccept(positionInfo -> {
            recoverFromCursorPosition(positionInfo);
            callback.operationComplete();
        }).exceptionally(throwable -> {
            String errMsg = String.format("[%s] recover existing cursor %s failed", ledger.getName(), name);
            log.error(errMsg, throwable);
            callback.operationFailed(new ManagedLedgerException(errMsg, throwable));
            return null;
        });
    }

    private CompletableFuture<CursorPosition> recoverFromWriteOwnerBroker() {
        return cursorClient.getCursorAsync(name)
                .thenCompose(cursorData -> {
                    CursorDataImpl data = (CursorDataImpl) cursorData;
                    return ((RemoteManagedLedgerImpl) ledger)
                            .waitSync(data.getVersion(), data.getLastConfirmedEntry())
                            .thenApply(v -> data.getPosition());
                });
    }

    @Override
    protected void internalAsyncMarkDelete(PositionImpl newPosition, Map<String, Long> properties,
                                           AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {

        ledger.mbean.addMarkDeleteOp();

        MarkDeleteEntry mdEntry = new MarkDeleteEntry(newPosition, properties, callback, ctx);

        // We cannot write to the ledger during the switch, need to wait until the new metadata ledger is available
        synchronized (pendingMarkDeleteOps) {
            // The state might have changed while we were waiting on the queue mutex
            switch (STATE_UPDATER.get(this)) {
                case Closed:
                    callback.markDeleteFailed(new ManagedLedgerException
                            .CursorAlreadyClosedException("Cursor was already closed"), ctx);
                    return;
                case NoLedger:
                    if (PENDING_READ_OPS_UPDATER.get(this) > 0) {
                        // Wait until no read operation are pending
                        pendingMarkDeleteOps.add(mdEntry);
                    } else {
                        // Execute the mark delete immediately
                        internalMarkDelete(mdEntry);
                    }
                    break;

                default:
                    log.error("[{}][{}] Invalid cursor state: {}", ledger.getName(), name, state);
                    callback.markDeleteFailed(new ManagedLedgerException("Cursor was in invalid state: " + state), ctx);
                    break;
            }
        }
    }

    /**
     * Persist cursor position info by calling {@link CursorClient#updateCursorAsync(String, CursorData)}
     *
     * @param mdEntry  new persist data of this cursor.
     * @param callback callback for persist result.
     */
    @Override
    protected void persistPosition(MarkDeleteEntry mdEntry, VoidCallback callback) {
        CursorPosition position = buildCursorPosition(mdEntry);
        cursorClient.updateCursorAsync(name, new CursorDataImpl(position))
                .thenRun(callback::operationComplete)
                .exceptionally(throwable -> {
                    callback.operationFailed(
                            new ManagedLedgerException("Persist position to remote writer broker failed.", throwable));
                    return null;
                });
    }


    /**
     * Update lastConfirmedEntry of the RemoteManagedLedgerImpl before doing super.checkForNewEntries()
     */
    @Override
    protected void checkForNewEntries(OpReadEntry op, AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        ((RemoteManagedLedgerImpl) ledger).asyncUpdateLastConfirmedEntry().whenComplete((ignore, throwable) -> {
            if (throwable != null) {
                log.warn("asyncUpdateLastConfirmedEntry failed when checkForNewEntries for {}", name, throwable);
            }
            super.checkForNewEntries(op, callback, ctx);
        });
    }

    /**
     * Delete cursor by calling {@link CursorClient#deleteCursorAsync(String)}
     *
     * @return A {@link CompletableFuture} tracking the deleting process.
     */
    public CompletableFuture<Void> asyncDeleteCursor() {
        STATE_UPDATER.set(this, State.Closed);
        return cursorClient.deleteCursorAsync(name);
    }
}
