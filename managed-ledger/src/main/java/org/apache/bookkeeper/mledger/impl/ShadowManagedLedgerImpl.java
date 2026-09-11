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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.Errors.isNoSuchLedgerExistsException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.NestedPositionInfo;
import org.apache.pulsar.metadata.api.Stat;

/**
 * Detailed design can be found in <a href="https://github.com/apache/pulsar/issues/16153">PIP-180</a>.
 */
@CustomLog
public class ShadowManagedLedgerImpl extends ManagedLedgerImpl {
    private final String sourceMLName;
    private volatile Stat sourceLedgersStat;

    public ShadowManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper,
                                   MetaStore store, ManagedLedgerConfig config,
                                   OrderedScheduler scheduledExecutor,
                                   String name, final Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
        super(factory, bookKeeper, store, config, scheduledExecutor, name, mlOwnershipChecker);
        this.sourceMLName = config.getShadowSourceName();
        // ShadowManagedLedgerImpl does not implement add entry timeout yet, so this variable will always be false.
        this.currentLedgerTimeoutTriggered = new AtomicBoolean(false);
    }

    /**
     * ShadowManagedLedger init steps:
     * 1. this.initialize : read source managedLedgerInfo
     * 2. super.initialize : read its own read source managedLedgerInfo
     * 3. this.initializeBookKeeper
     * 4. super.initializeCursors
     */
    @Override
    synchronized void initialize(ManagedLedgerInitializeLedgerCallback callback, Object ctx) {
        log.info().attr("name", name).attr("source", sourceMLName).log("Opening shadow managed ledger");
        executor.execute(() -> doInitialize(callback, ctx));
    }

    private void doInitialize(ManagedLedgerInitializeLedgerCallback callback, Object ctx) {
        // Fetch the list of existing ledgers in the source managed ledger
        store.watchManagedLedgerInfo(sourceMLName, (managedLedgerInfo, stat) ->
                executor.execute(() -> processSourceManagedLedgerInfo(managedLedgerInfo, stat))
        );
        store.getManagedLedgerInfo(sourceMLName, false, null, new MetaStore.MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                log.debug().attr("name", name)
                        .attr("source", sourceMLName)
                        .attr("mlInfo", mlInfo)
                        .log("Source ML info");
                if (sourceLedgersStat != null && sourceLedgersStat.getVersion() >= stat.getVersion()) {
                    log.warn().attr("previousStat", sourceLedgersStat)
                            .attr("currentStat", stat)
                            .log("Newer version of mlInfo is already processed");
                    return;
                }
                sourceLedgersStat = stat;
                if (mlInfo.getLedgerInfosCount() == 0) {
                    // Small chance here, since shadow topic is created after source topic exists.
                    log.warn().attr("name", name)
                            .attr("source", sourceMLName)
                            .attr("mlInfo", mlInfo)
                            .attr("stat", stat)
                            .log("Source topic ledger list is empty");
                    ShadowManagedLedgerImpl.super.initialize(callback, ctx);
                    return;
                }

                if (mlInfo.hasTerminatedPosition()) {
                    NestedPositionInfo terminatedPosition = mlInfo.getTerminatedPosition();
                    lastConfirmedEntry =
                            PositionFactory.create(terminatedPosition.getLedgerId(), terminatedPosition.getEntryId());
                    log.info().attr("name", name)
                            .attr("source", sourceMLName)
                            .attr("lastConfirmedEntry", lastConfirmedEntry)
                            .log("Recovering managed ledger terminated");
                }

                for (int i = 0; i < mlInfo.getLedgerInfosCount(); i++) {
                    LedgerInfo ls = mlInfo.getLedgerInfoAt(i);
                    ledgers.put(ls.getLedgerId(), ls);
                }

                final long lastLedgerId = ledgers.lastKey();
                mbean.startDataLedgerOpenOp();
                AsyncCallback.OpenCallback opencb = (rc, lh, ctx1) -> executor.execute(() -> {
                    mbean.endDataLedgerOpenOp();
                    log.debug().attr("name", name).attr("ledgerId", lastLedgerId).log("Opened source ledger");
                    if (rc == BKException.Code.OK) {
                        LedgerInfo info =
                                new LedgerInfo()
                                        .setLedgerId(lastLedgerId)
                                        .setEntries(lh.getLastAddConfirmed() + 1)
                                        .setSize(lh.getLength())
                                        .setTimestamp(clock.millis());
                        ledgers.put(lastLedgerId, info);

                        //Always consider the last ledger is opened in source.
                        STATE_UPDATER.set(ShadowManagedLedgerImpl.this, State.LedgerOpened);
                        currentLedger = lh;

                        if (managedLedgerInterceptor != null) {
                            managedLedgerInterceptor
                                    .onManagedLedgerLastLedgerInitialize(name, createLastEntryHandle(lh))
                                    .thenRun(() -> ShadowManagedLedgerImpl.super.initialize(callback, ctx))
                                    .exceptionally(ex -> {
                                        callback.initializeFailed(
                                                new ManagedLedgerException.ManagedLedgerInterceptException(
                                                        ex.getCause()));
                                        return null;
                                    });
                        } else {
                            ShadowManagedLedgerImpl.super.initialize(callback, ctx);
                        }
                    } else if (isNoSuchLedgerExistsException(rc)) {
                        log.warn().attr("name", name).attr("ledgerId", lastLedgerId).log("Source ledger not found");
                        ledgers.remove(lastLedgerId);
                        ShadowManagedLedgerImpl.super.initialize(callback, ctx);
                    } else {
                        log.error().attr("name", name)
                                .attr("ledgerId", lastLedgerId)
                                .attr("errorMessage", BKException.getMessage(rc))
                                .log("Failed to open source ledger");
                        callback.initializeFailed(createManagedLedgerException(rc));
                    }
                });
                //open ledger in readonly mode.
                bookKeeper.asyncOpenLedgerNoRecovery(lastLedgerId, digestType, config.getPassword(), opencb, null);

            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                if (e instanceof ManagedLedgerException.MetadataNotFoundException) {
                    callback.initializeFailed(new ManagedLedgerException.ManagedLedgerNotFoundException(e));
                } else {
                    callback.initializeFailed(new ManagedLedgerException(e));
                }
            }
        });
    }

    @Override
    protected synchronized void initializeBookKeeper(ManagedLedgerInitializeLedgerCallback callback) {
        log.debug().attr("name", name).attr("ledgers", ledgers).log("Initializing bookkeeper for shadowManagedLedger");

        // Calculate total entries and size
        Iterator<LedgerInfo> iterator = ledgers.values().iterator();
        while (iterator.hasNext()) {
            LedgerInfo li = iterator.next();
            if (li.getEntries() > 0) {
                NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, li.getEntries());
                TOTAL_SIZE_UPDATER.addAndGet(this, li.getSize());
            } else if (li.getLedgerId() != currentLedger.getId()) {
                //do not remove the last empty ledger.
                iterator.remove();
            }
        }

        initLastConfirmedEntry();
        // Save it back to ensure all nodes exist and properties are persisted.
        store.asyncUpdateLedgerIds(name, getManagedLedgerInfo(), ledgersStat, new MetaStore.MetaStoreCallback<>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                ledgersStat = stat;
                initializeCursors(callback);
            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                handleBadVersion(e);
                callback.initializeFailed(new ManagedLedgerException(e));
            }
        });
    }

    private void initLastConfirmedEntry() {
        if (currentLedger == null) {
            return;
        }
        lastConfirmedEntry = PositionFactory.create(currentLedger.getId(), currentLedger.getLastAddConfirmed());
        // bypass empty ledgers, find last ledger with Message if possible.
        while (lastConfirmedEntry.getEntryId() == -1) {
            Map.Entry<Long, LedgerInfo> formerLedger = ledgers.lowerEntry(lastConfirmedEntry.getLedgerId());
            if (formerLedger != null) {
                LedgerInfo ledgerInfo = formerLedger.getValue();
                lastConfirmedEntry = PositionFactory.create(ledgerInfo.getLedgerId(), ledgerInfo.getEntries() - 1);
            } else {
                break;
            }
        }
    }

    @Override
    protected synchronized void internalAsyncAddEntry(OpAddEntry addOperation) {
        if (!beforeAddEntry(addOperation)) {
            return;
        }
        if (state != State.LedgerOpened) {
            addOperation.failed(new ManagedLedgerException("Managed ledger is not opened"));
            return;
        }

        if (addOperation.getCtx() == null || !(addOperation.getCtx() instanceof Position position)) {
            addOperation.failed(new ManagedLedgerException("Illegal addOperation context object."));
            return;
        }

        log.debug().attr("name", name)
                .attr("ledgerId", currentLedger.getId())
                .attr("entries", currentLedgerEntries)
                .attr("posLedgerId", position.getLedgerId())
                .attr("posEntryId", position.getEntryId())
                .log("Add entry into shadow ledger");
        pendingAddEntries.add(addOperation);
        if (position.getLedgerId() <= currentLedger.getId()) {
            // Write into lastLedger
            if (position.getLedgerId() == currentLedger.getId()) {
                addOperation.setLedger(currentLedger);
            }
            currentLedgerEntries = position.getEntryId();
            currentLedgerSize += addOperation.data.readableBytes();
            addOperation.initiateShadowWrite();
        } // for addOperation with ledgerId > currentLedger, will be processed in `updateLedgersIdsComplete`
        lastAddEntryTimeMs = System.currentTimeMillis();
    }

    /**
     * terminate is not allowed on shadow topic.
     * @param callback
     * @param ctx
     */
    @Override
    public synchronized void asyncTerminate(AsyncCallbacks.TerminateCallback callback, Object ctx) {
        callback.terminateFailed(new ManagedLedgerException("Terminate is not allowed on shadow topic."), ctx);
    }

    /**
     * Handle source ManagedLedgerInfo updates.
     * Update types:
     * 1. new ledgers.
     * 2. old ledgers deleted.
     * 3. old ledger offload info updated (including ledger deleted from bookie by offloader)
     */
    private synchronized void processSourceManagedLedgerInfo(ManagedLedgerInfo mlInfo, Stat stat) {

        log.debug().attr("name", name)
                .attr("source", sourceMLName)
                .attr("mlInfo", mlInfo)
                .attr("previousStat", sourceLedgersStat)
                .attr("stat", stat)
                .log("New SourceManagedLedgerInfo");
        if (sourceLedgersStat != null && sourceLedgersStat.getVersion() >= stat.getVersion()) {
            log.warn().attr("previousStat", sourceLedgersStat)
                    .attr("currentStat", stat)
                    .log("Newer version of mlInfo is already processed");
            return;
        }
        sourceLedgersStat = stat;

        if (mlInfo.hasTerminatedPosition()) {
            NestedPositionInfo terminatedPosition = mlInfo.getTerminatedPosition();
            lastConfirmedEntry =
                    PositionFactory.create(terminatedPosition.getLedgerId(), terminatedPosition.getEntryId());
            log.info().attr("name", name)
                    .attr("source", sourceMLName)
                    .attr("lastConfirmedEntry", lastConfirmedEntry)
                    .log("Process managed ledger terminated");
        }

        TreeMap<Long, LedgerInfo> newLedgerInfos = new TreeMap<>();
        for (int i = 0; i < mlInfo.getLedgerInfosCount(); i++) {
            LedgerInfo ls = mlInfo.getLedgerInfoAt(i);
            newLedgerInfos.put(ls.getLedgerId(), ls);
        }

        for (Map.Entry<Long, LedgerInfo> ledgerInfoEntry : newLedgerInfos.entrySet()) {
            Long ledgerId = ledgerInfoEntry.getKey();
            LedgerInfo ledgerInfo = ledgerInfoEntry.getValue();
            if (ledgerInfo.getEntries() > 0) {
                LedgerInfo oldLedgerInfo = ledgers.put(ledgerId, ledgerInfo);
                if (oldLedgerInfo == null) {
                    log.info().attr("name", name).attr("ledgerId", ledgerId).log("Read new ledger info from source");
                } else {
                    if (!oldLedgerInfo.equals(ledgerInfo)) {
                        log.info().attr("name", name)
                                .attr("ledgerId", ledgerId)
                                .log("Old ledger info updated in source");
                        // ledger deleted from bookkeeper by offloader.
                        if (ledgerInfo.hasOffloadContext()
                                && ledgerInfo.getOffloadContext().isBookkeeperDeleted()
                                && (!oldLedgerInfo.hasOffloadContext() || !oldLedgerInfo.getOffloadContext()
                                .isBookkeeperDeleted())) {
                            log.info().attr("name", name)
                                    .attr("ledgerId", ledgerId)
                                    .log("Old ledger removed from bookkeeper"
                                            + " by offloader in source");
                            invalidateReadHandle(ledgerId);
                        }
                    }
                }
            }
        }
        Long lastLedgerId = newLedgerInfos.lastKey();
        // open the last ledger.
        if (lastLedgerId != null && !(currentLedger != null && currentLedger.getId() == lastLedgerId)) {
            ledgers.put(lastLedgerId, newLedgerInfos.get(lastLedgerId));
            mbean.startDataLedgerOpenOp();
            //open ledger in readonly mode.
            bookKeeper.asyncOpenLedgerNoRecovery(lastLedgerId, digestType, config.getPassword(),
                    (rc, lh, ctx1) -> executor.execute(() -> {
                        mbean.endDataLedgerOpenOp();
                        log.debug().attr("name", name).attr("ledgerId", lastLedgerId).log("Opened new source ledger");
                        if (rc == BKException.Code.OK) {
                            LedgerInfo info = new LedgerInfo()
                                    .setLedgerId(lastLedgerId)
                                    .setEntries(lh.getLastAddConfirmed() + 1)
                                    .setSize(lh.getLength())
                                    .setTimestamp(clock.millis());
                            ledgers.put(lastLedgerId, info);
                            currentLedger = lh;
                            currentLedgerEntries = 0;
                            currentLedgerSize = 0;
                            initLastConfirmedEntry();
                            updateLedgersIdsComplete(null);
                            maybeUpdateCursorBeforeTrimmingConsumedLedger();
                        } else if (isNoSuchLedgerExistsException(rc)) {
                            log.warn().attr("name", name).attr("ledgerId", lastLedgerId).log("Source ledger not found");
                            ledgers.remove(lastLedgerId);
                        } else {
                            log.error().attr("name", name)
                                    .attr("ledgerId", lastLedgerId)
                                    .attr("errorMessage", BKException.getMessage(rc))
                                    .log("Failed to open source ledger");
                        }
                    }), null);
        }

        //handle old ledgers deleted.
        List<LedgerInfo> ledgersToDelete = new ArrayList<>(ledgers.headMap(newLedgerInfos.firstKey(), false).values());
        if (!ledgersToDelete.isEmpty()) {
            log.info().attr("name", name).attr("size", ledgersToDelete.size()).log("Ledgers deleted in source");
            try {
                advanceCursorsIfNecessary(ledgersToDelete);
            } catch (ManagedLedgerException.LedgerNotExistException e) {
                log.info().attr("name", name).log("First non deleted Ledger is not found, advanceCursors fails");
            }
            doDeleteLedgers(ledgersToDelete);
        }
    }


    @Override
    public synchronized void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        store.unwatchManagedLedgerInfo(sourceMLName);
        super.asyncClose(callback, ctx);
    }

    @Override
    protected synchronized void updateLedgersIdsComplete(LedgerHandle originalCurrentLedger) {
        STATE_UPDATER.set(this, State.LedgerOpened);
        updateLastLedgerCreatedTimeAndScheduleRolloverTask();

        log.debug().attr("name", name)
                .attr("pendingMessages", pendingAddEntries.size())
                .log("Resending pending messages");

        createNewOpAddEntryForNewLedger();

        // Process all the pending addEntry requests
        for (OpAddEntry op : pendingAddEntries) {
            Position position = (Position) op.getCtx();
            if (position.getLedgerId() <= currentLedger.getId()) {
                if (position.getLedgerId() == currentLedger.getId()) {
                    op.setLedger(currentLedger);
                } else {
                    op.setLedger(null);
                }
                currentLedgerEntries = position.getEntryId();
                currentLedgerSize += op.data.readableBytes();
                op.initiateShadowWrite();
            } else {
                break;
            }
        }
    }

    @Override
    protected void updateLastLedgerCreatedTimeAndScheduleRolloverTask() {
        this.lastLedgerCreatedTimestamp = clock.millis();
    }

    @Override
    boolean shouldCacheAddedEntry() {
        return false;
    }
}
