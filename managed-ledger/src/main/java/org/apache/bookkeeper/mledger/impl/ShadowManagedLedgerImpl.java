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
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.metadata.api.Stat;

/**
 * Detailed design can be found in <a href="https://github.com/apache/pulsar/issues/16153">PIP-180</a>.
 */
@Slf4j
public class ShadowManagedLedgerImpl extends ManagedLedgerImpl {
    private final String sourceMLName;
    private volatile Stat sourceLedgersStat;

    public ShadowManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper,
                                   MetaStore store, ManagedLedgerConfig config,
                                   OrderedScheduler scheduledExecutor,
                                   String name, final Supplier<Boolean> mlOwnershipChecker) {
        super(factory, bookKeeper, store, config, scheduledExecutor, name, mlOwnershipChecker);
        this.sourceMLName = config.getShadowSourceName();
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
        log.info("Opening shadow managed ledger {} with source={}", name, sourceMLName);
        executor.execute(safeRun(() -> doInitialize(callback, ctx)));
    }

    private void doInitialize(ManagedLedgerInitializeLedgerCallback callback, Object ctx) {
        // Fetch the list of existing ledgers in the source managed ledger
        store.watchManagedLedgerInfo(sourceMLName, (managedLedgerInfo, stat) ->
                executor.execute(safeRun(() -> processSourceManagedLedgerInfo(managedLedgerInfo, stat)))
        );
        store.getManagedLedgerInfo(sourceMLName, false, null, new MetaStore.MetaStoreCallback<>() {
            @Override
            public void operationComplete(MLDataFormats.ManagedLedgerInfo mlInfo, Stat stat) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Source ML info:{}", name, sourceMLName, mlInfo);
                }
                if (sourceLedgersStat != null && sourceLedgersStat.getVersion() >= stat.getVersion()) {
                    log.warn("Newer version of mlInfo is already processed. Previous stat={}, current stat={}",
                            sourceLedgersStat, stat);
                    return;
                }
                sourceLedgersStat = stat;
                if (mlInfo.getLedgerInfoCount() == 0) {
                    // Small chance here, since shadow topic is created after source topic exists.
                    log.warn("[{}] Source topic ledger list is empty! source={},mlInfo={},stat={}", name, sourceMLName,
                            mlInfo, stat);
                    ShadowManagedLedgerImpl.super.initialize(callback, ctx);
                    return;
                }

                if (mlInfo.hasTerminatedPosition()) {
                    lastConfirmedEntry = new PositionImpl(mlInfo.getTerminatedPosition());
                    log.info("[{}][{}] Recovering managed ledger terminated at {}", name, sourceMLName,
                            lastConfirmedEntry);
                }

                for (LedgerInfo ls : mlInfo.getLedgerInfoList()) {
                    ledgers.put(ls.getLedgerId(), ls);
                }

                final long lastLedgerId = ledgers.lastKey();
                mbean.startDataLedgerOpenOp();
                AsyncCallback.OpenCallback opencb = (rc, lh, ctx1) -> executor.execute(safeRun(() -> {
                    mbean.endDataLedgerOpenOp();
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Opened source ledger {}", name, lastLedgerId);
                    }
                    if (rc == BKException.Code.OK) {
                        LedgerInfo info =
                                LedgerInfo.newBuilder()
                                        .setLedgerId(lastLedgerId)
                                        .setEntries(lh.getLastAddConfirmed() + 1)
                                        .setSize(lh.getLength())
                                        .setTimestamp(clock.millis()).build();
                        ledgers.put(lastLedgerId, info);

                        //Always consider the last ledger is opened in source.
                        STATE_UPDATER.set(ShadowManagedLedgerImpl.this, State.LedgerOpened);
                        currentLedger = lh;

                        if (managedLedgerInterceptor != null) {
                            managedLedgerInterceptor.onManagedLedgerLastLedgerInitialize(name, lh)
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
                        log.warn("[{}] Source ledger not found: {}", name, lastLedgerId);
                        ledgers.remove(lastLedgerId);
                        ShadowManagedLedgerImpl.super.initialize(callback, ctx);
                    } else {
                        log.error("[{}] Failed to open source ledger {}: {}", name, lastLedgerId,
                                BKException.getMessage(rc));
                        callback.initializeFailed(createManagedLedgerException(rc));
                    }
                }));
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
        if (log.isDebugEnabled()) {
            log.debug("[{}] initializing bookkeeper for shadowManagedLedger; ledgers {}", name, ledgers);
        }

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
        lastConfirmedEntry = new PositionImpl(currentLedger.getId(), currentLedger.getLastAddConfirmed());
        // bypass empty ledgers, find last ledger with Message if possible.
        while (lastConfirmedEntry.getEntryId() == -1) {
            Map.Entry<Long, LedgerInfo> formerLedger = ledgers.lowerEntry(lastConfirmedEntry.getLedgerId());
            if (formerLedger != null) {
                LedgerInfo ledgerInfo = formerLedger.getValue();
                lastConfirmedEntry = PositionImpl.get(ledgerInfo.getLedgerId(), ledgerInfo.getEntries() - 1);
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

        if (log.isDebugEnabled()) {
            log.debug("[{}] Add entry into shadow ledger lh={} entries={}, pos=({},{})",
                    name, currentLedger.getId(), currentLedgerEntries, position.getLedgerId(), position.getEntryId());
        }
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
    private synchronized void processSourceManagedLedgerInfo(MLDataFormats.ManagedLedgerInfo mlInfo, Stat stat) {

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] new SourceManagedLedgerInfo:{}, prevStat={},stat={}", name, sourceMLName, mlInfo,
                    sourceLedgersStat, stat);
        }
        if (sourceLedgersStat != null && sourceLedgersStat.getVersion() >= stat.getVersion()) {
            log.warn("Newer version of mlInfo is already processed. Previous stat={}, current stat={}",
                    sourceLedgersStat, stat);
            return;
        }
        sourceLedgersStat = stat;

        if (mlInfo.hasTerminatedPosition()) {
            lastConfirmedEntry = new PositionImpl(mlInfo.getTerminatedPosition());
            log.info("[{}][{}] Process managed ledger terminated at {}", name, sourceMLName, lastConfirmedEntry);
        }

        TreeMap<Long, LedgerInfo> newLedgerInfos = new TreeMap<>();
        for (LedgerInfo ls : mlInfo.getLedgerInfoList()) {
            newLedgerInfos.put(ls.getLedgerId(), ls);
        }

        for (Map.Entry<Long, LedgerInfo> ledgerInfoEntry : newLedgerInfos.entrySet()) {
            Long ledgerId = ledgerInfoEntry.getKey();
            LedgerInfo ledgerInfo = ledgerInfoEntry.getValue();
            if (ledgerInfo.getEntries() > 0) {
                LedgerInfo oldLedgerInfo = ledgers.put(ledgerId, ledgerInfo);
                if (oldLedgerInfo == null) {
                    log.info("[{}]Read new ledger info from source,ledgerId={}", name, ledgerId);
                } else {
                    if (!oldLedgerInfo.equals(ledgerInfo)) {
                        log.info("[{}] Old ledger info updated in source,ledgerId={}", name, ledgerId);
                        // ledger deleted from bookkeeper by offloader.
                        if (ledgerInfo.hasOffloadContext()
                                && ledgerInfo.getOffloadContext().getBookkeeperDeleted()
                                && (!oldLedgerInfo.hasOffloadContext() || !oldLedgerInfo.getOffloadContext()
                                .getBookkeeperDeleted())) {
                            log.info("[{}] Old ledger removed from bookkeeper by offloader in source,ledgerId={}", name,
                                    ledgerId);
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
                    (rc, lh, ctx1) -> executor.execute(safeRun(() -> {
                        mbean.endDataLedgerOpenOp();
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Opened new source ledger {}", name, lastLedgerId);
                        }
                        if (rc == BKException.Code.OK) {
                            LedgerInfo info = LedgerInfo.newBuilder()
                                    .setLedgerId(lastLedgerId)
                                    .setEntries(lh.getLastAddConfirmed() + 1)
                                    .setSize(lh.getLength())
                                    .setTimestamp(clock.millis()).build();
                            ledgers.put(lastLedgerId, info);
                            currentLedger = lh;
                            currentLedgerEntries = 0;
                            currentLedgerSize = 0;
                            initLastConfirmedEntry();
                            updateLedgersIdsComplete();
                            maybeUpdateCursorBeforeTrimmingConsumedLedger();
                        } else if (isNoSuchLedgerExistsException(rc)) {
                            log.warn("[{}] Source ledger not found: {}", name, lastLedgerId);
                            ledgers.remove(lastLedgerId);
                        } else {
                            log.error("[{}] Failed to open source ledger {}: {}", name, lastLedgerId,
                                    BKException.getMessage(rc));
                        }
                    })), null);
        }

        //handle old ledgers deleted.
        List<LedgerInfo> ledgersToDelete = new ArrayList<>(ledgers.headMap(newLedgerInfos.firstKey(), false).values());
        if (!ledgersToDelete.isEmpty()) {
            log.info("[{}]ledgers deleted in source, size={}", name, ledgersToDelete.size());
            try {
                advanceCursorsIfNecessary(ledgersToDelete);
            } catch (ManagedLedgerException.LedgerNotExistException e) {
                log.info("[{}] First non deleted Ledger is not found, advanceCursors fails", name);
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
    protected synchronized void updateLedgersIdsComplete() {
        STATE_UPDATER.set(this, State.LedgerOpened);
        updateLastLedgerCreatedTimeAndScheduleRolloverTask();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Resending {} pending messages", name, pendingAddEntries.size());
        }

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
}
