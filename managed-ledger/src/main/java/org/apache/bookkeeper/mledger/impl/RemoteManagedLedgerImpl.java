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


import static org.apache.bookkeeper.mledger.ManagedLedgerException.getManagedLedgerException;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import io.netty.buffer.ByteBuf;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.client.api.CursorClient;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.EntryPosition;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ToStringUtil;
import org.apache.pulsar.metadata.api.Stat;

/**
 * When writerNamespace is set, assuming it's NS-W, in the {@link org.apache.pulsar.common.policies.data.Policies}
 * of a namespace NS-R, we can consume all the topics from NS-W through NS-R.
 *
 * For example, if a consumer client subscribe to a reader topic "persistent://tenant/NS-R/topic", it's effective
 * equals to subscribing to the writer topic "persistent://tenant/NS-W/topic".
 *
 * {@link RemoteManagedLedgerImpl} is used to track the managed ledgers in writer topic for reader topic. Any ledger
 * write operation such as addEntry or truncate is not supported in RemoteManagedLedgerImpl.
 * Meta of the ledgers, such as {@link org.apache.bookkeeper.mledger.ManagedLedgerInfo} and Cursors, is read from zk,
 * and message data is directly read from bookie.
 *
 * NS-W and NS-R can be assigned on different brokers using isolation mechanism, so that we can achieve more consuming
 * throughput for a single topic and some other benefits. More info can be found here
 * <a href="https://github.com/apache/pulsar/wiki/PIP-63%3A-Readonly-Topic-Ownership-Support">PIP 63: Readonly Topic
 * Ownership Support</a>
 */
@Slf4j
public class RemoteManagedLedgerImpl extends ManagedLedgerImpl {

    private final Supplier<PulsarClient> clientSupplier;
    private final String readerTopic;
    private CursorClient cursorClient;


    public RemoteManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper,
                                   MetaStore store, ManagedLedgerConfig config,
                                   OrderedScheduler scheduledExecutor, String name,
                                   Supplier<Boolean> mlOwnershipChecker,
                                   Supplier<PulsarClient> clientSupplier) {
        super(factory, bookKeeper, store, config, scheduledExecutor,
                config.getWriterTopic().getPersistenceNamingEncoding(), mlOwnershipChecker);
        readerTopic = name;
        this.clientSupplier = clientSupplier;
    }


    /**
     * RemoteManagedLedgerImpl initialize process:
     * 1. set up cursorClient.
     * 2. start watching ManagedLedgerInfo data in zk, for new ledgers info syncing.
     * 3. read ManagedLedgerInfo from zk, calls super.initialize(...)
     * 4. this.initializeBookKeeper(), init lastConfirmedEntry.
     * 5. super.initializeCursors
     *
     * @param callback
     * @param ctx
     */
    @Override
    synchronized void initialize(ManagedLedgerInitializeLedgerCallback callback, Object ctx) {
        clientSupplier.get()
                .newCursorClient()
                .topic(config.getWriterTopic().toString())
                .createAsync()
                .thenAccept(cursorClient -> {
                    executor.executeOrdered(name, safeRun(() -> {
                        this.cursorClient = cursorClient;
                        store.watchManagedLedgerInfo(name,
                                new MetaStore.MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
                                    @Override
                                    public void operationComplete(MLDataFormats.ManagedLedgerInfo mlInfo, Stat stat) {
                                        executor.executeOrdered(name, safeRun(() -> {
                                            processManagedLedgerInfoUpdate(mlInfo, stat);

                                        }));
                                    }

                                    @Override
                                    public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                                        log.warn("Get ManagedLedgerInfo for {} failed", readerTopic, e);
                                    }
                                });
                        super.initialize(callback, ctx);
                    }));
                }).exceptionally(e -> {
            callback.initializeFailed(ManagedLedgerException.getManagedLedgerException(e));
            return null;
        });

    }

    /**
     * process new ManagedLedgerInfo from meta store.
     * Add new ledgers and delete truncated ledgers and triggers asyncUpdateLastConfirmedEntry.
     */
    private void processManagedLedgerInfoUpdate(MLDataFormats.ManagedLedgerInfo mlInfo, Stat stat) {
        log.info("[{}] Receive new ManagedLedgerInfo:{}, stat:{}", readerTopic, mlInfo, stat);
        if (ledgersStat != null && ledgersStat.getVersion() > stat.getVersion()) {
            log.info("[{}] ledger info stat.version={} is older than current version({})",
                    readerTopic, stat.getVersion(), ledgersStat.getVersion());
            return;
        }
        //handle new ledgers and remove deleted ledgers.
        ledgersStat = stat;
        if (mlInfo.hasTerminatedPosition()) {
            state = State.Terminated;
            lastConfirmedEntry = new PositionImpl(mlInfo.getTerminatedPosition());
            log.info("[{}] Recovering managed ledger terminated at {}", readerTopic, lastConfirmedEntry);
        }

        long minLedgerId = Long.MAX_VALUE;
        long maxLedgerId = Long.MIN_VALUE;
        for (LedgerInfo ledgerInfo : mlInfo.getLedgerInfoList()) {
            long ledgerId = ledgerInfo.getLedgerId();
            minLedgerId = Math.min(minLedgerId, ledgerId);
            maxLedgerId = Math.max(maxLedgerId, ledgerId);
            if (ledgerId == lastConfirmedEntry.getLedgerId()) {
                if (ledgerInfo.getEntries() > lastConfirmedEntry.getEntryId()) {
                    ledgers.put(ledgerId, ledgerInfo);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] ledgerInfo of id {} is not updated.", readerTopic, ledgerId);
                    }
                }
            } else {
                ledgers.put(ledgerInfo.getLedgerId(), ledgerInfo);
            }
        }
        //remove deleted ledgers < minLedgerId
        while (!ledgers.isEmpty() && ledgers.firstKey() < minLedgerId) {
            Map.Entry<Long, LedgerInfo> firstEntry = ledgers.pollFirstEntry();
            log.info("[{}] ledger {} is deleted in writerTopic. ledgerInfo={}, minLedgerId={}", readerTopic,
                    firstEntry.getKey(), firstEntry.getValue(), minLedgerId);
        }

        if (maxLedgerId > lastConfirmedEntry.getLedgerId()) {
            asyncUpdateLastConfirmedEntry();
        }
    }

    @Override
    protected synchronized void initializeBookKeeper(ManagedLedgerInitializeLedgerCallback callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] initializing bookkeeper; ledgers {}", readerTopic, ledgers);
        }

        // Calculate total entries and size
        Iterator<LedgerInfo> iterator = ledgers.values().iterator();
        while (iterator.hasNext()) {
            LedgerInfo li = iterator.next();
            if (li.getEntries() > 0) {
                NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, li.getEntries());
                TOTAL_SIZE_UPDATER.addAndGet(this, li.getSize());
            } else {
                if (ledgers.size() == 1) { //do not remove the only ledger.
                    iterator.remove();
                }
            }
        }
        if (lastConfirmedEntry == null) {
            if (!ledgers.isEmpty()) {
                LedgerInfo lastLedger = ledgers.lastEntry().getValue();
                lastConfirmedEntry = PositionImpl.get(lastLedger.getLedgerId(), lastLedger.getEntries() - 1);
            } else {
                lastConfirmedEntry = PositionImpl.earliest;
            }
        }

        super.initializeCursors(callback);
    }

    public String getReaderTopic() {
        return readerTopic;
    }

    @Override
    protected ManagedCursorImpl createManagedCursor(String cursorName) {
        return new RemoteManagedCursorImpl(bookKeeper, config, this, cursorName, cursorClient);
    }

    @Override
    protected boolean isReadOnly() {
        return true;
    }

    /**
     * Not supported ledger writing operation.
     */
    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {
        promise.completeExceptionally(new ManagedLedgerException.ManagedLedgerNotWritableException());
    }

    /**
     * Not supported ledger writing operation.
     */
    @Override
    public CompletableFuture<Void> asyncTruncate() {
        return FutureUtil.failedFuture(new ManagedLedgerException.ManagedLedgerNotWritableException());
    }

    /**
     * Not supported ledger writing operation.
     */
    @Override
    public void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AsyncCallbacks.AddEntryCallback callback,
                              Object ctx) {
        callback.addFailed(new ManagedLedgerException.ManagedLedgerNotWritableException(), ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        callback.addFailed(new ManagedLedgerException.ManagedLedgerNotWritableException(), ctx);
    }

    /**
     * Init new created remote managed cursor.
     */
    @Override
    protected void doInitializeCursor(ManagedCursorImpl cursor, String cursorName,
                                      CommandSubscribe.InitialPosition initialPosition, Map<String, Long> properties,
                                      AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        PositionImpl position = CommandSubscribe.InitialPosition.Earliest == initialPosition ?
                PositionImpl.earliest : PositionImpl.latest;
        cursor.initialize(position, properties, new ManagedCursorImpl.VoidCallback() {
            @Override
            public void operationComplete() {
                log.info("[{}] Opened new cursor: {}", readerTopic, cursor);
                cursor.setActive();

                synchronized (RemoteManagedLedgerImpl.this) {
                    cursors.add(cursor);
                    uninitializedCursors.remove(cursorName).complete(cursor);
                }
                callback.openCursorComplete(cursor, ctx);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                log.warn("[{}] Failed to open cursor: {}", readerTopic, cursor);

                synchronized (RemoteManagedLedgerImpl.this) {
                    uninitializedCursors.remove(cursorName).completeExceptionally(exception);
                }
                callback.openCursorFailed(exception, ctx);
            }
        });
    }

    @Override
    public synchronized void asyncDeleteCursor(String consumerName, AsyncCallbacks.DeleteCursorCallback callback,
                                               Object ctx) {
        final RemoteManagedCursorImpl cursor = (RemoteManagedCursorImpl) cursors.get(consumerName);
        if (cursor == null) {
            callback.deleteCursorFailed(new ManagedLedgerException.CursorNotFoundException("ManagedCursor not found: "
                    + consumerName), ctx);
            return;
        } else if (!cursor.isDurable()) {
            cursors.removeCursor(consumerName);
            callback.deleteCursorComplete(ctx);
            return;
        }
        cursor.asyncDeleteCursor().thenAccept(v -> {
            callback.deleteCursorComplete(ctx);
        }).exceptionally(throwable -> {
            callback.deleteCursorFailed(getManagedLedgerException(throwable), ctx);
            return null;
        });
    }

    @Override
    protected void asyncReadEntries(OpReadEntry opReadEntry) {
        final State state = STATE_UPDATER.get(this);
        if (state == State.Fenced || state == State.Closed) {
            opReadEntry.readEntriesFailed(new ManagedLedgerException.ManagedLedgerFencedException(), opReadEntry.ctx);
            return;
        }

        long ledgerId = opReadEntry.readPosition.getLedgerId();

        LedgerInfo ledgerInfo = ledgers.get(ledgerId);
        if (ledgerInfo == null || ledgerInfo.getEntries() == 0) {
            // Cursor is pointing to a empty ledger, there's no need to try opening it. Skip this ledger and
            // move to the next one
            opReadEntry.checkReadCompletion();
            return;
        }

        // Get a ledger handle to read from
        getLedgerHandle(ledgerId)
                .thenAccept(ledger -> internalReadFromLedger(ledger, opReadEntry))
                .exceptionally(ex -> {
                    log.error("[{}] Error opening ledger for reading at position {} - {}",
                            readerTopic, opReadEntry.readPosition, ex.getMessage());
                    opReadEntry.readEntriesFailed(getManagedLedgerException(ex.getCause()), opReadEntry.ctx);
                    return null;
                });
    }

    /**
     * update lastConfirmedEntry in asynchronous mode.
     *
     * @return A {@link CompletableFuture} tracking the update process.
     */
    public CompletableFuture<Long> asyncUpdateLastConfirmedEntry() {
        if (ledgers.isEmpty()) {
            return FutureUtil.failedFuture(new ManagedLedgerException.EmptyLedgersException());
        }
        Long lastLedgerId = ledgers.lastKey();
        if (lastLedgerId == null) {
            return FutureUtil.failedFuture(new ManagedLedgerException.EmptyLedgersException());
        }
        if (lastConfirmedEntry.getLedgerId() < lastLedgerId) {
            lastConfirmedEntry = PositionImpl.get(lastLedgerId, -1);
        }
        return getLedgerHandle(lastLedgerId).thenCompose(lh -> {
            long oldLac = lh.getLastAddConfirmed(); //this is local cached lac.
            if (oldLac > lastConfirmedEntry.getEntryId()) {
                doUpdateLastConfirmedEntry(lastLedgerId, oldLac);

            }
            return lh.readLastAddConfirmedAsync().whenComplete((lac, throwable) -> {
                if (throwable != null) {
                    log.error("[{}] asyncUpdateLastConfirmedEntry failed, ledger {}", readerTopic, lastLedgerId,
                            throwable);
                }
                if (log.isDebugEnabled()) {
                    log.debug("[{}] readLastAddConfirmedAsync of ledger {} returned:{}, cached:{}, oldLac:{}",
                            readerTopic, lastLedgerId, lac, lh.getLastAddConfirmed(), oldLac);
                }
                if (lac > lastConfirmedEntry.getEntryId()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[ML={}] LAC of LedgerId={} is updated from {} to {} in write owner broker.",
                                readerTopic, lastLedgerId, oldLac, lac);
                    }
                    doUpdateLastConfirmedEntry(lastLedgerId, lac);
                }
            });
        });

    }

    private void doUpdateLastConfirmedEntry(long lastLedgerId, long lac) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] UpdateLastConfirmedEntry to {}->{}:{}", readerTopic, lastConfirmedEntry, lastLedgerId, lac);
        }
        lastConfirmedEntry = PositionImpl.get(lastLedgerId, lac);
        LedgerInfo oldInfo = ledgers.get(lastLedgerId);
        if (oldInfo.getEntries() == 0) {
            ledgers.put(lastLedgerId, LedgerInfo.newBuilder()
                    .mergeFrom(oldInfo)
                    .setEntries(lac + 1)
                    .build()
            );
        }
    }

    /**
     * @return A {@link CompletableFuture} which will complete when the version of ledgersStat and lastConfirmedEntry
     * is up to date with the giving version and position.
     */
    public CompletableFuture<Void> waitSync(long version, EntryPosition position) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] waitSync version {}->{}, position {} -> {}", readerTopic, version,
                    ledgersStat == null ? null : ledgersStat.getVersion(),
                    ToStringUtil.pbObjectToString(position), lastConfirmedEntry);
        }
        long startTs = System.currentTimeMillis();
        PositionImpl lacInWriter = PositionImpl.get(position.getLedgerId(), position.getEntryId());
        if (ledgersStat != null && ledgersStat.getVersion() >= version
                && lacInWriter.compareTo(lastConfirmedEntry) <= 0) {
            return CompletableFuture.completedFuture(null);
        } else {
            CompletableFuture<Void> waitFuture = new CompletableFuture<>();
            getScheduledExecutor().scheduleOrdered(name, safeRun(() -> {
                doWaitSync(version, lacInWriter, waitFuture, startTs);
            }), 10, TimeUnit.MILLISECONDS);
            return waitFuture;
        }
    }

    private void doWaitSync(long version, PositionImpl lacInWriter, CompletableFuture<Void> waitFuture, long startTs) {
        long elapse = System.currentTimeMillis() - startTs;
        log.debug("[{}] doWaitSync version {}->{}, position {} -> {}, timeElapse:{}ms", readerTopic, version,
                ledgersStat == null ? null : ledgersStat.getVersion(),
                ToStringUtil.pbObjectToString(lacInWriter),
                lastConfirmedEntry,
                elapse);
        if (ledgersStat != null && ledgersStat.getVersion() >= version
                && lacInWriter.compareTo(lastConfirmedEntry) <= 0) {
            waitFuture.complete(null);
        } else {
            long delayTime = elapse; // double the waiting time each time.
            delayTime = Math.max(delayTime, 100);
            delayTime = Math.min(delayTime, 3000); // make delay in range [100, 3000]
            getScheduledExecutor().scheduleOrdered(name,
                    safeRun(() -> doWaitSync(version, lacInWriter, waitFuture, startTs)),
                    delayTime, TimeUnit.MILLISECONDS);
        }
    }


}
