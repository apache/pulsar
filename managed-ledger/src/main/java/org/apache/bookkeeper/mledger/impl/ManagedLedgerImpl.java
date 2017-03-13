/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.BadVersionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.Stat;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.CallbackMutex;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.UnboundArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.RateLimiter;
import com.yahoo.pulsar.common.api.Commands;
import com.yahoo.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import com.yahoo.pulsar.common.util.collections.ConcurrentLongHashMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ManagedLedgerImpl implements ManagedLedger, CreateCallback {
    private final static long MegaByte = 1024 * 1024;

    protected final static int AsyncOperationTimeoutSeconds = 30;
    private final static long maxActiveCursorBacklogEntries = 100;
    private static long maxMessageCacheRetentionTimeMillis = 10 * 1000;

    private final BookKeeper bookKeeper;
    private final String name;

    private final ManagedLedgerConfig config;
    private final MetaStore store;

    private final ConcurrentLongHashMap<CompletableFuture<LedgerHandle>> ledgerCache = new ConcurrentLongHashMap<>();
    private final NavigableMap<Long, LedgerInfo> ledgers = new ConcurrentSkipListMap<>();
    private volatile Stat ledgersStat;

    private final ManagedCursorContainer cursors = new ManagedCursorContainer();
    private final ManagedCursorContainer activeCursors = new ManagedCursorContainer();

    // Ever increasing counter of entries added
    static final AtomicLongFieldUpdater<ManagedLedgerImpl> ENTRIES_ADDED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ManagedLedgerImpl.class, "entriesAddedCounter");
    private volatile long entriesAddedCounter = 0;

    static final AtomicLongFieldUpdater<ManagedLedgerImpl> NUMBER_OF_ENTRIES_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ManagedLedgerImpl.class, "numberOfEntries");
    private volatile long numberOfEntries = 0;
    static final AtomicLongFieldUpdater<ManagedLedgerImpl> TOTAL_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ManagedLedgerImpl.class, "totalSize");
    private volatile long totalSize = 0;

    private RateLimiter updateCursorRateLimit;

    // Cursors that are waiting to be notified when new entries are persisted
    final ConcurrentLinkedQueue<ManagedCursorImpl> waitingCursors;

    // This map is used for concurrent open cursor requests, where the 2nd request will attach a listener to the
    // uninitialized cursor future from the 1st request
    final Map<String, CompletableFuture<ManagedCursor>> uninitializedCursors;

    final EntryCache entryCache;

    /**
     * This lock is held while the ledgers list is updated asynchronously on the metadata store. Since we use the store
     * version, we cannot have multiple concurrent updates.
     */
    private final CallbackMutex ledgersListMutex = new CallbackMutex();
    private final CallbackMutex trimmerMutex = new CallbackMutex();

    private volatile LedgerHandle currentLedger;
    private long currentLedgerEntries = 0;
    private long currentLedgerSize = 0;
    private long lastLedgerCreatedTimestamp = 0;
    private long lastLedgerCreationFailureTimestamp = 0;
    private long lastLedgerCreationInitiationTimestamp = 0;

    private static final Random random = new Random(System.currentTimeMillis());
    private long maximumRolloverTimeMs;

    // Time period in which new write requests will not be accepted, after we fail in creating a new ledger.
    final static long WaitTimeAfterLedgerCreationFailureMs = 10000;

    volatile PositionImpl lastConfirmedEntry;

    enum State {
        None, // Uninitialized
        LedgerOpened, // A ledger is ready to write into
        ClosingLedger, // Closing current ledger
        ClosedLedger, // Current ledger has been closed and there's no pending
                      // operation
        CreatingLedger, // Creating a new ledger
        Closed, // ManagedLedger has been closed
        Fenced, // A managed ledger is fenced when there is some concurrent
                // access from a different session/machine. In this state the
                // managed ledger will throw exception for all operations, since
                // the new instance will take over
    }

    // define boundaries for position based seeks and searches
    enum PositionBound {
        startIncluded, startExcluded
    }

    private static final AtomicReferenceFieldUpdater<ManagedLedgerImpl, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedLedgerImpl.class, State.class, "state");
    private volatile State state = null;

    private final ScheduledExecutorService scheduledExecutor;
    private final OrderedSafeExecutor executor;
    final ManagedLedgerFactoryImpl factory;
    protected final ManagedLedgerMBeanImpl mbean;

    /**
     * Queue of pending entries to be added to the managed ledger. Typically entries are queued when a new ledger is
     * created asynchronously and hence there is no ready ledger to write into.
     */
    final Queue<OpAddEntry> pendingAddEntries = new UnboundArrayBlockingQueue<>();

    // //////////////////////////////////////////////////////////////////////

    public ManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, MetaStore store,
            ManagedLedgerConfig config, ScheduledExecutorService scheduledExecutor, OrderedSafeExecutor orderedExecutor,
            final String name) {
        this.factory = factory;
        this.bookKeeper = bookKeeper;
        this.config = config;
        this.store = store;
        this.name = name;
        this.scheduledExecutor = scheduledExecutor;
        this.executor = orderedExecutor;
        TOTAL_SIZE_UPDATER.set(this, 0);
        NUMBER_OF_ENTRIES_UPDATER.set(this, 0);
        ENTRIES_ADDED_COUNTER_UPDATER.set(this, 0);
        STATE_UPDATER.set(this, State.None);
        this.ledgersStat = null;
        this.mbean = new ManagedLedgerMBeanImpl(this);
        this.entryCache = factory.getEntryCacheManager().getEntryCache(this);
        this.waitingCursors = Queues.newConcurrentLinkedQueue();
        this.uninitializedCursors = Maps.newHashMap();
        this.updateCursorRateLimit = RateLimiter.create(1);

        // Get the next rollover time. Add a random value upto 5% to avoid rollover multiple ledgers at the same time
        this.maximumRolloverTimeMs = (long) (config.getMaximumRolloverTimeMs() * (1 + random.nextDouble() * 5 / 100.0));
    }

    synchronized void initialize(final ManagedLedgerInitializeLedgerCallback callback, final Object ctx) {
        log.info("Opening managed ledger {}", name);

        // Fetch the list of existing ledgers in the managed ledger
        store.getManagedLedgerInfo(name, new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                ledgersStat = stat;
                for (LedgerInfo ls : mlInfo.getLedgerInfoList()) {
                    ledgers.put(ls.getLedgerId(), ls);
                }

                // Last ledger stat may be zeroed, we must update it
                if (ledgers.size() > 0) {
                    final long id = ledgers.lastKey();
                    OpenCallback opencb = (rc, lh, ctx1) -> {
                        executor.submitOrdered(name, safeRun(() -> {
                            mbean.endDataLedgerOpenOp();
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Opened ledger {}: ", name, id, BKException.getMessage(rc));
                            }
                            if (rc == BKException.Code.OK) {
                                LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(id)
                                        .setEntries(lh.getLastAddConfirmed() + 1).setSize(lh.getLength())
                                        .setTimestamp(System.currentTimeMillis()).build();
                                ledgers.put(id, info);
                                initializeBookKeeper(callback);
                            } else if (rc == BKException.Code.NoSuchLedgerExistsException) {
                                log.warn("[{}] Ledger not found: {}", name, ledgers.lastKey());
                                ledgers.remove(ledgers.lastKey());
                                initializeBookKeeper(callback);
                            } else {
                                log.error("[{}] Failed to open ledger {}: {}", name, id, BKException.getMessage(rc));
                                callback.initializeFailed(new ManagedLedgerException(BKException.getMessage(rc)));
                                return;
                            }
                        }));
                    };

                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Opening legder {}", name, id);
                    }
                    mbean.startDataLedgerOpenOp();
                    bookKeeper.asyncOpenLedger(id, config.getDigestType(), config.getPassword(), opencb, null);
                } else {
                    initializeBookKeeper(callback);
                }
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                callback.initializeFailed(new ManagedLedgerException(e));
            }
        });
    }

    private synchronized void initializeBookKeeper(final ManagedLedgerInitializeLedgerCallback callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] initializing bookkeeper; ledgers {}", name, ledgers);
        }

        // Calculate total entries and size
        Iterator<LedgerInfo> iterator = ledgers.values().iterator();
        while (iterator.hasNext()) {
            LedgerInfo li = iterator.next();
            if (li.getEntries() > 0) {
                NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, li.getEntries());
                TOTAL_SIZE_UPDATER.addAndGet(this, li.getSize());
            } else {
                iterator.remove();
                bookKeeper.asyncDeleteLedger(li.getLedgerId(), (rc, ctx) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Deleted empty ledger ledgerId={} rc={}", name, li.getLedgerId(), rc);
                    }
                }, null);
            }
        }

        final MetaStoreCallback<Void> storeLedgersCb = new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void v, Stat stat) {
                ledgersStat = stat;
                initializeCursors(callback);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                callback.initializeFailed(new ManagedLedgerException(e));
            }
        };

        // Create a new ledger to start writing
        this.lastLedgerCreationInitiationTimestamp = System.nanoTime();
        mbean.startDataLedgerCreateOp();
        bookKeeper.asyncCreateLedger(config.getEnsembleSize(), config.getWriteQuorumSize(), config.getAckQuorumSize(),
                config.getDigestType(), config.getPassword(), (rc, lh, ctx) -> {
                    executor.submitOrdered(name, safeRun(() -> {
                        mbean.endDataLedgerCreateOp();
                        if (rc != BKException.Code.OK) {
                            callback.initializeFailed(new ManagedLedgerException(BKException.getMessage(rc)));
                            return;
                        }

                        log.info("[{}] Created ledger {}", name, lh.getId());
                        STATE_UPDATER.set(this, State.LedgerOpened);
                        lastLedgerCreatedTimestamp = System.currentTimeMillis();
                        currentLedger = lh;
                        lastConfirmedEntry = new PositionImpl(lh.getId(), -1);
                        LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(lh.getId()).setTimestamp(0).build();
                        ledgers.put(lh.getId(), info);
                        // Save it back to ensure all nodes exist

                        ManagedLedgerInfo mlInfo = ManagedLedgerInfo.newBuilder().addAllLedgerInfo(ledgers.values())
                                .build();
                        store.asyncUpdateLedgerIds(name, mlInfo, ledgersStat, storeLedgersCb);
                    }));
                }, null);
    }

    private void initializeCursors(final ManagedLedgerInitializeLedgerCallback callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] initializing cursors", name);
        }
        store.getCursors(name, new MetaStoreCallback<List<String>>() {
            @Override
            public void operationComplete(List<String> consumers, Stat s) {
                // Load existing cursors
                final AtomicInteger cursorCount = new AtomicInteger(consumers.size());
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Found {} cursors", name, consumers.size());
                }

                if (consumers.isEmpty()) {
                    callback.initializeComplete();
                    return;
                }

                for (final String cursorName : consumers) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Loading cursor {}", name, cursorName);
                    }
                    final ManagedCursorImpl cursor;
                    cursor = new ManagedCursorImpl(bookKeeper, config, ManagedLedgerImpl.this, cursorName);

                    cursor.recover(new VoidCallback() {
                        @Override
                        public void operationComplete() {
                            log.info("[{}] Recovery for cursor {} completed. pos={} -- todo={}", name, cursorName,
                                    cursor.getMarkDeletedPosition(), cursorCount.get() - 1);
                            cursor.setActive();
                            cursors.add(cursor);

                            if (cursorCount.decrementAndGet() == 0) {
                                // The initialization is now completed, register the jmx mbean
                                callback.initializeComplete();
                            }
                        }

                        @Override
                        public void operationFailed(ManagedLedgerException exception) {
                            log.warn("[{}] Recovery for cursor {} failed", name, cursorName, exception);
                            cursorCount.set(-1);
                            callback.initializeFailed(exception);
                        }
                    });
                }
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn("[{}] Failed to get the cursors list", name, e);
                callback.initializeFailed(new ManagedLedgerException(e));
            }
        });
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Position addEntry(byte[] data) throws InterruptedException, ManagedLedgerException {
        return addEntry(data, 0, data.length);
    }

    @Override
    public Position addEntry(byte[] data, int offset, int length) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        // Result list will contain the status exception and the resulting
        // position
        class Result {
            ManagedLedgerException status = null;
            Position position = null;
        }
        final Result result = new Result();

        asyncAddEntry(data, offset, length, new AddEntryCallback() {
            @Override
            public void addComplete(Position position, Object ctx) {
                result.position = position;
                counter.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                result.status = exception;
                counter.countDown();
            }
        }, null);

        counter.await();

        if (result.status != null) {
            log.error("[{}] Error adding entry", name, result.status);
            throw result.status;
        }

        return result.position;
    }

    @Override
    public void asyncAddEntry(final byte[] data, final AddEntryCallback callback, final Object ctx) {
        asyncAddEntry(data, 0, data.length, callback, ctx);
    }

    @Override
    public void asyncAddEntry(final byte[] data, int offset, int length, final AddEntryCallback callback,
            final Object ctx) {
        ByteBuf buffer = Unpooled.wrappedBuffer(data, offset, length);
        asyncAddEntry(buffer, callback, ctx);
    }

    @Override
    public synchronized void asyncAddEntry(ByteBuf buffer, AddEntryCallback callback, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] asyncAddEntry size={} state={}", name, buffer.readableBytes(), state);
        }
        final State state = STATE_UPDATER.get(this);
        if (state == State.Fenced) {
            callback.addFailed(new ManagedLedgerFencedException(), ctx);
            return;
        } else if (state == State.Closed) {
            callback.addFailed(new ManagedLedgerException("Managed ledger was already closed"), ctx);
            return;
        }

        OpAddEntry addOperation = OpAddEntry.create(this, buffer, callback, ctx);
        pendingAddEntries.add(addOperation);

        if (state == State.ClosingLedger || state == State.CreatingLedger) {
            // We don't have a ready ledger to write into
            // We are waiting for a new ledger to be created
            if (log.isDebugEnabled()) {
                log.debug("[{}] Queue addEntry request", name);
            }
        } else if (state == State.ClosedLedger) {
            long now = System.currentTimeMillis();
            if (now < lastLedgerCreationFailureTimestamp + WaitTimeAfterLedgerCreationFailureMs) {
                // Deny the write request, since we haven't waited enough time since last attempt to create a new ledger
                pendingAddEntries.remove(addOperation);
                callback.addFailed(new ManagedLedgerException("Waiting for new ledger creation to complete"), ctx);
                return;
            }

            // No ledger and no pending operations. Create a new ledger
            if (log.isDebugEnabled()) {
                log.debug("[{}] Creating a new ledger", name);
            }
            if (STATE_UPDATER.compareAndSet(this, State.ClosedLedger, State.CreatingLedger)) {
                this.lastLedgerCreationInitiationTimestamp = System.nanoTime();
                mbean.startDataLedgerCreateOp();
                bookKeeper.asyncCreateLedger(config.getEnsembleSize(), config.getWriteQuorumSize(),
                        config.getAckQuorumSize(), config.getDigestType(), config.getPassword(), this, ctx);
            }
        } else {
            checkArgument(state == State.LedgerOpened);

            // Write into lastLedger
            addOperation.setLedger(currentLedger);

            ++currentLedgerEntries;
            currentLedgerSize += buffer.readableBytes();

            if (log.isDebugEnabled()) {
                log.debug("[{}] Write into current ledger lh={} entries={}", name, currentLedger.getId(),
                        currentLedgerEntries);
            }

            if (currentLedgerIsFull()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Closing current ledger lh={}", name, currentLedger.getId());
                }
                // This entry will be the last added to current ledger
                addOperation.setCloseWhenDone(true);
                STATE_UPDATER.set(this, State.ClosingLedger);
            }

            addOperation.initiate();
        }
    }

    @Override
    public ManagedCursor openCursor(String cursorName) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedCursor cursor = null;
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncOpenCursor(cursorName, new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                result.cursor = cursor;
                counter.countDown();
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during open-cursor operation");
        }

        if (result.exception != null) {
            log.error("Error adding entry", result.exception);
            throw result.exception;
        }

        return result.cursor;
    }

    @Override
    public synchronized void asyncOpenCursor(final String cursorName, final OpenCursorCallback callback,
            final Object ctx) {

        try {
            checkManagedLedgerIsOpen();
            checkFenced();
        } catch (ManagedLedgerException e) {
            callback.openCursorFailed(e, ctx);
            return;
        }

        if (uninitializedCursors.containsKey(cursorName)) {
            uninitializedCursors.get(cursorName).thenAccept(cursor -> {
                callback.openCursorComplete(cursor, ctx);
            }).exceptionally(ex -> {
                callback.openCursorFailed((ManagedLedgerException) ex, ctx);
                return null;
            });
            return;
        }
        ManagedCursor cachedCursor = cursors.get(cursorName);
        if (cachedCursor != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cursor was already created {}", name, cachedCursor);
            }
            callback.openCursorComplete(cachedCursor, ctx);
            return;
        }

        // Create a new one and persist it
        if (log.isDebugEnabled()) {
            log.debug("[{}] Creating new cursor: {}", name, cursorName);
        }
        final ManagedCursorImpl cursor = new ManagedCursorImpl(bookKeeper, config, this, cursorName);
        CompletableFuture<ManagedCursor> cursorFuture = new CompletableFuture<>();
        uninitializedCursors.put(cursorName, cursorFuture);
        cursor.initialize(getLastPosition(), new VoidCallback() {
            @Override
            public void operationComplete() {
                log.info("[{}] Opened new cursor: {}", name, cursor);
                cursor.setActive();
                // Update the ack position (ignoring entries that were written while the cursor was being created)
                cursor.initializeCursorPosition(getLastPositionAndCounter());

                synchronized (this) {
                    cursors.add(cursor);
                    uninitializedCursors.remove(cursorName).complete(cursor);
                }
                callback.openCursorComplete(cursor, ctx);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                log.warn("[{}] Failed to open cursor: {}", name, cursor);

                synchronized (this) {
                    uninitializedCursors.remove(cursorName).completeExceptionally(exception);
                }
                callback.openCursorFailed(exception, ctx);
            }
        });
    }

    @Override
    public synchronized void asyncDeleteCursor(final String consumerName, final DeleteCursorCallback callback,
            final Object ctx) {
        final ManagedCursorImpl cursor = (ManagedCursorImpl) cursors.get(consumerName);
        if (cursor == null) {
            callback.deleteCursorFailed(new ManagedLedgerException("ManagedCursor not found: " + consumerName), ctx);
            return;
        }

        // First remove the consumer form the MetaStore. If this operation succeeds and the next one (removing the
        // ledger from BK) don't, we end up having a loose ledger leaked but the state will be consistent.
        store.asyncRemoveCursor(ManagedLedgerImpl.this.name, consumerName, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                cursor.asyncDeleteCursorLedger();
                cursors.removeCursor(consumerName);

                // Redo invalidation of entries in cache
                PositionImpl slowestConsumerPosition = cursors.getSlowestReaderPosition();
                if (slowestConsumerPosition != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Doing cache invalidation up to {}", slowestConsumerPosition);
                    }
                    entryCache.invalidateEntries(slowestConsumerPosition);
                } else {
                    entryCache.clear();
                }

                trimConsumedLedgersInBackground();

                log.info("[{}] [{}] Deleted cursor", name, consumerName);
                callback.deleteCursorComplete(ctx);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                callback.deleteCursorFailed(e, ctx);
            }

        });
    }

    @Override
    public void deleteCursor(String name) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncDeleteCursor(name, new DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during delete-cursors operation");
        }

        if (result.exception != null) {
            log.error("Deleting cursor", result.exception);
            throw result.exception;
        }
    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        return cursors;
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        return activeCursors;
    }

    /**
     * Tells whether the managed ledger has any active-cursor registered.
     *
     * @return true if at least a cursor exists
     */
    public boolean hasActiveCursors() {
        return !activeCursors.isEmpty();
    }

    @Override
    public long getNumberOfEntries() {
        return NUMBER_OF_ENTRIES_UPDATER.get(this);
    }

    @Override
    public long getNumberOfActiveEntries() {
        long totalEntries = getNumberOfEntries();
        PositionImpl pos = cursors.getSlowestReaderPosition();
        if (pos == null) {
            // If there are no consumers, there are no active entries
            return 0;
        } else {
            // The slowest consumer will be in the first ledger in the list. We need to subtract the entries it has
            // already consumed in order to get the active entries count.
            return totalEntries - (pos.getEntryId() + 1);
        }
    }

    @Override
    public long getTotalSize() {
        return TOTAL_SIZE_UPDATER.get(this);
    }

    @Override
    public void checkBackloggedCursors() {

        // activate caught up cursors
        cursors.forEach(cursor -> {
            if (cursor.getNumberOfEntries() < maxActiveCursorBacklogEntries) {
                cursor.setActive();
            }
        });

        // deactivate backlog cursors
        Iterator<ManagedCursor> cursors = activeCursors.iterator();
        while (cursors.hasNext()) {
            ManagedCursor cursor = cursors.next();
            long backlogEntries = cursor.getNumberOfEntries();
            if (backlogEntries > maxActiveCursorBacklogEntries) {
                PositionImpl readPosition = (PositionImpl) cursor.getReadPosition();
                readPosition = isValidPosition(readPosition) ? readPosition : getNextValidPosition(readPosition);
                if (readPosition == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Couldn't find valid read position [{}] {}", name, cursor.getName(),
                                cursor.getReadPosition());
                    }
                    continue;
                }
                try {
                    asyncReadEntry(readPosition, new ReadEntryCallback() {

                        @Override
                        public void readEntryFailed(ManagedLedgerException e, Object ctx) {
                            log.warn("[{}] Failed while reading entries on [{}] {}", name, cursor.getName(),
                                    e.getMessage(), e);

                        }

                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            MessageMetadata msgMetadata = null;
                            try {
                                msgMetadata = Commands.parseMessageMetadata(entry.getDataBuffer());
                                long msgTimeSincePublish = (System.currentTimeMillis() - msgMetadata.getPublishTime());
                                if (msgTimeSincePublish > maxMessageCacheRetentionTimeMillis) {
                                    cursor.setInactive();
                                }
                            } finally {
                                if (msgMetadata != null) {
                                    msgMetadata.recycle();
                                }
                                entry.release();
                            }

                        }
                    }, null);
                } catch (Exception e) {
                    log.warn("[{}] Failed while reading entries from cache on [{}] {}", name, cursor.getName(),
                            e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public long getEstimatedBacklogSize() {

        PositionImpl pos = getMarkDeletePositionOfSlowestConsumer();

        while (true) {
            if (pos == null) {
                return 0;
            }
            long size = 0;
            final long slowestConsumerLedgerId = pos.getLedgerId();

            // Subtract size of ledgers that were already fully consumed but not trimmed yet
            synchronized (this) {
                size = getTotalSize();
                size -= ledgers.values().stream().filter(li -> li.getLedgerId() < slowestConsumerLedgerId)
                        .mapToLong(li -> li.getSize()).sum();
            }

            LedgerInfo ledgerInfo = null;
            synchronized (this) {
                ledgerInfo = ledgers.get(pos.getLedgerId());
            }
            if (ledgerInfo == null) {
                // ledger was removed
                if (pos.compareTo(getMarkDeletePositionOfSlowestConsumer()) == 0) {
                    // position still has not moved
                    return size;
                }
                // retry with new slowest consumer
                pos = getMarkDeletePositionOfSlowestConsumer();
                continue;
            }

            long numEntries = pos.getEntryId();
            if (ledgerInfo.getEntries() == 0) {
                size -= consumedLedgerSize(currentLedgerSize, currentLedgerEntries, numEntries);
                return size;
            } else {
                size -= consumedLedgerSize(ledgerInfo.getSize(), ledgerInfo.getEntries(), numEntries);
                return size;
            }
        }
    }

    private long consumedLedgerSize(long ledgerSize, long ledgerEntries, long consumedEntries) {
        if (ledgerEntries <= 0) {
            return 0;
        }
        long averageSize = ledgerSize / ledgerEntries;
        return consumedEntries >= 0 ? (consumedEntries + 1) * averageSize : 0;
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during managed ledger close");
        }

        if (result.exception != null) {
            log.error("[{}] Error closing managed ledger", name, result.exception);
            throw result.exception;
        }
    }

    @Override
    public synchronized void asyncClose(final CloseCallback callback, final Object ctx) {
        State state = STATE_UPDATER.get(this);
        if (state == State.Fenced) {
            factory.close(this);
            callback.closeFailed(new ManagedLedgerFencedException(), ctx);
            return;
        } else if (state == State.Closed) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignoring request to close a closed managed ledger", name);
            }
            callback.closeComplete(ctx);
            return;
        }

        log.info("[{}] Closing managed ledger", name);

        factory.close(this);
        STATE_UPDATER.set(this, State.Closed);

        LedgerHandle lh = currentLedger;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Closing current writing ledger {}", name, lh.getId());
        }

        mbean.startDataLedgerCloseOp();
        lh.asyncClose((rc, lh1, ctx1) -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Close complete for ledger {}: rc = {}", name, lh.getId(), rc);
            }
            mbean.endDataLedgerCloseOp();
            if (rc != BKException.Code.OK) {
                callback.closeFailed(new ManagedLedgerException(BKException.getMessage(rc)), ctx);
                return;
            }

            // Close all cursors in parallel
            List<CompletableFuture<Void>> futures = Lists.newArrayList();
            for (ManagedCursor cursor : cursors) {
                Futures.CloseFuture closeFuture = new Futures.CloseFuture();
                cursor.asyncClose(closeFuture, null);
                futures.add(closeFuture);
            }

            Futures.waitForAll(futures).thenRun(() -> {
                callback.closeComplete(ctx);
            }).exceptionally(exception -> {
                callback.closeFailed(new ManagedLedgerException(exception), ctx);
                return null;
            });

        }, null);
    }

    // //////////////////////////////////////////////////////////////////////
    // Callbacks

    @Override
    public synchronized void createComplete(int rc, final LedgerHandle lh, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] createComplete rc={} ledger={}", name, rc, lh != null ? lh.getId() : -1);
        }
        mbean.endDataLedgerCreateOp();
        if (rc != BKException.Code.OK) {
            log.error("[{}] Error creating ledger rc={} {}", name, rc, BKException.getMessage(rc));
            ManagedLedgerException status = new ManagedLedgerException(BKException.getMessage(rc));

            // Empty the list of pending requests and make all of them fail
            clearPendingAddEntries(status);
            lastLedgerCreationFailureTimestamp = System.currentTimeMillis();
            STATE_UPDATER.set(this, State.ClosedLedger);
        } else {
            log.info("[{}] Created new ledger {}", name, lh.getId());
            ledgers.put(lh.getId(), LedgerInfo.newBuilder().setLedgerId(lh.getId()).setTimestamp(0).build());
            currentLedger = lh;
            currentLedgerEntries = 0;
            currentLedgerSize = 0;

            final MetaStoreCallback<Void> cb = new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void v, Stat stat) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Updating of ledgers list after create complete. version={}", name, stat);
                    }
                    ledgersStat = stat;
                    ledgersListMutex.unlock();
                    updateLedgersIdsComplete(stat);
                    synchronized (ManagedLedgerImpl.this) {
                        mbean.addLedgerSwitchLatencySample(System.nanoTime() - lastLedgerCreationInitiationTimestamp,
                                TimeUnit.NANOSECONDS);
                    }
                }

                @Override
                public void operationFailed(MetaStoreException e) {
                    if (e instanceof BadVersionException) {
                        synchronized (ManagedLedgerImpl.this) {
                            log.error(
                                    "[{}] Failed to udpate ledger list. z-node version mismatch. Closing managed ledger",
                                    name);
                            STATE_UPDATER.set(ManagedLedgerImpl.this, State.Fenced);
                            clearPendingAddEntries(e);
                            return;
                        }
                    }

                    log.warn("[{}] Error updating meta data with the new list of ledgers: {}", name, e.getMessage());

                    // Remove the ledger, since we failed to update the list
                    ledgers.remove(lh.getId());
                    mbean.startDataLedgerDeleteOp();
                    bookKeeper.asyncDeleteLedger(lh.getId(), (rc1, ctx1) -> {
                        mbean.endDataLedgerDeleteOp();
                        if (rc1 != BKException.Code.OK) {
                            log.warn("[{}] Failed to delete ledger {}: {}", name, lh.getId(),
                                    BKException.getMessage(rc1));
                        }
                    }, null);

                    ledgersListMutex.unlock();

                    synchronized (ManagedLedgerImpl.this) {
                        lastLedgerCreationFailureTimestamp = System.currentTimeMillis();
                        STATE_UPDATER.set(ManagedLedgerImpl.this, State.ClosedLedger);
                        clearPendingAddEntries(e);
                    }
                }
            };

            updateLedgersListAfterRollover(cb);
        }
    }

    private void updateLedgersListAfterRollover(MetaStoreCallback<Void> callback) {
        if (!ledgersListMutex.tryLock()) {
            // Defer update for later
            scheduledExecutor.schedule(() -> updateLedgersListAfterRollover(callback), 100, TimeUnit.MILLISECONDS);
            return;
        }

        ManagedLedgerInfo mlInfo = ManagedLedgerInfo.newBuilder().addAllLedgerInfo(ledgers.values()).build();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Updating ledgers ids with new ledger. version={}", name, ledgersStat);
        }
        store.asyncUpdateLedgerIds(name, mlInfo, ledgersStat, callback);
    }

    public synchronized void updateLedgersIdsComplete(Stat stat) {
        STATE_UPDATER.set(this, State.LedgerOpened);
        lastLedgerCreatedTimestamp = System.currentTimeMillis();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Resending {} pending messages", name, pendingAddEntries.size());
        }

        // Process all the pending addEntry requests
        for (OpAddEntry op : pendingAddEntries) {
            op.setLedger(currentLedger);
            ++currentLedgerEntries;
            currentLedgerSize += op.data.readableBytes();

            if (log.isDebugEnabled()) {
                log.debug("[{}] Sending {}", name, op);
            }

            if (currentLedgerIsFull()) {
                STATE_UPDATER.set(this, State.ClosingLedger);
                op.setCloseWhenDone(true);
                op.initiate();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Stop writing into ledger {} queue={}", name, currentLedger.getId(),
                            pendingAddEntries.size());
                }
                break;
            } else {
                op.initiate();
            }
        }
    }

    // //////////////////////////////////////////////////////////////////////
    // Private helpers

    synchronized void ledgerClosed(final LedgerHandle lh) {
        final State state = STATE_UPDATER.get(this);
        if (state == State.ClosingLedger || state == State.LedgerOpened) {
            STATE_UPDATER.set(this, State.ClosedLedger);
        } else {
            // In case we get multiple write errors for different outstanding write request, we should close the ledger
            // just once
            return;
        }

        long entriesInLedger = lh.getLastAddConfirmed() + 1;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Ledger has been closed id={} entries={}", name, lh.getId(), entriesInLedger);
        }
        if (entriesInLedger > 0) {
            LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(lh.getId()).setEntries(entriesInLedger)
                    .setSize(lh.getLength()).setTimestamp(System.currentTimeMillis()).build();
            ledgers.put(lh.getId(), info);
        } else {
            // The last ledger was empty, so we can discard it
            ledgers.remove(lh.getId());
            mbean.startDataLedgerDeleteOp();
            bookKeeper.asyncDeleteLedger(lh.getId(), (rc, ctx) -> {
                mbean.endDataLedgerDeleteOp();
                log.info("[{}] Delete complete for empty ledger {}. rc={}", name, lh.getId(), rc);
            }, null);
        }

        trimConsumedLedgersInBackground();

        if (!pendingAddEntries.isEmpty()) {
            // Need to create a new ledger to write pending entries
            if (log.isDebugEnabled()) {
                log.debug("[{}] Creating a new ledger", name);
            }
            STATE_UPDATER.set(this, State.CreatingLedger);
            this.lastLedgerCreationInitiationTimestamp = System.nanoTime();
            mbean.startDataLedgerCreateOp();
            bookKeeper.asyncCreateLedger(config.getEnsembleSize(), config.getWriteQuorumSize(),
                    config.getAckQuorumSize(), config.getDigestType(), config.getPassword(), this, null);
        }
    }

    void clearPendingAddEntries(ManagedLedgerException e) {
        while (!pendingAddEntries.isEmpty()) {
            OpAddEntry op = pendingAddEntries.poll();
            op.data.release();
            op.failed(e);
        }
    }

    void asyncReadEntries(OpReadEntry opReadEntry) {
        final State state = STATE_UPDATER.get(this);
        if (state == State.Fenced || state == State.Closed) {
            opReadEntry.readEntriesFailed(new ManagedLedgerFencedException(), opReadEntry.ctx);
            return;
        }

        long ledgerId = opReadEntry.readPosition.getLedgerId();

        LedgerHandle currentLedger = this.currentLedger;

        if (ledgerId == currentLedger.getId()) {
            // Current writing ledger is not in the cache (since we don't want
            // it to be automatically evicted), and we cannot use 2 different
            // ledger handles (read & write)for the same ledger.
            internalReadFromLedger(currentLedger, opReadEntry);
        } else {
            LedgerInfo ledgerInfo = ledgers.get(ledgerId);
            if (ledgerInfo == null || ledgerInfo.getEntries() == 0) {
                // Cursor is pointing to a empty ledger, there's no need to try opening it. Skip this ledger and
                // move to the next one
                opReadEntry.updateReadPosition(new PositionImpl(opReadEntry.readPosition.getLedgerId() + 1, 0));
                opReadEntry.checkReadCompletion();
                return;
            }

            // Get a ledger handle to read from
            getLedgerHandle(ledgerId).thenAccept(ledger -> {
                internalReadFromLedger(ledger, opReadEntry);
            }).exceptionally(ex -> {
                log.error("[{}] Error opening ledger for reading at position {} - {}", name, opReadEntry.readPosition,
                        ex.getMessage());
                opReadEntry.readEntriesFailed(new ManagedLedgerException(ex), opReadEntry.ctx);
                return null;
            });
        }
    }

    CompletableFuture<LedgerHandle> getLedgerHandle(long ledgerId) {
        CompletableFuture<LedgerHandle> ledgerHandle = ledgerCache.get(ledgerId);
        if (ledgerHandle != null) {
            return ledgerHandle;
        }

        // If not present try again and create if necessary
        return ledgerCache.computeIfAbsent(ledgerId, lid -> {
            // Open the ledger for reading if it was not already opened
            CompletableFuture<LedgerHandle> future = new CompletableFuture<>();

            if (log.isDebugEnabled()) {
                log.debug("[{}] Asynchronously opening ledger {} for read", name, ledgerId);
            }
            mbean.startDataLedgerOpenOp();
            bookKeeper.asyncOpenLedger(ledgerId, config.getDigestType(), config.getPassword(),
                    (int rc, LedgerHandle lh, Object ctx) -> {
                        executor.submit(safeRun(() -> {
                            mbean.endDataLedgerOpenOp();
                            if (rc != BKException.Code.OK) {
                                // Remove the ledger future from cache to give chance to reopen it later
                                ledgerCache.remove(ledgerId, future);
                                future.completeExceptionally(new ManagedLedgerException(BKException.getMessage(rc)));
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Successfully opened ledger {} for reading", name, lh.getId());
                                }
                                future.complete(lh);
                            }
                        }));
                    }, null);
            return future;
        });
    }

    void invalidateLedgerHandle(LedgerHandle ledgerHandle, int rc) {
        long ledgerId = ledgerHandle.getId();
        if (ledgerId != currentLedger.getId()) {
            // remove handle from ledger cache since we got a (read) error
            ledgerCache.remove(ledgerId);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Removed ledger {} from cache (after read error: {})", name, ledgerId, rc);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ledger that encountered read error {} is current ledger", name, rc);
            }
        }
    }

    void asyncReadEntry(PositionImpl position, ReadEntryCallback callback, Object ctx) {
        LedgerHandle currentLedger = this.currentLedger;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entry ledger {}: {}", name, position.getLedgerId(), position.getEntryId());
        }
        if (position.getLedgerId() == currentLedger.getId()) {
            LedgerHandle ledger = currentLedger;
            entryCache.asyncReadEntry(ledger, position, callback, ctx);
        } else {
            getLedgerHandle(position.getLedgerId()).thenAccept(ledger -> {
                entryCache.asyncReadEntry(ledger, position, callback, ctx);
            }).exceptionally(ex -> {
                log.error("[{}] Error opening ledger for reading at position {} - {}", name, position, ex.getMessage());
                callback.readEntryFailed(new ManagedLedgerException(ex), ctx);
                return null;
            });
        }

    }

    private void internalReadFromLedger(LedgerHandle ledger, OpReadEntry opReadEntry) {

        // Perform the read
        long firstEntry = opReadEntry.readPosition.getEntryId();
        long lastEntryInLedger;
        final ManagedCursorImpl cursor = opReadEntry.cursor;

        PositionImpl lastPosition = lastConfirmedEntry;

        if (ledger.getId() == lastPosition.getLedgerId()) {
            // For the current ledger, we only give read visibility to the last entry we have received a confirmation in
            // the managed ledger layer
            lastEntryInLedger = lastPosition.getEntryId();
        } else {
            // For other ledgers, already closed the BK lastAddConfirmed is appropriate
            lastEntryInLedger = ledger.getLastAddConfirmed();
        }

        if (firstEntry > lastEntryInLedger) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No more messages to read from ledger={} lastEntry={} readEntry={}", name,
                        ledger.getId(), lastEntryInLedger, firstEntry);
            }

            if (ledger.getId() != currentLedger.getId()) {
                // Cursor was placed past the end of one ledger, move it to the
                // beginning of the next ledger
                Long nextLedgerId = ledgers.ceilingKey(ledger.getId() + 1);
                opReadEntry.updateReadPosition(new PositionImpl(nextLedgerId, 0));
            }

            opReadEntry.checkReadCompletion();
            return;
        }

        long lastEntry = min(firstEntry + opReadEntry.getNumberOfEntriesToRead() - 1, lastEntryInLedger);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entries from ledger {} - first={} last={}", name, ledger.getId(), firstEntry,
                    lastEntry);
        }
        entryCache.asyncReadEntry(ledger, firstEntry, lastEntry, false, opReadEntry, opReadEntry.ctx);

        if (updateCursorRateLimit.tryAcquire()) {
            if (isCursorActive(cursor)) {
                final PositionImpl lastReadPosition = PositionImpl.get(ledger.getId(), lastEntry);
                discardEntriesFromCache(cursor, lastReadPosition);
                lastReadPosition.recycle();
            }
        }
    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return mbean;
    }

    boolean hasMoreEntries(PositionImpl position) {
        PositionImpl lastPos = lastConfirmedEntry;
        boolean result = position.compareTo(lastPos) <= 0;
        if (log.isDebugEnabled()) {
            log.debug("[{}] hasMoreEntries: pos={} lastPos={} res={}", name, position, lastPos, result);
        }
        return result;
    }

    void discardEntriesFromCache(ManagedCursorImpl cursor, PositionImpl newPosition) {
        Pair<PositionImpl, PositionImpl> pair = activeCursors.cursorUpdated(cursor, newPosition);
        if (pair != null) {
            entryCache.invalidateEntries(pair.second);
        }
    }

    void updateCursor(ManagedCursorImpl cursor, PositionImpl newPosition) {
        Pair<PositionImpl, PositionImpl> pair = cursors.cursorUpdated(cursor, newPosition);
        if (pair == null) {
            // Cursor has been removed in the meantime
            trimConsumedLedgersInBackground();
            return;
        }

        PositionImpl previousSlowestReader = pair.first;
        PositionImpl currentSlowestReader = pair.second;

        if (previousSlowestReader.compareTo(currentSlowestReader) == 0) {
            // The slowest consumer has not changed position. Nothing to do right now
            return;
        }

        // Only trigger a trimming when switching to the next ledger
        if (previousSlowestReader.getLedgerId() != newPosition.getLedgerId()) {
            trimConsumedLedgersInBackground();
        }
    }

    PositionImpl startReadOperationOnLedger(PositionImpl position) {
        long ledgerId = ledgers.ceilingKey(position.getLedgerId());
        if (ledgerId != position.getLedgerId()) {
            // The ledger pointed by this position does not exist anymore. It was deleted because it was empty. We need
            // to skip on the next available ledger
            position = new PositionImpl(ledgerId, 0);
        }

        return position;
    }

    void notifyCursors() {
        while (true) {
            final ManagedCursorImpl waitingCursor = waitingCursors.poll();
            if (waitingCursor == null) {
                break;
            }

            executor.submit(safeRun(() -> waitingCursor.notifyEntriesAvailable()));
        }
    }

    private void trimConsumedLedgersInBackground() {
        executor.submitOrdered(name, safeRun(() -> {
            internalTrimConsumedLedgers();
        }));
    }

    private void scheduleDeferredTrimming() {
        scheduledExecutor.schedule(safeRun(() -> trimConsumedLedgersInBackground()), 100, TimeUnit.MILLISECONDS);
    }

    private boolean hasLedgerRetentionExpired(long ledgerTimestamp) {
        long elapsedMs = System.currentTimeMillis() - ledgerTimestamp;
        return elapsedMs > config.getRetentionTimeMillis();
    }

    /**
     * Checks whether there are ledger that have been fully consumed and deletes them
     *
     * @throws Exception
     */
    void internalTrimConsumedLedgers() {
        // Ensure only one trimming operation is active
        if (!trimmerMutex.tryLock()) {
            scheduleDeferredTrimming();
            return;
        }

        List<LedgerInfo> ledgersToDelete = Lists.newArrayList();

        synchronized (this) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Start TrimConsumedLedgers. ledgers={} totalSize={}", name, ledgers.keySet(),
                        TOTAL_SIZE_UPDATER.get(this));
            }
            if (STATE_UPDATER.get(this) == State.Closed) {
                log.debug("[{}] Ignoring trimming request since the managed ledger was already closed", name);
                trimmerMutex.unlock();
                return;
            }

            long slowestReaderLedgerId = -1;
            if (cursors.isEmpty()) {
                // At this point the lastLedger will be pointing to the
                // ledger that has just been closed, therefore the +1 to
                // include lastLedger in the trimming.
                slowestReaderLedgerId = currentLedger.getId() + 1;
            } else {
                PositionImpl slowestReaderPosition = cursors.getSlowestReaderPosition();
                if (slowestReaderPosition != null) {
                    slowestReaderLedgerId = slowestReaderPosition.getLedgerId();
                } else {
                    trimmerMutex.unlock();
                    return;
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Slowest consumer ledger id: {}", name, slowestReaderLedgerId);
            }

            // skip ledger if retention constraint met
            for (LedgerInfo ls : ledgers.headMap(slowestReaderLedgerId, false).values()) {
                boolean expired = hasLedgerRetentionExpired(ls.getTimestamp());
                boolean overRetentionQuota = TOTAL_SIZE_UPDATER.get(this) > ((long) config.getRetentionSizeInMB()) * 1024 * 1024;
                if (ls.getLedgerId() == currentLedger.getId() || (!expired && !overRetentionQuota)) {
                    if (log.isDebugEnabled()) {
                        if (!expired) {
                            log.debug("[{}] ledger id skipped for deletion as unexpired: {}", name, ls.getLedgerId());
                        }
                        if (!overRetentionQuota) {
                            log.debug("[{}] ledger id: {} skipped for deletion as size: {} under quota: {} MB", name,
                                    ls.getLedgerId(), TOTAL_SIZE_UPDATER.get(this), config.getRetentionSizeInMB());
                        }
                    }
                    break;
                }

                ledgersToDelete.add(ls);
                ledgerCache.remove(ls.getLedgerId());
            }

            if (ledgersToDelete.isEmpty()) {
                trimmerMutex.unlock();
                return;
            }

            if (STATE_UPDATER.get(this) == State.CreatingLedger // Give up now and schedule a new trimming
                    || !ledgersListMutex.tryLock()) { // Avoid deadlocks with other operations updating the ledgers list
                scheduleDeferredTrimming();
                trimmerMutex.unlock();
                return;
            }

            // Update metadata
            for (LedgerInfo ls : ledgersToDelete) {
                ledgers.remove(ls.getLedgerId());
                NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, -ls.getEntries());
                TOTAL_SIZE_UPDATER.addAndGet(this, -ls.getSize());

                entryCache.invalidateAllEntries(ls.getLedgerId());
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Updating of ledgers list after trimming", name);
            }
            ManagedLedgerInfo mlInfo = ManagedLedgerInfo.newBuilder().addAllLedgerInfo(ledgers.values()).build();
            store.asyncUpdateLedgerIds(name, mlInfo, ledgersStat, new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void result, Stat stat) {
                    log.info("[{}] End TrimConsumedLedgers. ledgers={} totalSize={}", name, ledgers.size(),
                            TOTAL_SIZE_UPDATER.get(ManagedLedgerImpl.this));
                    ledgersStat = stat;
                    ledgersListMutex.unlock();
                    trimmerMutex.unlock();

                    for (LedgerInfo ls : ledgersToDelete) {
                        log.info("[{}] Removing ledger {} - size: {}", name, ls.getLedgerId(), ls.getSize());
                        bookKeeper.asyncDeleteLedger(ls.getLedgerId(), (rc, ctx) -> {
                            if (rc == BKException.Code.NoSuchLedgerExistsException) {
                                log.warn("[{}] Ledger was already deleted {}", name, ls.getLedgerId());
                            } else if (rc != BKException.Code.OK) {
                                log.error("[{}] Error deleting ledger {}", name, ls.getLedgerId(),
                                        BKException.getMessage(rc));
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Deleted ledger {}", name, ls.getLedgerId());
                                }
                            }
                        }, null);
                    }
                }

                @Override
                public void operationFailed(MetaStoreException e) {
                    log.warn("[{}] Failed to update the list of ledgers after trimming", name, e);
                    ledgersListMutex.unlock();
                    trimmerMutex.unlock();
                }
            });
        }
    }

    /**
     * Delete this ManagedLedger completely from the system.
     *
     * @throws Exception
     */
    @Override
    public void delete() throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        final AtomicReference<ManagedLedgerException> exception = new AtomicReference<>();

        asyncDelete(new DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException e, Object ctx) {
                exception.set(e);
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during managed ledger delete operation");
        }

        if (exception.get() != null) {
            log.error("[{}] Error deleting managed ledger", name, exception.get());
            throw exception.get();
        }
    }

    @Override
    public void asyncDelete(final DeleteLedgerCallback callback, final Object ctx) {
        // Delete the managed ledger without closing, since we are not interested in gracefully closing cursors and
        // ledgers
        STATE_UPDATER.set(this, State.Fenced);

        List<ManagedCursor> cursors = Lists.newArrayList(this.cursors);
        if (cursors.isEmpty()) {
            // No cursors to delete, proceed with next step
            deleteAllLedgers(callback, ctx);
            return;
        }

        AtomicReference<ManagedLedgerException> cursorDeleteException = new AtomicReference<>();
        AtomicInteger cursorsToDelete = new AtomicInteger(cursors.size());
        for (ManagedCursor cursor : cursors) {
            asyncDeleteCursor(cursor.getName(), new DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    if (cursorsToDelete.decrementAndGet() == 0) {
                        if (cursorDeleteException.get() != null) {
                            // Some cursor failed to delete
                            callback.deleteLedgerFailed(cursorDeleteException.get(), ctx);
                            return;
                        }

                        // All cursors deleted, continue with deleting all ledgers
                        deleteAllLedgers(callback, ctx);
                    }
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.warn("[{}] Failed to delete cursor {}", name, cursor, exception);
                    cursorDeleteException.compareAndSet(null, exception);
                    if (cursorsToDelete.decrementAndGet() == 0) {
                        // Trigger callback only once
                        callback.deleteLedgerFailed(exception, ctx);
                    }
                }
            }, null);
        }
    }

    private void deleteAllLedgers(DeleteLedgerCallback callback, Object ctx) {
        List<LedgerInfo> ledgers = Lists.newArrayList(ManagedLedgerImpl.this.ledgers.values());
        AtomicInteger ledgersToDelete = new AtomicInteger(ledgers.size());
        if (ledgers.isEmpty()) {
            // No ledgers to delete, proceed with deleting metadata
            deleteMetadata(callback, ctx);
            return;
        }

        for (LedgerInfo ls : ledgers) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Deleting ledger {}", name, ls);
            }
            bookKeeper.asyncDeleteLedger(ls.getLedgerId(), (rc, ctx1) -> {
                switch (rc) {
                case BKException.Code.NoSuchLedgerExistsException:
                    log.warn("[{}] Ledger {} not found when deleting it", name, ls.getLedgerId());
                    // Continue anyway

                case BKException.Code.OK:
                    if (ledgersToDelete.decrementAndGet() == 0) {
                        // All ledgers deleted, now remove ML metadata
                        deleteMetadata(callback, ctx);
                    }
                    break;

                default:
                    // Handle error
                    log.warn("[{}] Failed to delete ledger {} -- {}", name, ls.getLedgerId(),
                            BKException.getMessage(rc));
                    int toDelete = ledgersToDelete.get();
                    if (toDelete != -1 && ledgersToDelete.compareAndSet(toDelete, -1)) {
                        // Trigger callback only once
                        callback.deleteLedgerFailed(new ManagedLedgerException(BKException.getMessage(rc)), ctx);
                    }
                }
            }, null);
        }
    }

    private void deleteMetadata(DeleteLedgerCallback callback, Object ctx) {
        store.removeManagedLedger(name, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                log.info("[{}] Successfully deleted managed ledger", name);
                factory.close(ManagedLedgerImpl.this);
                callback.deleteLedgerComplete(ctx);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn("[{}] Failed to delete managed ledger", name, e);
                factory.close(ManagedLedgerImpl.this);
                callback.deleteLedgerFailed(e, ctx);
            }
        });
    }

    /**
     * Get the number of entries between a contiguous range of two positions
     *
     * @param range
     *            the position range
     * @return the count of entries
     */
    long getNumberOfEntries(Range<PositionImpl> range) {
        PositionImpl fromPosition = range.lowerEndpoint();
        boolean fromIncluded = range.lowerBoundType() == BoundType.CLOSED;
        PositionImpl toPosition = range.upperEndpoint();
        boolean toIncluded = range.upperBoundType() == BoundType.CLOSED;

        if (fromPosition.getLedgerId() == toPosition.getLedgerId()) {
            // If the 2 positions are in the same ledger
            long count = toPosition.getEntryId() - fromPosition.getEntryId() - 1;
            count += fromIncluded ? 1 : 0;
            count += toIncluded ? 1 : 0;
            return count;
        } else {
            long count = 0;
            // If the from & to are pointing to different ledgers, then we need to :
            // 1. Add the entries in the ledger pointed by toPosition
            count += toPosition.getEntryId();
            count += toIncluded ? 1 : 0;

            // 2. Add the entries in the ledger pointed by fromPosition
            LedgerInfo li = ledgers.get(fromPosition.getLedgerId());
            if (li != null) {
                count += li.getEntries() - (fromPosition.getEntryId() + 1);
                count += fromIncluded ? 1 : 0;
            }

            // 3. Add the whole ledgers entries in between
            for (LedgerInfo ls : ledgers.subMap(fromPosition.getLedgerId(), false, toPosition.getLedgerId(), false)
                    .values()) {
                count += ls.getEntries();
            }

            return count;
        }
    }

    /**
     * Get the entry position at a given distance from a given position
     *
     * @param startPosition
     *            starting position
     * @param n
     *            number of entries to skip ahead
     * @param startRange
     *            specifies whether or not to include the start position in calculating the distance
     * @return the new position that is n entries ahead
     */
    PositionImpl getPositionAfterN(final PositionImpl startPosition, long n, PositionBound startRange) {
        long entriesToSkip = n;
        long currentLedgerId;
        long currentEntryId;

        if (startRange == PositionBound.startIncluded) {
            currentLedgerId = startPosition.getLedgerId();
            currentEntryId = startPosition.getEntryId();
        } else {
            // e.g. a mark-delete position
            PositionImpl nextValidPosition = getNextValidPosition(startPosition);
            currentLedgerId = nextValidPosition.getLedgerId();
            currentEntryId = nextValidPosition.getEntryId();
        }

        boolean lastLedger = false;
        long totalEntriesInCurrentLedger;

        while (entriesToSkip >= 0) {
            // for the current ledger, the number of entries written is deduced from the lastConfirmedEntry
            // for previous ledgers, LedgerInfo in ZK has the number of entries
            if (currentLedgerId == currentLedger.getId()) {
                lastLedger = true;
                totalEntriesInCurrentLedger = lastConfirmedEntry.getEntryId() + 1;
            } else {
                totalEntriesInCurrentLedger = ledgers.get(currentLedgerId).getEntries();
            }

            long unreadEntriesInCurrentLedger = totalEntriesInCurrentLedger - currentEntryId;

            if (unreadEntriesInCurrentLedger >= entriesToSkip) {
                // if the current ledger has more entries than what we need to skip
                // then the return position is in the same ledger
                currentEntryId += entriesToSkip;
                break;
            } else {
                // skip remaining entry from the next ledger
                entriesToSkip -= unreadEntriesInCurrentLedger;
                if (lastLedger) {
                    // there are no more ledgers, return the last position
                    currentEntryId = totalEntriesInCurrentLedger;
                    break;
                } else {
                    currentLedgerId = ledgers.ceilingKey(currentLedgerId + 1);
                    currentEntryId = 0;
                }
            }
        }

        PositionImpl positionToReturn = getPreviousPosition(PositionImpl.get(currentLedgerId, currentEntryId));
        if (log.isDebugEnabled()) {
            log.debug("getPositionAfterN: Start position {}:{}, startIncluded: {}, Return position {}:{}",
                    startPosition.getLedgerId(), startPosition.getEntryId(), startRange, positionToReturn.getLedgerId(),
                    positionToReturn.getEntryId());
        }

        return positionToReturn;
    }

    /**
     * Get the entry position that come before the specified position in the message stream, using information from the
     * ledger list and each ledger entries count.
     *
     * @param position
     *            the current position
     * @return the previous position
     */
    PositionImpl getPreviousPosition(PositionImpl position) {
        if (position.getEntryId() > 0) {
            return PositionImpl.get(position.getLedgerId(), position.getEntryId() - 1);
        }

        // The previous position will be the last position of an earlier ledgers
        NavigableMap<Long, LedgerInfo> headMap = ledgers.headMap(position.getLedgerId(), false);

        if (headMap.isEmpty()) {
            // There is no previous ledger, return an invalid position in the current ledger
            return PositionImpl.get(position.getLedgerId(), -1);
        }

        // We need to find the most recent non-empty ledger
        for (long ledgerId : headMap.descendingKeySet()) {
            LedgerInfo li = headMap.get(ledgerId);
            if (li.getEntries() > 0) {
                return PositionImpl.get(li.getLedgerId(), li.getEntries() - 1);
            }
        }

        // in case there are only empty ledgers, we return a position in the first one
        return PositionImpl.get(headMap.firstEntry().getKey(), -1);
    }

    /**
     * Validate whether a specified position is valid for the current managed ledger.
     *
     * @param position
     *            the position to validate
     * @return true if the position is valid, false otherwise
     */
    boolean isValidPosition(PositionImpl position) {
        PositionImpl last = lastConfirmedEntry;
        if (log.isDebugEnabled()) {
            log.debug("IsValid position: {} -- last: {}", position, last);
        }

        if (position.getEntryId() < 0) {
            return false;
        } else if (position.getLedgerId() > last.getLedgerId()) {
            return false;
        } else if (position.getLedgerId() == last.getLedgerId()) {
            return position.getEntryId() <= (last.getEntryId() + 1);
        } else {
            // Look in the ledgers map
            LedgerInfo ls = ledgers.get(position.getLedgerId());

            if (ls == null) {
                if (position.getLedgerId() < last.getLedgerId()) {
                    // Pointing to a non existing ledger that is older than the current ledger is invalid
                    return false;
                } else {
                    // Pointing to a non existing ledger is only legitimate if the ledger was empty
                    return position.getEntryId() == 0;
                }
            }

            return position.getEntryId() < ls.getEntries();
        }
    }

    boolean ledgerExists(long ledgerId) {
        return ledgers.get(ledgerId) != null;
    }

    long getNextValidLedger(long ledgerId) {
        return ledgers.ceilingKey(ledgerId + 1);
    }

    PositionImpl getNextValidPosition(final PositionImpl position) {
        PositionImpl nextPosition = position.getNext();
        while (!isValidPosition(nextPosition)) {
            Long nextLedgerId = ledgers.ceilingKey(nextPosition.getLedgerId() + 1);
            if (nextLedgerId == null) {
                return null;
            }
            nextPosition = PositionImpl.get(nextLedgerId.longValue(), 0);
        }
        return nextPosition;
    }

    PositionImpl getFirstPosition() {
        Long ledgerId = ledgers.firstKey();
        return ledgerId == null ? null : new PositionImpl(ledgerId, -1);
    }

    PositionImpl getLastPosition() {
        return lastConfirmedEntry;
    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        return cursors.getSlowestReader();
    }

    PositionImpl getMarkDeletePositionOfSlowestConsumer() {
        ManagedCursor slowestCursor = getSlowestConsumer();
        return slowestCursor == null ? null : (PositionImpl) slowestCursor.getMarkDeletedPosition();
    }

    /**
     * Get the last position written in the managed ledger, alongside with the associated counter
     */
    Pair<PositionImpl, Long> getLastPositionAndCounter() {
        PositionImpl pos;
        long count;

        do {
            pos = lastConfirmedEntry;
            count = ENTRIES_ADDED_COUNTER_UPDATER.get(this);

            // Ensure no entry was written while reading the two values
        } while (pos.compareTo(lastConfirmedEntry) != 0);

        return Pair.create(pos, count);
    }

    public void activateCursor(ManagedCursor cursor) {
        if (activeCursors.get(cursor.getName()) == null) {
            activeCursors.add(cursor);
        }
    }

    public void deactivateCursor(ManagedCursor cursor) {
        if (activeCursors.get(cursor.getName()) != null) {
            activeCursors.removeCursor(cursor.getName());
            if (activeCursors.isEmpty()) {
                // cleanup cache if there is no active subscription
                entryCache.clear();
            } else {
                // if removed subscription was the slowest subscription : update cursor and let it clear cache: till
                // new slowest-cursor's read-position
                discardEntriesFromCache((ManagedCursorImpl) activeCursors.getSlowestReader(),
                        getPreviousPosition((PositionImpl) activeCursors.getSlowestReader().getReadPosition()));
            }
        }
    }

    public boolean isCursorActive(ManagedCursor cursor) {
        return activeCursors.get(cursor.getName()) != null;
    }

    private boolean currentLedgerIsFull() {
        boolean spaceQuotaReached = (currentLedgerEntries >= config.getMaxEntriesPerLedger()
                || currentLedgerSize >= (config.getMaxSizePerLedgerMb() * MegaByte));

        long timeSinceLedgerCreationMs = System.currentTimeMillis() - lastLedgerCreatedTimestamp;
        boolean maxLedgerTimeReached = timeSinceLedgerCreationMs >= maximumRolloverTimeMs;

        if (spaceQuotaReached || maxLedgerTimeReached) {
            if (config.getMinimumRolloverTimeMs() > 0) {

                boolean switchLedger = timeSinceLedgerCreationMs > config.getMinimumRolloverTimeMs();
                if (log.isDebugEnabled()) {
                    log.debug("Diff: {}, threshold: {} -- switch: {}",
                            System.currentTimeMillis() - lastLedgerCreatedTimestamp, config.getMinimumRolloverTimeMs(),
                            switchLedger);
                }
                return switchLedger;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    public List<LedgerInfo> getLedgersInfoAsList() {
        return Lists.newArrayList(ledgers.values());
    }

    public NavigableMap<Long, LedgerInfo> getLedgersInfo() {
        return ledgers;
    }

    ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    OrderedSafeExecutor getExecutor() {
        return executor;
    }

    /**
     * Throws an exception if the managed ledger has been previously fenced
     *
     * @throws ManagedLedgerException
     */
    private void checkFenced() throws ManagedLedgerException {
        if (STATE_UPDATER.get(this) == State.Fenced) {
            log.error("[{}] Attempted to use a fenced managed ledger", name);
            throw new ManagedLedgerFencedException();
        }
    }

    private void checkManagedLedgerIsOpen() throws ManagedLedgerException {
        if (STATE_UPDATER.get(this) == State.Closed) {
            throw new ManagedLedgerException("ManagedLedger " + name + " has already been closed");
        }
    }

    synchronized void setFenced() {
        STATE_UPDATER.set(this, State.Fenced);
    }

    MetaStore getStore() {
        return store;
    }

    ManagedLedgerConfig getConfig() {
        return config;
    }

    static interface ManagedLedgerInitializeLedgerCallback {
        public void initializeComplete();

        public void initializeFailed(ManagedLedgerException e);
    }

    // Expose internal values for debugging purposes
    public long getEntriesAddedCounter() {
        return ENTRIES_ADDED_COUNTER_UPDATER.get(this);
    }

    public long getCurrentLedgerEntries() {
        return currentLedgerEntries;
    }

    public long getCurrentLedgerSize() {
        return currentLedgerSize;
    }

    public long getLastLedgerCreatedTimestamp() {
        return lastLedgerCreatedTimestamp;
    }

    public long getLastLedgerCreationFailureTimestamp() {
        return lastLedgerCreationFailureTimestamp;
    }

    public int getWaitingCursorsCount() {
        return waitingCursors.size();
    }

    public int getPendingAddEntriesCount() {
        return pendingAddEntries.size();
    }

    public PositionImpl getLastConfirmedEntry() {
        return lastConfirmedEntry;
    }

    public String getState() {
        return STATE_UPDATER.get(this).toString();
    }

    public ManagedLedgerMBeanImpl getMBean() {
        return mbean;
    }

    public long getCacheSize() {
        return entryCache.getSize();
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerImpl.class);

}
