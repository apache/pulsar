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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.min;
import static org.apache.bookkeeper.mledger.util.Errors.isNoSuchLedgerExistsException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.Retries;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.UpdatePropertiesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.BadVersionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.LedgerNotExistException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerInterceptException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetadataNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NonRecoverableLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.OffloadedLedgerHandle;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.WaitingEntryCallBack;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.NestedPositionInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.OffloadContext;
import org.apache.bookkeeper.mledger.util.CallbackMutex;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.LazyLoadableValue;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.metadata.api.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ManagedLedgerImpl implements ManagedLedger, CreateCallback {
    private static final long MegaByte = 1024 * 1024;

    protected static final int AsyncOperationTimeoutSeconds = 30;

    protected final BookKeeper bookKeeper;
    protected final String name;
    private final Map<String, byte[]> ledgerMetadata;
    protected final BookKeeper.DigestType digestType;

    protected ManagedLedgerConfig config;
    protected Map<String, String> propertiesMap;
    protected final MetaStore store;

    final ConcurrentLongHashMap<CompletableFuture<ReadHandle>> ledgerCache =
            ConcurrentLongHashMap.<CompletableFuture<ReadHandle>>newBuilder()
                    .expectedItems(16) // initial capacity
                    .concurrencyLevel(1) // number of sections
                    .build();
    protected final NavigableMap<Long, LedgerInfo> ledgers = new ConcurrentSkipListMap<>();
    protected volatile Stat ledgersStat;

    // contains all cursors, where durable cursors are ordered by mark delete position
    private final ManagedCursorContainer cursors = new ManagedCursorContainer();
    // contains active cursors eligible for caching,
    // ordered by read position (when cacheEvictionByMarkDeletedPosition=false) or by mark delete position
    // (when cacheEvictionByMarkDeletedPosition=true)
    private final ManagedCursorContainer activeCursors = new ManagedCursorContainer();


    // Ever-increasing counter of entries added
    @VisibleForTesting
    static final AtomicLongFieldUpdater<ManagedLedgerImpl> ENTRIES_ADDED_COUNTER_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ManagedLedgerImpl.class, "entriesAddedCounter");
    @SuppressWarnings("unused")
    private volatile long entriesAddedCounter = 0;

    static final AtomicLongFieldUpdater<ManagedLedgerImpl> NUMBER_OF_ENTRIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ManagedLedgerImpl.class, "numberOfEntries");
    @SuppressWarnings("unused")
    private volatile long numberOfEntries = 0;
    static final AtomicLongFieldUpdater<ManagedLedgerImpl> TOTAL_SIZE_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ManagedLedgerImpl.class, "totalSize");
    @SuppressWarnings("unused")
    private volatile long totalSize = 0;

    // Cursors that are waiting to be notified when new entries are persisted
    final ConcurrentLinkedQueue<ManagedCursorImpl> waitingCursors;

    // Objects that are waiting to be notified when new entries are persisted
    final ConcurrentLinkedQueue<WaitingEntryCallBack> waitingEntryCallBacks;

    // This map is used for concurrent open cursor requests, where the 2nd request will attach a listener to the
    // uninitialized cursor future from the 1st request
    final Map<String, CompletableFuture<ManagedCursor>> uninitializedCursors;

    final EntryCache entryCache;

    private ScheduledFuture<?> timeoutTask;
    private ScheduledFuture<?> checkLedgerRollTask;

    /**
     * This lock is held while the ledgers list or propertiesMap is updated asynchronously on the metadata store.
     * Since we use the store version, we cannot have multiple concurrent updates.
     */
    private final CallbackMutex metadataMutex = new CallbackMutex();
    private final CallbackMutex trimmerMutex = new CallbackMutex();

    private final CallbackMutex offloadMutex = new CallbackMutex();
    private static final CompletableFuture<PositionImpl> NULL_OFFLOAD_PROMISE = CompletableFuture
            .completedFuture(PositionImpl.LATEST);
    @VisibleForTesting
    @Getter
    protected volatile LedgerHandle currentLedger;
    protected volatile long currentLedgerEntries = 0;
    protected volatile long currentLedgerSize = 0;
    protected volatile long lastLedgerCreatedTimestamp = 0;
    private volatile long lastLedgerCreationFailureTimestamp = 0;
    private long lastLedgerCreationInitiationTimestamp = 0;

    private long lastOffloadLedgerId = 0;
    private volatile long lastOffloadSuccessTimestamp = 0;
    private volatile long lastOffloadFailureTimestamp = 0;

    protected volatile ManagedLedgerException interceptorException = null;

    private int minBacklogCursorsForCaching = 0;
    private int minBacklogEntriesForCaching = 1000;
    private int maxBacklogBetweenCursorsForCaching = 1000;

    private static final Random random = new Random(System.currentTimeMillis());
    private long maximumRolloverTimeMs;
    protected final Supplier<CompletableFuture<Boolean>> mlOwnershipChecker;

    volatile PositionImpl lastConfirmedEntry;

    protected ManagedLedgerInterceptor managedLedgerInterceptor;

    protected volatile long lastAddEntryTimeMs = 0;
    private long inactiveLedgerRollOverTimeMs = 0;

    /** A signal that may trigger all the subsequent OpAddEntry of current ledger to be failed due to timeout. **/
    protected volatile AtomicBoolean currentLedgerTimeoutTriggered;

    protected static final int DEFAULT_LEDGER_DELETE_RETRIES = 3;
    protected static final int DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC = 60;
    private static final String MIGRATION_STATE_PROPERTY = "migrated";

    public enum State {
        None, // Uninitialized
        LedgerOpened, // A ledger is ready to write into
        ClosingLedger, // Closing current ledger
        ClosedLedger, // Current ledger has been closed and there's no pending
                      // operation
        CreatingLedger, // Creating a new ledger
        Closed, // ManagedLedger has been closed
        Fenced {
            @Override
            public boolean isFenced() {
                return true;
            }
        }, // A managed ledger is fenced when there is some concurrent
                // access from a different session/machine. In this state the
                // managed ledger will throw exception for all operations, since
                // the new instance will take over
        FencedForDeletion {
            @Override
            public boolean isFenced() {
                return true;
            }
        }, // A managed ledger is fenced for deletion
        // which allows truncate/delete operation to proceed but the rest
        // of teh rules from the Fenced state apply.
        Terminated, // Managed ledger was terminated and no more entries
                    // are allowed to be added. Reads are allowed
        WriteFailed; // The state that is transitioned to when a BK write failure happens
                    // After handling the BK write failure, managed ledger will get signalled to create a new ledger

        public boolean isFenced() {
            return false;
        }
    }

    // define boundaries for position based seeks and searches
    public enum PositionBound {
        startIncluded, startExcluded
    }

    protected static final AtomicReferenceFieldUpdater<ManagedLedgerImpl, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedLedgerImpl.class, State.class, "state");
    protected volatile State state = null;
    private volatile boolean migrated = false;

    @Getter
    private final OrderedScheduler scheduledExecutor;

    @Getter
    protected final Executor executor;

    @Getter
    private final ManagedLedgerFactoryImpl factory;

    @Getter
    protected final ManagedLedgerMBeanImpl mbean;
    protected final Clock clock;

    private static final AtomicLongFieldUpdater<ManagedLedgerImpl> READ_OP_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ManagedLedgerImpl.class, "readOpCount");
    private volatile long readOpCount = 0;
    protected static final AtomicLongFieldUpdater<ManagedLedgerImpl> ADD_OP_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ManagedLedgerImpl.class, "addOpCount");
    private volatile long addOpCount = 0;

    // last read-operation's callback to check read-timeout on it.
    private volatile ReadEntryCallbackWrapper lastReadCallback = null;
    private static final AtomicReferenceFieldUpdater<ManagedLedgerImpl, ReadEntryCallbackWrapper>
            LAST_READ_CALLBACK_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(ManagedLedgerImpl.class, ReadEntryCallbackWrapper.class, "lastReadCallback");

    /**
     * Queue of pending entries to be added to the managed ledger. Typically, entries are queued when a new ledger is.
     * created asynchronously and hence there is no ready ledger to write into.
     */
    final ConcurrentLinkedQueue<OpAddEntry> pendingAddEntries = new ConcurrentLinkedQueue<>();

    /**
     * This variable is used for testing the tests.
     * ManagedLedgerTest#testManagedLedgerWithPlacementPolicyInCustomMetadata()
     */
    @VisibleForTesting
    Map<String, byte[]> createdLedgerCustomMetadata;

    private long lastEvictOffloadedLedgers;
    private static final int MINIMUM_EVICTION_INTERVAL_DIVIDER = 10;

    public ManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, MetaStore store,
            ManagedLedgerConfig config, OrderedScheduler scheduledExecutor,
            final String name) {
        this(factory, bookKeeper, store, config, scheduledExecutor, name, null);
    }
    public ManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, MetaStore store,
            ManagedLedgerConfig config, OrderedScheduler scheduledExecutor,
            final String name, final Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
        this.factory = factory;
        this.bookKeeper = bookKeeper;
        this.config = config;
        this.store = store;
        this.name = name;
        this.ledgerMetadata = LedgerMetadataUtils.buildBaseManagedLedgerMetadata(name);
        this.digestType = BookKeeper.DigestType.fromApiDigestType(config.getDigestType());
        this.scheduledExecutor = scheduledExecutor;
        this.executor = bookKeeper.getMainWorkerPool().chooseThread(name);
        TOTAL_SIZE_UPDATER.set(this, 0);
        NUMBER_OF_ENTRIES_UPDATER.set(this, 0);
        ENTRIES_ADDED_COUNTER_UPDATER.set(this, 0);
        STATE_UPDATER.set(this, State.None);
        this.ledgersStat = null;
        this.mbean = new ManagedLedgerMBeanImpl(this);
        if (config.getManagedLedgerInterceptor() != null) {
            this.managedLedgerInterceptor = config.getManagedLedgerInterceptor();
        }
        this.entryCache = factory.getEntryCacheManager().getEntryCache(this);
        this.waitingCursors = Queues.newConcurrentLinkedQueue();
        this.waitingEntryCallBacks = Queues.newConcurrentLinkedQueue();
        this.uninitializedCursors = new HashMap();
        this.clock = config.getClock();

        // Get the next rollover time. Add a random value upto 5% to avoid rollover multiple ledgers at the same time
        this.maximumRolloverTimeMs = getMaximumRolloverTimeMs(config);
        this.mlOwnershipChecker = mlOwnershipChecker;
        this.propertiesMap = new ConcurrentHashMap<>();
        this.inactiveLedgerRollOverTimeMs = config.getInactiveLedgerRollOverTimeMs();
        if (config.getManagedLedgerInterceptor() != null) {
            this.managedLedgerInterceptor = config.getManagedLedgerInterceptor();
        }
        this.minBacklogCursorsForCaching = config.getMinimumBacklogCursorsForCaching();
        this.minBacklogEntriesForCaching = config.getMinimumBacklogEntriesForCaching();
        this.maxBacklogBetweenCursorsForCaching = config.getMaxBacklogBetweenCursorsForCaching();
    }

    synchronized void initialize(final ManagedLedgerInitializeLedgerCallback callback, final Object ctx) {
        log.info("Opening managed ledger {}", name);

        // Fetch the list of existing ledgers in the managed ledger
        store.getManagedLedgerInfo(name, config.isCreateIfMissing(), config.getProperties(),
                new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                ledgersStat = stat;
                if (mlInfo.hasTerminatedPosition()) {
                    state = State.Terminated;
                    lastConfirmedEntry = new PositionImpl(mlInfo.getTerminatedPosition());
                    log.info("[{}] Recovering managed ledger terminated at {}", name, lastConfirmedEntry);
                }
                for (LedgerInfo ls : mlInfo.getLedgerInfoList()) {
                    ledgers.put(ls.getLedgerId(), ls);
                }

                if (mlInfo.getPropertiesCount() > 0) {
                    propertiesMap = new HashMap();
                    for (int i = 0; i < mlInfo.getPropertiesCount(); i++) {
                        MLDataFormats.KeyValue property = mlInfo.getProperties(i);
                        propertiesMap.put(property.getKey(), property.getValue());
                    }
                }
                migrated = mlInfo.hasTerminatedPosition() && propertiesMap.containsKey(MIGRATION_STATE_PROPERTY);
                if (managedLedgerInterceptor != null) {
                    managedLedgerInterceptor.onManagedLedgerPropertiesInitialize(propertiesMap);
                }

                // Last ledger stat may be zeroed, we must update it
                if (!ledgers.isEmpty()) {
                    final long id = ledgers.lastKey();
                    OpenCallback opencb = (rc, lh, ctx1) -> {
                        executor.execute(() -> {
                            mbean.endDataLedgerOpenOp();
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Opened ledger {}: {}", name, id, BKException.getMessage(rc));
                            }
                            if (rc == BKException.Code.OK) {
                                if (State.Terminated.equals(state)) {
                                    currentLedger = lh;
                                }
                                LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(id)
                                        .setEntries(lh.getLastAddConfirmed() + 1).setSize(lh.getLength())
                                        .setTimestamp(clock.millis()).build();
                                ledgers.put(id, info);
                                if (managedLedgerInterceptor != null) {
                                    managedLedgerInterceptor.onManagedLedgerLastLedgerInitialize(name, lh)
                                        .thenRun(() -> initializeBookKeeper(callback))
                                        .exceptionally(ex -> {
                                            callback.initializeFailed(
                                                    new ManagedLedgerInterceptException(ex.getCause()));
                                            return null;
                                        });
                                } else {
                                    initializeBookKeeper(callback);
                                }
                            } else if (isNoSuchLedgerExistsException(rc)) {
                                log.warn("[{}] Ledger not found: {}", name, id);
                                ledgers.remove(id);
                                initializeBookKeeper(callback);
                            } else {
                                log.error("[{}] Failed to open ledger {}: {}", name, id, BKException.getMessage(rc));
                                callback.initializeFailed(createManagedLedgerException(rc));
                                return;
                            }
                        });
                    };

                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Opening ledger {}", name, id);
                    }
                    mbean.startDataLedgerOpenOp();
                    bookKeeper.asyncOpenLedger(id, digestType, config.getPassword(), opencb, null);
                } else {
                    initializeBookKeeper(callback);
                }
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                handleBadVersion(e);
                if (e instanceof MetadataNotFoundException) {
                    callback.initializeFailed(new ManagedLedgerNotFoundException(e));
                } else {
                    callback.initializeFailed(new ManagedLedgerException(e));
                }
            }
        });

        scheduleTimeoutTask();
    }

    protected synchronized void initializeBookKeeper(final ManagedLedgerInitializeLedgerCallback callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] initializing bookkeeper; ledgers {}", name, ledgers);
        }

        // Calculate total entries and size
        final List<Long> emptyLedgersToBeDeleted = Collections.synchronizedList(new ArrayList<>());
        Iterator<LedgerInfo> iterator = ledgers.values().iterator();
        while (iterator.hasNext()) {
            LedgerInfo li = iterator.next();
            if (li.getEntries() > 0) {
                NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, li.getEntries());
                TOTAL_SIZE_UPDATER.addAndGet(this, li.getSize());
            } else {
                iterator.remove();
                emptyLedgersToBeDeleted.add(li.getLedgerId());
            }
        }

        if (state == State.Terminated) {
            // When recovering a terminated managed ledger, we don't need to create
            // a new ledger for writing, since no more writes are allowed.
            // We just move on to the next stage
            initializeCursors(callback);
            return;
        }

        final MetaStoreCallback<Void> storeLedgersCb = new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void v, Stat stat) {
                ledgersStat = stat;
                emptyLedgersToBeDeleted.forEach(ledgerId -> {
                    bookKeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
                        log.info("[{}] Deleted empty ledger ledgerId={} rc={}", name, ledgerId, rc);
                    }, null);
                });
                initializeCursors(callback);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                handleBadVersion(e);
                callback.initializeFailed(new ManagedLedgerException(e));
            }
        };

        // Create a new ledger to start writing
        this.lastLedgerCreationInitiationTimestamp = System.currentTimeMillis();
        mbean.startDataLedgerCreateOp();

        asyncCreateLedger(bookKeeper, config, digestType, (rc, lh, ctx) -> {

            if (checkAndCompleteLedgerOpTask(rc, lh, ctx)) {
                return;
            }

            executor.execute(() -> {
                mbean.endDataLedgerCreateOp();
                if (rc != BKException.Code.OK) {
                    log.error("[{}] Error creating ledger rc={} {}", name, rc, BKException.getMessage(rc));
                    callback.initializeFailed(createManagedLedgerException(rc));
                    return;
                }

                log.info("[{}] Created ledger {} after closed {}", name, lh.getId(),
                        currentLedger == null ? "null" : currentLedger.getId());
                STATE_UPDATER.set(this, State.LedgerOpened);
                updateLastLedgerCreatedTimeAndScheduleRolloverTask();
                currentLedger = lh;
                currentLedgerTimeoutTriggered = new AtomicBoolean();

                lastConfirmedEntry = new PositionImpl(lh.getId(), -1);
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

                LedgerInfo info = LedgerInfo.newBuilder().setLedgerId(lh.getId()).setTimestamp(0).build();
                ledgers.put(lh.getId(), info);

                // Save it back to ensure all nodes exist
                store.asyncUpdateLedgerIds(name, getManagedLedgerInfo(), ledgersStat, storeLedgersCb);
            });
        }, ledgerMetadata);
    }

    protected void initializeCursors(final ManagedLedgerInitializeLedgerCallback callback) {
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

                if (!ManagedLedgerImpl.this.config.isLazyCursorRecovery()) {
                    log.debug("[{}] Loading cursors", name);

                    for (final String cursorName : consumers) {
                        log.info("[{}] Loading cursor {}", name, cursorName);
                        final ManagedCursorImpl cursor;
                        cursor = new ManagedCursorImpl(bookKeeper, ManagedLedgerImpl.this, cursorName);

                        cursor.recover(new VoidCallback() {
                            @Override
                            public void operationComplete() {
                                log.info("[{}] Recovery for cursor {} completed. pos={} -- todo={}", name, cursorName,
                                        cursor.getMarkDeletedPosition(), cursorCount.get() - 1);
                                cursor.setActive();
                                addCursor(cursor);

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
                } else {
                    // Lazily recover cursors by put them to uninitializedCursors map.
                    for (final String cursorName : consumers) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Recovering cursor {} lazily", name, cursorName);
                        }
                        final ManagedCursorImpl cursor;
                        cursor = new ManagedCursorImpl(bookKeeper, ManagedLedgerImpl.this, cursorName);
                        CompletableFuture<ManagedCursor> cursorRecoveryFuture = new CompletableFuture<>();
                        uninitializedCursors.put(cursorName, cursorRecoveryFuture);

                        cursor.recover(new VoidCallback() {
                            @Override
                            public void operationComplete() {
                                log.info("[{}] Lazy recovery for cursor {} completed. pos={} -- todo={}", name,
                                        cursorName, cursor.getMarkDeletedPosition(), cursorCount.get() - 1);
                                cursor.setActive();
                                synchronized (ManagedLedgerImpl.this) {
                                    addCursor(cursor);
                                    uninitializedCursors.remove(cursor.getName()).complete(cursor);
                                }
                            }

                            @Override
                            public void operationFailed(ManagedLedgerException exception) {
                                log.warn("[{}] Lazy recovery for cursor {} failed", name, cursorName, exception);
                                synchronized (ManagedLedgerImpl.this) {
                                    uninitializedCursors.remove(cursor.getName()).completeExceptionally(exception);
                                }
                            }
                        });
                    }
                    // Complete ledger recovery.
                    callback.initializeComplete();
                }
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn("[{}] Failed to get the cursors list", name, e);
                callback.initializeFailed(new ManagedLedgerException(e));
            }
        });
    }

    private void addCursor(ManagedCursorImpl cursor) {
        Position positionForOrdering = null;
        if (cursor.isDurable()) {
            positionForOrdering = cursor.getMarkDeletedPosition();
            if (positionForOrdering == null) {
                positionForOrdering = PositionImpl.EARLIEST;
            }
        }
        cursors.add(cursor, positionForOrdering);
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
    public Position addEntry(byte[] data, int numberOfMessages) throws InterruptedException, ManagedLedgerException {
        return addEntry(data, numberOfMessages, 0, data.length);
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
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
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
    public Position addEntry(byte[] data, int numberOfMessages, int offset, int length) throws InterruptedException,
            ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        // Result list will contain the status exception and the resulting
        // position
        class Result {
            ManagedLedgerException status = null;
            Position position = null;
        }
        final Result result = new Result();

        asyncAddEntry(data, numberOfMessages, offset, length, new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
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
    public void asyncAddEntry(final byte[] data, int numberOfMessages, int offset, int length,
                              final AddEntryCallback callback, final Object ctx) {
        ByteBuf buffer = Unpooled.wrappedBuffer(data, offset, length);
        asyncAddEntry(buffer, numberOfMessages, callback, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, AddEntryCallback callback, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] asyncAddEntry size={} state={}", name, buffer.readableBytes(), state);
        }

        // retain buffer in this thread
        buffer.retain();

        // Jump to specific thread to avoid contention from writers writing from different threads
        executor.execute(() -> {
            OpAddEntry addOperation = OpAddEntry.createNoRetainBuffer(this, buffer, callback, ctx,
                    currentLedgerTimeoutTriggered);
            internalAsyncAddEntry(addOperation);
        });
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AddEntryCallback callback, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] asyncAddEntry size={} state={}", name, buffer.readableBytes(), state);
        }

        // retain buffer in this thread
        buffer.retain();

        // Jump to specific thread to avoid contention from writers writing from different threads
        executor.execute(() -> {
            OpAddEntry addOperation = OpAddEntry.createNoRetainBuffer(this, buffer, numberOfMessages, callback, ctx,
                    currentLedgerTimeoutTriggered);
            internalAsyncAddEntry(addOperation);
        });
    }

    protected synchronized void internalAsyncAddEntry(OpAddEntry addOperation) {
        if (!beforeAddEntry(addOperation)) {
            return;
        }
        final State state = STATE_UPDATER.get(this);
        if (state.isFenced()) {
            addOperation.failed(new ManagedLedgerFencedException());
            return;
        } else if (state == State.Terminated) {
            addOperation.failed(new ManagedLedgerTerminatedException("Managed ledger was already terminated"));
            return;
        } else if (state == State.Closed) {
            addOperation.failed(new ManagedLedgerAlreadyClosedException("Managed ledger was already closed"));
            return;
        } else if (state == State.WriteFailed) {
            addOperation.failed(new ManagedLedgerAlreadyClosedException("Waiting to recover from failure"));
            return;
        }
        pendingAddEntries.add(addOperation);

        if (state == State.ClosingLedger || state == State.CreatingLedger) {
            // We don't have a ready ledger to write into
            // We are waiting for a new ledger to be created
            if (log.isDebugEnabled()) {
                log.debug("[{}] Queue addEntry request", name);
            }
            if (State.CreatingLedger == state) {
                long elapsedMs = System.currentTimeMillis() - this.lastLedgerCreationInitiationTimestamp;
                if (elapsedMs > TimeUnit.SECONDS.toMillis(2 * config.getMetadataOperationsTimeoutSeconds())) {
                    log.info("[{}] Ledger creation was initiated {} ms ago but it never completed and creation timeout"
                        + " task didn't kick in as well. Force to fail the create ledger operation.", name, elapsedMs);
                    this.createComplete(Code.TimeoutException, null, null);
                }
            }
        } else if (state == State.ClosedLedger) {
            // No ledger and no pending operations. Create a new ledger
            if (STATE_UPDATER.compareAndSet(this, State.ClosedLedger, State.CreatingLedger)) {
                log.info("[{}] Creating a new ledger", name);
                this.lastLedgerCreationInitiationTimestamp = System.currentTimeMillis();
                mbean.startDataLedgerCreateOp();
                asyncCreateLedger(bookKeeper, config, digestType, this, Collections.emptyMap());
            }
        } else {
            checkArgument(state == State.LedgerOpened, "ledger=%s is not opened", state);

            // Write into lastLedger
            addOperation.setLedger(currentLedger);
            addOperation.setTimeoutTriggered(currentLedgerTimeoutTriggered);

            ++currentLedgerEntries;
            currentLedgerSize += addOperation.data.readableBytes();

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
        // mark add entry activity
        lastAddEntryTimeMs = System.currentTimeMillis();
    }

    protected void afterFailedAddEntry(int numOfMessages) {
        if (managedLedgerInterceptor == null) {
            return;
        }
        managedLedgerInterceptor.afterFailedAddEntry(numOfMessages);
    }

    protected boolean beforeAddEntry(OpAddEntry addOperation) {
        // if no interceptor, just return true to make sure addOperation will be initiate()
        if (managedLedgerInterceptor == null) {
            return true;
        }
        try {
            managedLedgerInterceptor.beforeAddEntry(addOperation, addOperation.getNumberOfMessages());
            return true;
        } catch (Exception e) {
            addOperation.failed(
                    new ManagedLedgerInterceptException("Interceptor managed ledger before add to bookie failed."));
            log.error("[{}] Failed to intercept adding an entry to bookie.", name, e);
            return false;
        }
    }

    @Override
    public void readyToCreateNewLedger() {
       // only set transition state to ClosedLedger if current state is WriteFailed
       if (STATE_UPDATER.compareAndSet(this, State.WriteFailed, State.ClosedLedger)){
           log.info("[{}] Managed ledger is now ready to accept writes again", name);
       }
    }

    @Override
    public ManagedCursor openCursor(String cursorName) throws InterruptedException, ManagedLedgerException {
        return openCursor(cursorName, InitialPosition.Latest);
    }


    @Override
    public ManagedCursor openCursor(String cursorName, InitialPosition initialPosition)
            throws InterruptedException, ManagedLedgerException {
        return openCursor(cursorName, initialPosition, Collections.emptyMap(), Collections.emptyMap());
    }

    @Override
    public ManagedCursor openCursor(String cursorName, InitialPosition initialPosition, Map<String, Long> properties,
                                    Map<String, String> cursorProperties)
            throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedCursor cursor = null;
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncOpenCursor(cursorName, initialPosition, properties, cursorProperties, new OpenCursorCallback() {
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
    public void asyncOpenCursor(final String cursorName, final OpenCursorCallback callback, Object ctx) {
        this.asyncOpenCursor(cursorName, InitialPosition.Latest, callback, ctx);
    }

    @Override
    public void asyncOpenCursor(final String cursorName, final InitialPosition initialPosition,
            final OpenCursorCallback callback, final Object ctx) {
        this.asyncOpenCursor(cursorName, initialPosition, Collections.emptyMap(), Collections.emptyMap(),
                callback, ctx);
    }

    @Override
    public synchronized void asyncOpenCursor(final String cursorName, final InitialPosition initialPosition,
            Map<String, Long> properties, Map<String, String> cursorProperties,
                                             final OpenCursorCallback callback, final Object ctx) {
        try {
            checkManagedLedgerIsOpen();
            checkFenced();
        } catch (ManagedLedgerException e) {
            callback.openCursorFailed(e, ctx);
            return;
        }

        if (uninitializedCursors.containsKey(cursorName)) {
            uninitializedCursors.get(cursorName).thenAccept(cursor -> callback.openCursorComplete(cursor, ctx))
                    .exceptionally(ex -> {
                callback.openCursorFailed(ManagedLedgerException
                        .getManagedLedgerException(FutureUtil.unwrapCompletionException(ex)), ctx);
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
        final ManagedCursorImpl cursor = new ManagedCursorImpl(bookKeeper, this, cursorName);
        CompletableFuture<ManagedCursor> cursorFuture = new CompletableFuture<>();
        uninitializedCursors.put(cursorName, cursorFuture);
        PositionImpl position = InitialPosition.Earliest == initialPosition ? getFirstPosition() : getLastPosition();
        cursor.initialize(position, properties, cursorProperties, new VoidCallback() {
            @Override
            public void operationComplete() {
                log.info("[{}] Opened new cursor: {}", name, cursor);
                cursor.setActive();
                synchronized (ManagedLedgerImpl.this) {
                    // Update the ack position (ignoring entries that were written while the cursor was being created)
                    cursor.initializeCursorPosition(InitialPosition.Earliest == initialPosition
                            ? getFirstPositionAndCounter()
                            : getLastPositionAndCounter());
                    addCursor(cursor);
                    uninitializedCursors.remove(cursorName).complete(cursor);
                }
                callback.openCursorComplete(cursor, ctx);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                log.warn("[{}] Failed to open cursor: {}", name, cursor);

                synchronized (ManagedLedgerImpl.this) {
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
            callback.deleteCursorFailed(new ManagedLedgerException.CursorNotFoundException("ManagedCursor not found: "
                    + consumerName), ctx);
            return;
        } else if (!cursor.isDurable()) {
            cursor.setState(ManagedCursorImpl.State.Closed);
            cursor.cancelPendingReadRequest();
            cursors.removeCursor(consumerName);
            deactivateCursorByName(consumerName);
            callback.deleteCursorComplete(ctx);
            return;
        }

        // First remove the consumer form the MetaStore. If this operation succeeds and the next one (removing the
        // ledger from BK) don't, we end up having a loose ledger leaked but the state will be consistent.
        store.asyncRemoveCursor(ManagedLedgerImpl.this.name, consumerName, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                cursor.asyncDeleteCursorLedger();
                cursors.removeCursor(consumerName);
                deactivateCursorByName(consumerName);

                trimConsumedLedgersInBackground();

                log.info("[{}] [{}] Deleted cursor", name, consumerName);
                callback.deleteCursorComplete(ctx);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                handleBadVersion(e);
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
    public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException {
        return newNonDurableCursor(
            startCursorPosition,
            "non-durable-cursor-" + UUID.randomUUID());
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName)
            throws ManagedLedgerException {
        return newNonDurableCursor(startPosition, subscriptionName, InitialPosition.Latest, false);
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startCursorPosition, String cursorName,
                                             InitialPosition initialPosition, boolean isReadCompacted)
            throws ManagedLedgerException {
        Objects.requireNonNull(cursorName, "cursor name can't be null");
        checkManagedLedgerIsOpen();
        checkFenced();

        ManagedCursor cachedCursor = cursors.get(cursorName);
        if (cachedCursor != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cursor was already created {}", name, cachedCursor);
            }
            return cachedCursor;
        }

        // The backlog of a non-durable cursor could be incorrect if the cursor is created before `internalTrimLedgers`
        // and added to the managed ledger after `internalTrimLedgers`.
        // For more details, see https://github.com/apache/pulsar/pull/23951.
        synchronized (this) {
            NonDurableCursorImpl cursor = new NonDurableCursorImpl(bookKeeper, this, cursorName,
                    (PositionImpl) startCursorPosition, initialPosition, isReadCompacted);
            cursor.setActive();
            log.info("[{}] Opened new cursor: {}", name, cursor);
            addCursor(cursor);
            return cursor;
        }
    }

    @Override
    public ManagedCursorContainer getCursors() {
        return cursors;
    }

    @Override
    public ManagedCursorContainer getActiveCursors() {
        return activeCursors;
    }

    /**
     * Tells whether the managed ledger has any active-cursor registered.
     *
     * @return true if at least a cursor exists
     */
    public boolean hasActiveCursors() {
        // Use hasCursors instead of isEmpty because isEmpty does not take into account non-durable cursors
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
                        .mapToLong(LedgerInfo::getSize).sum();
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

    @Override
    public CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog() {
        PositionImpl pos = getMarkDeletePositionOfSlowestConsumer();

        return getEarliestMessagePublishTimeOfPos(pos);
    }

    public CompletableFuture<Long> getEarliestMessagePublishTimeOfPos(PositionImpl pos) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        if (pos == null) {
            future.complete(0L);
            return future;
        }
        PositionImpl nextPos = getNextValidPosition(pos);

        if (nextPos.compareTo(lastConfirmedEntry) > 0) {
            return CompletableFuture.completedFuture(-1L);
        }

        asyncReadEntry(nextPos, new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                    future.complete(entryTimestamp);
                } catch (IOException e) {
                    log.error("Error deserializing message for message position {}", nextPos, e);
                    future.completeExceptionally(e);
                } finally {
                    entry.release();
                }
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Error read entry for position {}", nextPos, exception);
                future.completeExceptionally(exception);
            }

            @Override
            public String toString() {
                return String.format("ML [%s] get earliest message publish time of pos",
                        ManagedLedgerImpl.this.name);
            }
        }, null);

        return future;
    }

    /**
     * Get estimated backlog size from a specific position.
     */
    public long getEstimatedBacklogSize(PositionImpl pos) {
        if (pos == null) {
            return 0;
        }
        return estimateBacklogFromPosition(pos);
    }

    long estimateBacklogFromPosition(PositionImpl pos) {
        synchronized (this) {
            long sizeBeforePosLedger = ledgers.headMap(pos.getLedgerId()).values()
                    .stream().mapToLong(LedgerInfo::getSize).sum();
            LedgerInfo ledgerInfo = ledgers.get(pos.getLedgerId());
            long sizeAfter = getTotalSize() - sizeBeforePosLedger;
            if (ledgerInfo == null) {
                return sizeAfter;
            } else if (pos.getLedgerId() == currentLedger.getId()) {
                return sizeAfter - consumedLedgerSize(currentLedgerSize, currentLedgerEntries, pos.getEntryId());
            } else {
                return sizeAfter - consumedLedgerSize(ledgerInfo.getSize(), ledgerInfo.getEntries(), pos.getEntryId());
            }
        }
    }

    private long consumedLedgerSize(long ledgerSize, long ledgerEntries, long consumedEntries) {
        if (ledgerEntries <= 0) {
            return 0;
        }
        if (ledgerEntries <= (consumedEntries + 1)) {
            return ledgerSize;
        } else {
            long averageSize = ledgerSize / ledgerEntries;
            return consumedEntries >= 0 ? (consumedEntries + 1) * averageSize : 0;
        }
    }

    public CompletableFuture<Position> asyncMigrate() {
        propertiesMap.put(MIGRATION_STATE_PROPERTY, Boolean.TRUE.toString());
        CompletableFuture<Position> result = new CompletableFuture<>();
        asyncTerminate(new TerminateCallback() {

            @Override
            public void terminateComplete(Position lastCommittedPosition, Object ctx) {
                migrated = true;
                log.info("[{}] topic successfully terminated and migrated at {}", name, lastCommittedPosition);
                result.complete(lastCommittedPosition);
            }

            @Override
            public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                log.info("[{}] topic failed to terminate and migrate ", name, exception);
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
    }

    @Override
    public CompletableFuture<Void> asyncAddLedgerProperty(long ledgerId, String key, String value) {
        if (state.isFenced()) {
            return CompletableFuture.failedFuture(new ManagedLedgerFencedException());
        }
        LedgerInfo li = ledgers.get(ledgerId);
        if (li == null) {
            return CompletableFuture.failedFuture(new ManagedLedgerNotFoundException("Ledger not found"));
        }

        CompletableFuture<Void> f = new CompletableFuture<>();
        transformLedgerInfo(ledgerId,
                oldInfo -> {
                    List<MLDataFormats.KeyValue> oldProperties = oldInfo.getPropertiesList();
                    Map<String, String> newPropertiesMap = new HashMap<>();
                    oldProperties.forEach(kv -> newPropertiesMap.put(kv.getKey(), kv.getValue()));
                    newPropertiesMap.put(key, value);
                    List<MLDataFormats.KeyValue> newProperties = newPropertiesMap.entrySet().stream()
                            .map(e -> MLDataFormats.KeyValue.newBuilder()
                                    .setKey(e.getKey()).setValue(e.getValue()).build()).toList();
                    return oldInfo.toBuilder().clearProperties().addAllProperties(newProperties).build();
                })
                .thenAccept(v -> f.complete(null))
                .exceptionally(t -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(t);
                    if (cause instanceof OffloadConflict) {
                        f.completeExceptionally(new ManagedLedgerNotFoundException(cause.getMessage()));
                    } else {
                        f.completeExceptionally(cause);
                    }
                    return null;
                });
        return f;
    }

    @Override
    public CompletableFuture<Void> asyncRemoveLedgerProperty(long ledgerId, String key) {
        if (state.isFenced()) {
            return CompletableFuture.failedFuture(new ManagedLedgerFencedException());
        }
        LedgerInfo li = ledgers.get(ledgerId);
        if (li == null) {
            return CompletableFuture.failedFuture(new ManagedLedgerNotFoundException("Ledger not found"));
        }

        CompletableFuture<Void> f = new CompletableFuture<>();
        transformLedgerInfo(ledgerId,
                oldInfo -> {
                    List<MLDataFormats.KeyValue> oldProperties = oldInfo.getPropertiesList();
                    Map<String, String> newPropertiesMap = new HashMap<>();
                    oldProperties.forEach(kv -> newPropertiesMap.put(kv.getKey(), kv.getValue()));
                    newPropertiesMap.remove(key);
                    List<MLDataFormats.KeyValue> newProperties = newPropertiesMap.entrySet().stream()
                            .map(e -> MLDataFormats.KeyValue.newBuilder()
                                    .setKey(e.getKey()).setValue(e.getValue()).build()).toList();
                    return oldInfo.toBuilder().clearProperties().addAllProperties(newProperties).build();
                })
                .thenAccept(v -> f.complete(null))
                .exceptionally(t -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(t);
                    if (cause instanceof OffloadConflict) {
                        f.completeExceptionally(new ManagedLedgerNotFoundException(cause.getMessage()));
                    } else {
                        f.completeExceptionally(cause);
                    }
                    return null;
                });
        return f;
    }

    @Override
    public CompletableFuture<String> asyncGetLedgerProperty(long ledgerId, String key) {
        if (state.isFenced()) {
            return FutureUtil.failedFuture(new ManagedLedgerFencedException());
        }
        LedgerInfo li = ledgers.get(ledgerId);
        if (li == null) {
            return FutureUtil.failedFuture(new ManagedLedgerNotFoundException("Ledger not found"));
        }
        if (li.getPropertiesCount() <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        for (MLDataFormats.KeyValue kv : li.getPropertiesList()) {
            if (kv.getKey().equals(key)) {
                return CompletableFuture.completedFuture(kv.getValue());
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized void asyncTerminate(TerminateCallback callback, Object ctx) {
        if (state.isFenced()) {
            callback.terminateFailed(new ManagedLedgerFencedException(), ctx);
            return;
        } else if (state == State.Terminated) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignoring request to terminate an already terminated managed ledger", name);
            }
            callback.terminateComplete(lastConfirmedEntry, ctx);
            return;
        }

        log.info("[{}] Terminating managed ledger", name);
        state = State.Terminated;

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
                callback.terminateFailed(createManagedLedgerException(rc), ctx);
            } else {
                lastConfirmedEntry = new PositionImpl(lh.getId(), lh.getLastAddConfirmed());
                // Store the new state in metadata
                store.asyncUpdateLedgerIds(name, getManagedLedgerInfo(), ledgersStat, new MetaStoreCallback<Void>() {
                    @Override
                    public void operationComplete(Void result, Stat stat) {
                        ledgersStat = stat;
                        log.info("[{}] Terminated managed ledger at {}", name, lastConfirmedEntry);
                        callback.terminateComplete(lastConfirmedEntry, ctx);
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        log.error("[{}] Failed to terminate managed ledger: {}", name, e.getMessage());
                        handleBadVersion(e);
                        callback.terminateFailed(new ManagedLedgerException(e), ctx);
                    }
                });
            }
        }, null);
    }

    @Override
    public Position terminate() throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            Position lastPosition = null;
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncTerminate(new TerminateCallback() {
            @Override
            public void terminateComplete(Position lastPosition, Object ctx) {
                result.lastPosition = lastPosition;
                counter.countDown();
            }

            @Override
            public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during managed ledger terminate");
        }

        if (result.exception != null) {
            log.error("[{}] Error terminating managed ledger", name, result.exception);
            throw result.exception;
        }

        return result.lastPosition;
    }

    @Override
    public boolean isTerminated() {
        return state == State.Terminated;
    }

    @Override
    public boolean isMigrated() {
        return migrated;
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
        if (state.isFenced()) {
            cancelScheduledTasks();
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
        cancelScheduledTasks();

        LedgerHandle lh = currentLedger;

        if (lh == null) {
            // No ledger to close, proceed with next step
            closeAllCursors(callback, ctx);
            return;
        }

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
                callback.closeFailed(createManagedLedgerException(rc), ctx);
                return;
            }

            ledgerCache.forEach((ledgerId, readHandle) -> {
                invalidateReadHandle(ledgerId);
            });

            closeAllCursors(callback, ctx);
        }, null);


    }

    private void closeAllCursors(CloseCallback callback, final Object ctx) {
        // Close all cursors in parallel
        List<CompletableFuture<Void>> futures = new ArrayList();
        for (ManagedCursor cursor : cursors) {
            Futures.CloseFuture closeFuture = new Futures.CloseFuture();
            cursor.asyncClose(closeFuture, null);
            futures.add(closeFuture);
        }

        Futures.waitForAll(futures)
                .thenRun(() -> callback.closeComplete(ctx))
                .exceptionally(exception -> {
            callback.closeFailed(ManagedLedgerException.getManagedLedgerException(exception.getCause()), ctx);
            return null;
        });
    }

    // //////////////////////////////////////////////////////////////////////
    // Callbacks

    @Override
    public synchronized void createComplete(int rc, final LedgerHandle lh, Object ctx) {
        if (STATE_UPDATER.get(this) == State.Closed) {
            if (lh != null) {
                log.warn("[{}] ledger create completed after the managed ledger is closed rc={} ledger={}, so just"
                        + " close this ledger handle.", name, rc, lh != null ? lh.getId() : -1);
                lh.closeAsync();
            }
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] createComplete rc={} ledger={}", name, rc, lh != null ? lh.getId() : -1);
        }

        if (checkAndCompleteLedgerOpTask(rc, lh, ctx)) {
            return;
        }

        mbean.endDataLedgerCreateOp();
        if (rc != BKException.Code.OK) {
            log.error("[{}] Error creating ledger rc={} {}", name, rc, BKException.getMessage(rc));
            ManagedLedgerException status = createManagedLedgerException(rc);

            // no pending entries means that creating this new ledger is NOT caused by write failure
            if (pendingAddEntries.isEmpty()) {
                STATE_UPDATER.set(this, State.ClosedLedger);
            } else {
                STATE_UPDATER.set(this, State.WriteFailed);
            }

            // Empty the list of pending requests and make all of them fail
            clearPendingAddEntries(status);
            lastLedgerCreationFailureTimestamp = clock.millis();
        } else {
            log.info("[{}] Created new ledger {}", name, lh.getId());
            LedgerInfo newLedger = LedgerInfo.newBuilder().setLedgerId(lh.getId()).setTimestamp(0).build();
            final MetaStoreCallback<Void> cb = new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void v, Stat stat) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Updating of ledgers list after create complete. version={}", name, stat);
                    }
                    ledgersStat = stat;
                    synchronized (ManagedLedgerImpl.this) {
                        LedgerHandle originalCurrentLedger = currentLedger;
                        ledgers.put(lh.getId(), newLedger);
                        currentLedger = lh;
                        currentLedgerTimeoutTriggered = new AtomicBoolean();
                        currentLedgerEntries = 0;
                        currentLedgerSize = 0;
                        updateLedgersIdsComplete(originalCurrentLedger);
                        mbean.addLedgerSwitchLatencySample(System.currentTimeMillis()
                                - lastLedgerCreationInitiationTimestamp, TimeUnit.MILLISECONDS);
                    }
                    metadataMutex.unlock();

                    // May need to update the cursor position
                    maybeUpdateCursorBeforeTrimmingConsumedLedger();
                }

                @Override
                public void operationFailed(MetaStoreException e) {
                    log.warn("[{}] Error updating meta data with the new list of ledgers: {}", name, e.getMessage());
                    handleBadVersion(e);
                    mbean.startDataLedgerDeleteOp();
                    bookKeeper.asyncDeleteLedger(lh.getId(), (rc1, ctx1) -> {
                        mbean.endDataLedgerDeleteOp();
                        if (rc1 != BKException.Code.OK) {
                            log.warn("[{}] Failed to delete ledger {}: {}", name, lh.getId(),
                                    BKException.getMessage(rc1));
                        }
                    }, null);
                    if (e instanceof BadVersionException) {
                        synchronized (ManagedLedgerImpl.this) {
                            log.error(
                                "[{}] Failed to update ledger list. z-node version mismatch. Closing managed ledger",
                                name);
                            lastLedgerCreationFailureTimestamp = clock.millis();
                            // Return ManagedLedgerFencedException to addFailed callback
                            // to indicate that the ledger is now fenced and topic needs to be closed
                            clearPendingAddEntries(new ManagedLedgerFencedException(e));
                            // Do not need to unlock metadataMutex here because we are going to close to topic
                            // anyways
                            return;
                        }
                    }

                    metadataMutex.unlock();

                    synchronized (ManagedLedgerImpl.this) {
                        lastLedgerCreationFailureTimestamp = clock.millis();
                        STATE_UPDATER.set(ManagedLedgerImpl.this, State.ClosedLedger);
                        clearPendingAddEntries(e);
                    }
                }
            };

            updateLedgersListAfterRollover(cb, newLedger);
        }
    }

    protected void handleBadVersion(Throwable e) {
        if (e instanceof BadVersionException) {
            setFenced();
        }
    }
    private void updateLedgersListAfterRollover(MetaStoreCallback<Void> callback, LedgerInfo newLedger) {
        if (!metadataMutex.tryLock()) {
            // Defer update for later
            scheduledExecutor.schedule(() -> updateLedgersListAfterRollover(callback, newLedger),
                    100, TimeUnit.MILLISECONDS);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Updating ledgers ids with new ledger. version={}", name, ledgersStat);
        }
        ManagedLedgerInfo mlInfo = getManagedLedgerInfo(newLedger);
        store.asyncUpdateLedgerIds(name, mlInfo, ledgersStat, callback);
    }

    @VisibleForTesting
    void createNewOpAddEntryForNewLedger() {
        // Avoid use same OpAddEntry between different ledger handle
        int pendingSize = pendingAddEntries.size();
        OpAddEntry existsOp;
        do {
            existsOp = pendingAddEntries.poll();
            if (existsOp != null) {
                // If op is used by another ledger handle, we need to close it and create a new one
                if (existsOp.ledger != null) {
                    existsOp = existsOp.duplicateAndClose(currentLedgerTimeoutTriggered);
                } else {
                    // It may happen when the following operations execute at the same time, so it is expected.
                    // - Adding entry.
                    // - Switching ledger.
                    existsOp.setTimeoutTriggered(currentLedgerTimeoutTriggered);
                }
                existsOp.setLedger(currentLedger);
                pendingAddEntries.add(existsOp);
            }
        } while (existsOp != null && --pendingSize > 0);
    }

    protected synchronized void updateLedgersIdsComplete(@Nullable LedgerHandle originalCurrentLedger) {
        STATE_UPDATER.set(this, State.LedgerOpened);
        // Delete original "currentLedger" if it has been removed from "ledgers".
        if (originalCurrentLedger != null && !ledgers.containsKey(originalCurrentLedger.getId())){
            bookKeeper.asyncDeleteLedger(originalCurrentLedger.getId(), (rc, ctx) -> {
                mbean.endDataLedgerDeleteOp();
                log.info("[{}] Delete complete for empty ledger {}. rc={}", name, originalCurrentLedger.getId(), rc);
            }, null);
        }
        updateLastLedgerCreatedTimeAndScheduleRolloverTask();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Resending {} pending messages", name, pendingAddEntries.size());
        }

        createNewOpAddEntryForNewLedger();

        // Process all the pending addEntry requests
        for (OpAddEntry op : pendingAddEntries) {
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
        LedgerHandle currentLedger = this.currentLedger;
        if (currentLedger == lh && (state == State.ClosingLedger || state == State.LedgerOpened)) {
            STATE_UPDATER.set(this, State.ClosedLedger);
        } else if (state == State.Closed) {
            // The managed ledger was closed during the write operation
            clearPendingAddEntries(new ManagedLedgerAlreadyClosedException("Managed ledger was already closed"));
            return;
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
                    .setSize(lh.getLength()).setTimestamp(clock.millis()).build();
            ledgers.put(lh.getId(), info);
        } else {
            // The last ledger was empty, so we can discard it
            ledgers.remove(lh.getId());
            mbean.startDataLedgerDeleteOp();
        }

        trimConsumedLedgersInBackground();

        maybeOffloadInBackground(NULL_OFFLOAD_PROMISE);

        createLedgerAfterClosed();
    }

    @Override
    public void skipNonRecoverableLedger(long ledgerId){
        for (ManagedCursor managedCursor : cursors) {
            managedCursor.skipNonRecoverableLedger(ledgerId);
        }
    }

    synchronized void createLedgerAfterClosed() {
        if (isNeededCreateNewLedgerAfterCloseLedger()) {
            log.info("[{}] Creating a new ledger after closed {}", name,
                    currentLedger == null ? "null" : currentLedger.getId());
            STATE_UPDATER.set(this, State.CreatingLedger);
            this.lastLedgerCreationInitiationTimestamp = System.currentTimeMillis();
            mbean.startDataLedgerCreateOp();
            // Use the executor here is to avoid use the Zookeeper thread to create the ledger which will lead
            // to deadlock at the zookeeper client, details to see https://github.com/apache/pulsar/issues/13736
            this.executor.execute(() ->
                    asyncCreateLedger(bookKeeper, config, digestType, this, Collections.emptyMap()));
        }
    }

    boolean isNeededCreateNewLedgerAfterCloseLedger() {
        final State state = STATE_UPDATER.get(this);
        if (state != State.CreatingLedger && state != State.LedgerOpened) {
            return true;
        }
        return false;
    }

    @VisibleForTesting
    @Override
    public void rollCurrentLedgerIfFull() {
        log.info("[{}] Start checking if current ledger is full", name);
        if (currentLedgerEntries > 0 && currentLedgerIsFull()
                && STATE_UPDATER.compareAndSet(this, State.LedgerOpened, State.ClosingLedger)) {
            currentLedger.asyncClose(new AsyncCallback.CloseCallback() {
                @Override
                public void closeComplete(int rc, LedgerHandle lh, Object o) {
                    checkArgument(currentLedger.getId() == lh.getId(), "ledgerId %s doesn't match with "
                                  + "acked ledgerId %s", currentLedger.getId(), lh.getId());

                    if (rc == BKException.Code.OK) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Successfully closed ledger {}, trigger by rollover full ledger",
                                    name, lh.getId());
                        }
                    } else {
                        log.warn("[{}] Error when closing ledger {}, trigger by rollover full ledger, Status={}",
                                name, lh.getId(), BKException.getMessage(rc));
                    }

                    ledgerClosed(lh);
                }
            }, null);
        }
    }

    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {

        CompletableFuture<Position> future = new CompletableFuture<>();
        Long firstLedgerId = ledgers.firstKey();
        final PositionImpl startPosition = firstLedgerId == null ? null : new PositionImpl(firstLedgerId, 0);
        if (startPosition == null) {
            future.complete(null);
            return future;
        }
        AsyncCallbacks.FindEntryCallback findEntryCallback = new AsyncCallbacks.FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                final Position finalPosition;
                if (position == null) {
                    finalPosition = startPosition;
                    log.info("[{}] Unable to find position for predicate {}. Use the first position {} instead.", name,
                            predicate, startPosition);
                } else {
                    finalPosition = getNextValidPosition((PositionImpl) position);
                }
                future.complete(finalPosition);
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition,
                                        Object ctx) {
                log.warn("[{}] Unable to find position for predicate {}.", name, predicate);
                future.complete(null);
            }
        };
        long max = getNumberOfEntries() - 1;
        OpFindNewest op = new OpFindNewest(this, startPosition, predicate, max, findEntryCallback, null);
        op.find();
        return future;
    }

    @Override
    public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
        return managedLedgerInterceptor;
    }

    void clearPendingAddEntries(ManagedLedgerException e) {
        while (!pendingAddEntries.isEmpty()) {
            OpAddEntry op = pendingAddEntries.poll();
            op.failed(e);
        }
    }

    void asyncReadEntries(OpReadEntry opReadEntry) {
        final State state = STATE_UPDATER.get(this);
        if (state.isFenced() || state == State.Closed) {
            opReadEntry.readEntriesFailed(new ManagedLedgerFencedException(), opReadEntry.ctx);
            return;
        }

        long ledgerId = opReadEntry.readPosition.getLedgerId();

        LedgerHandle currentLedger = this.currentLedger;

        if (currentLedger != null && ledgerId == currentLedger.getId()) {
            // Current writing ledger is not in the cache (since we don't want
            // it to be automatically evicted), and we cannot use 2 different
            // ledger handles (read & write)for the same ledger.
            internalReadFromLedger(currentLedger, opReadEntry);
        } else {
            LedgerInfo ledgerInfo = ledgers.get(ledgerId);
            if (ledgerInfo == null || ledgerInfo.getEntries() == 0) {
                // Cursor is pointing to an empty ledger, there's no need to try opening it. Skip this ledger and
                // move to the next one
                opReadEntry.updateReadPosition(new PositionImpl(opReadEntry.readPosition.getLedgerId() + 1, 0));
                opReadEntry.checkReadCompletion();
                return;
            }

            // Get a ledger handle to read from
            getLedgerHandle(ledgerId).thenAccept(ledger -> internalReadFromLedger(ledger, opReadEntry)).exceptionally(ex
                    -> {
                log.error("[{}] Error opening ledger for reading at position {} - {}", name, opReadEntry.readPosition,
                        ex.getMessage());
                opReadEntry.readEntriesFailed(ManagedLedgerException.getManagedLedgerException(ex.getCause()),
                        opReadEntry.ctx);
                return null;
            });
        }
    }

    public CompletableFuture<String> getLedgerMetadata(long ledgerId) {
        LedgerHandle currentLedger = this.currentLedger;
        if (currentLedger != null && ledgerId == currentLedger.getId()) {
            return CompletableFuture.completedFuture(currentLedger.getLedgerMetadata().toSafeString());
        } else {
            return getLedgerHandle(ledgerId).thenApply(rh -> rh.getLedgerMetadata().toSafeString());
        }
    }

    @Override
    public CompletableFuture<LedgerInfo> getLedgerInfo(long ledgerId) {
        CompletableFuture<LedgerInfo> result = new CompletableFuture<>();
        final LedgerInfo ledgerInfo = ledgers.get(ledgerId);
        result.complete(ledgerInfo);
        return result;
    }

    @Override
    public Optional<LedgerInfo> getOptionalLedgerInfo(long ledgerId) {
        return Optional.ofNullable(ledgers.get(ledgerId));
    }

    CompletableFuture<ReadHandle> getLedgerHandle(long ledgerId) {
        CompletableFuture<ReadHandle> ledgerHandle = ledgerCache.get(ledgerId);
        if (ledgerHandle != null) {
            return ledgerHandle;
        }

        // If not present try again and create if necessary
        return ledgerCache.computeIfAbsent(ledgerId, lid -> {
            // Open the ledger for reading if it was not already opened
            if (log.isDebugEnabled()) {
                log.debug("[{}] Asynchronously opening ledger {} for read", name, ledgerId);
            }
            mbean.startDataLedgerOpenOp();

            CompletableFuture<ReadHandle> promise = new CompletableFuture<>();

            LedgerInfo info = ledgers.get(ledgerId);
            CompletableFuture<ReadHandle> openFuture;

            if (config.getLedgerOffloader() != null
                    && config.getLedgerOffloader().getOffloadPolicies() != null
                    && config.getLedgerOffloader().getOffloadPolicies()
                    .getManagedLedgerOffloadedReadPriority() == OffloadedReadPriority.BOOKKEEPER_FIRST
                    && info != null && info.hasOffloadContext()
                    && !info.getOffloadContext().getBookkeeperDeleted()) {
                openFuture = bookKeeper.newOpenLedgerOp().withRecovery(!isReadOnly()).withLedgerId(ledgerId)
                        .withDigestType(config.getDigestType()).withPassword(config.getPassword()).execute();

            } else if (info != null && info.hasOffloadContext() && info.getOffloadContext().getComplete()) {

                UUID uid = new UUID(info.getOffloadContext().getUidMsb(), info.getOffloadContext().getUidLsb());
                // TODO: improve this to load ledger offloader by driver name recorded in metadata
                Map<String, String> offloadDriverMetadata = OffloadUtils.getOffloadDriverMetadata(info);
                offloadDriverMetadata.put("ManagedLedgerName", name);
                log.info("[{}] Opening ledger {} from offload driver {} with uid {}", name, ledgerId,
                        config.getLedgerOffloader().getOffloadDriverName(), uid);
                openFuture = config.getLedgerOffloader().readOffloaded(ledgerId, uid,
                        offloadDriverMetadata);
            } else {
                openFuture = bookKeeper.newOpenLedgerOp().withRecovery(!isReadOnly()).withLedgerId(ledgerId)
                        .withDigestType(config.getDigestType()).withPassword(config.getPassword()).execute();
            }
            openFuture.whenCompleteAsync((res, ex) -> {
                mbean.endDataLedgerOpenOp();
                if (ex != null) {
                    ledgerCache.remove(ledgerId, promise);
                    promise.completeExceptionally(createManagedLedgerException(ex));
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Successfully opened ledger {} for reading", name, ledgerId);
                    }
                    promise.complete(res);
                }
            }, executor);
            return promise;
        });
    }

    void invalidateReadHandle(long ledgerId) {
        CompletableFuture<ReadHandle> rhf = ledgerCache.remove(ledgerId);
        if (rhf != null) {
            rhf.thenCompose(r -> {
                if (r instanceof OffloadedLedgerHandle) {
                    log.info("[{}] Closing ledger {} from offload driver {}", name, ledgerId,
                            config.getLedgerOffloader().getOffloadDriverName());
                }
                return r.closeAsync().exceptionally(ex -> {
                    log.warn("[{}] Failed to close ledger {} ReadHandle with type {}", name, ledgerId,
                            r.getClass().getName(), ex);
                    return null;
                });
            }).exceptionally(ex -> {
                log.warn("[{}] Failed to close Ledger ReadHandle {}:", name, ledgerId, ex);
                return null;
            });
        }
    }

    public void invalidateLedgerHandle(ReadHandle ledgerHandle) {
        long ledgerId = ledgerHandle.getId();
        LedgerHandle currentLedger = this.currentLedger;

        if (currentLedger != null && ledgerId != currentLedger.getId()) {
            // remove handle from ledger cache since we got a (read) error
            ledgerCache.remove(ledgerId);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Removed ledger read handle {} from cache", name, ledgerId);
            }
            ledgerHandle.closeAsync()
                    .exceptionally(ex -> {
                        log.warn("[{}] Failed to close a Ledger ReadHandle:", name, ex);
                        return null;
                    });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ledger that encountered read error is current ledger", name);
            }
        }
    }

    public void asyncReadEntry(PositionImpl position, ReadEntryCallback callback, Object ctx) {
        LedgerHandle currentLedger = this.currentLedger;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entry ledger {}: {}", name, position.getLedgerId(), position.getEntryId());
        }
        if (position.getLedgerId() == currentLedger.getId()) {
            asyncReadEntry(currentLedger, position, callback, ctx);
        } else if (ledgers.containsKey(position.getLedgerId())) {
            getLedgerHandle(position.getLedgerId()).thenAccept(ledger -> asyncReadEntry(ledger, position, callback,
                    ctx)).exceptionally(ex -> {
                log.error("[{}] Error opening ledger for reading at position {} - {}", name, position, ex.getMessage());
                callback.readEntryFailed(ManagedLedgerException.getManagedLedgerException(ex.getCause()), ctx);
                return null;
            });
        } else {
            log.error("[{}] Failed to get message with ledger {}:{} the ledgerId does not belong to this topic "
                    + "or has been deleted.", name, position.getLedgerId(), position.getEntryId());
            callback.readEntryFailed(new LedgerNotExistException("Message not found, "
                    + "the ledgerId does not belong to this topic or has been deleted"), ctx);
        }

    }

    private void internalReadFromLedger(ReadHandle ledger, OpReadEntry opReadEntry) {

        if (opReadEntry.readPosition.compareTo(opReadEntry.maxPosition) > 0) {
            opReadEntry.checkReadCompletion();
            return;
        }
        // Perform the read
        long firstEntry = opReadEntry.readPosition.getEntryId();
        long lastEntryInLedger;

        PositionImpl lastPosition = lastConfirmedEntry;

        if (ledger.getId() == lastPosition.getLedgerId()) {
            // For the current ledger, we only give read visibility to the last entry we have received a confirmation in
            // the managed ledger layer
            lastEntryInLedger = lastPosition.getEntryId();
        } else {
            // For other ledgers, already closed the BK lastAddConfirmed is appropriate
            lastEntryInLedger = ledger.getLastAddConfirmed();
        }

        // can read max position entryId
        if (ledger.getId() == opReadEntry.maxPosition.getLedgerId()) {
            lastEntryInLedger = min(opReadEntry.maxPosition.getEntryId(), lastEntryInLedger);
        }

        if (firstEntry > lastEntryInLedger) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No more messages to read from ledger={} lastEntry={} readEntry={}", name,
                        ledger.getId(), lastEntryInLedger, firstEntry);
            }

            if (currentLedger == null || ledger.getId() != currentLedger.getId()) {
                // Cursor was placed past the end of one ledger, move it to the
                // beginning of the next ledger
                Long nextLedgerId = ledgers.ceilingKey(ledger.getId() + 1);
                if (nextLedgerId != null) {
                    opReadEntry.updateReadPosition(new PositionImpl(nextLedgerId, 0));
                } else {
                    opReadEntry.updateReadPosition(new PositionImpl(ledger.getId() + 1, 0));
                }
            } else {
                opReadEntry.updateReadPosition(opReadEntry.readPosition);
            }

            opReadEntry.checkReadCompletion();
            return;
        }

        long lastEntry = min(firstEntry + opReadEntry.getNumberOfEntriesToRead() - 1, lastEntryInLedger);

        // Filer out and skip unnecessary read entry
        if (opReadEntry.skipCondition != null) {
            long firstValidEntry = -1L;
            long lastValidEntry = -1L;
            long entryId = firstEntry;
            for (; entryId <= lastEntry; entryId++) {
                if (opReadEntry.skipCondition.test(PositionImpl.get(ledger.getId(), entryId))) {
                    if (firstValidEntry != -1L) {
                        break;
                    }
                } else {
                    if (firstValidEntry == -1L) {
                        firstValidEntry = entryId;
                    }

                    lastValidEntry = entryId;
                }
            }

            // If all messages in [firstEntry...lastEntry] are filter out,
            // then manual call internalReadEntriesComplete to advance read position.
            if (firstValidEntry == -1L) {
                final var nextReadPosition = new PositionImpl(ledger.getId(), lastEntry).getNext();
                opReadEntry.updateReadPosition(nextReadPosition);
                opReadEntry.checkReadCompletion();
                return;
            }

            firstEntry = firstValidEntry;
            lastEntry = lastValidEntry;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entries from ledger {} - first={} last={}", name, ledger.getId(), firstEntry,
                    lastEntry);
        }
        asyncReadEntry(ledger, firstEntry, lastEntry, opReadEntry, opReadEntry.ctx);
    }

    protected void asyncReadEntry(ReadHandle ledger, PositionImpl position, ReadEntryCallback callback, Object ctx) {
        mbean.addEntriesRead(1);
        if (config.getReadEntryTimeoutSeconds() > 0) {
            // set readOpCount to uniquely validate if ReadEntryCallbackWrapper is already recycled
            long readOpCount = READ_OP_COUNT_UPDATER.incrementAndGet(this);
            long createdTime = System.nanoTime();
            ReadEntryCallbackWrapper readCallback = ReadEntryCallbackWrapper.create(name, position.getLedgerId(),
                    position.getEntryId(), callback, readOpCount, createdTime, ctx);
            lastReadCallback = readCallback;
            entryCache.asyncReadEntry(ledger, position, readCallback, readOpCount);
        } else {
            entryCache.asyncReadEntry(ledger, position, callback, ctx);
        }
    }

    protected void asyncReadEntry(ReadHandle ledger, long firstEntry, long lastEntry, OpReadEntry opReadEntry,
            Object ctx) {
        if (config.getReadEntryTimeoutSeconds() > 0) {
            // set readOpCount to uniquely validate if ReadEntryCallbackWrapper is already recycled
            long readOpCount = READ_OP_COUNT_UPDATER.incrementAndGet(this);
            long createdTime = System.nanoTime();
            ReadEntryCallbackWrapper readCallback = ReadEntryCallbackWrapper.create(name, ledger.getId(), firstEntry,
                    opReadEntry, readOpCount, createdTime, ctx);
            lastReadCallback = readCallback;
            entryCache.asyncReadEntry(ledger, firstEntry, lastEntry, opReadEntry.cursor.isCacheReadEntry(),
                    readCallback, readOpCount);
        } else {
            entryCache.asyncReadEntry(ledger, firstEntry, lastEntry, opReadEntry.cursor.isCacheReadEntry(), opReadEntry,
                    ctx);
        }
    }

    static final class ReadEntryCallbackWrapper implements ReadEntryCallback, ReadEntriesCallback {

        volatile ReadEntryCallback readEntryCallback;
        volatile ReadEntriesCallback readEntriesCallback;
        String name;
        long ledgerId;
        long entryId;
        volatile long readOpCount = -1;
        private static final AtomicLongFieldUpdater<ReadEntryCallbackWrapper> READ_OP_COUNT_UPDATER =
                AtomicLongFieldUpdater.newUpdater(ReadEntryCallbackWrapper.class, "readOpCount");
        volatile long createdTime = -1;
        volatile Object cntx;

        final Handle<ReadEntryCallbackWrapper> recyclerHandle;

        private ReadEntryCallbackWrapper(Handle<ReadEntryCallbackWrapper> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static ReadEntryCallbackWrapper create(String name, long ledgerId, long entryId, ReadEntryCallback callback,
                long readOpCount, long createdTime, Object ctx) {
            ReadEntryCallbackWrapper readCallback = RECYCLER.get();
            readCallback.name = name;
            readCallback.ledgerId = ledgerId;
            readCallback.entryId = entryId;
            readCallback.readEntryCallback = callback;
            readCallback.cntx = ctx;
            readCallback.readOpCount = readOpCount;
            readCallback.createdTime = createdTime;
            return readCallback;
        }

        static ReadEntryCallbackWrapper create(String name, long ledgerId, long entryId, ReadEntriesCallback callback,
                long readOpCount, long createdTime, Object ctx) {
            ReadEntryCallbackWrapper readCallback = RECYCLER.get();
            readCallback.name = name;
            readCallback.ledgerId = ledgerId;
            readCallback.entryId = entryId;
            readCallback.readEntriesCallback = callback;
            readCallback.cntx = ctx;
            readCallback.readOpCount = readOpCount;
            readCallback.createdTime = createdTime;
            return readCallback;
        }

        @Override
        public void readEntryComplete(Entry entry, Object ctx) {
            long reOpCount = reOpCount(ctx);
            ReadEntryCallback callback = this.readEntryCallback;
            Object cbCtx = this.cntx;
            if (recycle(reOpCount)) {
                callback.readEntryComplete(entry, cbCtx);
                return;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] read entry already completed for {}-{}", name, ledgerId, entryId);
                }
                entry.release();
                return;
            }
        }

        @Override
        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
            long reOpCount = reOpCount(ctx);
            ReadEntryCallback callback = this.readEntryCallback;
            Object cbCtx = this.cntx;
            if (recycle(reOpCount)) {
                callback.readEntryFailed(exception, cbCtx);
                return;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] read entry already completed for {}-{}", name, ledgerId, entryId);
                }
            }
        }

        @Override
        public void readEntriesComplete(List<Entry> returnedEntries, Object ctx) {
            long reOpCount = reOpCount(ctx);
            ReadEntriesCallback callback = this.readEntriesCallback;
            Object cbCtx = this.cntx;
            if (recycle(reOpCount)) {
                callback.readEntriesComplete(returnedEntries, cbCtx);
                return;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] read entry already completed for {}-{}", name, ledgerId, entryId);
                }
                returnedEntries.forEach(Entry::release);
                return;
            }
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            long reOpCount = reOpCount(ctx);
            ReadEntriesCallback callback = this.readEntriesCallback;
            Object cbCtx = this.cntx;
            if (recycle(reOpCount)) {
                callback.readEntriesFailed(exception, cbCtx);
                return;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] read entry already completed for {}-{}", name, ledgerId, entryId);
                }
                return;
            }
        }

        private long reOpCount(Object ctx) {
            return (ctx instanceof Long) ? (long) ctx : -1;
        }

        public void readFailed(ManagedLedgerException exception, Object ctx) {
            if (readEntryCallback != null) {
                readEntryFailed(exception, ctx);
            } else if (readEntriesCallback != null) {
                readEntriesFailed(exception, ctx);
            }
            // It happens when timeout-thread and read-callback both recycles at the same time.
            // this read-callback has already been recycled so, do nothing..
        }

        private boolean recycle(long readOpCount) {
            if (readOpCount != -1
                    && READ_OP_COUNT_UPDATER.compareAndSet(ReadEntryCallbackWrapper.this, readOpCount, -1)) {
                createdTime = -1;
                readEntryCallback = null;
                readEntriesCallback = null;
                ledgerId = -1;
                entryId = -1;
                name = null;
                recyclerHandle.recycle(this);
                return true;
            }
            return false;
        }

        private static final Recycler<ReadEntryCallbackWrapper> RECYCLER = new Recycler<ReadEntryCallbackWrapper>() {
            @Override
            protected ReadEntryCallbackWrapper newObject(Handle<ReadEntryCallbackWrapper> handle) {
                return new ReadEntryCallbackWrapper(handle);
            }
        };

    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return mbean;
    }

    public boolean hasMoreEntries(PositionImpl position) {
        PositionImpl lastPos = lastConfirmedEntry;
        boolean result = position.compareTo(lastPos) <= 0;
        if (log.isDebugEnabled()) {
            log.debug("[{}] hasMoreEntries: pos={} lastPos={} res={}", name, position, lastPos, result);
        }
        return result;
    }

    void doCacheEviction(long maxTimestamp) {
        if (entryCache.getSize() > 0) {
            entryCache.invalidateEntriesBeforeTimestamp(maxTimestamp);
        }
    }

    // slowest reader position is earliest mark delete position when cacheEvictionByMarkDeletedPosition=true
    // it is the earliest read position when cacheEvictionByMarkDeletedPosition=false
    private void invalidateEntriesUpToSlowestReaderPosition() {
        if (entryCache.getSize() <= 0) {
            return;
        }
        if (!activeCursors.isEmpty()) {
            PositionImpl evictionPos = activeCursors.getSlowestReaderPosition();
            if (evictionPos != null) {
                entryCache.invalidateEntries(evictionPos);
            }
        } else {
            entryCache.clear();
        }
    }

    void onCursorMarkDeletePositionUpdated(ManagedCursorImpl cursor, PositionImpl newPosition) {
        if (config.isCacheEvictionByMarkDeletedPosition()) {
            updateActiveCursor(cursor, newPosition);
        }
        if (!cursor.isDurable()) {
            // non-durable cursors aren't tracked for trimming
            return;
        }
        Pair<PositionImpl, PositionImpl> pair = cursors.cursorUpdated(cursor, newPosition);
        if (pair == null) {
            // Cursor has been removed in the meantime
            trimConsumedLedgersInBackground();
            return;
        }

        PositionImpl previousSlowestReader = pair.getLeft();
        PositionImpl currentSlowestReader = pair.getRight();

        if (previousSlowestReader.compareTo(currentSlowestReader) == 0) {
            // The slowest consumer has not changed position. Nothing to do right now
            return;
        }

        // Only trigger a trimming when switching to the next ledger
        if (previousSlowestReader.getLedgerId() != newPosition.getLedgerId()) {
            trimConsumedLedgersInBackground();
        }
    }

    private void updateActiveCursor(ManagedCursorImpl cursor, Position newPosition) {
        Pair<PositionImpl, PositionImpl> slowestPositions = activeCursors.cursorUpdated(cursor, newPosition);
        if (slowestPositions != null
                && !slowestPositions.getLeft().equals(slowestPositions.getRight())) {
            invalidateEntriesUpToSlowestReaderPosition();
        }
    }

    public void onCursorReadPositionUpdated(ManagedCursorImpl cursor, Position newReadPosition) {
        if (!config.isCacheEvictionByMarkDeletedPosition()) {
            updateActiveCursor(cursor, newReadPosition);
        }
    }

    PositionImpl startReadOperationOnLedger(PositionImpl position) {
        Long ledgerId = ledgers.ceilingKey(position.getLedgerId());
        if (ledgerId != null && ledgerId != position.getLedgerId()) {
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

            executor.execute(waitingCursor::notifyEntriesAvailable);
        }
    }

    void notifyWaitingEntryCallBacks() {
        while (true) {
            final WaitingEntryCallBack cb = waitingEntryCallBacks.poll();
            if (cb == null) {
                break;
            }

            executor.execute(cb::entriesAvailable);
        }
    }

    public void addWaitingEntryCallBack(WaitingEntryCallBack cb) {
        this.waitingEntryCallBacks.add(cb);
    }

    public void maybeUpdateCursorBeforeTrimmingConsumedLedger() {
        for (ManagedCursor cursor : cursors) {
            PositionImpl lastAckedPosition = (PositionImpl) cursor.getMarkDeletedPosition();
            LedgerInfo currPointedLedger = ledgers.get(lastAckedPosition.getLedgerId());
            LedgerInfo nextPointedLedger = Optional.ofNullable(ledgers.higherEntry(lastAckedPosition.getLedgerId()))
                    .map(Map.Entry::getValue).orElse(null);

            if (currPointedLedger != null) {
                if (nextPointedLedger != null) {
                    if (lastAckedPosition.getEntryId() != -1
                            && lastAckedPosition.getEntryId() + 1 >= currPointedLedger.getEntries()) {
                        lastAckedPosition = new PositionImpl(nextPointedLedger.getLedgerId(), -1);
                    }
                } else {
                    log.debug("No need to reset cursor: {}, current ledger is the last ledger.", cursor);
                }
            } else {
                log.warn("Cursor: {} does not exist in the managed-ledger.", cursor);
            }

            if (!lastAckedPosition.equals(cursor.getMarkDeletedPosition())) {
                try {
                    log.info("Reset cursor:{} to {} since ledger consumed completely", cursor, lastAckedPosition);
                    onCursorMarkDeletePositionUpdated((ManagedCursorImpl) cursor, lastAckedPosition);
                } catch (Exception e) {
                    log.warn("Failed to reset cursor: {} from {} to {}. Trimming thread will retry next time.",
                            cursor, cursor.getMarkDeletedPosition(), lastAckedPosition);
                    log.warn("Caused by", e);
                }
            }
        }
    }

    private void trimConsumedLedgersInBackground() {
        asyncTrimConsumedLedgers();
    }

    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {
        CompletableFuture<List<LedgerInfo>> future = new CompletableFuture<>();
        executor.execute(() -> internalTrimConsumedLedgers(future));
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                promise.completeExceptionally(ex);
            } else {
                promise.complete(null);
            }
        });
    }

    @Override
    public CompletableFuture<List<LedgerInfo>> asyncTrimConsumedLedgers() {
        CompletableFuture<List<LedgerInfo>> future = new CompletableFuture<>();
        executor.execute(() -> internalTrimConsumedLedgers(future));
        return future;
    }

    public void trimConsumedLedgersInBackground(boolean isTruncate, CompletableFuture<List<LedgerInfo>> promise) {
        executor.execute(() -> internalTrimLedgers(isTruncate, promise));
    }

    private void scheduleDeferredTrimming(boolean isTruncate, CompletableFuture<List<LedgerInfo>> promise) {
        scheduledExecutor.schedule(() -> trimConsumedLedgersInBackground(isTruncate, promise),
                100, TimeUnit.MILLISECONDS);
    }

    public void maybeOffloadInBackground(CompletableFuture<PositionImpl> promise) {
        if (getOffloadPoliciesIfAppendable().isEmpty()) {
            return;
        }

        final OffloadPoliciesImpl policies = config.getLedgerOffloader().getOffloadPolicies();
        final long offloadThresholdInBytes =
                Optional.ofNullable(policies.getManagedLedgerOffloadThresholdInBytes()).orElse(-1L);
        final long offloadThresholdInSeconds =
                Optional.ofNullable(policies.getManagedLedgerOffloadThresholdInSeconds()).orElse(-1L);
        if (offloadThresholdInBytes >= 0 || offloadThresholdInSeconds >= 0) {
            executor.execute(() -> maybeOffload(offloadThresholdInBytes, offloadThresholdInSeconds, promise));
        }
    }

    private void maybeOffload(long offloadThresholdInBytes, long offloadThresholdInSeconds,
                              CompletableFuture<PositionImpl> finalPromise) {
        if (getOffloadPoliciesIfAppendable().isEmpty()) {
            String msg = String.format("[%s] Nothing to offload due to offloader or offloadPolicies is NULL", name);
            finalPromise.completeExceptionally(new IllegalArgumentException(msg));
            return;
        }

        if (offloadThresholdInBytes < 0 && offloadThresholdInSeconds < 0) {
            String msg = String.format("[%s] Nothing to offload due to [managedLedgerOffloadThresholdInBytes] and "
                    + "[managedLedgerOffloadThresholdInSeconds] less than 0.", name);
            finalPromise.completeExceptionally(new IllegalArgumentException(msg));
            return;
        }

        if (!offloadMutex.tryLock()) {
            scheduledExecutor.schedule(() -> maybeOffloadInBackground(finalPromise),
                    100, TimeUnit.MILLISECONDS);
            return;
        }

        CompletableFuture<PositionImpl> unlockingPromise = new CompletableFuture<>();
        unlockingPromise.whenComplete((res, ex) -> {
            offloadMutex.unlock();
            if (ex != null) {
                finalPromise.completeExceptionally(ex);
            } else {
                finalPromise.complete(res);
            }
        });

        long sizeSummed = 0;
        long toOffloadSize = 0;
        long alreadyOffloadedSize = 0;
        ConcurrentLinkedDeque<LedgerInfo> toOffload = new ConcurrentLinkedDeque<>();
        final long offloadTimeThresholdMillis = TimeUnit.SECONDS.toMillis(offloadThresholdInSeconds);

        for (Map.Entry<Long, LedgerInfo> e : ledgers.descendingMap().entrySet()) {
            final LedgerInfo info = e.getValue();
            // Skip current active ledger, an active ledger can't be offloaded.
            // Can't `info.getLedgerId() == currentLedger.getId()` here, trigger offloading is before create ledger.
            if (info.getTimestamp() == 0L) {
                continue;
            }

            final long size = info.getSize();
            final long timestamp = info.getTimestamp();
            final long now = System.currentTimeMillis();
            sizeSummed += size;

            final boolean alreadyOffloaded = info.hasOffloadContext() && info.getOffloadContext().getComplete();
            if (alreadyOffloaded) {
                alreadyOffloadedSize += size;
            } else {
                if ((offloadThresholdInBytes >= 0 && sizeSummed > offloadThresholdInBytes)
                        || (offloadTimeThresholdMillis >= 0 && now - timestamp >= offloadTimeThresholdMillis)) {
                    toOffloadSize += size;
                    toOffload.addFirst(info);
                }
            }
        }

        if (toOffload.size() > 0) {
            log.info("[{}] Going to automatically offload ledgers {}"
                            + ", total size = {}, already offloaded = {}, to offload = {}",
                    name, toOffload.stream().map(LedgerInfo::getLedgerId).collect(Collectors.toList()),
                    sizeSummed, alreadyOffloadedSize, toOffloadSize);
            offloadLoop(unlockingPromise, toOffload, PositionImpl.LATEST, Optional.empty());
        } else {
            // offloadLoop will complete immediately with an empty list to offload
            log.debug("[{}] Nothing to offload, total size = {}, already offloaded = {}, "
                            + "threshold = [managedLedgerOffloadThresholdInBytes:{}, "
                            + "managedLedgerOffloadThresholdInSeconds:{}]",
                    name, sizeSummed, alreadyOffloadedSize, offloadThresholdInBytes,
                    TimeUnit.MILLISECONDS.toSeconds(offloadTimeThresholdMillis));
            unlockingPromise.complete(PositionImpl.LATEST);
        }
    }

    private boolean hasLedgerRetentionExpired(long retentionTimeMs, long ledgerTimestamp) {
        return retentionTimeMs >= 0 && clock.millis() - ledgerTimestamp > retentionTimeMs;
    }

    private boolean isLedgerRetentionOverSizeQuota(long retentionSizeInMB, long totalSizeOfML, long sizeToDelete) {
        // Handle the -1 size limit as "infinite" size quota
        return retentionSizeInMB >= 0 && totalSizeOfML - sizeToDelete >= retentionSizeInMB * MegaByte;
    }

    boolean isOffloadedNeedsDelete(OffloadContext offload, Optional<OffloadPolicies> offloadPolicies) {
        long elapsedMs = clock.millis() - offload.getTimestamp();
        return offloadPolicies.filter(policies -> offload.getComplete() && !offload.getBookkeeperDeleted()
                && policies.getManagedLedgerOffloadDeletionLagInMillis() != null
                && elapsedMs > policies.getManagedLedgerOffloadDeletionLagInMillis()).isPresent();
    }

    /**
     * Checks whether there are ledger that have been fully consumed and deletes them.
     *
     * @throws Exception
     */
    void internalTrimConsumedLedgers(CompletableFuture<List<LedgerInfo>> promise) {
        internalTrimLedgers(false, promise);
    }

    private Optional<OffloadPolicies> getOffloadPoliciesIfAppendable() {
        LedgerOffloader ledgerOffloader = config.getLedgerOffloader();
        if (ledgerOffloader == null
                || !ledgerOffloader.isAppendable()
                || ledgerOffloader.getOffloadPolicies() == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(ledgerOffloader.getOffloadPolicies());
    }

    @VisibleForTesting
    synchronized List<Long> internalEvictOffloadedLedgers() {
        long inactiveOffloadedLedgerEvictionTimeMs = config.getInactiveOffloadedLedgerEvictionTimeMs();
        if (inactiveOffloadedLedgerEvictionTimeMs <= 0) {
            return Collections.emptyList();
        }

        long now = clock.millis();
        long minimumEvictionIntervalMs = inactiveOffloadedLedgerEvictionTimeMs / MINIMUM_EVICTION_INTERVAL_DIVIDER;
        if (now - lastEvictOffloadedLedgers < minimumEvictionIntervalMs) {
            // skip eviction if we have done it recently
            return Collections.emptyList();
        }

        try {
            List<Long> ledgersToRelease = new ArrayList<>();

            ledgerCache.forEach((ledgerId, ledger) -> {
                if (ledger.isDone() && !ledger.isCompletedExceptionally()) {
                    ReadHandle readHandle = ledger.join();
                    if (readHandle instanceof OffloadedLedgerHandle offloadedLedgerHandle) {
                        int pendingRead = offloadedLedgerHandle.getPendingRead();
                        long lastAccessTimestamp = offloadedLedgerHandle.lastAccessTimestamp();
                        if (lastAccessTimestamp >= 0 && pendingRead == 0) {
                            long delta = now - offloadedLedgerHandle.lastAccessTimestamp();
                            if (delta >= inactiveOffloadedLedgerEvictionTimeMs) {
                                log.info("[{}] Offloaded ledger {} can be released ({} ms elapsed since last access)",
                                        name, ledgerId, delta);
                                ledgersToRelease.add(ledgerId);
                            } else if (log.isDebugEnabled()) {
                                log.debug(
                                        "[{}] Offloaded ledger {} cannot be released ({} ms elapsed since last access)",
                                        name, ledgerId, delta);
                            }
                        } else if (pendingRead < 0) {
                            log.error("[{}] Offloaded ledger {} went to a wrong state because its pending read is a"
                                + " negative value {}. Please raise an issue to https://github.com/apache/pulsar", name,
                                ledgerId, pendingRead);
                        }
                    }
                }
            });
            for (Long ledgerId : ledgersToRelease) {
                invalidateReadHandle(ledgerId);
            }
            return ledgersToRelease;
        } finally {
            lastEvictOffloadedLedgers = now;
        }
    }

    void internalTrimLedgers(boolean isTruncate, CompletableFuture<List<LedgerInfo>> promise) {

        internalEvictOffloadedLedgers();

        if (!factory.isMetadataServiceAvailable()) {
            // Defer trimming of ledger if we cannot connect to metadata service
            promise.completeExceptionally(new MetaStoreException("Metadata service is not available"));
            return;
        }

        // Ensure only one trimming operation is active
        if (!trimmerMutex.tryLock()) {
            scheduleDeferredTrimming(isTruncate, promise);
            return;
        }

        List<LedgerInfo> ledgersToDelete = new ArrayList<>();
        List<LedgerInfo> offloadedLedgersToDelete = new ArrayList<>();
        Optional<OffloadPolicies> optionalOffloadPolicies = getOffloadPoliciesIfAppendable();
        synchronized (this) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Start TrimConsumedLedgers. ledgers={} totalSize={}", name, ledgers.keySet(),
                        TOTAL_SIZE_UPDATER.get(this));
            }
            State currentState = STATE_UPDATER.get(this);
            if (currentState == State.Closed) {
                log.debug("[{}] Ignoring trimming request since the managed ledger was already closed", name);
                trimmerMutex.unlock();
                promise.completeExceptionally(new ManagedLedgerAlreadyClosedException("Can't trim closed ledger"));
                return;
            }
            // Allow for FencedForDeletion
            if (currentState == State.Fenced) {
                log.debug("[{}] Ignoring trimming request since the managed ledger was already fenced", name);
                trimmerMutex.unlock();
                promise.completeExceptionally(new ManagedLedgerFencedException("Can't trim fenced ledger"));
                return;
            }

            long slowestReaderLedgerId = -1;
            final LazyLoadableValue<Long> slowestNonDurationLedgerId =
                    new LazyLoadableValue(() -> getTheSlowestNonDurationReadPosition().getLedgerId());
            final long retentionSizeInMB = config.getRetentionSizeInMB();
            final long retentionTimeMs = config.getRetentionTimeMillis();
            final long totalSizeOfML = TOTAL_SIZE_UPDATER.get(this);
            if (!cursors.hasDurableCursors()) {
                // At this point the lastLedger will be pointing to the
                // ledger that has just been closed, therefore the +1 to
                // include lastLedger in the trimming.
                slowestReaderLedgerId = currentLedger.getId() + 1;
            } else {
                PositionImpl slowestReaderPosition = cursors.getSlowestReaderPosition();
                if (slowestReaderPosition != null) {
                    // The slowest reader position is the mark delete position.
                    // If the slowest reader position point the last entry in the ledger x,
                    // the slowestReaderLedgerId should be x + 1 and the ledger x could be deleted.
                    LedgerInfo ledgerInfo = ledgers.get(slowestReaderPosition.getLedgerId());
                    if (ledgerInfo != null && ledgerInfo.getLedgerId() != currentLedger.getId()
                            && ledgerInfo.getEntries() == slowestReaderPosition.getEntryId() + 1) {
                        slowestReaderLedgerId = slowestReaderPosition.getLedgerId() + 1;
                    } else {
                        slowestReaderLedgerId = slowestReaderPosition.getLedgerId();
                    }
                } else {
                    promise.completeExceptionally(new ManagedLedgerException("Couldn't find reader position"));
                    trimmerMutex.unlock();
                    return;
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Slowest consumer ledger id: {}", name, slowestReaderLedgerId);
            }

            long totalSizeToDelete = 0;
            // skip ledger if retention constraint met
            Iterator<LedgerInfo> ledgerInfoIterator =
                    ledgers.headMap(slowestReaderLedgerId, false).values().iterator();
            while (ledgerInfoIterator.hasNext()){
                LedgerInfo ls = ledgerInfoIterator.next();
                // currentLedger can not be deleted
                if (ls.getLedgerId() == currentLedger.getId()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Ledger {} skipped for deletion as it is currently being written to", name,
                                ls.getLedgerId());
                    }
                    break;
                }
                // if truncate, all ledgers besides currentLedger are going to be deleted
                if (isTruncate) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Ledger {} will be truncated with ts {}",
                                name, ls.getLedgerId(), ls.getTimestamp());
                    }
                    ledgersToDelete.add(ls);
                    continue;
                }

                totalSizeToDelete += ls.getSize();
                boolean overRetentionQuota = isLedgerRetentionOverSizeQuota(retentionSizeInMB, totalSizeOfML,
                        totalSizeToDelete);
                boolean expired = hasLedgerRetentionExpired(retentionTimeMs, ls.getTimestamp());
                if (log.isDebugEnabled()) {
                    log.debug(
                            "[{}] Checking ledger {} -- time-old: {} sec -- "
                                    + "expired: {} -- over-quota: {} -- current-ledger: {}",
                            name, ls.getLedgerId(), (clock.millis() - ls.getTimestamp()) / 1000.0, expired,
                            overRetentionQuota, currentLedger.getId());
                }

                if (expired || overRetentionQuota) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Ledger {} has expired or over quota, expired is: {}, ts: {}, "
                                        + "overRetentionQuota is: {}, ledge size: {}",
                                name, ls.getLedgerId(), expired, ls.getTimestamp(), overRetentionQuota, ls.getSize());
                    }
                    ledgersToDelete.add(ls);
                } else {
                    // once retention constraint has been met, skip check
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Ledger {} not deleted. Neither expired nor over-quota", name, ls.getLedgerId());
                    }
                    releaseReadHandleIfNoLongerRead(ls.getLedgerId(), slowestNonDurationLedgerId.getValue());
                    break;
                }
            }

            while (ledgerInfoIterator.hasNext()) {
                LedgerInfo ls = ledgerInfoIterator.next();
                if (!releaseReadHandleIfNoLongerRead(ls.getLedgerId(), slowestNonDurationLedgerId.getValue())) {
                    break;
                }
            }

            for (LedgerInfo ls : ledgers.values()) {
                if (isOffloadedNeedsDelete(ls.getOffloadContext(), optionalOffloadPolicies)
                        && !ledgersToDelete.contains(ls)) {
                    log.debug("[{}] Ledger {} has been offloaded, bookkeeper ledger needs to be deleted", name,
                            ls.getLedgerId());
                    offloadedLedgersToDelete.add(ls);
                }
            }

            if (ledgersToDelete.isEmpty() && offloadedLedgersToDelete.isEmpty()) {
                trimmerMutex.unlock();
                promise.complete(null);
                return;
            }

            if (currentState == State.CreatingLedger // Give up now and schedule a new trimming
                    || !metadataMutex.tryLock()) { // Avoid deadlocks with other operations updating the ledgers list
                scheduleDeferredTrimming(isTruncate, promise);
                trimmerMutex.unlock();
                return;
            }

            try {
                advanceCursorsIfNecessary(ledgersToDelete);
            } catch (LedgerNotExistException e) {
                log.info("First non deleted Ledger is not found, stop trimming");
                metadataMutex.unlock();
                trimmerMutex.unlock();
                return;
            }

            doDeleteLedgers(ledgersToDelete);

            for (LedgerInfo ls : offloadedLedgersToDelete) {
                LedgerInfo.Builder newInfoBuilder = ls.toBuilder();
                newInfoBuilder.getOffloadContextBuilder().setBookkeeperDeleted(true);
                String driverName = OffloadUtils.getOffloadDriverName(ls,
                        config.getLedgerOffloader().getOffloadDriverName());
                Map<String, String> driverMetadata = OffloadUtils.getOffloadDriverMetadata(ls,
                        config.getLedgerOffloader().getOffloadDriverMetadata());
                OffloadUtils.setOffloadDriverMetadata(newInfoBuilder, driverName, driverMetadata);
                ledgers.put(ls.getLedgerId(), newInfoBuilder.build());
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Updating of ledgers list after trimming", name);
            }

            store.asyncUpdateLedgerIds(name, getManagedLedgerInfo(), ledgersStat, new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void result, Stat stat) {
                    log.info("[{}] End TrimConsumedLedgers. ledgers={} totalSize={}", name, ledgers.size(),
                            TOTAL_SIZE_UPDATER.get(ManagedLedgerImpl.this));
                    ledgersStat = stat;
                    metadataMutex.unlock();
                    trimmerMutex.unlock();

                    for (LedgerInfo ls : ledgersToDelete) {
                        log.info("[{}] Removing ledger {} - size: {}", name, ls.getLedgerId(), ls.getSize());
                        asyncDeleteLedger(ls.getLedgerId(), ls);
                    }
                    for (LedgerInfo ls : offloadedLedgersToDelete) {
                        log.info("[{}] Deleting offloaded ledger {} from bookkeeper - size: {}", name, ls.getLedgerId(),
                                ls.getSize());
                        invalidateReadHandle(ls.getLedgerId());
                        asyncDeleteLedgerFromBookKeeper(ls.getLedgerId()).thenAccept(__ -> {
                            log.info("[{}] Deleted and invalidated offloaded ledger {} from bookkeeper - size: {}",
                                    name, ls.getLedgerId(), ls.getSize());
                        }).exceptionally(ex -> {
                            log.error("[{}] Failed to delete offloaded ledger {} from bookkeeper - size: {}",
                                    name, ls.getLedgerId(), ls.getSize(), ex);
                            return null;
                        });
                    }

                    promise.complete(ledgersToDelete);
                }

                @Override
                public void operationFailed(MetaStoreException e) {
                    log.warn("[{}] Failed to update the list of ledgers after trimming", name, e);
                    metadataMutex.unlock();
                    trimmerMutex.unlock();
                    handleBadVersion(e);

                    promise.completeExceptionally(e);
                }
            });
        }
    }

    /**
     * @param ledgerId the ledger handle which maybe will be released.
     * @return if the ledger handle was released.
     */
    private boolean releaseReadHandleIfNoLongerRead(long ledgerId, long slowestNonDurationLedgerId) {
        if (ledgerId < slowestNonDurationLedgerId) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ledger {} no longer needs to be read, close the cached readHandle", name, ledgerId);
            }
            invalidateReadHandle(ledgerId);
            return true;
        }
        return false;
    }

    protected void doDeleteLedgers(List<LedgerInfo> ledgersToDelete) {
        PositionImpl currentLastConfirmedEntry = lastConfirmedEntry;
        // Update metadata
        for (LedgerInfo ls : ledgersToDelete) {
            if (currentLastConfirmedEntry != null && ls.getLedgerId() == currentLastConfirmedEntry.getLedgerId()) {
                // this info is relevant because the lastMessageId won't be available anymore
                log.info("[{}] Ledger {} contains the current last confirmed entry {}, and it is going to be "
                        + "deleted", name, ls.getLedgerId(), currentLastConfirmedEntry);
            }

            invalidateReadHandle(ls.getLedgerId());

            ledgers.remove(ls.getLedgerId());
            NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, -ls.getEntries());
            TOTAL_SIZE_UPDATER.addAndGet(this, -ls.getSize());

            entryCache.invalidateAllEntries(ls.getLedgerId());
        }
    }

    /**
     * Non-durable cursors have to be moved forward when data is trimmed since they are not retain that data.
     * This is to make sure that the `consumedEntries` counter is correctly updated with the number of skipped
     * entries and the stats are reported correctly.
     */
    @VisibleForTesting
    void advanceCursorsIfNecessary(List<LedgerInfo> ledgersToDelete) throws LedgerNotExistException {
        if (ledgersToDelete.isEmpty()) {
            return;
        }

        // Just ack messages like a consumer. Normally, consumers will not confirm a position that does not exist, so
        // find the latest existing position to ack.
        PositionImpl highestPositionToDelete = calculateLastEntryInLedgerList(ledgersToDelete);
        if (highestPositionToDelete == null) {
            log.warn("[{}] The ledgers to be trim are all empty, skip to advance non-durable cursors: {}",
                    name, ledgersToDelete);
            return;
        }
        cursors.forEach(cursor -> {
            // move the mark delete position to the highestPositionToDelete only if it is smaller than the add confirmed
            // to prevent the edge case where the cursor is caught up to the latest and highestPositionToDelete may be
            // larger than the last add confirmed
            if (highestPositionToDelete.compareTo((PositionImpl) cursor.getMarkDeletedPosition()) > 0
                    && highestPositionToDelete.compareTo((PositionImpl) cursor.getManagedLedger()
                    .getLastConfirmedEntry()) <= 0 && !(!cursor.isDurable() && cursor instanceof NonDurableCursorImpl
                    && ((NonDurableCursorImpl) cursor).isReadCompacted())) {
                cursor.asyncMarkDelete(highestPositionToDelete, cursor.getProperties(), new MarkDeleteCallback() {
                    @Override
                    public void markDeleteComplete(Object ctx) {
                    }

                    @Override
                    public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                        log.warn("[{}] Failed to mark delete while trimming data ledgers: {}", name,
                                exception.getMessage());
                    }
                }, null);
            }
        });
    }

    /**
     * @return null if all ledgers is empty.
     */
    private PositionImpl calculateLastEntryInLedgerList(List<LedgerInfo> ledgersToDelete) {
        for (int i = ledgersToDelete.size() - 1; i >= 0; i--) {
            LedgerInfo ledgerInfo = ledgersToDelete.get(i);
            if (ledgerInfo != null && ledgerInfo.hasEntries() && ledgerInfo.getEntries() > 0) {
                return PositionImpl.get(ledgerInfo.getLedgerId(), ledgerInfo.getEntries() - 1);
            }
        }
        return null;
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
        setFencedForDeletion();
        cancelScheduledTasks();

        // Truncate to ensure the offloaded data is not orphaned.
        // Also ensures the BK ledgers are deleted and not just scheduled for deletion
        CompletableFuture<Void> truncateFuture = asyncTruncate();
        truncateFuture.whenComplete((ignore, exc) -> {
            if (exc != null) {
                log.error("[{}] Error truncating ledger for deletion", name, exc);
                callback.deleteLedgerFailed(ManagedLedgerException.getManagedLedgerException(
                        FutureUtil.unwrapCompletionException(exc)), ctx);
            } else {
                asyncDeleteInternal(callback, ctx);
            }
        });

    }

    private void asyncDeleteInternal(final DeleteLedgerCallback callback, final Object ctx) {

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
                    if (exception instanceof CursorNotFoundException) {
                        // This could happen if a cursor is getting deleted while we are deleting the topic
                        // Treating this as a "success" case, since the cursor is gone in any case.
                        deleteCursorComplete(ctx);
                        return;
                    }

                    log.warn("[{}] Failed to delete cursor {}: {}", name, cursor, exception.getMessage(), exception);
                    cursorDeleteException.compareAndSet(null, exception);
                    if (cursorsToDelete.decrementAndGet() == 0) {
                        // Trigger callback only once
                        callback.deleteLedgerFailed(exception, ctx);
                    }
                }
            }, null);
        }
    }

    private CompletableFuture<Void> asyncDeleteLedgerFromBookKeeper(long ledgerId) {
        return asyncDeleteLedger(ledgerId, DEFAULT_LEDGER_DELETE_RETRIES);
    }

    private void asyncDeleteLedger(long ledgerId, LedgerInfo info) {
        if (!info.getOffloadContext().getBookkeeperDeleted()) {
            // only delete if it hasn't been previously deleted for offload
            asyncDeleteLedger(ledgerId, DEFAULT_LEDGER_DELETE_RETRIES);
        }

        if (info.getOffloadContext().hasUidMsb()) {
            UUID uuid = new UUID(info.getOffloadContext().getUidMsb(), info.getOffloadContext().getUidLsb());
            OffloadUtils.cleanupOffloaded(ledgerId, uuid, config,
                    OffloadUtils.getOffloadDriverMetadata(info, config.getLedgerOffloader().getOffloadDriverMetadata()),
                    "Trimming", name, scheduledExecutor);
        }
    }

    private CompletableFuture<Void> asyncDeleteLedger(long ledgerId, long retry) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        asyncDeleteLedgerWithRetry(future, ledgerId, retry);
        return future;
    }

    private void asyncDeleteLedgerWithRetry(CompletableFuture<Void> future, long ledgerId, long retry) {
        bookKeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
            if (isNoSuchLedgerExistsException(rc)) {
                log.warn("[{}] Ledger was already deleted {}", name, ledgerId);
                future.complete(null);
            } else if (rc != BKException.Code.OK) {
                log.error("[{}] Error deleting ledger {} : {}", name, ledgerId, BKException.getMessage(rc));
                if (retry <= 1) {
                    // The latest once of retry has failed
                    log.warn("[{}] Failed to delete ledger after retries {}, code: {}", name, ledgerId, rc);
                    future.completeExceptionally(BKException.create(rc));
                    return;
                }
                scheduledExecutor.schedule(() -> asyncDeleteLedger(ledgerId, retry - 1),
                        DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC, TimeUnit.SECONDS);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Deleted ledger {}", name, ledgerId);
                }
                future.complete(null);
            }
        }, null);
    }

    @SuppressWarnings("checkstyle:fallthrough")
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
                case Code.NoSuchLedgerExistsException:
                case Code.NoSuchLedgerExistsOnMetadataServerException:
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
                            BKException.getMessage(rc) + " code " + rc);
                    int toDelete = ledgersToDelete.get();
                    if (toDelete != -1 && ledgersToDelete.compareAndSet(toDelete, -1)) {
                        // Trigger callback only once
                        callback.deleteLedgerFailed(createManagedLedgerException(rc), ctx);
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

    @Override
    public Position offloadPrefix(Position pos) throws InterruptedException, ManagedLedgerException {
        CompletableFuture<Position> promise = new CompletableFuture<>();

        asyncOffloadPrefix(pos, new OffloadCallback() {
            @Override
            public void offloadComplete(Position offloadedTo, Object ctx) {
                promise.complete(offloadedTo);
            }

            @Override
            public void offloadFailed(ManagedLedgerException e, Object ctx) {
                promise.completeExceptionally(e);
            }
        }, null);

        try {
            return promise.get(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
            throw new ManagedLedgerException("Timeout during managed ledger offload operation");
        } catch (ExecutionException e) {
            log.error("[{}] Error offloading. pos = {}", name, pos, e.getCause());
            throw ManagedLedgerException.getManagedLedgerException(e.getCause());
        }
    }

    @Override
    public void asyncOffloadPrefix(Position pos, OffloadCallback callback, Object ctx) {
        LedgerOffloader ledgerOffloader = config.getLedgerOffloader();
        if (ledgerOffloader != null && !ledgerOffloader.isAppendable()) {
            String msg = String.format("[%s] does not support offload", ledgerOffloader.getClass().getSimpleName());
            callback.offloadFailed(new ManagedLedgerException(msg), ctx);
            return;
        }
        PositionImpl requestOffloadTo = (PositionImpl) pos;
        if (!isValidPosition(requestOffloadTo)
                // Also consider the case where the last ledger is currently
                // empty. In this the passed position is not technically
                // "valid", per the above check, given that it's not written
                // yes, but it will be valid for the logic here
                && !(requestOffloadTo.getLedgerId() == currentLedger.getId()
                        && requestOffloadTo.getEntryId() == 0)) {
            log.warn("[{}] Cannot start offload at position {} - LastConfirmedEntry: {}", name, pos,
                    lastConfirmedEntry);
            callback.offloadFailed(new InvalidCursorPositionException("Invalid position for offload: " + pos), ctx);
            return;
        }

        PositionImpl firstUnoffloaded;

        Queue<LedgerInfo> ledgersToOffload = new ConcurrentLinkedQueue<>();
        synchronized (this) {
            log.info("[{}] Start ledgersOffload. ledgers={} totalSize={}", name, ledgers.keySet(),
                    TOTAL_SIZE_UPDATER.get(this));

            if (STATE_UPDATER.get(this) == State.Closed) {
                log.info("[{}] Ignoring offload request since the managed ledger was already closed", name);
                callback.offloadFailed(
                        new ManagedLedgerAlreadyClosedException("Can't offload closed managed ledger (" + name + ")"),
                        ctx);
                return;
            }

            if (ledgers.isEmpty()) {
                log.info("[{}] Tried to offload a managed ledger with no ledgers, giving up", name);
                callback.offloadFailed(new ManagedLedgerAlreadyClosedException(
                        "Can't offload managed ledger (" + name + ") with no ledgers"), ctx);
                return;
            }

            long current = ledgers.lastKey();

            // the first ledger which will not be offloaded. Defaults to current,
            // in the case that the whole headmap is offloaded. Otherwise, it will
            // be set as we iterate through the headmap values
            long firstLedgerRetained = current;
            for (LedgerInfo ls : ledgers.headMap(current).values()) {
                if (requestOffloadTo.getLedgerId() > ls.getLedgerId()) {
                    // don't offload if ledger has already been offloaded, or is empty
                    if (!ls.getOffloadContext().getComplete() && ls.getSize() > 0) {
                        ledgersToOffload.add(ls);
                    }
                } else {
                    firstLedgerRetained = ls.getLedgerId();
                    break;
                }
            }
            firstUnoffloaded = PositionImpl.get(firstLedgerRetained, 0);
        }

        if (ledgersToOffload.isEmpty()) {
            log.info("[{}] No ledgers to offload", name);
            callback.offloadComplete(firstUnoffloaded, ctx);
            return;
        }

        if (offloadMutex.tryLock()) {
            log.info("[{}] Going to offload ledgers {}", name,
                    ledgersToOffload.stream().map(LedgerInfo::getLedgerId).collect(Collectors.toList()));

            CompletableFuture<PositionImpl> promise = new CompletableFuture<>();
            promise.whenComplete((result, exception) -> {
                offloadMutex.unlock();
                if (exception != null) {
                    callback.offloadFailed(ManagedLedgerException.getManagedLedgerException(exception), ctx);
                } else {
                    callback.offloadComplete(result, ctx);
                }
            });
            offloadLoop(promise, ledgersToOffload, firstUnoffloaded, Optional.empty());
        } else {
            callback.offloadFailed(
                    new ManagedLedgerException.OffloadInProgressException("Offload operation already running"), ctx);
        }
    }

    void offloadLoop(CompletableFuture<PositionImpl> promise, Queue<LedgerInfo> ledgersToOffload,
            PositionImpl firstUnoffloaded, Optional<Throwable> firstError) {
        State currentState = getState();
        if (currentState == State.Closed) {
            promise.completeExceptionally(new ManagedLedgerAlreadyClosedException(
                    String.format("managed ledger [%s] has already closed", name)));
            return;
        }
        if (currentState.isFenced()) {
            promise.completeExceptionally(new ManagedLedgerFencedException(
                    String.format("managed ledger [%s] is fenced", name)));
            return;
        }
        LedgerInfo info = ledgersToOffload.poll();
        if (info == null) {
            if (firstError.isPresent()) {
                promise.completeExceptionally(firstError.get());
            } else {
                promise.complete(firstUnoffloaded);
            }
        } else {
            long ledgerId = info.getLedgerId();
            UUID uuid = UUID.randomUUID();
            Map<String, String> extraMetadata = Map.of("ManagedLedgerName", name);

            String driverName = config.getLedgerOffloader().getOffloadDriverName();
            Map<String, String> driverMetadata = config.getLedgerOffloader().getOffloadDriverMetadata();

            prepareLedgerInfoForOffloaded(ledgerId, uuid, driverName, driverMetadata)
                .thenCompose((ignore) -> getLedgerHandle(ledgerId))
                .thenCompose(readHandle -> config.getLedgerOffloader().offload(readHandle, uuid, extraMetadata))
                .thenCompose((ignore) -> {
                        return Retries.run(Backoff.exponentialJittered(TimeUnit.SECONDS.toMillis(1),
                                                                       TimeUnit.HOURS.toMillis(1)).limit(10),
                                           FAIL_ON_CONFLICT,
                                           () -> completeLedgerInfoForOffloaded(ledgerId, uuid),
                                           scheduledExecutor, name)
                            .whenComplete((ignore2, exception) -> {
                                    if (exception != null) {
                                        Throwable e = FutureUtil.unwrapCompletionException(exception);
                                        if (e instanceof MetaStoreException) {
                                            // When a MetaStore exception happens, we can not make sure the metadata
                                            // update is failed or not. Because we have a retry on the connection loss,
                                            // it is possible to get a BadVersion or other exception after retrying.
                                            // So we don't clean up the data if it has metadata operation exception.
                                            log.error("[{}] Failed to update offloaded metadata for the ledgerId {}, "
                                                            + "the offloaded data will not be cleaned up",
                                                    name, ledgerId, exception);
                                            return;
                                        } else {
                                            log.error("[{}] Failed to offload data for the ledgerId {}, "
                                                            + "clean up the offloaded data",
                                                    name, ledgerId, exception);
                                        }
                                        OffloadUtils.cleanupOffloaded(
                                            ledgerId, uuid, config,
                                            driverMetadata,
                                            "Metastore failure", name, scheduledExecutor);
                                    }
                                });
                    })
                .whenComplete((ignore, exception) -> {
                        if (exception != null) {
                            lastOffloadFailureTimestamp = System.currentTimeMillis();
                            log.warn("[{}] Exception occurred for ledgerId {} timestamp {} during offload", name,
                                    ledgerId, lastOffloadFailureTimestamp, exception);
                            PositionImpl newFirstUnoffloaded = PositionImpl.get(ledgerId, 0);
                            if (newFirstUnoffloaded.compareTo(firstUnoffloaded) > 0) {
                                newFirstUnoffloaded = firstUnoffloaded;
                            }
                            Optional<Throwable> errorToReport = firstError;
                            synchronized (ManagedLedgerImpl.this) {
                                // if the ledger doesn't exist anymore, ignore the error
                                if (ledgers.containsKey(ledgerId)) {
                                    errorToReport = Optional.of(firstError.orElse(exception));
                                }
                            }

                            offloadLoop(promise, ledgersToOffload,
                                        newFirstUnoffloaded,
                                        errorToReport);
                        } else {
                            lastOffloadSuccessTimestamp = System.currentTimeMillis();
                            log.info("[{}] offload for ledgerId {} timestamp {} succeed", name, ledgerId,
                                    lastOffloadSuccessTimestamp);
                            lastOffloadLedgerId = ledgerId;
                            invalidateReadHandle(ledgerId);
                            offloadLoop(promise, ledgersToOffload, firstUnoffloaded, firstError);
                        }
                    });
        }
    }

    interface LedgerInfoTransformation {
        LedgerInfo transform(LedgerInfo oldInfo) throws ManagedLedgerException;
    }

    static final Predicate<Throwable> FAIL_ON_CONFLICT = (throwable) -> {
        return !(throwable instanceof OffloadConflict) && Retries.NonFatalPredicate.test(throwable);
    };

    static class OffloadConflict extends ManagedLedgerException {
        OffloadConflict(String msg) {
            super(msg);
        }
    }

    private CompletableFuture<Void> transformLedgerInfo(long ledgerId, LedgerInfoTransformation transformation) {
        CompletableFuture<Void> promise = new CompletableFuture<>();

        tryTransformLedgerInfo(ledgerId, transformation, promise);

        return promise;
    }

    private void tryTransformLedgerInfo(long ledgerId, LedgerInfoTransformation transformation,
            CompletableFuture<Void> finalPromise) {
        synchronized (this) {
            if (!metadataMutex.tryLock()) {
                // retry in 100 milliseconds
                scheduledExecutor.schedule(
                        () -> tryTransformLedgerInfo(ledgerId, transformation, finalPromise), 100,
                        TimeUnit.MILLISECONDS);
            } else { // lock acquired
                CompletableFuture<Void> unlockingPromise = new CompletableFuture<>();
                unlockingPromise.whenComplete((res, ex) -> {
                    metadataMutex.unlock();
                    if (ex != null) {
                        finalPromise.completeExceptionally(ex);
                    } else {
                        finalPromise.complete(res);
                    }
                });

                LedgerInfo oldInfo = ledgers.get(ledgerId);
                if (oldInfo == null) {
                    unlockingPromise.completeExceptionally(new OffloadConflict(
                            "Ledger " + ledgerId + " no longer exists in ManagedLedger, likely trimmed"));
                } else {
                    try {
                        LedgerInfo newInfo = transformation.transform(oldInfo);
                        final HashMap<Long, LedgerInfo> newLedgers = new HashMap<>(ledgers);
                        newLedgers.put(ledgerId, newInfo);
                        store.asyncUpdateLedgerIds(name, buildManagedLedgerInfo(newLedgers), ledgersStat,
                                new MetaStoreCallback<Void>() {
                                    @Override
                                    public void operationComplete(Void result, Stat stat) {
                                        ledgersStat = stat;
                                        ledgers.put(ledgerId, newInfo);
                                        unlockingPromise.complete(null);
                                    }

                                    @Override
                                    public void operationFailed(MetaStoreException e) {
                                        handleBadVersion(e);
                                        unlockingPromise.completeExceptionally(e);
                                    }
                                });
                    } catch (ManagedLedgerException mle) {
                        unlockingPromise.completeExceptionally(mle);
                    }
                }
            }
        }
    }

    private CompletableFuture<Void> prepareLedgerInfoForOffloaded(long ledgerId, UUID uuid, String offloadDriverName,
            Map<String, String> offloadDriverMetadata) {
        log.info("[{}] Preparing metadata to offload ledger {} with uuid {}", name, ledgerId, uuid);
        return transformLedgerInfo(ledgerId,
                                   (oldInfo) -> {
                                       if (oldInfo.getOffloadContext().hasUidMsb()) {
                                           UUID oldUuid = new UUID(oldInfo.getOffloadContext().getUidMsb(),
                                                                   oldInfo.getOffloadContext().getUidLsb());
                                           log.info("[{}] Found previous offload attempt for ledger {}, uuid {}"
                                                    + ", cleaning up", name, ledgerId, uuid);
                                           OffloadUtils.cleanupOffloaded(
                                               ledgerId,
                                               oldUuid,
                                               config,
                                               OffloadUtils.getOffloadDriverMetadata(oldInfo,
                                                   config.getLedgerOffloader().getOffloadDriverMetadata()),
                                               "Previous failed offload",
                                               name,
                                               scheduledExecutor);
                                       }
                                       LedgerInfo.Builder builder = oldInfo.toBuilder();
                                       builder.getOffloadContextBuilder()
                                           .setUidMsb(uuid.getMostSignificantBits())
                                           .setUidLsb(uuid.getLeastSignificantBits());
                                       OffloadUtils.setOffloadDriverMetadata(
                                           builder,
                                           offloadDriverName,
                                           offloadDriverMetadata
                                       );
                                       return builder.build();
                                   })
            .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.warn("[{}] Failed to prepare ledger {} for offload, uuid {}",
                                 name, ledgerId, uuid, exception);
                    } else {
                        log.info("[{}] Metadata prepared for offload of ledger {} with uuid {}", name, ledgerId, uuid);
                    }
                });
    }

    private CompletableFuture<Void> completeLedgerInfoForOffloaded(long ledgerId, UUID uuid) {
        log.info("[{}] Completing metadata for offload of ledger {} with uuid {}", name, ledgerId, uuid);
        return transformLedgerInfo(ledgerId,
                                   (oldInfo) -> {
                                       UUID existingUuid = new UUID(oldInfo.getOffloadContext().getUidMsb(),
                                                                    oldInfo.getOffloadContext().getUidLsb());
                                       if (existingUuid.equals(uuid)) {
                                           LedgerInfo.Builder builder = oldInfo.toBuilder();
                                           builder.getOffloadContextBuilder()
                                               .setTimestamp(clock.millis())
                                               .setComplete(true);

                                           String driverName = OffloadUtils.getOffloadDriverName(
                                               oldInfo, config.getLedgerOffloader().getOffloadDriverName());
                                           Map<String, String> driverMetadata = OffloadUtils.getOffloadDriverMetadata(
                                               oldInfo, config.getLedgerOffloader().getOffloadDriverMetadata());
                                           OffloadUtils.setOffloadDriverMetadata(
                                               builder,
                                               driverName,
                                               driverMetadata
                                           );
                                           return builder.build();
                                       } else {
                                           throw new OffloadConflict(
                                                   "Existing UUID(" + existingUuid + ") in metadata for offload"
                                                   + " of ledgerId " + ledgerId + " does not match the UUID(" + uuid
                                                   + ") for the offload we are trying to complete");
                                       }
                                   })
            .whenComplete((result, exception) -> {
                    if (exception == null) {
                        log.info("[{}] End Offload. ledger={}, uuid={}", name, ledgerId, uuid);
                    } else {
                        log.warn("[{}] Failed to complete offload of ledger {}, uuid {}",
                                 name, ledgerId, uuid, exception);
                    }
                });
    }

    /**
     * Get the number of entries between a contiguous range of two positions.
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
     * Get the entry position at a given distance from a given position.
     *
     * @param startPosition
     *            starting position
     * @param n
     *            number of entries to skip ahead
     * @param startRange
     *            specifies whether to include the start position in calculating the distance
     * @return the new position that is n entries ahead
     */
    public PositionImpl getPositionAfterN(final PositionImpl startPosition, long n, PositionBound startRange) {
        long entriesToSkip = n;
        long currentLedgerId;
        long currentEntryId;
        if (startRange == PositionBound.startIncluded) {
            currentLedgerId = startPosition.getLedgerId();
            currentEntryId = startPosition.getEntryId();
        } else {
            PositionImpl nextValidPosition = getNextValidPosition(startPosition);
            currentLedgerId = nextValidPosition.getLedgerId();
            currentEntryId = nextValidPosition.getEntryId();
        }
        boolean lastLedger = false;
        long totalEntriesInCurrentLedger;
        while (entriesToSkip >= 0) {
            // for the current ledger, the number of entries written is deduced from the lastConfirmedEntry
            // for previous ledgers, LedgerInfo in ZK has the number of entries
            if (currentLedger != null && currentLedgerId == currentLedger.getId()) {
                lastLedger = true;
                if (currentLedgerEntries > 0) {
                    totalEntriesInCurrentLedger = lastConfirmedEntry.getEntryId() + 1;
                } else {
                    totalEntriesInCurrentLedger = 0;
                }
            } else {
                LedgerInfo ledgerInfo = ledgers.get(currentLedgerId);
                totalEntriesInCurrentLedger = ledgerInfo != null ? ledgerInfo.getEntries() : 0;
            }
            long unreadEntriesInCurrentLedger = totalEntriesInCurrentLedger > 0
                    ? totalEntriesInCurrentLedger - currentEntryId : 0;
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
                }
                Long lid = ledgers.ceilingKey(currentLedgerId + 1);
                currentLedgerId = lid != null ? lid : ledgers.lastKey();
                currentEntryId = 0;
            }
        }

        PositionImpl positionToReturn = getPreviousPosition(PositionImpl.get(currentLedgerId, currentEntryId));
        if (positionToReturn.compareTo(lastConfirmedEntry) > 0) {
            positionToReturn = lastConfirmedEntry;
        }
        if (log.isDebugEnabled()) {
            log.debug("getPositionAfterN: Start position {}:{}, startIncluded: {}, Return position {}:{}",
                    startPosition.getLedgerId(), startPosition.getEntryId(), startRange, positionToReturn.getLedgerId(),
                    positionToReturn.getEntryId());
        }

        return positionToReturn;
    }

    public boolean isNoMessagesAfterPos(PositionImpl pos) {
        PositionImpl lac = (PositionImpl) getLastConfirmedEntry();
        return isNoMessagesAfterPosForSpecifiedLac(lac, pos);
    }

    private boolean isNoMessagesAfterPosForSpecifiedLac(PositionImpl specifiedLac, PositionImpl pos) {
        if (pos.compareTo(specifiedLac) >= 0) {
            return true;
        }
        if (specifiedLac.getEntryId() < 0) {
            // Calculate the meaningful LAC.
            PositionImpl actLac = getPreviousPosition(specifiedLac);
            if (actLac.getEntryId() >= 0) {
                return pos.compareTo(actLac) >= 0;
            } else {
                // If the actual LAC is still not meaningful.
                if (actLac.equals(specifiedLac)) {
                    // No entries in maneged ledger.
                    return true;
                } else {
                    // Continue to find a valid LAC.
                    return isNoMessagesAfterPosForSpecifiedLac(actLac, pos);
                }
            }
        }
        return false;
    }

    /**
     * Get the entry position that come before the specified position in the message stream, using information from the
     * ledger list and each ledger entries count.
     *
     * @param position
     *            the current position
     * @return the previous position
     */
    public PositionImpl getPreviousPosition(PositionImpl position) {
        if (position.getEntryId() > 0) {
            return PositionImpl.get(position.getLedgerId(), position.getEntryId() - 1);
        }

        // The previous position will be the last position of an earlier ledgers
        NavigableMap<Long, LedgerInfo> headMap = ledgers.headMap(position.getLedgerId(), false);

        final Map.Entry<Long, LedgerInfo> firstEntry = headMap.firstEntry();
        if (firstEntry == null) {
            // There is no previous ledger, return an invalid position in the current ledger
            return PositionImpl.get(position.getLedgerId(), -1);
        }

        // We need to find the most recent non-empty ledger
        for (long ledgerId : headMap.descendingKeySet()) {
            LedgerInfo li = headMap.get(ledgerId);
            if (li != null && li.getEntries() > 0) {
                return PositionImpl.get(li.getLedgerId(), li.getEntries() - 1);
            }
        }

        // in case there are only empty ledgers, we return a position in the first one
        return PositionImpl.get(firstEntry.getKey(), -1);
    }

    /**
     * Validate whether a specified position is valid for the current managed ledger.
     *
     * @param position
     *            the position to validate
     * @return true if the position is valid, false otherwise
     */
    public boolean isValidPosition(PositionImpl position) {
        PositionImpl lac = lastConfirmedEntry;
        if (log.isDebugEnabled()) {
            log.debug("IsValid position: {} -- last: {}", position, lac);
        }

        if (!ledgers.containsKey(position.getLedgerId())){
            return false;
        } else if (position.getEntryId() < 0) {
            return false;
        } else if (currentLedger != null && position.getLedgerId() == currentLedger.getId()) {
            // If current ledger is empty, the largest read position can be "{current_ledger: 0}".
            // Else, the read position can be set to "{LAC + 1}" when subscribe at LATEST,
            return (position.getLedgerId() == lac.getLedgerId() && position.getEntryId() <= lac.getEntryId() + 1)
                    || position.getEntryId() == 0;
        } else if (position.getLedgerId() == lac.getLedgerId()) {
            // The ledger witch maintains LAC was closed, and there is an empty current ledger.
            // If entry id is larger than LAC, it should be "{current_ledger: 0}".
            return position.getEntryId() <= lac.getEntryId();
        } else {
            // Look in the ledgers map
            LedgerInfo ls = ledgers.get(position.getLedgerId());

            if (ls == null) {
                if (position.getLedgerId() < lac.getLedgerId()) {
                    // Pointing to a non-existing ledger that is older than the current ledger is invalid
                    return false;
                } else {
                    // Pointing to a non-existing ledger is only legitimate if the ledger was empty
                    return position.getEntryId() == 0;
                }
            }

            return position.getEntryId() < ls.getEntries();
        }
    }

    public boolean ledgerExists(long ledgerId) {
        return ledgers.get(ledgerId) != null;
    }

    public Long getNextValidLedger(long ledgerId) {
        return ledgers.ceilingKey(ledgerId + 1);
    }

    public PositionImpl getNextValidPosition(final PositionImpl position) {
        return getValidPositionAfterSkippedEntries(position, 1);
    }
    public PositionImpl getValidPositionAfterSkippedEntries(final PositionImpl position, int skippedEntryNum) {
        PositionImpl skippedPosition = position.getPositionAfterEntries(skippedEntryNum);
        while (!isValidPosition(skippedPosition)) {
            Long nextLedgerId = ledgers.ceilingKey(skippedPosition.getLedgerId() + 1);
            // This means it has jumped to the last position
            if (nextLedgerId == null) {
                if (currentLedgerEntries == 0 && currentLedger != null) {
                    return PositionImpl.get(currentLedger.getId(), 0);
                }
                return lastConfirmedEntry.getNext();
            }
            skippedPosition = PositionImpl.get(nextLedgerId, 0);
        }
        return skippedPosition;
    }
    public PositionImpl getNextValidPositionInternal(final PositionImpl position) {
        PositionImpl nextPosition = position.getNext();
        while (!isValidPosition(nextPosition)) {
            Long nextLedgerId = ledgers.ceilingKey(nextPosition.getLedgerId() + 1);
            if (nextLedgerId == null) {
                throw new NullPointerException("nextLedgerId is null. No valid next position after " + position);
            }
            nextPosition = PositionImpl.get(nextLedgerId, 0);
        }
        return nextPosition;
    }

    public PositionImpl getFirstPosition() {
        Long ledgerId = ledgers.firstKey();
        if (ledgerId == null) {
            return null;
        }
        if (ledgerId > lastConfirmedEntry.getLedgerId()) {
            checkState(ledgers.get(ledgerId).getEntries() == 0);
            ledgerId = lastConfirmedEntry.getLedgerId();
        }
        return new PositionImpl(ledgerId, -1);
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
     * Get the last position written in the managed ledger, alongside with the associated counter.
     */
    Pair<PositionImpl, Long> getLastPositionAndCounter() {
        PositionImpl pos;
        long count;

        do {
            pos = lastConfirmedEntry;
            count = ENTRIES_ADDED_COUNTER_UPDATER.get(this);

            // Ensure no entry was written while reading the two values
        } while (pos.compareTo(lastConfirmedEntry) != 0);

        return Pair.of(pos, count);
    }

    /**
     * Get the first position written in the managed ledger, alongside with the associated counter.
     */
    Pair<PositionImpl, Long> getFirstPositionAndCounter() {
        PositionImpl pos;
        long count;
        Pair<PositionImpl, Long> lastPositionAndCounter;

        do {
            pos = getFirstPosition();
            lastPositionAndCounter = getLastPositionAndCounter();
            count = lastPositionAndCounter.getRight()
                    - getNumberOfEntries(Range.openClosed(pos, lastPositionAndCounter.getLeft()));
        } while (pos.compareTo(getFirstPosition()) != 0
                || lastPositionAndCounter.getLeft().compareTo(getLastPosition()) != 0);
        return Pair.of(pos, count);
    }

    public void activateCursor(ManagedCursor cursor) {
        synchronized (activeCursors) {
            if (!isCursorActive(cursor)) {
                Position positionForOrdering = config.isCacheEvictionByMarkDeletedPosition()
                        ? cursor.getMarkDeletedPosition()
                        : cursor.getReadPosition();
                if (positionForOrdering == null) {
                    positionForOrdering = PositionImpl.EARLIEST;
                }
                activeCursors.add(cursor, positionForOrdering);
            }
        }
    }

    public void deactivateCursor(ManagedCursor cursor) {
        deactivateCursorByName(cursor.getName());
    }

    private void deactivateCursorByName(String cursorName) {
        synchronized (activeCursors) {
            if (activeCursors.removeCursor(cursorName)) {
                invalidateEntriesUpToSlowestReaderPosition();
            }
        }
    }


    public void removeWaitingCursor(ManagedCursor cursor) {
        this.waitingCursors.remove(cursor);
    }

    public void addWaitingCursor(ManagedCursorImpl cursor) {
        this.waitingCursors.add(cursor);
    }

    public boolean isCursorActive(ManagedCursor cursor) {
        return activeCursors.get(cursor.getName()) != null;
    }

    private boolean currentLedgerIsFull() {
        if (!factory.isMetadataServiceAvailable()) {
            // We don't want to trigger metadata operations if we already know that we're currently disconnected
            return false;
        }

        boolean spaceQuotaReached = (currentLedgerEntries >= config.getMaxEntriesPerLedger()
                || currentLedgerSize >= (config.getMaxSizePerLedgerMb() * MegaByte));

        long timeSinceLedgerCreationMs = clock.millis() - lastLedgerCreatedTimestamp;
        boolean maxLedgerTimeReached = timeSinceLedgerCreationMs >= config.getMaximumRolloverTimeMs();

        if (spaceQuotaReached || maxLedgerTimeReached) {
            if (config.getMinimumRolloverTimeMs() > 0) {

                boolean switchLedger = timeSinceLedgerCreationMs > config.getMinimumRolloverTimeMs();
                if (log.isDebugEnabled()) {
                    log.debug("Diff: {}, threshold: {} -- switch: {}", clock.millis() - lastLedgerCreatedTimestamp,
                            config.getMinimumRolloverTimeMs(), switchLedger);
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

    @Override
    public NavigableMap<Long, LedgerInfo> getLedgersInfo() {
        return ledgers;
    }

    protected ManagedLedgerInfo getManagedLedgerInfo() {
        return buildManagedLedgerInfo(ledgers);
    }

    private ManagedLedgerInfo getManagedLedgerInfo(LedgerInfo newLedger) {
        ManagedLedgerInfo.Builder mlInfo = ManagedLedgerInfo.newBuilder().addAllLedgerInfo(ledgers.values())
                .addLedgerInfo(newLedger);
        return buildManagedLedgerInfo(mlInfo);
    }
    private ManagedLedgerInfo buildManagedLedgerInfo(Map<Long, LedgerInfo> ledgers) {
        ManagedLedgerInfo.Builder mlInfo = ManagedLedgerInfo.newBuilder().addAllLedgerInfo(ledgers.values());
        return buildManagedLedgerInfo(mlInfo);
    }

    private ManagedLedgerInfo buildManagedLedgerInfo(ManagedLedgerInfo.Builder mlInfo) {
        if (state == State.Terminated) {
            mlInfo.setTerminatedPosition(NestedPositionInfo.newBuilder().setLedgerId(lastConfirmedEntry.getLedgerId())
                    .setEntryId(lastConfirmedEntry.getEntryId()));
        }
        if (managedLedgerInterceptor != null) {
            managedLedgerInterceptor.onUpdateManagedLedgerInfo(propertiesMap);
        }
        for (Map.Entry<String, String> property : propertiesMap.entrySet()) {
            mlInfo.addProperties(MLDataFormats.KeyValue.newBuilder()
                    .setKey(property.getKey()).setValue(property.getValue()));
        }

        return mlInfo.build();
    }

    /**
     * Throws an exception if the managed ledger has been previously fenced.
     *
     * @throws ManagedLedgerException
     */
    private void checkFenced() throws ManagedLedgerException {
        if (STATE_UPDATER.get(this).isFenced()) {
            log.error("[{}] Attempted to use a fenced managed ledger", name);
            throw new ManagedLedgerFencedException();
        }
    }

    private void checkManagedLedgerIsOpen() throws ManagedLedgerException {
        if (STATE_UPDATER.get(this) == State.Closed) {
            throw new ManagedLedgerException("ManagedLedger " + name + " has already been closed");
        }
    }

    @VisibleForTesting
    public synchronized void setFenced() {
        log.info("{} Moving to Fenced state", name);
        STATE_UPDATER.set(this, State.Fenced);
    }

    synchronized void setFencedForDeletion() {
        log.info("{} Moving to FencedForDeletion state", name);
        STATE_UPDATER.set(this, State.FencedForDeletion);
    }

    MetaStore getStore() {
        return store;
    }

    @Override
    public ManagedLedgerConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(ManagedLedgerConfig config) {
        this.config = config;
        this.maximumRolloverTimeMs = getMaximumRolloverTimeMs(config);
        this.cursors.forEach(c -> c.setThrottleMarkDelete(config.getThrottleMarkDelete()));
    }

    private static long getMaximumRolloverTimeMs(ManagedLedgerConfig config) {
        return (long) (config.getMaximumRolloverTimeMs() * (1 + random.nextDouble() * 5 / 100.0));
    }

    interface ManagedLedgerInitializeLedgerCallback {
        void initializeComplete();

        void initializeFailed(ManagedLedgerException e);
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

    @Override
    public Position getLastConfirmedEntry() {
        return lastConfirmedEntry;
    }

    public State getState() {
        return STATE_UPDATER.get(this);
    }

    public long getCacheSize() {
        return entryCache.getSize();
    }

    protected boolean isReadOnly() {
        // Default managed ledger implementation is read-write
        return false;
    }

    /**
     * return BK error codes that are considered not likely to be recoverable.
     */
    private static boolean isBkErrorNotRecoverable(int rc) {
        switch (rc) {
        case Code.NoSuchLedgerExistsException:
        case Code.NoSuchLedgerExistsOnMetadataServerException:
        case Code.NoSuchEntryException:
            return true;

        default:
            return false;
        }
    }

    private static boolean isLedgerNotExistException(int rc) {
        switch (rc) {
            case Code.NoSuchLedgerExistsException:
            case Code.NoSuchLedgerExistsOnMetadataServerException:
                return true;

            default:
                return false;
        }
    }

    public static ManagedLedgerException createManagedLedgerException(int bkErrorCode) {
        if (bkErrorCode == BKException.Code.TooManyRequestsException) {
            return new TooManyRequestsException("Too many request error from bookies");
        } else if (isBkErrorNotRecoverable(bkErrorCode)) {
            if (isLedgerNotExistException(bkErrorCode)) {
                return new LedgerNotExistException(BKException.getMessage(bkErrorCode));
            } else {
                return new NonRecoverableLedgerException(BKException.getMessage(bkErrorCode));
            }
        } else {
            return new ManagedLedgerException(BKException.getMessage(bkErrorCode) + " error code: " + bkErrorCode);
        }
    }

    public static ManagedLedgerException createManagedLedgerException(Throwable t) {
        if (t instanceof org.apache.bookkeeper.client.api.BKException) {
            return createManagedLedgerException(((org.apache.bookkeeper.client.api.BKException) t).getCode());
        } else if (t instanceof ManagedLedgerException) {
            return (ManagedLedgerException) t;
        } else if (t instanceof CompletionException
                && !(t.getCause() instanceof CompletionException) /* check to avoid stackoverlflow */) {
            return createManagedLedgerException(t.getCause());
        } else {
            log.error("Unknown exception for ManagedLedgerException.", t);
            return new ManagedLedgerException("Other exception", t);
        }
    }

    /**
     * Create ledger async and schedule a timeout task to check ledger-creation is complete else it fails the callback
     * with TimeoutException.
     *
     * @param bookKeeper
     * @param config
     * @param digestType
     * @param cb
     * @param metadata
     */
    protected void asyncCreateLedger(BookKeeper bookKeeper, ManagedLedgerConfig config, DigestType digestType,
            CreateCallback cb, Map<String, byte[]> metadata) {
        CompletableFuture<LedgerHandle> ledgerFutureHook = new CompletableFuture<>();
        Map<String, byte[]> finalMetadata = new HashMap<>();
        finalMetadata.putAll(ledgerMetadata);
        finalMetadata.putAll(metadata);
        if (config.getBookKeeperEnsemblePlacementPolicyClassName() != null
            && config.getBookKeeperEnsemblePlacementPolicyProperties() != null) {
            try {
                finalMetadata.putAll(LedgerMetadataUtils.buildMetadataForPlacementPolicyConfig(
                    config.getBookKeeperEnsemblePlacementPolicyClassName(),
                    config.getBookKeeperEnsemblePlacementPolicyProperties()
                ));
            } catch (EnsemblePlacementPolicyConfig.ParseEnsemblePlacementPolicyConfigException e) {
                log.error("[{}] Serialize the placement configuration failed", name, e);
                cb.createComplete(Code.UnexpectedConditionException, null, ledgerFutureHook);
                return;
            }
        }
        createdLedgerCustomMetadata = finalMetadata;
        try {
            bookKeeper.asyncCreateLedger(config.getEnsembleSize(), config.getWriteQuorumSize(),
                    config.getAckQuorumSize(), digestType, config.getPassword(), cb, ledgerFutureHook, finalMetadata);
        } catch (Throwable cause) {
            log.error("[{}] Encountered unexpected error when creating ledger",
                name, cause);
            ledgerFutureHook.completeExceptionally(cause);
            cb.createComplete(Code.UnexpectedConditionException, null, ledgerFutureHook);
            return;
        }

        ScheduledFuture timeoutChecker = scheduledExecutor.schedule(() -> {
            if (!ledgerFutureHook.isDone()
                    && ledgerFutureHook.completeExceptionally(new TimeoutException(name + " Create ledger timeout"))) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Timeout creating ledger", name);
                }
                cb.createComplete(BKException.Code.TimeoutException, null, ledgerFutureHook);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Ledger already created when timeout task is triggered", name);
                }
            }
        }, config.getMetadataOperationsTimeoutSeconds(), TimeUnit.SECONDS);

        ledgerFutureHook.whenComplete((ignore, ex) -> {
            timeoutChecker.cancel(false);
        });
    }

    public Clock getClock() {
        return clock;
    }

    /**
     * check if ledger-op task is already completed by timeout-task. If completed then delete the created ledger
     * @return
     */
    protected boolean checkAndCompleteLedgerOpTask(int rc, LedgerHandle lh, Object ctx) {
        if (ctx instanceof CompletableFuture) {
            // ledger-creation is already timed out and callback is already completed so, delete this ledger and return.
            if (((CompletableFuture) ctx).complete(lh) || rc == BKException.Code.TimeoutException) {
                return false;
            } else {
                if (rc == BKException.Code.OK) {
                    log.warn("[{}]-{} ledger creation timed-out, deleting ledger", this.name, lh.getId());
                    asyncDeleteLedger(lh.getId(), DEFAULT_LEDGER_DELETE_RETRIES);
                }
                return true;
            }
        }
        return false;
    }

    private void scheduleTimeoutTask() {
        // disable timeout task checker if timeout <= 0
        if (config.getAddEntryTimeoutSeconds() > 0 || config.getReadEntryTimeoutSeconds() > 0) {
            long timeoutSec = Math.min(config.getAddEntryTimeoutSeconds(), config.getReadEntryTimeoutSeconds());
            timeoutSec = timeoutSec <= 0
                    ? Math.max(config.getAddEntryTimeoutSeconds(), config.getReadEntryTimeoutSeconds())
                    : timeoutSec;
            this.timeoutTask = this.scheduledExecutor.scheduleAtFixedRate(
                    this::checkTimeouts, timeoutSec, timeoutSec, TimeUnit.SECONDS);
        }
    }

    private void checkTimeouts() {
        final State state = STATE_UPDATER.get(this);
        if (state == State.Closed
                || state.isFenced()) {
            return;
        }
        checkAddTimeout();
        checkReadTimeout();
    }

    private void checkAddTimeout() {
        long timeoutSec = config.getAddEntryTimeoutSeconds();
        if (timeoutSec < 1) {
            return;
        }
        OpAddEntry opAddEntry = pendingAddEntries.peek();
        if (opAddEntry != null) {
            boolean isTimedOut = opAddEntry.lastInitTime != -1
                    && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - opAddEntry.lastInitTime) >= timeoutSec;
            if (isTimedOut) {
                log.warn("[{}] Failed to add entry {}:{} in time-out {} sec", this.name,
                        opAddEntry.ledger != null ? opAddEntry.ledger.getId() : -1,
                        opAddEntry.entryId, timeoutSec);
                currentLedgerTimeoutTriggered.set(true);
                opAddEntry.handleAddFailure(opAddEntry.ledger);
            }
        }
    }

    private void checkReadTimeout() {
        long timeoutSec = config.getReadEntryTimeoutSeconds();
        if (timeoutSec < 1) {
            return;
        }
        ReadEntryCallbackWrapper callback = this.lastReadCallback;
        long readOpCount = callback != null ? callback.readOpCount : 0;
        boolean timeout = callback != null && (TimeUnit.NANOSECONDS
                .toSeconds(System.nanoTime() - callback.createdTime) >= timeoutSec);
        if (readOpCount > 0 && timeout) {
            log.warn("[{}]-{}-{} read entry timeout after {} sec", this.name, this.lastReadCallback.ledgerId,
                    this.lastReadCallback.entryId, timeoutSec);
            callback.readFailed(createManagedLedgerException(BKException.Code.TimeoutException), readOpCount);
            LAST_READ_CALLBACK_UPDATER.compareAndSet(this, callback, null);
        }
    }

    @Override
    public long getOffloadedSize() {
        long offloadedSize = 0;
        for (LedgerInfo li : ledgers.values()) {
            if (li.hasOffloadContext() && li.getOffloadContext().getComplete()) {
                offloadedSize += li.getSize();
            }
        }

        return offloadedSize;
    }

    @Override
    public void unfenceForInterceptorException() {
        this.interceptorException = null;
    }

    protected void fenceForInterceptorException(ManagedLedgerException e) {
        this.interceptorException = e;
    }

    @Override
    public long getLastOffloadedLedgerId() {
        return lastOffloadLedgerId;
    }

    @Override
    public long getLastOffloadedSuccessTimestamp() {
        return lastOffloadSuccessTimestamp;
    }

    @Override
    public long getLastOffloadedFailureTimestamp() {
        return lastOffloadFailureTimestamp;
    }

    @Override
    public Map<String, String> getProperties() {
        return propertiesMap;
    }

    @Override
    public void setProperty(String key, String value) throws InterruptedException, ManagedLedgerException {
        Map<String, String> map = new HashMap<>();
        map.put(key, value);
        updateProperties(map, false, null);
    }

    @Override
    public void asyncSetProperty(String key, String value, final UpdatePropertiesCallback callback, Object ctx) {
        Map<String, String> map = new HashMap<>();
        map.put(key, value);
        asyncUpdateProperties(map, false, null, callback, ctx);
    }

    @Override
    public void deleteProperty(String key) throws InterruptedException, ManagedLedgerException {
        updateProperties(null, true, key);
    }

    @Override
    public void asyncDeleteProperty(String key, final UpdatePropertiesCallback callback, Object ctx) {
        asyncUpdateProperties(null, true, key, callback, ctx);
    }

    @Override
    public void setProperties(Map<String, String> properties) throws InterruptedException, ManagedLedgerException {
        updateProperties(properties, false, null);
    }

    @Override
    public void asyncSetProperties(Map<String, String> properties, final UpdatePropertiesCallback callback,
        Object ctx) {
        asyncUpdateProperties(properties, false, null, callback, ctx);
    }

    private void updateProperties(Map<String, String> properties, boolean isDelete,
        String deleteKey) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch latch = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();
        this.asyncUpdateProperties(properties, isDelete, deleteKey, new UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                latch.countDown();
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                latch.countDown();
            }
        }, null);

        if (!latch.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during update managedLedger's properties");
        }

        if (result.exception != null) {
            log.error("[{}] Update managedLedger's properties failed", name, result.exception);
            throw result.exception;
        }
    }

    private void asyncUpdateProperties(Map<String, String> properties, boolean isDelete,
        String deleteKey, final UpdatePropertiesCallback callback, Object ctx) {
        if (!metadataMutex.tryLock()) {
            // Defer update for later
            scheduledExecutor.schedule(() -> asyncUpdateProperties(properties, isDelete, deleteKey,
                callback, ctx), 100, TimeUnit.MILLISECONDS);
            return;
        }
        if (isDelete) {
            propertiesMap.remove(deleteKey);
        } else {
            propertiesMap.putAll(properties);
        }
        store.asyncUpdateLedgerIds(name, getManagedLedgerInfo(), ledgersStat, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat version) {
                ledgersStat = version;
                callback.updatePropertiesComplete(propertiesMap, ctx);
                metadataMutex.unlock();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.error("[{}] Update managedLedger's properties failed", name, e);
                handleBadVersion(e);
                callback.updatePropertiesFailed(e, ctx);
                metadataMutex.unlock();
            }
        });
    }

    @VisibleForTesting
    public void setEntriesAddedCounter(long count) {
        ENTRIES_ADDED_COUNTER_UPDATER.set(this, count);
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerImpl.class);

    @Override
    public CompletableFuture<Void> asyncTruncate() {

        final List<CompletableFuture<Void>> futures = new ArrayList();
        for (ManagedCursor cursor : cursors) {
            final CompletableFuture<Void> future = new CompletableFuture<>();
            cursor.asyncClearBacklog(new AsyncCallbacks.ClearBacklogCallback() {
                @Override
                public void clearBacklogComplete(Object ctx) {
                    future.complete(null);
                }

                @Override
                public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, null);
            futures.add(future);
        }
        CompletableFuture<List<LedgerInfo>> future = new CompletableFuture();
        FutureUtil.waitForAll(futures).thenAccept(p -> {
            internalTrimLedgers(true, future);
        }).exceptionally(e -> {
            future.completeExceptionally(e);
            return null;
        });
        return future.thenRun(() -> {
        });
    }

    @Override
    public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(boolean includeLedgerMetadata) {
        CompletableFuture<ManagedLedgerInternalStats> statFuture = new CompletableFuture<>();
        ManagedLedgerInternalStats stats = new ManagedLedgerInternalStats();

        stats.entriesAddedCounter = this.getEntriesAddedCounter();
        stats.numberOfEntries = this.getNumberOfEntries();
        stats.totalSize = this.getTotalSize();
        stats.currentLedgerEntries = this.getCurrentLedgerEntries();
        stats.currentLedgerSize = this.getCurrentLedgerSize();
        stats.lastLedgerCreatedTimestamp = DateFormatter.format(this.getLastLedgerCreatedTimestamp());
        if (this.getLastLedgerCreationFailureTimestamp() != 0) {
            stats.lastLedgerCreationFailureTimestamp =
                    DateFormatter.format(this.getLastLedgerCreationFailureTimestamp());
        }

        stats.waitingCursorsCount = this.getWaitingCursorsCount();
        stats.pendingAddEntriesCount = this.getPendingAddEntriesCount();

        stats.lastConfirmedEntry = this.getLastConfirmedEntry().toString();
        stats.state = this.getState().toString();

        stats.cursors = new HashMap();
        this.getCursors().forEach(c -> {
            ManagedCursorImpl cursor = (ManagedCursorImpl) c;
            PersistentTopicInternalStats.CursorStats cs = new PersistentTopicInternalStats.CursorStats();
            cs.markDeletePosition = cursor.getMarkDeletedPosition().toString();
            cs.readPosition = cursor.getReadPosition().toString();
            cs.waitingReadOp = cursor.hasPendingReadRequest();
            cs.pendingReadOps = cursor.getPendingReadOpsCount();
            cs.messagesConsumedCounter = cursor.getMessagesConsumedCounter();
            cs.cursorLedger = cursor.getCursorLedger();
            cs.cursorLedgerLastEntry = cursor.getCursorLedgerLastEntry();
            cs.individuallyDeletedMessages = cursor.getIndividuallyDeletedMessages();
            cs.lastLedgerSwitchTimestamp = DateFormatter.format(cursor.getLastLedgerSwitchTimestamp());
            cs.state = cursor.getState();
            cs.active = cursor.isActive();
            cs.numberOfEntriesSinceFirstNotAckedMessage = cursor.getNumberOfEntriesSinceFirstNotAckedMessage();
            cs.totalNonContiguousDeletedMessagesRange = cursor.getTotalNonContiguousDeletedMessagesRange();
            cs.properties = cursor.getProperties();
            stats.cursors.put(cursor.getName(), cs);
        });

        // make a snapshot of the ledgers infos since we are iterating it twice when metadata is included
        // a list is sufficient since there's no need to lookup by the ledger id
        List<LedgerInfo> ledgersInfos = new ArrayList<>(this.getLedgersInfo().values());

        // add asynchronous metadata retrieval operations to a hashmap
        Map<Long, CompletableFuture<String>> ledgerMetadataFutures = new HashMap();
        if (includeLedgerMetadata) {
            ledgersInfos.forEach(li -> {
                long ledgerId = li.getLedgerId();
                ledgerMetadataFutures.put(ledgerId, this.getLedgerMetadata(ledgerId).exceptionally(throwable -> {
                    log.warn("Getting metadata for ledger {} failed.", ledgerId, throwable);
                    return null;
                }));
            });
        }

        // wait until metadata has been retrieved
        FutureUtil.waitForAll(ledgerMetadataFutures.values()).thenAccept(__ -> {
            stats.ledgers = new ArrayList();
            ledgersInfos.forEach(li -> {
                ManagedLedgerInternalStats.LedgerInfo info = new ManagedLedgerInternalStats.LedgerInfo();
                info.ledgerId = li.getLedgerId();
                info.entries = li.getEntries();
                info.size = li.getSize();
                info.offloaded = li.hasOffloadContext() && li.getOffloadContext().getComplete();
                if (includeLedgerMetadata) {
                    // lookup metadata from the hashmap which contains completed async operations
                    info.metadata = ledgerMetadataFutures.get(li.getLedgerId()).getNow(null);
                }
                if (li.getPropertiesCount() > 0) {
                    Map<String, String> properties = new HashMap<>(li.getPropertiesCount());
                    for (MLDataFormats.KeyValue kv : li.getPropertiesList()) {
                        properties.put(kv.getKey(), kv.getValue());
                    }
                    info.properties = properties;
                }
                stats.ledgers.add(info);
            });
            statFuture.complete(stats);
        });

        return statFuture;
    }

    public CompletableFuture<Set<BookieId>> getEnsemblesAsync(long ledgerId) {
        LedgerInfo ledgerInfo = ledgers.get(ledgerId);
        if (ledgerInfo != null && ledgerInfo.hasOffloadContext()) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        CompletableFuture<ReadHandle> ledgerHandleFuture;
        LedgerHandle currentLedger = this.currentLedger;
        if (currentLedger != null && ledgerId == currentLedger.getId()) {
            ledgerHandleFuture = CompletableFuture.completedFuture(currentLedger);
        } else {
            ledgerHandleFuture = getLedgerHandle(ledgerId);
        }

        return ledgerHandleFuture.thenCompose(lh -> {
            Set<BookieId> ensembles = new HashSet<>();
            lh.getLedgerMetadata().getAllEnsembles().values().forEach(ensembles::addAll);
            return CompletableFuture.completedFuture(ensembles);
        });
    }

    protected void updateLastLedgerCreatedTimeAndScheduleRolloverTask() {
        this.lastLedgerCreatedTimestamp = clock.millis();
        if (config.getMaximumRolloverTimeMs() > 0) {
            if (checkLedgerRollTask != null && !checkLedgerRollTask.isDone()) {
                // new ledger has been created successfully
                // and the previous checkLedgerRollTask is not done, we could cancel it
                checkLedgerRollTask.cancel(true);
            }
            this.checkLedgerRollTask = this.scheduledExecutor.schedule(
                    this::rollCurrentLedgerIfFull, this.maximumRolloverTimeMs, TimeUnit.MILLISECONDS);
        }
    }

    private void cancelScheduledTasks() {
        if (this.timeoutTask != null) {
            this.timeoutTask.cancel(false);
        }

        if (this.checkLedgerRollTask != null) {
            this.checkLedgerRollTask.cancel(false);
        }
    }

    @Override
    public boolean checkInactiveLedgerAndRollOver() {
        if (factory.isMetadataServiceAvailable()
                && currentLedgerEntries > 0
                && inactiveLedgerRollOverTimeMs > 0
                && System.currentTimeMillis() > (lastAddEntryTimeMs + inactiveLedgerRollOverTimeMs)) {
            log.info("[{}] Closing inactive ledger, last-add entry {}", name, lastAddEntryTimeMs);
            if (STATE_UPDATER.compareAndSet(this, State.LedgerOpened, State.ClosingLedger)) {
                LedgerHandle currentLedger = this.currentLedger;
                currentLedger.asyncClose((rc, lh, o) -> {
                    checkArgument(currentLedger.getId() == lh.getId(), "ledgerId %s doesn't match with "
                            + "acked ledgerId %s", currentLedger.getId(), lh.getId());

                    if (rc == BKException.Code.OK) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Successfully closed ledger {}, trigger by inactive ledger check",
                                    name, lh.getId());
                        }
                    } else {
                        log.warn("[{}] Error when closing ledger {}, trigger by inactive ledger check, Status={}",
                                name, lh.getId(), BKException.getMessage(rc));
                    }

                    ledgerClosed(lh);
                    // we do not create ledger here, since topic is inactive for a long time.
                }, null);
                return true;
            }
        }
        return false;
    }


    public void checkCursorsToCacheEntries() {
        if (minBacklogCursorsForCaching < 1) {
            return;
        }
        Iterator<ManagedCursor> it = cursors.iterator();
        Map<ManagedCursorImpl, Long> cursorBacklogMap = new HashMap<>();
        while (it.hasNext()) {
            ManagedCursorImpl cursor = (ManagedCursorImpl) it.next();
            if (cursor.isDurable()) {
                cursorBacklogMap.put(cursor, cursor.getNumberOfEntries());
            }
        }
        int cursorsInSameBacklogRange = 0;
        for (java.util.Map.Entry<ManagedCursorImpl, Long> cursor : cursorBacklogMap.entrySet()) {
            cursorsInSameBacklogRange = 0;
            for (java.util.Map.Entry<ManagedCursorImpl, Long> other : cursorBacklogMap.entrySet()) {
                if (cursor.equals(other)) {
                    continue;
                }
                long backlog = cursor.getValue();
                // if backlog difference is > maxBacklogBetweenCursorsForCaching (eg: 10000) then cached entry might be
                // invalidated by the time so, skip caching such long range messages.
                if (backlog < minBacklogEntriesForCaching) {
                    continue;
                }
                if (Math.abs(backlog - other.getValue()) <= maxBacklogBetweenCursorsForCaching) {
                    cursorsInSameBacklogRange++;
                }
            }
            cursor.getKey().setCacheReadEntry(cursorsInSameBacklogRange >= minBacklogCursorsForCaching);
            if (log.isDebugEnabled()) {
                log.info("{} Enabling cache read = {} for {}", name,
                        cursorsInSameBacklogRange >= minBacklogCursorsForCaching, cursor.getKey().getName());
            }
        }
    }

    public Position getTheSlowestNonDurationReadPosition() {
        PositionImpl theSlowestNonDurableReadPosition = PositionImpl.LATEST;
        for (ManagedCursor cursor : cursors) {
            if (cursor instanceof NonDurableCursorImpl) {
                PositionImpl readPosition = (PositionImpl) cursor.getReadPosition();
                if (readPosition.compareTo(theSlowestNonDurableReadPosition) < 0) {
                    theSlowestNonDurableReadPosition = readPosition;
                }
            }
        }
        return theSlowestNonDurableReadPosition;
    }
}