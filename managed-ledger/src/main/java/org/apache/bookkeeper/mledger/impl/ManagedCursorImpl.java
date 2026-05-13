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
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.mledger.ManagedLedgerException.getManagedLedgerException;
import static org.apache.bookkeeper.mledger.impl.EntryCountEstimator.estimateEntryCountByBytesSize;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import static org.apache.bookkeeper.mledger.util.Errors.isNoSuchLedgerExistsException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.RateLimiter;
import io.github.merlimat.slog.Logger;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.LongStream;
import lombok.Getter;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException.Code;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ScanCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.SkipEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorAttributes;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ScanOutcome;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.LongListMap;
import org.apache.bookkeeper.mledger.proto.LongProperty;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.NestedPositionInfo;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.mledger.proto.StringProperty;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.LongPairConsumer;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.RangeBoundConsumer;
import org.apache.pulsar.metadata.api.Stat;
import org.jspecify.annotations.Nullable;

@SuppressWarnings("checkstyle:javadoctype")
public class ManagedCursorImpl implements ManagedCursor {
    static final Comparator<Entry> ENTRY_COMPARATOR = (e1, e2) -> {
        if (e1.getLedgerId() != e2.getLedgerId()) {
            return e1.getLedgerId() < e2.getLedgerId() ? -1 : 1;
        }

        if (e1.getEntryId() != e2.getEntryId()) {
            return e1.getEntryId() < e2.getEntryId() ? -1 : 1;
        }

        return 0;
    };
    private static final Logger slog = Logger.get(ManagedCursorImpl.class);

    protected final BookKeeper bookkeeper;
    protected final ManagedLedgerImpl ledger;
    private final String name;
    protected final Logger log;

    private volatile Map<String, String> cursorProperties;
    private final BookKeeper.DigestType digestType;

    protected volatile Position markDeletePosition;

    // this position is have persistent mark delete position
    protected volatile Position persistentMarkDeletePosition;
    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, Position>
            INPROGRESS_MARKDELETE_PERSIST_POSITION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, Position.class,
                    "inProgressMarkDeletePersistPosition");
    protected volatile Position inProgressMarkDeletePersistPosition;

    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, Position> READ_POSITION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, Position.class, "readPosition");
    protected volatile Position readPosition;
    // keeps sample of last read-position for validation and monitoring if read-position is not moving forward.
    protected volatile Position statsLastReadPosition;

    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, MarkDeleteEntry>
            LAST_MARK_DELETE_ENTRY_UPDATER = AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class,
            MarkDeleteEntry.class, "lastMarkDeleteEntry");
    protected volatile MarkDeleteEntry lastMarkDeleteEntry;

    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, OpReadEntry> WAITING_READ_OP_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, OpReadEntry.class, "waitingReadOp");
    @SuppressWarnings("unused")
    private volatile OpReadEntry waitingReadOp = null;

    public static final int FALSE = 0;
    public static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<ManagedCursorImpl> RESET_CURSOR_IN_PROGRESS_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ManagedCursorImpl.class, "resetCursorInProgress");
    @SuppressWarnings("unused")
    private volatile int resetCursorInProgress = FALSE;
    private static final AtomicIntegerFieldUpdater<ManagedCursorImpl> PENDING_READ_OPS_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ManagedCursorImpl.class, "pendingReadOps");
    @SuppressWarnings("unused")
    private volatile int pendingReadOps = 0;

    private static final AtomicLongFieldUpdater<ManagedCursorImpl> MSG_CONSUMED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ManagedCursorImpl.class, "messagesConsumedCounter");
    // This counters are used to compute the numberOfEntries and numberOfEntriesInBacklog values, without having to look
    // at the list of ledgers in the ml. They are initialized to (-backlog) at opening, and will be incremented each
    // time a message is read or deleted.
    protected volatile long messagesConsumedCounter;

    // Current ledger used to append the mark-delete position
    @VisibleForTesting
    volatile LedgerHandle cursorLedger;

    // Wether the current cursorLedger is read-only or writable
    private boolean isCursorLedgerReadOnly = true;
    private boolean ledgerForceRecovery;

    // Stat of the cursor z-node
    // NOTE: Don't update cursorLedgerStat alone,
    // please use updateCursorLedgerStat method to update cursorLedgerStat and managedCursorInfo at the same time.
    private volatile Stat cursorLedgerStat;
    private volatile ManagedCursorInfo managedCursorInfo;

    private static final LongPairConsumer<Position> positionRangeConverter = PositionFactory::create;

    private static final RangeBoundConsumer<Position> positionRangeReverseConverter =
            (position) -> new LongPairRangeSet.LongPair(position.getLedgerId(), position.getEntryId());

    private static final LongPairConsumer<PositionRecyclable> recyclePositionRangeConverter = PositionRecyclable::get;
    protected final RangeSetWrapper<Position> individualDeletedMessages;

    // Maintain the deletion status for batch messages
    // (ledgerId, entryId) -> deletion indexes
    @Getter
    @VisibleForTesting
    @Nullable protected final ConcurrentSkipListMap<Position, BitSet> batchDeletedIndexes;
    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Reusable LightProto object for cursor position serialization (used only from persistPositionToLedger)
    private final PositionInfo reusablePositionInfo = new PositionInfo();

    private RateLimiter markDeleteLimiter;
    // The cursor is considered "dirty" when there are mark-delete updates that are only applied in memory,
    // because of the rate limiting.
    private volatile boolean isDirty = false;

    private boolean alwaysInactive = false;

    private static final long NO_MAX_SIZE_LIMIT = ManagedLedgerUtils.NO_MAX_SIZE_LIMIT;

    private long entriesReadCount;
    private long entriesReadSize;
    private int individualDeletedMessagesSerializedSize;
    private static final String COMPACTION_CURSOR_NAME = "__compaction";
    private volatile boolean cacheReadEntry = false;

    // active state cache in ManagedCursor. It should be in sync with the state in activeCursors in ManagedLedger.
    private volatile boolean isActive = false;

    // Emit the truncation WARN logs exactly once per crossing.
    private final AtomicBoolean lastCursorDataFullyPersistable = new AtomicBoolean(true);
    private final AtomicBoolean lastBatchDeletedIndexFullyPersistable = new AtomicBoolean(true);

    // This is a lock used to update the registration state of the cursor in the managed ledger.
    private final Object registerToWaitingCursorsLock = new Object();
    // This is used to track if the cursor is registered in the managed ledger's waitingCursors queue
    boolean registeredToWaitingCursors = false;

    class MarkDeleteEntry {
        final Position newPosition;
        final MarkDeleteCallback callback;
        final Object ctx;
        final Map<String, Long> properties;
        final Runnable alignAcknowledgeStatusAfterPersisted;

        // If the callbackGroup is set, it means this mark-delete request was done on behalf of a group of request (just
        // persist the last one in the chain). In this case we need to trigger the callbacks for every request in the
        // group.
        List<MarkDeleteEntry> callbackGroup;

        public MarkDeleteEntry(Position newPosition, Map<String, Long> properties,
                MarkDeleteCallback callback, Object ctx) {
            this(newPosition, properties, callback, ctx, null);
        }

        public MarkDeleteEntry(Position newPosition, Map<String, Long> properties,
                MarkDeleteCallback callback, Object ctx, Runnable alignAcknowledgeStatusAfterPersisted) {
            if (alignAcknowledgeStatusAfterPersisted == null) {
                alignAcknowledgeStatusAfterPersisted = () -> {
                    if (batchDeletedIndexes != null) {
                        batchDeletedIndexes.subMap(PositionFactory.EARLIEST,
                                false, PositionFactory.create(newPosition.getLedgerId(),
                                        newPosition.getEntryId()), true).clear();
                    }
                    persistentMarkDeletePosition = newPosition;
                };
            }
            this.newPosition = newPosition;
            this.properties = properties;
            this.callback = callback;
            this.ctx = ctx;
            this.alignAcknowledgeStatusAfterPersisted = alignAcknowledgeStatusAfterPersisted;
        }

        public void triggerComplete() {
            // Trigger the final callback after having (eventually) triggered the switchin-ledger operation. This
            // will ensure that no race condition will happen between the next mark-delete and the switching
            // operation.
            if (callbackGroup != null) {
                // Trigger the callback for every request in the group
                for (MarkDeleteEntry e : callbackGroup) {
                    e.callback.markDeleteComplete(e.ctx);
                }
            } else if (callback != null) {
                // Only trigger the callback for the current request
                callback.markDeleteComplete(ctx);
            }
        }

        public void alignAcknowledgeStatus() {
            this.alignAcknowledgeStatusAfterPersisted.run();
        }

        public void triggerFailed(ManagedLedgerException exception) {
            if (callbackGroup != null) {
                for (MarkDeleteEntry e : callbackGroup) {
                    e.callback.markDeleteFailed(exception, e.ctx);
                }
            } else if (callback != null) {
                callback.markDeleteFailed(exception, ctx);
            }
        }
    }

    protected final ArrayDeque<MarkDeleteEntry> pendingMarkDeleteOps = new ArrayDeque<>();
    private static final AtomicIntegerFieldUpdater<ManagedCursorImpl> PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ManagedCursorImpl.class, "pendingMarkDeletedSubmittedCount");
    @SuppressWarnings("unused")
    private volatile int pendingMarkDeletedSubmittedCount = 0;
    private volatile long lastLedgerSwitchTimestamp;
    private final Clock clock;

    // The last active time (Unix time, milliseconds) of the cursor
    private volatile long lastActive;

    public enum State {
        Uninitialized(false), // Cursor is being initialized
        NoLedger(false), // There is no metadata ledger open for writing
        Open(false), // Metadata ledger is ready
        SwitchingLedger(false), // The metadata ledger is being switched
        Closing(true), // The managed cursor is closing
        Closed(true), // The managed cursor has been closed
        Deleting(true), // The managed cursor is being deleted
        Deleted(true), // The managed cursor has been deleted
        DeletingFailed(true); // The managed cursor deletion failed, state allows retrying deletion.

        // Indicate if the cursor is in a state that is considered closed
        private final boolean closedState;

        State(boolean closedState) {
            this.closedState = closedState;
        }

        /**
         * Returns true if the state is considered closed.
         */
        public boolean isClosed() {
            return closedState;
        }

        public boolean isDeletingOrDeleted() {
            return this == Deleting || this == Deleted;
        }
    }

    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, State> STATE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, State.class, "state");
    protected volatile State state = State.Uninitialized;

    protected final ManagedCursorMXBean mbean;

    private volatile ManagedCursorAttributes managedCursorAttributes;
    private static final AtomicReferenceFieldUpdater<ManagedCursorImpl, ManagedCursorAttributes> ATTRIBUTES_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, ManagedCursorAttributes.class,
                    "managedCursorAttributes");

    @SuppressWarnings("checkstyle:javadoctype")
    public interface VoidCallback {
        void operationComplete();

        void operationFailed(ManagedLedgerException exception);
    }

    @VisibleForTesting
    protected ManagedCursorImpl(BookKeeper bookkeeper, ManagedLedgerImpl ledger, String cursorName) {
        this.bookkeeper = bookkeeper;
        this.cursorProperties = Collections.emptyMap();
        this.ledger = ledger;
        this.name = cursorName;
        this.log = slog.with().attr("managedLedger", ledger.getName()).attr("cursor", name).build();
        this.individualDeletedMessages = new RangeSetWrapper<>(positionRangeConverter,
                positionRangeReverseConverter, this);
        if (getConfig().isDeletionAtBatchIndexLevelEnabled()) {
            this.batchDeletedIndexes = new ConcurrentSkipListMap<>();
        } else {
            this.batchDeletedIndexes = null;
        }
        this.digestType = BookKeeper.DigestType.fromApiDigestType(getConfig().getDigestType());
        PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.set(this, 0);
        PENDING_READ_OPS_UPDATER.set(this, 0);
        RESET_CURSOR_IN_PROGRESS_UPDATER.set(this, FALSE);
        WAITING_READ_OP_UPDATER.set(this, null);
        this.clock = getConfig().getClock();
        this.lastActive = this.clock.millis();
        this.lastLedgerSwitchTimestamp = this.clock.millis();

        if (getConfig().getThrottleMarkDelete() > 0.0) {
            markDeleteLimiter = RateLimiter.create(getConfig().getThrottleMarkDelete());
        } else {
            // Disable mark-delete rate limiter
            markDeleteLimiter = null;
        }
        this.mbean = new ManagedCursorMXBeanImpl(this);
        this.ledgerForceRecovery = getConfig().isLedgerForceRecovery();
    }

    private void updateCursorLedgerStat(ManagedCursorInfo cursorInfo, Stat stat) {
        this.managedCursorInfo = cursorInfo;
        this.cursorLedgerStat = stat;
    }

    @Override
    public Map<String, Long> getProperties() {
        return lastMarkDeleteEntry != null ? lastMarkDeleteEntry.properties : Collections.emptyMap();
    }

    @Override
    public boolean isCursorDataFullyPersistable() {
        lock.readLock().lock();
        try {
            return individualDeletedMessages.size() <= getConfig().getMaxUnackedRangesToPersist();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<String, String> getCursorProperties() {
        return cursorProperties;
    }

    private CompletableFuture<Void> computeCursorProperties(
            final Function<Map<String, String>, Map<String, String>> updateFunction) {
        CompletableFuture<Void> updateCursorPropertiesResult = new CompletableFuture<>();

        final Stat lastCursorLedgerStat = ManagedCursorImpl.this.cursorLedgerStat;

        Map<String, String> newProperties = updateFunction.apply(ManagedCursorImpl.this.cursorProperties);
        if (!isDurable()) {
            this.cursorProperties = Collections.unmodifiableMap(newProperties);
            updateCursorPropertiesResult.complete(null);
            return updateCursorPropertiesResult;
        }

        ManagedCursorInfo copy = new ManagedCursorInfo();
        copy.copyFrom(ManagedCursorImpl.this.managedCursorInfo);
        copy.clearCursorProperties();
        copy.addAllCursorProperties(buildStringPropertiesMap(newProperties));

        ledger.getStore().asyncUpdateCursorInfo(ledger.getName(),
                name, copy, lastCursorLedgerStat, new MetaStoreCallback<>() {
                    @Override
                    public void operationComplete(Void result, Stat stat) {
                        log.info("Updated ledger cursor");
                        ManagedCursorImpl.this.cursorProperties = Collections.unmodifiableMap(newProperties);
                        updateCursorLedgerStat(copy, stat);
                        updateCursorPropertiesResult.complete(result);
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        log.error()
                                .attr("properties", newProperties)
                                .exception(e)
                                .log("Error while updating ledger cursor properties");
                        updateCursorPropertiesResult.completeExceptionally(e);
                    }
                });

        return updateCursorPropertiesResult;
    }

    @Override
    public CompletableFuture<Void> setCursorProperties(Map<String, String> cursorProperties) {
        Map<String, String> newProperties =
                cursorProperties == null ? new HashMap<>() : new HashMap<>(cursorProperties);

        // Prohibit setting of internal properties
        Set<String> keys = newProperties.keySet();
        for (String key : keys) {
            if (key.startsWith(CURSOR_INTERNAL_PROPERTY_PREFIX)) {
                return FutureUtil.failedFuture(new IllegalArgumentException(
                        "The property key can't start with " + CURSOR_INTERNAL_PROPERTY_PREFIX));
            }
        }

        return computeCursorProperties(lastRead -> {
            if (lastRead != null) {
                lastRead.forEach((k, v) -> {
                    if (k.startsWith(CURSOR_INTERNAL_PROPERTY_PREFIX)) {
                        newProperties.put(k, v);
                    }
                });
            }
            return newProperties;
        });
    }

    @Override
    public CompletableFuture<Void> putCursorProperty(String key, String value) {
        return computeCursorProperties(lastRead -> {
            Map<String, String> newProperties = lastRead == null ? new HashMap<>() : new HashMap<>(lastRead);
            newProperties.put(key, value);
            return newProperties;
        });
    }

    @Override
    public CompletableFuture<Void> removeCursorProperty(String key) {
        return computeCursorProperties(lastRead -> {
            Map<String, String> newProperties = lastRead == null ? new HashMap<>() : new HashMap<>(lastRead);
            newProperties.remove(key);
            return newProperties;
        });
    }

    @Override
    public boolean putProperty(String key, Long value) {
        if (lastMarkDeleteEntry != null) {
            LAST_MARK_DELETE_ENTRY_UPDATER.updateAndGet(this, last -> {
                Map<String, Long> properties = last.properties;
                Map<String, Long> newProperties = properties == null ? new HashMap<>() : new HashMap<>(properties);
                newProperties.put(key, value);

                MarkDeleteEntry newLastMarkDeleteEntry = new MarkDeleteEntry(last.newPosition, newProperties,
                        last.callback, last.ctx);
                newLastMarkDeleteEntry.callbackGroup = last.callbackGroup;

                return newLastMarkDeleteEntry;
            });
            return true;
        }
        return false;
    }

    @Override
    public boolean removeProperty(String key) {
        if (lastMarkDeleteEntry != null) {
            LAST_MARK_DELETE_ENTRY_UPDATER.updateAndGet(this, last -> {
                Map<String, Long> properties = last.properties;
                if (properties != null && properties.containsKey(key)) {
                    Map<String, Long> newProperties = new HashMap<>(properties);
                    newProperties.remove(key);

                    MarkDeleteEntry newLastMarkDeleteEntry = new MarkDeleteEntry(last.newPosition, newProperties,
                            last.callback, last.ctx);
                    newLastMarkDeleteEntry.callbackGroup = last.callbackGroup;

                    return newLastMarkDeleteEntry;
                }
                return last;
            });
            return true;
        }
        return false;
    }

    /**
     * Performs the initial recovery, reading the mark-deleted position from the ledger and then calling initialize to
     * have a new opened ledger.
     */
    void recover(final VoidCallback callback) {
        // Read the meta-data ledgerId from the store
        log.info("Recovering from bookkeeper ledger cursor");
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), name, new MetaStoreCallback<ManagedCursorInfo>() {
            @Override
            public void operationComplete(ManagedCursorInfo info, Stat stat) {
                updateCursorLedgerStat(info, stat);

                log.debug().attr("lastActive", lastActive).log("Recover cursor last active");

                Map<String, String> recoveredCursorProperties = Collections.emptyMap();
                if (info.getCursorPropertiesCount() > 0) {
                    // Recover properties map
                    recoveredCursorProperties = new HashMap<>();
                    for (int i = 0; i < info.getCursorPropertiesCount(); i++) {
                        StringProperty property = info.getCursorPropertyAt(i);
                        recoveredCursorProperties.put(property.getName(), property.getValue());
                    }
                }
                cursorProperties = recoveredCursorProperties;

                if (info.getCursorsLedgerId() == -1L) {
                    // There is no cursor ledger to read the last position from. It means the cursor has been properly
                    // closed and the last mark-delete position is stored in the ManagedCursorInfo itself.
                    Position recoveredPosition = PositionFactory.create(info.getMarkDeleteLedgerId(),
                            info.getMarkDeleteEntryId());
                    if (info.getIndividualDeletedMessagesCount() > 0) {
                        recoverIndividualDeletedMessages(info.getIndividualDeletedMessagesCount(),
                                info::getIndividualDeletedMessageAt);
                    }

                    Map<String, Long> recoveredProperties = Collections.emptyMap();
                    if (info.getPropertiesCount() > 0) {
                        // Recover properties map
                        recoveredProperties = new HashMap<>();
                        for (int i = 0; i < info.getPropertiesCount(); i++) {
                            LongProperty property = info.getPropertyAt(i);
                            recoveredProperties.put(property.getName(), property.getValue());
                        }
                    }

                    recoveredCursor(recoveredPosition, recoveredProperties, recoveredCursorProperties, null);
                    callback.operationComplete();
                } else {
                    // Need to proceed and read the last entry in the specified ledger to find out the last position
                    log.info().attr("cursorLedgerId", info.getCursorsLedgerId()).log("Meta-data recover from ledger");
                    recoverFromLedger(info, callback);
                }
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                callback.operationFailed(e);
            }
        });
    }

    protected void recoverFromLedger(final ManagedCursorInfo info, final VoidCallback callback) {
        // Read the acknowledged position from the metadata ledger, then create
        // a new ledger and write the position into it
        ledger.mbean.startCursorLedgerOpenOp();
        long ledgerId = info.getCursorsLedgerId();
        OpenCallback openCallback = (rc, lh, ctx) -> {
            log.info().attr("ledgerId", ledgerId).attr("rc", rc).log("Opened ledger");
            if (isBkErrorNotRecoverable(rc) || (rc != BKException.Code.OK && ledgerForceRecovery)) {
                log.error()
                        .attr("ledgerId", ledgerId)
                        .attr("errorMessage", BKException.getMessage(rc))
                        .log("Error opening metadata ledger");
                // Rewind to the oldest entry available
                initialize(getRollbackPosition(info), Collections.emptyMap(), cursorProperties, callback);
                return;
            } else if (rc != BKException.Code.OK) {
                log.warn()
                        .attr("ledgerId", ledgerId)
                        .attr("errorMessage", BKException.getMessage(rc))
                        .log("Error opening metadata ledger");
                callback.operationFailed(new ManagedLedgerException(BKException.getMessage(rc)));
                return;
            }

            // Read the last entry in the ledger
            long lastEntryInLedger = lh.getLastAddConfirmed();

            if (lastEntryInLedger < 0) {
                log.warn().attr("ledgerId", ledgerId).log("Error reading from metadata ledger: no entries in ledger");
                // Rewind to last cursor snapshot available
                initialize(getRollbackPosition(info), Collections.emptyMap(), cursorProperties, callback);
                return;
            }

            lh.asyncReadEntries(lastEntryInLedger, lastEntryInLedger, (rc1, lh1, seq, ctx1) -> {
                log.debug().attr("rc", rc1).attr("entryId", lh1.getLastAddConfirmed()).log("readComplete");
                if (isBkErrorNotRecoverable(rc1) || (rc1 != BKException.Code.OK && ledgerForceRecovery)) {
                    log.error()
                            .attr("ledgerId", ledgerId)
                            .attr("errorMessage", BKException.getMessage(rc1))
                            .log("Error reading from metadata ledger");
                    // Rewind to the oldest entry available
                    initialize(getRollbackPosition(info), Collections.emptyMap(), cursorProperties, callback);
                    return;
                } else if (rc1 != BKException.Code.OK) {
                    log.warn()
                            .attr("ledgerId", ledgerId)
                            .attr("errorMessage", BKException.getMessage(rc1))
                            .log("Error reading from metadata ledger");

                    callback.operationFailed(createManagedLedgerException(rc1));
                    return;
                }

                LedgerEntry entry = seq.nextElement();
                mbean.addReadCursorLedgerSize(entry.getLength());
                PositionInfo positionInfo = new PositionInfo();
                try {
                    positionInfo.parseFrom(entry.getEntry());
                } catch (Exception e) {
                    callback.operationFailed(new ManagedLedgerException(e));
                    return;
                }

                Map<String, Long> recoveredProperties = Collections.emptyMap();
                if (positionInfo.getPropertiesCount() > 0) {
                    // Recover properties map
                    recoveredProperties = new HashMap<>();
                    for (int i = 0; i < positionInfo.getPropertiesCount(); i++) {
                        LongProperty property = positionInfo.getPropertyAt(i);
                        recoveredProperties.put(property.getName(), property.getValue());
                    }
                }

                Position position = PositionFactory.create(positionInfo.getLedgerId(), positionInfo.getEntryId());
                recoverIndividualDeletedMessages(positionInfo);
                if (getConfig().isDeletionAtBatchIndexLevelEnabled()
                    && positionInfo.getBatchedEntryDeletionIndexInfosCount() > 0) {
                    recoverBatchDeletedIndexes(positionInfo.getBatchedEntryDeletionIndexInfosCount(),
                            positionInfo::getBatchedEntryDeletionIndexInfoAt);
                }
                recoveredCursor(position, recoveredProperties, cursorProperties, lh);
                callback.operationComplete();
            }, null);
        };
        try {
            bookkeeper.asyncOpenLedger(ledgerId, digestType, getConfig().getPassword(), openCallback,
                    null, true);
        } catch (Throwable t) {
            log.error().attr("ledgerId", ledgerId).exception(t).log("Encountered error on opening cursor ledger");
            openCallback.openComplete(BKException.Code.UnexpectedConditionException, null, null);
        }
    }

    public void recoverIndividualDeletedMessages(PositionInfo positionInfo) {
        if (positionInfo.getIndividualDeletedMessagesCount() > 0) {
            recoverIndividualDeletedMessages(positionInfo.getIndividualDeletedMessagesCount(),
                    positionInfo::getIndividualDeletedMessageAt);
        } else if (positionInfo.getIndividualDeletedMessageRangesCount() > 0) {
            lock.writeLock().lock();
            try {
                Map<Long, long[]> rangeMap = new HashMap<>(positionInfo.getIndividualDeletedMessageRangesCount());
                for (int i = 0; i < positionInfo.getIndividualDeletedMessageRangesCount(); i++) {
                    LongListMap list = positionInfo.getIndividualDeletedMessageRangeAt(i);
                    long[] values = new long[list.getValuesCount()];
                    for (int idx = 0; idx < values.length; idx++) {
                        values[idx] = list.getValueAt(idx);
                    }
                    rangeMap.put(list.getKey(), values);
                }
                // Guarantee compatability for the config "unackedRangesOpenCacheSetEnabled".
                if (getConfig().isUnackedRangesOpenCacheSetEnabled()) {
                    individualDeletedMessages.build(rangeMap);
                } else {
                    RangeSetWrapper<Position> rangeSetWrapperV2 = new RangeSetWrapper<>(positionRangeConverter,
                            positionRangeReverseConverter, true,
                            getConfig().isPersistentUnackedRangesWithMultipleEntriesEnabled());
                    rangeSetWrapperV2.build(rangeMap);
                    rangeSetWrapperV2.forEach(range -> {
                        individualDeletedMessages.addOpenClosed(range.lowerEndpoint().getLedgerId(),
                                range.lowerEndpoint().getEntryId(), range.upperEndpoint().getLedgerId(),
                                range.upperEndpoint().getEntryId());
                        return true;
                    });
                    rangeSetWrapperV2.clear();
                }
            } catch (Exception e) {
                log.warn().exception(e).log("Failed to recover individualDeletedMessages from serialized data");
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private List<LongListMap> buildLongPropertiesMap(Map<Long, long[]> properties) {
        if (properties.isEmpty()) {
            return Collections.emptyList();
        }
        List<LongListMap> longListMap = new ArrayList<>();
        MutableInt serializedSize = new MutableInt();
        properties.forEach((id, ranges) -> {
            if (ranges == null || ranges.length <= 0) {
                return;
            }
            LongListMap lm = new LongListMap().setKey(id);
            for (long range : ranges) {
                lm.addValue(range);
            }
            longListMap.add(lm);
            serializedSize.add(lm.getSerializedSize());
        });
        individualDeletedMessagesSerializedSize = serializedSize.toInteger();
        return longListMap;
    }

    private void recoverIndividualDeletedMessages(int count, IntFunction<MessageRange> accessor) {
        lock.writeLock().lock();
        try {
            individualDeletedMessages.clear();
            for (int i = 0; i < count; i++) {
                MessageRange messageRange = accessor.apply(i);
                NestedPositionInfo lowerEndpoint = messageRange.getLowerEndpoint();
                NestedPositionInfo upperEndpoint = messageRange.getUpperEndpoint();

                if (lowerEndpoint.getLedgerId() == upperEndpoint.getLedgerId()) {
                    individualDeletedMessages.addOpenClosed(lowerEndpoint.getLedgerId(), lowerEndpoint.getEntryId(),
                            upperEndpoint.getLedgerId(), upperEndpoint.getEntryId());
                } else {
                    // Store message ranges after splitting them by ledger ID
                    LedgerInfo lowerEndpointLedgerInfo = ledger.getLedgersInfo().get(lowerEndpoint.getLedgerId());
                    if (lowerEndpointLedgerInfo != null) {
                        individualDeletedMessages.addOpenClosed(lowerEndpoint.getLedgerId(), lowerEndpoint.getEntryId(),
                                lowerEndpoint.getLedgerId(), lowerEndpointLedgerInfo.getEntries() - 1);
                    } else {
                        log.warn()
                                .attr("ledgerId", lowerEndpoint.getLedgerId())
                                .attr("entryId", lowerEndpoint.getEntryId())
                                .log("No ledger info of lower endpoint");
                    }

                    for (LedgerInfo li : ledger.getLedgersInfo()
                            .subMap(lowerEndpoint.getLedgerId(), false, upperEndpoint.getLedgerId(), false).values()) {
                        individualDeletedMessages.addOpenClosed(li.getLedgerId(), -1, li.getLedgerId(),
                                li.getEntries() - 1);
                    }

                    individualDeletedMessages.addOpenClosed(upperEndpoint.getLedgerId(), -1,
                            upperEndpoint.getLedgerId(), upperEndpoint.getEntryId());
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void recoverBatchDeletedIndexes(int count, IntFunction<BatchedEntryDeletionIndexInfo> accessor) {
        Objects.requireNonNull(batchDeletedIndexes);
        lock.writeLock().lock();
        try {
            this.batchDeletedIndexes.clear();
            for (int i = 0; i < count; i++) {
                BatchedEntryDeletionIndexInfo batchDeletedIndexInfo = accessor.apply(i);
                if (batchDeletedIndexInfo.getDeleteSetsCount() > 0) {
                    long[] array = new long[batchDeletedIndexInfo.getDeleteSetsCount()];
                    for (int j = 0; j < array.length; j++) {
                        array[j] = batchDeletedIndexInfo.getDeleteSetAt(j);
                    }
                    this.batchDeletedIndexes.put(
                            PositionFactory.create(batchDeletedIndexInfo.getPosition().getLedgerId(),
                                    batchDeletedIndexInfo.getPosition().getEntryId()), BitSet.valueOf(array));
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void recoveredCursor(Position position, Map<String, Long> properties,
                                 Map<String, String> cursorProperties,
                                 LedgerHandle recoveredFromCursorLedger) {
        // if the position was at a ledger that didn't exist (since it will be deleted if it was previously empty),
        // we need to move to the next existing ledger
        if (position.getEntryId() == -1L && !ledger.ledgerExists(position.getLedgerId())) {
            Long nextExistingLedger = ledger.getNextValidLedger(position.getLedgerId());
            if (nextExistingLedger == null) {
                log.info().attr("position", position).log("Couldn't find next valid ledger for recovery");
            }
            position = nextExistingLedger != null ? PositionFactory.create(nextExistingLedger, -1) : position;
        }
        if (position.compareTo(ledger.getLastPosition()) > 0) {
            log.warn()
                    .attr("position", position)
                    .attr("lastPosition", ledger.getLastPosition())
                    .log("Current position is ahead of last position");
            position = ledger.getLastPosition();
        }
        log.info().attr("position", position).log("Recovered cursor");
        this.cursorProperties = cursorProperties == null ? Collections.emptyMap() : cursorProperties;
        messagesConsumedCounter = -getNumberOfEntries(Range.openClosed(position, ledger.getLastPosition()));
        markDeletePosition = position;
        persistentMarkDeletePosition = position;
        inProgressMarkDeletePersistPosition = null;
        readPosition = ledger.getNextValidPosition(position);
        ledger.onCursorReadPositionUpdated(this, readPosition);
        lastMarkDeleteEntry = new MarkDeleteEntry(markDeletePosition, properties, null, null);
        // assign cursor-ledger so, it can be deleted when new ledger will be switched
        this.cursorLedger = recoveredFromCursorLedger;
        this.isCursorLedgerReadOnly = true;
        changeStateIfNotClosed(State.NoLedger);
    }

    /**
     * Change the state of the cursor if it is not already considered closed.
     * This is to prevent invalid state transitions when the cursor is already closed.
     *
     * @param newState The new state to set
     * @return The previous state of the cursor
     */
    private State changeStateIfNotClosed(State newState) {
        return STATE_UPDATER.getAndUpdate(this, current -> {
            if (current.isClosed()) {
                return current;
            }
            return newState;
        });
    }

    void initialize(Position position, Map<String, Long> properties, Map<String, String> cursorProperties,
                    final VoidCallback callback) {
        recoveredCursor(position, properties, cursorProperties, null);
        log.debug()
                .attr("messagesConsumedCounter", messagesConsumedCounter)
                .attr("markDeletePosition", markDeletePosition)
                .attr("readPosition", readPosition)
                .log("Cursor initialized");
        persistPositionMetaStore(cursorLedger != null ? cursorLedger.getId() : -1L, position, properties,
                new MetaStoreCallback<>() {
                    @Override
                    public void operationComplete(Void result, Stat stat) {
                        changeStateIfNotClosed(State.NoLedger);
                        callback.operationComplete();
                    }
                    @Override
                    public void operationFailed(MetaStoreException e) {
                        callback.operationFailed(e);
                    }
        }, false);
    }

    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
        checkArgument(numberOfEntriesToRead > 0);

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            List<Entry> entries = null;
        }

        final Result result = new Result();

        asyncReadEntries(numberOfEntriesToRead, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                result.entries = entries;
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null, PositionFactory.LATEST);

        counter.await();

        if (result.exception != null) {
            throw result.exception;
        }

        return result.entries;
    }

    @Override
    public void asyncReadEntries(final int numberOfEntriesToRead, final ReadEntriesCallback callback,
                                 final Object ctx, Position maxPosition) {
        asyncReadEntries(numberOfEntriesToRead, NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes, ReadEntriesCallback callback,
                                 Object ctx, Position maxPosition) {
        asyncReadEntriesWithSkip(numberOfEntriesToRead, maxSizeBytes, callback, ctx, maxPosition, null);
    }

    @Override
    public void asyncReadEntriesWithSkip(int numberOfEntriesToRead, long maxSizeBytes, ReadEntriesCallback callback,
                                 Object ctx, Position maxPosition, Predicate<Position> skipCondition) {
        checkArgument(numberOfEntriesToRead > 0);
        if (isClosed()) {
            callback.readEntriesFailed(new ManagedLedgerException
                    .CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        int numOfEntriesToRead = applyMaxSizeCap(numberOfEntriesToRead, maxSizeBytes);

        PENDING_READ_OPS_UPDATER.incrementAndGet(this);
        // Skip deleted entries.
        skipCondition = skipCondition == null ? this::isMessageDeleted : skipCondition.or(this::isMessageDeleted);
        OpReadEntry op =
            OpReadEntry.create(this, readPosition, numOfEntriesToRead, callback, ctx, maxPosition, skipCondition, true);
        ledger.asyncReadEntries(op);
    }

    @Override
    public Entry getNthEntry(int n, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException {

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            Entry entry = null;
        }

        final Result result = new Result();

        asyncGetNthEntry(n, deletedEntries, new ReadEntryCallback() {

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                result.entry = entry;
                counter.countDown();
            }

            @Override
            public String toString() {
                return String.format("Cursor [%s] get Nth entry", ManagedCursorImpl.this);
            }
        }, null);

        counter.await(ledger.getConfig().getMetadataOperationsTimeoutSeconds(), TimeUnit.SECONDS);

        if (result.exception != null) {
            throw result.exception;
        }

        return result.entry;
    }

    @Override
    public void asyncGetNthEntry(int n, IndividualDeletedEntries deletedEntries, ReadEntryCallback callback,
            Object ctx) {
        checkArgument(n > 0);
        if (isClosed()) {
            callback.readEntryFailed(new ManagedLedgerException
                    .CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        Position startPosition = ledger.getNextValidPosition(markDeletePosition);
        Position endPosition = ledger.getLastPosition();
        if (startPosition.compareTo(endPosition) <= 0) {
            long numOfEntries = getNumberOfEntries(Range.closed(startPosition, endPosition));
            if (numOfEntries >= n) {
                long deletedMessages = 0;
                if (deletedEntries == IndividualDeletedEntries.Exclude) {
                    deletedMessages = getNumIndividualDeletedEntriesToSkip(n);
                }
                Position positionAfterN = ledger.getPositionAfterN(markDeletePosition, n + deletedMessages,
                        PositionBound.startExcluded);
                ledger.asyncReadEntry(positionAfterN, callback, ctx);
            } else {
                callback.readEntryComplete(null, ctx);
            }
        } else {
            callback.readEntryComplete(null, ctx);
        }
    }

    @Override
    public List<Entry> readEntriesOrWait(int numberOfEntriesToRead)
            throws InterruptedException, ManagedLedgerException {
        return readEntriesOrWait(numberOfEntriesToRead, NO_MAX_SIZE_LIMIT);
    }

    @Override
    public List<Entry> readEntriesOrWait(int numberOfEntriesToRead, long maxSizeBytes)
            throws InterruptedException, ManagedLedgerException {
        checkArgument(numberOfEntriesToRead > 0);

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            List<Entry> entries = null;
        }

        final Result result = new Result();

        asyncReadEntriesOrWait(numberOfEntriesToRead, maxSizeBytes, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                result.entries = entries;
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null, PositionFactory.LATEST);

        counter.await();

        if (result.exception != null) {
            throw result.exception;
        }

        return result.entries;
    }

    @Override
    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx,
                                       Position maxPosition) {
        asyncReadEntriesOrWait(numberOfEntriesToRead, NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes, ReadEntriesCallback callback, Object ctx,
                                       Position maxPosition) {
        asyncReadEntriesWithSkipOrWait(maxEntries, maxSizeBytes, callback, ctx, maxPosition, null);
    }

    @Override
    public void asyncReadEntriesWithSkipOrWait(int maxEntries, ReadEntriesCallback callback,
                                               Object ctx, Position maxPosition,
                                               Predicate<Position> skipCondition) {
        asyncReadEntriesWithSkipOrWait(maxEntries, NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition, skipCondition);
    }

    @Override
    public void asyncReadEntriesWithSkipOrWait(int maxEntries, long maxSizeBytes, ReadEntriesCallback callback,
                                               Object ctx, Position maxPosition,
                                               Predicate<Position> skipCondition) {
        checkArgument(maxEntries > 0);
        if (isClosed()) {
            callback.readEntriesFailed(new CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        int numberOfEntriesToRead = applyMaxSizeCap(maxEntries, maxSizeBytes);

        if (hasMoreEntries() && maxPosition.compareTo(readPosition) >= 0) {
            // If we have available entries, we can read them immediately
            log.debug("Read entries immediately");
            asyncReadEntriesWithSkip(numberOfEntriesToRead, NO_MAX_SIZE_LIMIT, callback, ctx,
                    maxPosition, skipCondition);
        } else {
            // Skip deleted entries.
            skipCondition = skipCondition == null ? this::isMessageDeleted : skipCondition.or(this::isMessageDeleted);
            OpReadEntry op = OpReadEntry.create(this, readPosition, numberOfEntriesToRead, callback,
                    ctx, maxPosition, skipCondition, true);
            int opReadId = op.id;
            if (!WAITING_READ_OP_UPDATER.compareAndSet(this, null, op)) {
                op.recycle();
                callback.readEntriesFailed(new ManagedLedgerException.ConcurrentWaitCallbackException(), ctx);
                return;
            }

            log.debug().attr("readPosition", op.readPosition).log("Deferring retry of read at position");

            // Check again for new entries after the configured time, then if still no entries are available register
            // to be notified
            if (getConfig().getNewEntriesCheckDelayInMillis() > 0) {
                ledger.getScheduledExecutor().schedule(() -> checkForNewEntries(opReadId, op, callback, ctx),
                        getConfig().getNewEntriesCheckDelayInMillis(), TimeUnit.MILLISECONDS);
            } else {
                // If there's no delay, check directly from the same thread
                checkForNewEntries(opReadId, op, callback, ctx);
            }
        }
    }

    // Please notice that OpReadEntry might be recycled due to sharing via waitingReadOp field logic
    // That's why the fields cannot be accessed before the reference is removed from waitingReadOp atomically
    // and the id matches the removed reference.
    private void checkForNewEntries(int opReadId, OpReadEntry op, ReadEntriesCallback callback, Object ctx) {
        try {
            log.debug().attr("opReadId", opReadId).log("Re-trying the read for op id");

            if (isClosed()) {
                callback.readEntriesFailed(new CursorAlreadyClosedException("Cursor was already closed"), ctx);
                return;
            }

            if (!hasMoreEntries()) {
                log.debug("Still no entries available, registering for notification");
                // Let the managed ledger know we want to be notified whenever a new entry is published
                ledger.addWaitingCursor(this);
            } else {
                log.debug("Skipping notification registering, entries available");
            }

            // Check again the entries count, since an entry could have been written between the time we
            // checked and the time we've asked to be notified by managed ledger
            if (hasMoreEntries()) {
                log.debug("Found more entries");
                // Try to cancel the notification request
                // Clear the waiting read op only if it matches the current instance and the id matches
                // the opReadId parameter. This avoids recycled OpReadEntry instances from matching since their
                // ids would be different after recycling.
                OpReadEntry waitingReadOpItem = WAITING_READ_OP_UPDATER.getAndUpdate(this,
                        current -> {
                            if (current == op && current.id == opReadId) {
                                // update the value to null to cancel the waiting read op
                                return null;
                            } else {
                                // keep the current waiting read op value
                                return current;
                            }
                        });
                // If the waiting read op was the same as the one we are trying to cancel, it means that it was now
                // cleared from the waitingReadOp field and therefore "cancelled"
                if (waitingReadOpItem == op && waitingReadOpItem.id == opReadId) {
                    log.debug()
                            .attr("readPosition", op.readPosition)
                            .log("Cancelled notification and scheduled read at");
                    PENDING_READ_OPS_UPDATER.incrementAndGet(this);
                    ledger.asyncReadEntries(op);
                } else {
                    log.debug().attr("opReadId", opReadId).log("Notification was already cancelled for op id");
                }
            } else if (ledger.isTerminated()) {
                // At this point we registered for notification and still there were no more available
                // entries.
                // If the managed ledger was indeed terminated, we need to notify the cursor
                callback.readEntriesFailed(new NoMoreEntriesToReadException("Topic was terminated"), ctx);
            }
        } catch (Throwable t) {
            callback.readEntriesFailed(new ManagedLedgerException(t), ctx);
        }
    }

    @Override
    public boolean isClosed() {
        return state.isClosed();
    }

    @Override
    public boolean cancelPendingReadRequest() {
        log.debug("Cancel pending read request");
        final OpReadEntry op = WAITING_READ_OP_UPDATER.getAndUpdate(this, current -> {
            if (current == OpReadEntry.WAITING_READ_OP_FOR_CLOSED_CURSOR) {
                return current;
            }
            return null;
        });
        if (op != null) {
            op.recycle();
        }
        return op != null && op != OpReadEntry.WAITING_READ_OP_FOR_CLOSED_CURSOR;
    }

    public boolean hasPendingReadRequest() {
        OpReadEntry opReadEntry = WAITING_READ_OP_UPDATER.get(this);
        return opReadEntry != null && opReadEntry != OpReadEntry.WAITING_READ_OP_FOR_CLOSED_CURSOR;
    }

    @Override
    public boolean hasMoreEntries() {
        // If writer and reader are on the same ledger, we just need to compare the entry id to know if we have more
        // entries.
        // If they are on different ledgers we have 2 cases :
        // * Writer pointing to valid entry --> should return true since we have available entries
        // * Writer pointing to "invalid" entry -1 (meaning no entries in that ledger) --> Need to check if the reader
        // is
        // at the last entry in the previous ledger
        Position writerPosition = ledger.getLastPosition();
        if (writerPosition.getEntryId() != -1) {
            return readPosition.compareTo(writerPosition) <= 0;
        } else {
            // Fall back to checking the number of entries to ensure we are at the last entry in ledger and no ledgers
            // are in the middle
            return getNumberOfEntries() > 0;
        }
    }

    @Override
    public long getNumberOfEntries() {
        Position readPos = readPosition;
        Position lastPosition = ledger.getLastPosition();
        Position nextPosition = lastPosition.getNext();
        if (readPos.compareTo(nextPosition) > 0) {
            log.debug()
                    .attr("readPosition", readPos)
                    .attr("lastPosition", lastPosition)
                    .log("Read position is ahead of last position, no entries to read");
            return 0;
        } else {
            return getNumberOfEntries(Range.closedOpen(readPos, nextPosition));
        }
    }

    @Override
    public long getNumberOfEntriesSinceFirstNotAckedMessage() {
        // sometimes for already caught up consumer: due to race condition markDeletePosition > readPosition. so,
        // validate it before preparing range
        Position markDeletePosition = this.markDeletePosition;
        Position readPosition = this.readPosition;
        return (markDeletePosition != null && readPosition != null && markDeletePosition.compareTo(readPosition) < 0)
                ? ledger.getNumberOfEntries(Range.openClosed(markDeletePosition, readPosition))
                : 0;
    }

    @Override
    public int getTotalNonContiguousDeletedMessagesRange() {
        lock.readLock().lock();
        try {
            return individualDeletedMessages.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int getNonContiguousDeletedMessagesRangeSerializedSize() {
        return this.individualDeletedMessagesSerializedSize;
    }

    @Override
    public long getEstimatedSizeSinceMarkDeletePosition() {
        long totalSize = ledger.estimateBacklogFromPosition(markDeletePosition);

        // Need to subtract size of individual deleted messages
        log.debug()
                .attr("markDeletePosition", markDeletePosition)
                .attr("totalSize", totalSize)
                .log("Calculating backlog size");

        // Get count of individually deleted entries in the backlog range
        long deletedCount = 0;
        lock.readLock().lock();
        try {
            Range<Position> backlogRange = Range.openClosed(markDeletePosition, ledger.getLastPosition());

            if (getConfig().isUnackedRangesOpenCacheSetEnabled()) {
                deletedCount = individualDeletedMessages.cardinality(
                        backlogRange.lowerEndpoint().getLedgerId(), backlogRange.lowerEndpoint().getEntryId(),
                        backlogRange.upperEndpoint().getLedgerId(), backlogRange.upperEndpoint().getEntryId());
            } else {
                AtomicLong deletedCounter = new AtomicLong(0);
                individualDeletedMessages.forEach((r) -> {
                    if (r.isConnected(backlogRange)) {
                        Range<Position> intersection = r.intersection(backlogRange);
                        long countInRange = ledger.getNumberOfEntries(intersection);
                        deletedCounter.addAndGet(countInRange);
                    }
                    return true;
                }, recyclePositionRangeConverter);
                deletedCount = deletedCounter.get();
            }
        } finally {
            lock.readLock().unlock();
        }

        if (deletedCount == 0) {
            return totalSize;
        }

        // Estimate size by using average entry size from the backlog range
        Range<Position> backlogRange = Range.openClosed(markDeletePosition, ledger.getLastPosition());
        long totalEntriesInBacklog = ledger.getNumberOfEntries(backlogRange);

        if (totalEntriesInBacklog <= deletedCount || totalEntriesInBacklog == 0) {
            // Should not happen, but avoid division by zero
            log.warn()
                    .attr("totalEntriesInBacklog", totalEntriesInBacklog)
                    .attr("deletedCount", deletedCount)
                    .log("Inconsistent backlog state");
            return Math.max(0, totalSize);  // Return the total size and log the issue
        }

        // Calculate average size in the backlog range
        long averageSize = totalSize / totalEntriesInBacklog;

        // Subtract size of deleted entries
        long deletedSize = deletedCount * averageSize;
        long adjustedSize = totalSize - deletedSize;

        log.debug()
                .attr("totalSize", totalSize)
                .attr("deletedCount", deletedCount)
                .attr("averageSize", averageSize)
                .attr("deletedSize", deletedSize)
                .attr("adjustedSize", adjustedSize)
                .log("Adjusted backlog size");

        return adjustedSize;
    }

    private long getNumberOfEntriesInBacklog() {
        if (markDeletePosition.compareTo(ledger.getLastPosition()) >= 0) {
            return 0;
        }
        return getNumberOfEntries(Range.openClosed(markDeletePosition, ledger.getLastPosition()));
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean isPrecise) {
        log.debug()
                .attr("value", ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.get(ledger))
                .attr("messagesConsumedCounter", messagesConsumedCounter)
                .attr("markDeletePosition", markDeletePosition)
                .attr("readPosition", readPosition)
                .log("Cursor backlog counters");
        if (isPrecise) {
            return getNumberOfEntriesInBacklog();
        }

        long backlog = ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.get(ledger) - messagesConsumedCounter;
        if (backlog < 0) {
            // In some case the counters get incorrect values, fall back to the precise backlog count
            backlog = getNumberOfEntriesInBacklog();
        }

        return backlog;
    }

    public long getNumberOfEntriesInStorage() {
        return ledger.getNumberOfEntries(Range.openClosed(markDeletePosition, ledger.getLastPosition()));
    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
        return findNewestMatching(FindPositionConstraint.SearchActiveEntries, condition);
    }

    @Override
    public CompletableFuture<ScanOutcome> scan(Optional<Position> position,
                                               Predicate<Entry> condition,
                                               int batchSize, long maxEntries, long timeOutMs) {
        Position startPosition = position.orElseGet(
                () -> ledger.getNextValidPosition(markDeletePosition));
        CompletableFuture<ScanOutcome> future = new CompletableFuture<>();
        OpScan op = new OpScan(this, batchSize, startPosition, condition, new ScanCallback() {
            @Override
            public void scanComplete(Position position, ScanOutcome scanOutcome, Object ctx) {
                future.complete(scanOutcome);
            }

            @Override
            public void scanFailed(ManagedLedgerException exception,
                                   Optional<Position> failedReadPosition, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, maxEntries, timeOutMs);
        op.find();
        return future;
    }

    @Override
    public Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition)
            throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            Position position = null;
        }

        final Result result = new Result();
        asyncFindNewestMatching(constraint, condition, new FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                result.position = position;
                counter.countDown();
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition,
                    Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        counter.await();
        if (result.exception != null) {
            throw result.exception;
        }

        return result.position;
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
            FindEntryCallback callback, Object ctx) {
        asyncFindNewestMatching(constraint, condition, callback, ctx, false);
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
            FindEntryCallback callback, Object ctx, boolean isFindFromLedger) {
        asyncFindNewestMatching(constraint, condition, null, null, callback, ctx,
                isFindFromLedger);
    }


    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        Position start, Position end, FindEntryCallback callback,
                                        Object ctx, boolean isFindFromLedger) {
        Position startPosition;
        switch (constraint) {
            case SearchAllAvailableEntries ->
                    startPosition = start == null ?  getFirstPosition() : start;
            case SearchActiveEntries -> {
                if (start == null) {
                    startPosition = ledger.getNextValidPosition(markDeletePosition);
                } else {
                    startPosition = start;
                    startPosition = startPosition.compareTo(markDeletePosition) <= 0
                            ? ledger.getNextValidPosition(startPosition) : startPosition;
                }
            }
            default -> {
                callback.findEntryFailed(
                        new ManagedLedgerException("Unknown position constraint"), Optional.empty(), ctx);
                return;
            }
        }
        // startPosition can't be null, should never go here.
        if (startPosition == null) {
            callback.findEntryFailed(new ManagedLedgerException("Couldn't find start position"),
                    Optional.empty(), ctx);
            return;
        }
        // Calculate the end position
        Position endPosition = end == null ? ledger.lastConfirmedEntry : end;
        endPosition = endPosition.compareTo(ledger.lastConfirmedEntry) > 0 ? ledger.lastConfirmedEntry : endPosition;
        // Calculate the number of entries between the startPosition and endPosition
        long max = 0;
        if (startPosition.compareTo(endPosition) <= 0) {
            max = ledger.getNumberOfEntries(Range.closed(startPosition, endPosition));
        }

        if (max <= 0) {
            callback.findEntryComplete(null, ctx);
            return;
        }

        OpFindNewest op;
        if (isFindFromLedger) {
            op = new OpFindNewest(this.ledger, startPosition, condition, max, callback, ctx);
        } else {
            op = new OpFindNewest(this, startPosition, condition, max, callback, ctx);
        }
        op.find();
    }

    @Override
    public void setActive() {
        if (!isActive && !alwaysInactive) {
            ledger.activateCursor(this);
            isActive = true;
        }
    }

    @Override
    public boolean isActive() {
        return isActive;
    }

    @Override
    public void setInactive() {
        if (isActive) {
            ledger.deactivateCursor(this);
            isActive = false;
        }
    }

    @Override
    public void setAlwaysInactive() {
        setInactive();
        this.alwaysInactive = true;
    }

    @Override
    public Position getFirstPosition() {
        Long firstLedgerId = ledger.getLedgersInfo().firstKey();
        return firstLedgerId == null ? null : PositionFactory.create(firstLedgerId, 0);
    }

    protected void internalResetCursor(Position proposedReadPosition,
                                       AsyncCallbacks.ResetCursorCallback resetCursorCallback) {
        final Position newReadPosition;
        if (proposedReadPosition.equals(PositionFactory.EARLIEST)) {
            newReadPosition = ledger.getFirstPosition();
        } else if (proposedReadPosition.equals(PositionFactory.LATEST)) {
            newReadPosition = ledger.getNextValidPosition(ledger.getLastPosition());
        } else {
            newReadPosition = proposedReadPosition;
        }

        log.info()
                .attr("oldReadPosition", readPosition)
                .attr("newReadPosition", newReadPosition)
                .log("Initiate reset readPosition");

        synchronized (pendingMarkDeleteOps) {
            if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                log.error()
                        .attr("readPosition", newReadPosition)
                        .log("Reset requested, previous reset in progress");
                resetCursorCallback.resetFailed(
                        new ManagedLedgerException.ConcurrentFindCursorPositionException("reset already in progress"),
                        newReadPosition);
                return;
            }
        }

        final AsyncCallbacks.ResetCursorCallback callback = resetCursorCallback;

        final Position newMarkDeletePosition;
        if (isCompactionCursor()) {
            newMarkDeletePosition = markDeletePosition;
        } else {
            newMarkDeletePosition = ledger.getPreviousPosition(newReadPosition);
        }

        Runnable alignAcknowledgeStatusAfterPersisted = () -> {
            // Correct the variable "messagesConsumedCounter".
            // BTW, no need to change "messagesConsumedCounter" if new "markDeletePosition" is the same as the
            // old one.
            int compareRes = ledger.comparePositions(markDeletePosition, newMarkDeletePosition);
            if (compareRes > 0) {
                MSG_CONSUMED_COUNTER_UPDATER.addAndGet(cursorImpl(), -getNumberOfEntries(
                        Range.openClosed(newMarkDeletePosition, markDeletePosition)));
            } else if (compareRes < 0) {
                long entries = getNumberOfEntries(Range.openClosed(markDeletePosition, newMarkDeletePosition));
                MSG_CONSUMED_COUNTER_UPDATER.addAndGet(ManagedCursorImpl.this, entries);
            }
            individualDeletedMessages.removeAtMost(newMarkDeletePosition.getLedgerId(),
                    newMarkDeletePosition.getEntryId());

            // Entries already acknowledged, which is larger than the new mark deleted position.
            MutableLong ackedEntriesAfterMdPosition = new MutableLong();
            individualDeletedMessages.forEach((r) -> {
                for (long i = r.lowerEndpoint().getEntryId() + 1; i <= r.upperEndpoint().getEntryId(); i++) {
                    ackedEntriesAfterMdPosition.incrementAndGet();
                }
                return true;
            });
            MSG_CONSUMED_COUNTER_UPDATER.addAndGet(ManagedCursorImpl.this,
                    -ackedEntriesAfterMdPosition.get().longValue());
            markDeletePosition = newMarkDeletePosition;
            lastMarkDeleteEntry = new MarkDeleteEntry(newMarkDeletePosition, isCompactionCursor()
                    ? getProperties() : Collections.emptyMap(), null, null);
            individualDeletedMessages.clear();
            if (batchDeletedIndexes != null) {
                batchDeletedIndexes.clear();
                AckSetStateUtil.maybeGetAckSetState(newReadPosition).ifPresent(ackSetState -> {
                    long[] resetWords = ackSetState.getAckSet();
                    if (resetWords != null) {
                        batchDeletedIndexes.put(newReadPosition, BitSet.valueOf(resetWords));
                    }
                });
            }

            Position oldReadPosition = readPosition;
            if (oldReadPosition.compareTo(newReadPosition) >= 0) {
                log.info()
                        .attr("readPosition", newReadPosition)
                        .attr("oldReadPosition", oldReadPosition)
                        .log("Reset readPosition to before current readPosition");
            } else {
                log.info()
                        .attr("readPosition", newReadPosition)
                        .attr("oldReadPosition", oldReadPosition)
                        .log("Reset readPosition to, skipping from current readPosition");
            }
            readPosition = newReadPosition;
        };

        VoidCallback finalCallback = new VoidCallback() {
            @Override
            public void operationComplete() {

                // modify mark delete and read position since we are able to persist new position for cursor
                lock.writeLock().lock();
                try {
                    ledger.onCursorReadPositionUpdated(ManagedCursorImpl.this, newReadPosition);
                } finally {
                    lock.writeLock().unlock();
                }
                synchronized (pendingMarkDeleteOps) {
                    pendingMarkDeleteOps.clear();
                    if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(ManagedCursorImpl.this, TRUE, FALSE)) {
                        log.error()
                                .attr("readPosition", newReadPosition)
                                .log("Expected reset readPosition, but another reset in progress");
                    }
                }
                updateLastActive();
                callback.resetComplete(newReadPosition);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                synchronized (pendingMarkDeleteOps) {
                    if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(ManagedCursorImpl.this, TRUE, FALSE)) {
                        log.error()
                                .attr("readPosition", newReadPosition)
                                .log("Expected reset readPosition, but another reset in progress");
                    }
                }
                callback.resetFailed(new ManagedLedgerException.InvalidCursorPositionException(
                        "unable to persist readPosition for cursor reset " + newReadPosition), newReadPosition);
            }

        };

        persistentMarkDeletePosition = null;
        inProgressMarkDeletePersistPosition = null;
        internalAsyncMarkDelete(newMarkDeletePosition, isCompactionCursor() ? getProperties() : Collections.emptyMap(),
                new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                finalCallback.operationComplete();
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                finalCallback.operationFailed(exception);
            }
        }, null, alignAcknowledgeStatusAfterPersisted);
    }

    @Override
    public void asyncResetCursor(Position newPos, boolean forceReset, AsyncCallbacks.ResetCursorCallback callback) {
        final Position newPosition = newPos;

        // order trim and reset operations on a ledger
        ledger.getExecutor().execute(() -> {
            Position actualPosition = newPosition;

            if (!ledger.isValidPosition(actualPosition)
                    && !actualPosition.equals(PositionFactory.EARLIEST)
                    && !actualPosition.equals(PositionFactory.LATEST)
                    && !forceReset) {
                actualPosition = ledger.getNextValidPosition(actualPosition);

                if (actualPosition == null) {
                    // next valid position would only return null when newPos
                    // is larger than all available positions, then it's latest in effect.
                    actualPosition = PositionFactory.LATEST;
                }
            }

            internalResetCursor(actualPosition, callback);
        });
    }

    @Override
    public void resetCursor(Position newPos) throws ManagedLedgerException, InterruptedException {
        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncResetCursor(newPos, false, new AsyncCallbacks.ResetCursorCallback() {
            @Override
            public void resetComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();

            }
        });

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            if (result.exception != null) {
                log.warn()
                        .attr("position", newPos)
                        .attr("error", result.exception)
                        .log("Reset cursor timed out");
            }
            throw new ManagedLedgerException("Timeout during reset cursor");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public List<Entry> replayEntries(Set<? extends Position> positions)
            throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            List<Entry> entries = null;
        }

        final Result result = new Result();

        asyncReplayEntries(positions, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                result.entries = entries;
                counter.countDown();
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        counter.await();

        if (result.exception != null) {
            throw result.exception;
        }

        return result.entries;
    }

    /**
     * Async replays given positions: a. before reading it filters out already-acked messages b. reads remaining entries
     * async and gives it to given ReadEntriesCallback c. returns all already-acked messages which are not replayed so,
     * those messages can be removed by caller(Dispatcher)'s replay-list and it won't try to replay it again
     *
     */
    @Override
    public Set<? extends Position> asyncReplayEntries(final Set<? extends Position> positions,
            ReadEntriesCallback callback, Object ctx) {
        return asyncReplayEntries(positions, callback, ctx, false);
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
            ReadEntriesCallback callback, Object ctx, boolean sortEntries) {
        List<Entry> entries = Lists.newArrayListWithExpectedSize(positions.size());
        if (positions.isEmpty()) {
            callback.readEntriesComplete(entries, ctx);
            return Collections.emptySet();
        }

        // filters out messages which are already acknowledged
        Set<Position> alreadyAcknowledgedPositions = new HashSet<>();
        lock.readLock().lock();
        try {
            positions.stream().filter(this::internalIsMessageDeleted).forEach(alreadyAcknowledgedPositions::add);
        } finally {
            lock.readLock().unlock();
        }

        final int totalValidPositions = positions.size() - alreadyAcknowledgedPositions.size();
        final AtomicReference<ManagedLedgerException> exception = new AtomicReference<>();
        ReadEntryCallback cb = new ReadEntryCallback() {
            int pendingCallbacks = totalValidPositions;

            @Override
            public synchronized void readEntryComplete(Entry entry, Object ctx) {
                if (exception.get() != null) {
                    // if there is already a failure for a different position, we should release the entry straight away
                    // and not add it to the list
                    entry.release();
                    if (--pendingCallbacks == 0) {
                        callback.readEntriesFailed(exception.get(), ctx);
                    }
                } else {
                    entries.add(entry);
                    if (--pendingCallbacks == 0) {
                        if (sortEntries) {
                            entries.sort(ENTRY_COMPARATOR);
                        }
                        callback.readEntriesComplete(entries, ctx);
                    }
                }
            }

            @Override
            public synchronized void readEntryFailed(ManagedLedgerException mle, Object ctx) {
                log.warn().exception(mle).log("Error while replaying entries");
                if (exception.compareAndSet(null, mle)) {
                    // release the entries just once, any further read success will release the entry straight away
                    entries.forEach(Entry::release);
                }
                if (--pendingCallbacks == 0) {
                    callback.readEntriesFailed(exception.get(), ctx);
                }
            }

            @Override
            public String toString() {
                return String.format("Cursor [%s] async replay entries", ManagedCursorImpl.this);
            }
        };

        positions.stream().filter(position -> !alreadyAcknowledgedPositions.contains(position))
                .forEach(p ->{
                    if (p.compareTo(this.readPosition) == 0) {
                        this.setReadPosition(this.readPosition.getNext());
                        log.warn()
                                .attr("replayPosition", p)
                                .attr("readPosition", this.readPosition)
                                .log("Replay position equals read position, setting next readPosition");
                    }
                    ledger.asyncReadEntry(p, cb, ctx);
                });

        return alreadyAcknowledgedPositions;
    }

    protected long getNumberOfEntries(Range<Position> range) {
        long allEntries = ledger.getNumberOfEntries(range);

        log.debug().attr("range", range).attr("allEntries", allEntries).log("getNumberOfEntries");

        AtomicLong deletedEntries = new AtomicLong(0);

        lock.readLock().lock();
        try {
            if (getConfig().isUnackedRangesOpenCacheSetEnabled()) {
                int cardinality = individualDeletedMessages.cardinality(
                        range.lowerEndpoint().getLedgerId(), range.lowerEndpoint().getEntryId(),
                        range.upperEndpoint().getLedgerId(), range.upperEndpoint().getEntryId());
                deletedEntries.addAndGet(cardinality);
            } else {
                individualDeletedMessages.forEach((r) -> {
                    try {
                        if (r.isConnected(range)) {
                            Range<Position> commonEntries = r.intersection(range);
                            long commonCount = ledger.getNumberOfEntries(commonEntries);
                            log.debug()
                                    .attr("count", commonCount)
                                    .attr("range", commonEntries)
                                    .log("Discounting entries for already deleted range");
                            deletedEntries.addAndGet(commonCount);
                        }
                        return true;
                    } finally {
                        if (r.lowerEndpoint() instanceof PositionRecyclable) {
                            ((PositionRecyclable) r.lowerEndpoint()).recycle();
                            ((PositionRecyclable) r.upperEndpoint()).recycle();
                        }
                    }
                }, recyclePositionRangeConverter);
            }
        } finally {
            lock.readLock().unlock();
        }

        log.debug()
                .attr("entries", allEntries - deletedEntries.get())
                .attr("deletedEntries", deletedEntries)
                .log("Found entries");
        return allEntries - deletedEntries.get();

    }

    @Override
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {
        markDelete(position, Collections.emptyMap());
    }

    @Override
    public void markDelete(Position position, Map<String, Long> properties)
            throws InterruptedException, ManagedLedgerException {
        requireNonNull(position);

        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncMarkDelete(position, properties, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during mark-delete operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void clearBacklog() throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncClearBacklog(new ClearBacklogCallback() {
            @Override
            public void clearBacklogComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void clearBacklogFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during clear backlog operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncClearBacklog(final ClearBacklogCallback callback, Object ctx) {
        asyncMarkDelete(ledger.getLastPosition(), new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                callback.clearBacklogComplete(ctx);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                if (exception.getCause() instanceof IllegalArgumentException) {
                    // There could be a race condition between calling clear backlog and other mark delete operations.
                    // If we get an exception it means the backlog was already cleared in the meantime.
                    callback.clearBacklogComplete(ctx);
                } else {
                    callback.clearBacklogFailed(exception, ctx);
                }
            }
        }, ctx);
    }

    @Override
    public void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncSkipEntries(numEntriesToSkip, deletedEntries, new SkipEntriesCallback() {
            @Override
            public void skipEntriesComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void skipEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during skip messages operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries,
            final SkipEntriesCallback callback, Object ctx) {
        log.info().attr("numEntriesToSkip", numEntriesToSkip).log("Skipping entries");
        long numDeletedMessages = 0;
        if (deletedEntries == IndividualDeletedEntries.Exclude) {
            numDeletedMessages = getNumIndividualDeletedEntriesToSkip(numEntriesToSkip);
        }

        asyncMarkDelete(ledger.getPositionAfterN(markDeletePosition, numEntriesToSkip + numDeletedMessages,
                PositionBound.startExcluded), new MarkDeleteCallback() {
                    @Override
                    public void markDeleteComplete(Object ctx) {
                        callback.skipEntriesComplete(ctx);
                    }

                    @Override
                    public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                        if (exception.getCause() instanceof IllegalArgumentException) {
                            // There could be a race condition between calling clear backlog and other mark delete
                            // operations.
                            // If we get an exception it means the backlog was already cleared in the meantime.
                            callback.skipEntriesComplete(ctx);
                        } else {
                            log.error()
                                    .attr("numEntriesToSkip", numEntriesToSkip)
                                    .exception(exception)
                                    .log("Skip entries failed");
                            callback.skipEntriesFailed(exception, ctx);
                        }
                    }
                }, ctx);
    }

    // required in getNumIndividualDeletedEntriesToSkip method
    // since individualDeletedMessages.forEach accepts a lambda and ordinary local variables
    // defined before the lambda cannot be mutated
    private static class InvidualDeletedMessagesHandlingState {
        long totalEntriesToSkip = 0L;
        long deletedMessages = 0L;
        Position startPosition;
        Position endPosition;

        InvidualDeletedMessagesHandlingState(Position startPosition) {
            this.startPosition = startPosition;
        }
    }

    long getNumIndividualDeletedEntriesToSkip(long numEntries) {
        lock.readLock().lock();
        try {
            InvidualDeletedMessagesHandlingState state = new InvidualDeletedMessagesHandlingState(markDeletePosition);
            individualDeletedMessages.forEach((r) -> {
                try {
                    state.endPosition = r.lowerEndpoint();
                    if (state.startPosition.compareTo(state.endPosition) <= 0) {
                        Range<Position> range = Range.openClosed(state.startPosition, state.endPosition);
                        long entries = ledger.getNumberOfEntries(range);
                        if (state.totalEntriesToSkip + entries >= numEntries) {
                            // do not process further
                            return false;
                        }
                        state.totalEntriesToSkip += entries;
                        state.deletedMessages += ledger.getNumberOfEntries(r);
                        state.startPosition = r.upperEndpoint();
                    } else {
                        log.debug()
                                .attr("markDeletePosition", markDeletePosition)
                                .attr("lowerEndpoint", r.lowerEndpoint())
                                .log("Delete position moved ahead without clearing deleted messages");
                    }
                    return true;
                } finally {
                    if (r.lowerEndpoint() instanceof PositionRecyclable) {
                        ((PositionRecyclable) r.lowerEndpoint()).recycle();
                    }
                }
            }, recyclePositionRangeConverter);
            return state.deletedMessages;
        } finally {
            lock.readLock().unlock();
        }
    }

    boolean hasMoreEntries(Position position) {
        Position lastPositionInLedger = ledger.getLastPosition();
        if (position.compareTo(lastPositionInLedger) <= 0) {
            return getNumberOfEntries(Range.closed(position, lastPositionInLedger)) > 0;
        }
        return false;
    }

    void initializeCursorPosition(Pair<Position, Long> lastPositionCounter) {
        readPosition = ledger.getNextValidPosition(lastPositionCounter.getLeft());
        ledger.onCursorReadPositionUpdated(this, readPosition);
        markDeletePosition = lastPositionCounter.getLeft();
        lastMarkDeleteEntry = new MarkDeleteEntry(markDeletePosition, getProperties(), null, null);
        persistentMarkDeletePosition = null;
        inProgressMarkDeletePersistPosition = null;

        // Initialize the counter such that the difference between the messages written on the ML and the
        // messagesConsumed is 0, to ensure the initial backlog count is 0.
        messagesConsumedCounter = lastPositionCounter.getRight();
    }

    /**
     *
     * @param newMarkDeletePosition
     *            the new acknowledged position
     * @return the previous acknowledged position
     */
    Position setAcknowledgedPosition(Position newMarkDeletePosition) {
        if (newMarkDeletePosition.compareTo(markDeletePosition) < 0) {
            throw new MarkDeletingMarkedPosition(
                    "Mark deleting an already mark-deleted position. Current mark-delete: " + markDeletePosition
                            + " -- attempted mark delete: " + newMarkDeletePosition);
        }

        Position oldMarkDeletePosition = markDeletePosition;

        if (!newMarkDeletePosition.equals(oldMarkDeletePosition)) {
            long skippedEntries = 0;
            if (newMarkDeletePosition.getLedgerId() == oldMarkDeletePosition.getLedgerId()
                    && newMarkDeletePosition.getEntryId() == oldMarkDeletePosition.getEntryId() + 1) {
                // Mark-deleting the position next to current one
                skippedEntries = individualDeletedMessages.contains(newMarkDeletePosition.getLedgerId(),
                        newMarkDeletePosition.getEntryId()) ? 0 : 1;
            } else {
                skippedEntries = getNumberOfEntries(Range.openClosed(oldMarkDeletePosition, newMarkDeletePosition));
            }

            Position positionAfterNewMarkDelete = ledger.getNextValidPosition(newMarkDeletePosition);
            // sometime ranges are connected but belongs to different ledgers so, they are placed sequentially
            // eg: (2:10..3:15] can be returned as (2:10..2:15],[3:0..3:15]. So, try to iterate over connected range and
            // found the last non-connected range which gives new markDeletePosition
            while (positionAfterNewMarkDelete.compareTo(ledger.lastConfirmedEntry) <= 0) {
                if (individualDeletedMessages.contains(positionAfterNewMarkDelete.getLedgerId(),
                        positionAfterNewMarkDelete.getEntryId())) {
                    Range<Position> rangeToBeMarkDeleted = individualDeletedMessages.rangeContaining(
                            positionAfterNewMarkDelete.getLedgerId(), positionAfterNewMarkDelete.getEntryId());
                    newMarkDeletePosition = rangeToBeMarkDeleted.upperEndpoint();
                    positionAfterNewMarkDelete = ledger.getNextValidPosition(newMarkDeletePosition);
                    // check if next valid position is also deleted and part of the deleted-range
                    continue;
                }
                break;
            }

            log.debug()
                    .attr("oldMarkDeletePosition", oldMarkDeletePosition)
                    .attr("markDeletePosition", newMarkDeletePosition)
                    .attr("skippedEntries", skippedEntries)
                    .log("Moved ack position");
            MSG_CONSUMED_COUNTER_UPDATER.addAndGet(this, skippedEntries);
        }

        // markDelete-position and clear out deletedMsgSet
        markDeletePosition = newMarkDeletePosition;
        individualDeletedMessages.removeAtMost(markDeletePosition.getLedgerId(), markDeletePosition.getEntryId());

        MutableBoolean readPositionUpdated = new MutableBoolean(false);
        Position updatedReadPosition = READ_POSITION_UPDATER.updateAndGet(this, currentReadPosition -> {
            if (currentReadPosition.compareTo(markDeletePosition) <= 0) {
                // If the position that is mark-deleted is past the read position, it
                // means that the client has skipped some entries. We need to move
                // read position forward
                Position newReadPosition = ledger.getNextValidPosition(markDeletePosition);
                log.debug()
                        .attr("oldReadPosition", currentReadPosition)
                        .attr("readPosition", newReadPosition)
                        .attr("markDeletePosition", markDeletePosition)
                        .log("Moved read position");
                readPositionUpdated.setTrue();
                return newReadPosition;
            } else {
                return currentReadPosition;
            }
        });
        if (readPositionUpdated.booleanValue()) {
            ledger.onCursorReadPositionUpdated(this, updatedReadPosition);
        }

        return newMarkDeletePosition;
    }

    @Override
    public void asyncMarkDelete(final Position position, final MarkDeleteCallback callback, final Object ctx) {
        asyncMarkDelete(position, Collections.emptyMap(), callback, ctx);
    }

    private final class MarkDeletingMarkedPosition extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        public MarkDeletingMarkedPosition(String s) {
            super(s);
        }
    }

    @Override
    public void asyncMarkDelete(final Position position, Map<String, Long> properties,
            final MarkDeleteCallback callback, final Object ctx) {
        requireNonNull(position);

        if (isClosed()) {
            callback.markDeleteFailed(new ManagedLedgerException
                    .CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        if (RESET_CURSOR_IN_PROGRESS_UPDATER.get(this) == TRUE) {
            log.debug().attr("position", position).log("Cursor reset in progress, ignoring mark delete");
            callback.markDeleteFailed(
                    new ManagedLedgerException("Reset cursor in progress - unable to mark delete position "
                            + position.toString()),
                    ctx);
            return;
        }

        log.debug().attr("position", position).log("Mark delete");

        Position newPosition = ackBatchPosition(position);
        Position markDeletePos = markDeletePosition;
        Position lastConfirmedEntry = ledger.getLastConfirmedEntry();
        if (lastConfirmedEntry.compareTo(newPosition) < 0) {
            boolean shouldCursorMoveForward = false;
            try {
                long ledgerEntries = ledger.getLedgerInfo(markDeletePos.getLedgerId()).get().getEntries();
                Long nextValidLedger = ledger.getNextValidLedger(lastConfirmedEntry.getLedgerId());
                shouldCursorMoveForward = nextValidLedger != null
                        && (markDeletePos.getEntryId() + 1 >= ledgerEntries)
                        && (newPosition.getLedgerId() == nextValidLedger);
            } catch (Exception e) {
                log.warn().exception(e).log("Failed to get ledger entries while setting mark-delete-position");
            }

            if (shouldCursorMoveForward) {
                log.info()
                        .attr("markDeletePosition", markDeletePos)
                        .attr("position", newPosition)
                        .log("Moving mark-delete position since all entries have been consumed");
            } else {
                log.debug()
                        .attr("position", position)
                        .attr("lastConfirmedEntry", lastConfirmedEntry)
                        .log("Failed mark delete: position is ahead of last confirmed entry");
                callback.markDeleteFailed(new ManagedLedgerException("Invalid mark deleted position"), ctx);
                return;
            }
        }

        lock.writeLock().lock();
        try {
            newPosition = setAcknowledgedPosition(newPosition);
        } catch (IllegalArgumentException e) {
            callback.markDeleteFailed(getManagedLedgerException(e), ctx);
            return;
        } finally {
            lock.writeLock().unlock();
        }

        // Apply rate limiting to mark-delete operations
        if (markDeleteLimiter != null && !markDeleteLimiter.tryAcquire()) {
            isDirty = true;
            updateLastMarkDeleteEntryToLatest(newPosition, properties);
            callback.markDeleteComplete(ctx);
            return;
        }
        internalAsyncMarkDelete(newPosition, properties, callback, ctx, null);
    }

    private Position ackBatchPosition(Position position) {
        return AckSetStateUtil.maybeGetAckSetState(position)
                .map(AckSetState::getAckSet)
                .map(ackSet -> {
                    if (batchDeletedIndexes == null) {
                        return ledger.getPreviousPosition(position);
                    }
                    // In order to prevent the batch index recorded in batchDeletedIndexes from rolling back,
                    // only update batchDeletedIndexes when the submitted batch index is greater
                    // than the recorded index.
                    final var givenBitSet = BitSet.valueOf(ackSet);
                    batchDeletedIndexes.compute(position, (k, v) -> {
                        if (v == null || givenBitSet.nextSetBit(0) > v.nextSetBit(0)) {
                            return givenBitSet;
                        } else {
                            return v;
                        }
                    });
                    final var newPosition = ledger.getPreviousPosition(position);
                    batchDeletedIndexes.subMap(PositionFactory.EARLIEST, newPosition).clear();
                    return newPosition;
                })
                .orElse(position);
    }

    protected void internalAsyncMarkDelete(final Position newPosition, Map<String, Long> properties,
            final MarkDeleteCallback callback, final Object ctx, Runnable alignAcknowledgeStatusAfterPersisted) {
        ledger.mbean.addMarkDeleteOp();

        // We cannot write to the ledger during the switch, need to wait until the new metadata ledger is available
        synchronized (pendingMarkDeleteOps) {
            // use given properties or when missing, use the properties from the previous field value
            MarkDeleteEntry last = pendingMarkDeleteOps.peekLast();
            Map<String, Long> propertiesToUse =
                    properties != null ? properties : (last != null ? last.properties : getProperties());
            MarkDeleteEntry mdEntry = new MarkDeleteEntry(newPosition, propertiesToUse, callback, ctx,
                    alignAcknowledgeStatusAfterPersisted);

            // The state might have changed while we were waiting on the queue mutex
            switch (state) {
            case Closed:
                callback.markDeleteFailed(new ManagedLedgerException
                        .CursorAlreadyClosedException("Cursor was already closed"), ctx);
                return;

            case NoLedger:
                pendingMarkDeleteOps.add(mdEntry);
                // We need to create a new ledger to write into.
                startCreatingNewMetadataLedger();
                break;
                // fall through
            case SwitchingLedger:
                pendingMarkDeleteOps.add(mdEntry);
                break;

            case Open:
                if (PENDING_READ_OPS_UPDATER.get(this) > 0) {
                    // Wait until no read operation are pending
                    pendingMarkDeleteOps.add(mdEntry);
                } else {
                    // Execute the mark delete immediately
                    internalMarkDelete(mdEntry);
                }
                break;

            default:
                log.error().attr("state", state).log("Invalid cursor state");
                callback.markDeleteFailed(new ManagedLedgerException("Cursor was in invalid state: " + state), ctx);
                break;
            }
        }
    }

    void internalMarkDelete(final MarkDeleteEntry mdEntry) {
        if (persistentMarkDeletePosition != null
                && mdEntry.newPosition.compareTo(persistentMarkDeletePosition) < 0) {
            log.info()
                    .attr("position", mdEntry.newPosition)
                    .attr("persistentMarkDeletePosition", persistentMarkDeletePosition)
                    .log("Skipping mark delete update, persisted position is later");
            // run with executor to prevent deadlock
            ledger.getExecutor().execute(() -> mdEntry.triggerComplete());
            return;
        }

        Position inProgressLatest = INPROGRESS_MARKDELETE_PERSIST_POSITION_UPDATER.updateAndGet(this, current -> {
            if (current != null && current.compareTo(mdEntry.newPosition) > 0) {
                return current;
            } else {
                return mdEntry.newPosition;
            }
        });

        // if there's a newer or equal mark delete update in progress, skip it.
        if (inProgressLatest != mdEntry.newPosition) {
            log.info()
                    .attr("position", mdEntry.newPosition)
                    .attr("inProgressLatest", inProgressLatest)
                    .log("Skipping mark delete update, in-progress position is later");
            // run with executor to prevent deadlock
            ledger.getExecutor().execute(() -> mdEntry.triggerComplete());
            return;
        }

        // The counter is used to mark all the pending mark-delete request that were submitted to BK and that are not
        // yet finished. While we have outstanding requests we cannot close the current ledger, so the switch to new
        // ledger is postponed to when the counter goes to 0.
        PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.incrementAndGet(this);

        LAST_MARK_DELETE_ENTRY_UPDATER.updateAndGet(this, last -> {
            if (last != null && last.newPosition.compareTo(mdEntry.newPosition) > 0) {
                // keep the current value since it's later then the mdEntry.newPosition
                return last;
            } else {
                return mdEntry;
            }
        });

        VoidCallback cb = new VoidCallback() {
            @Override
            public void operationComplete() {
                log.debug().attr("position", mdEntry.newPosition).log("Mark delete succeeded");

                INPROGRESS_MARKDELETE_PERSIST_POSITION_UPDATER.compareAndSet(ManagedCursorImpl.this,
                        mdEntry.newPosition, null);

                // Remove from the individual deleted messages all the entries before the new mark delete
                // point.
                lock.writeLock().lock();
                try {
                    mdEntry.alignAcknowledgeStatus();
                } finally {
                    lock.writeLock().unlock();
                }

                ledger.onCursorMarkDeletePositionUpdated(ManagedCursorImpl.this, mdEntry.newPosition);

                decrementPendingMarkDeleteCount();

                mdEntry.triggerComplete();
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                INPROGRESS_MARKDELETE_PERSIST_POSITION_UPDATER.compareAndSet(ManagedCursorImpl.this,
                        mdEntry.newPosition, null);
                isDirty = true;
                log.warn()
                        .attr("position", mdEntry.newPosition)
                        .log("Failed to mark delete position");
                log.debug()
                        .attr("messagesConsumedCounter", messagesConsumedCounter)
                        .attr("markDeletePosition", markDeletePosition)
                        .attr("readPosition", readPosition)
                        .log("Cursor mark delete failed");

                decrementPendingMarkDeleteCount();

                mdEntry.triggerFailed(exception);
            }
        };

        if (state == State.NoLedger) {
            if (ledger.isNoMessagesAfterPos(mdEntry.newPosition)) {
                log.error("Metadata ledger creation failed, try to persist the position in the metadata store.");
                persistPositionToMetaStore(mdEntry, cb);
            } else {
                cb.operationFailed(new ManagedLedgerException("Switch new cursor ledger failed"));
            }
        } else {
            persistPositionToLedger(cursorLedger, mdEntry, cb, false);
        }
    }

    @Override
    public void delete(final Position position) throws InterruptedException, ManagedLedgerException {
        delete(Collections.singletonList(position));
    }

    @Override
    public void asyncDelete(Position pos, final AsyncCallbacks.DeleteCallback callback, Object ctx) {
        asyncDelete(Collections.singletonList(pos), callback, ctx);
    }

    @Override
    public void delete(Iterable<Position> positions) throws InterruptedException, ManagedLedgerException {
        requireNonNull(positions);

        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);
        final AtomicBoolean timeout = new AtomicBoolean(false);

        asyncDelete(positions, new AsyncCallbacks.DeleteCallback() {
            @Override
            public void deleteComplete(Object ctx) {
                if (timeout.get()) {
                    log.warn()
                            .attr("positions", positions)
                            .log("Delete operation timeout, callback deleteComplete");
                }

                counter.countDown();
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;

                if (timeout.get()) {
                    log.warn()
                            .attr("positions", positions)
                            .log("Delete operation timeout, callback deleteFailed");
                }

                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            timeout.set(true);
            log.warn()
                    .attr("positions", positions)
                    .log("Delete operation timeout, no callback triggered");
            throw new ManagedLedgerException("Timeout during delete operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }


    @Override
    public void asyncDelete(Iterable<Position> positions, AsyncCallbacks.DeleteCallback callback, Object ctx) {
        if (isClosed()) {
            callback.deleteFailed(new ManagedLedgerException
                    .CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        Position newMarkDeletePosition = null;

        lock.writeLock().lock();
        boolean skipMarkDeleteBecauseAckedNothing = false;
        try {
            log.debug()
                    .attr("positions", positions)
                    .attr("deletedMessages", individualDeletedMessages)
                    .attr("markDeletePosition", markDeletePosition)
                    .log("Deleting individual messages");

            for (Position pos : positions) {
                Position position  = requireNonNull(pos);
                if (ledger.getLastConfirmedEntry().compareTo(position) < 0) {
                    log.debug()
                            .attr("position", position)
                            .attr("ledger_getLastConfirmedEntry", ledger.getLastConfirmedEntry())
                            .log("Failed mark delete: position is ahead of last confirmed entry");
                    callback.deleteFailed(new ManagedLedgerException("Invalid mark deleted position"), ctx);
                    return;
                }

                if (internalIsMessageDeleted(position)) {
                    if (batchDeletedIndexes != null) {
                        batchDeletedIndexes.remove(position);
                    }
                    log.debug().attr("position", position).log("Position was already deleted");
                    continue;
                }
                long[] ackSet = AckSetStateUtil.getAckSetArrayOrNull(position);
                if (ackSet == null || ackSet.length == 0) {
                    if (batchDeletedIndexes != null) {
                        batchDeletedIndexes.remove(position);
                    }
                    // Add a range (prev, pos] to the set. Adding the previous entry as an open limit to the range will
                    // make the RangeSet recognize the "continuity" between adjacent Positions.
                    // Before https://github.com/apache/pulsar/pull/21105 is merged, the range does not support crossing
                    // multi ledgers, so the first position's entryId maybe "-1".
                    Position previousPosition;
                    if (position.getEntryId() == 0) {
                        previousPosition = PositionFactory.create(position.getLedgerId(), -1);
                    } else {
                        previousPosition = ledger.getPreviousPosition(position);
                    }
                    individualDeletedMessages.addOpenClosed(previousPosition.getLedgerId(),
                        previousPosition.getEntryId(), position.getLedgerId(), position.getEntryId());
                    MSG_CONSUMED_COUNTER_UPDATER.incrementAndGet(this);

                    log.debug().attr("deletedMessages", individualDeletedMessages).log("Individually deleted messages");
                } else if (batchDeletedIndexes != null) {
                    final var givenBitSet = BitSet.valueOf(ackSet);
                    final var bitSet = batchDeletedIndexes.computeIfAbsent(position, __ -> givenBitSet);
                    if (givenBitSet != bitSet) {
                        bitSet.and(givenBitSet);
                    }
                    if (bitSet.isEmpty()) {
                        Position previousPosition = ledger.getPreviousPosition(position);
                        individualDeletedMessages.addOpenClosed(previousPosition.getLedgerId(),
                            previousPosition.getEntryId(),
                            position.getLedgerId(), position.getEntryId());
                        MSG_CONSUMED_COUNTER_UPDATER.incrementAndGet(this);
                        batchDeletedIndexes.remove(position);
                    }
                }
            }

            if (individualDeletedMessages.isEmpty()) {
                // No changes to individually deleted messages, so nothing to do at this point
                skipMarkDeleteBecauseAckedNothing = true;
                return;
            }

            // If the lower bound of the range set is the current mark delete position, then we can trigger a new
            // mark-delete to the upper bound of the first range segment
            Range<Position> range = individualDeletedMessages.firstRange();

            // If the upper bound is before the mark-delete position, we need to move ahead as these
            // individualDeletedMessages are now irrelevant
            if (range.upperEndpoint().compareTo(markDeletePosition) <= 0) {
                individualDeletedMessages.removeAtMost(markDeletePosition.getLedgerId(),
                        markDeletePosition.getEntryId());
                range = individualDeletedMessages.firstRange();
            }

            if (range == null) {
                // The set was completely cleaned up now
                skipMarkDeleteBecauseAckedNothing = true;
                return;
            }

            // If the lowerBound is ahead of MarkDelete, verify if there are any entries in-between
            if (range.lowerEndpoint().compareTo(markDeletePosition) <= 0 || ledger
                    .getNumberOfEntries(Range.openClosed(markDeletePosition, range.lowerEndpoint())) <= 0) {

                log.debug().attr("range", range).log("Found a position range to mark delete");

                newMarkDeletePosition = range.upperEndpoint();
            }

            if (newMarkDeletePosition != null) {
                newMarkDeletePosition = setAcknowledgedPosition(newMarkDeletePosition);
            } else {
                newMarkDeletePosition = markDeletePosition;
            }
        } catch (Exception e) {
            log.warn()
                    .attr("errorMessage", e.getMessage())
                    .exception(e)
                    .log("Error while updating individualDeletedMessages");
            callback.deleteFailed(getManagedLedgerException(e), ctx);
            return;
        } finally {
            lock.writeLock().unlock();
            if (skipMarkDeleteBecauseAckedNothing) {
                callback.deleteComplete(ctx);
            }
        }

        // Apply rate limiting to mark-delete operations
        if (markDeleteLimiter != null && !markDeleteLimiter.tryAcquire()) {
            isDirty = true;
            updateLastMarkDeleteEntryToLatest(newMarkDeletePosition, null);
            callback.deleteComplete(ctx);
            return;
        }

        try {
            internalAsyncMarkDelete(newMarkDeletePosition, null, new MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    callback.deleteComplete(ctx);
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    callback.deleteFailed(exception, ctx);
                }

            }, ctx, null);

        } catch (Exception e) {
            log.warn().attr("errorMessage", e.getMessage()).exception(e).log("Error doing asyncDelete");
            log.debug()
                    .attr("messagesConsumedCounter", messagesConsumedCounter)
                    .attr("markDeletePosition", markDeletePosition)
                    .attr("readPosition", readPosition)
                    .log("Cursor asyncDelete error");
            callback.deleteFailed(new ManagedLedgerException(e), ctx);
        }
    }

    // update lastMarkDeleteEntry field if newPosition is later than the current lastMarkDeleteEntry.newPosition
    private void updateLastMarkDeleteEntryToLatest(final Position newPosition,
                                                   final Map<String, Long> properties) {
        synchronized (pendingMarkDeleteOps) {
            // use given properties or when missing, use the properties from the previous field value
            MarkDeleteEntry lastPending = pendingMarkDeleteOps.peekLast();
            Map<String, Long> propertiesToUse =
                    properties != null ? properties : (lastPending != null ? lastPending.properties : getProperties());
            LAST_MARK_DELETE_ENTRY_UPDATER.updateAndGet(this, last -> {
                if (last != null && last.newPosition.compareTo(newPosition) > 0) {
                    // keep current value, don't update
                    return last;
                } else {
                    return new MarkDeleteEntry(newPosition, propertiesToUse, null, null);
                }
            });
        }
    }

    /**
     * Given a list of entries, filter out the entries that have already been individually deleted.
     *
     * @param entries
     *            a list of entries
     * @return a list of entries not containing deleted messages
     */
    List<Entry> filterReadEntries(List<Entry> entries) {
        lock.readLock().lock();
        try {
            Range<Position> entriesRange = Range.closed(entries.get(0).getPosition(),
                    entries.get(entries.size() - 1).getPosition());
            log.debug()
                    .attr("entriesRange", entriesRange)
                    .attr("deletedMessages", individualDeletedMessages)
                    .log("Filtering entries");
            Range<Position> span = individualDeletedMessages.isEmpty() ? null : individualDeletedMessages.span();
            if (span == null || !entriesRange.isConnected(span)) {
                // There are no individually deleted messages in this entry list, no need to perform filtering
                log.debug().attr("entriesRange", entriesRange).log("No filtering needed for entries");
                return entries;
            } else {
                // Remove from the entry list all the entries that were already marked for deletion
                return Lists.newArrayList(Collections2.filter(entries, entry -> {
                    boolean includeEntry = !individualDeletedMessages.contains(entry.getLedgerId(), entry.getEntryId());
                    if (!includeEntry) {
                        log.debug().attr("position", entry.getPosition()).log("Filtering entry, already deleted");

                        entry.release();
                    }
                    return includeEntry;
                }));
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized String toString() {
        return MoreObjects.toStringHelper(this)
                .add("ledger", ledger.getName())
                .add("name", name)
                .add("ackPos", markDeletePosition)
                .add("readPos", readPosition)
                .toString();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getLastActive() {
        return lastActive;
    }

    @Override
    public void updateLastActive() {
        lastActive = System.currentTimeMillis();
    }

    @Override
    public boolean isDurable() {
        return true;
    }

    @Override
    public Position getReadPosition() {
        return readPosition;
    }

    @Override
    public Position getMarkDeletedPosition() {
        return markDeletePosition;
    }

    @Override
    public Position getPersistentMarkDeletedPosition() {
        return this.persistentMarkDeletePosition;
    }

    @Override
    public void rewind() {
        rewind(false);
    }

    @Override
    public void rewind(boolean readCompacted) {
        lock.writeLock().lock();
        try {
            Position newReadPosition =
                    readCompacted ? markDeletePosition.getNext() : ledger.getNextValidPosition(markDeletePosition);
            Position oldReadPosition = readPosition;

            log.info()
                    .attr("oldReadPosition", oldReadPosition)
                    .attr("readPosition", newReadPosition)
                    .log("Rewind");

            readPosition = newReadPosition;
            ledger.onCursorReadPositionUpdated(ManagedCursorImpl.this, newReadPosition);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void seek(Position newReadPositionInt, boolean force) {
        Position newReadPosition = newReadPositionInt;

        lock.writeLock().lock();
        try {
            if (!force && newReadPosition.compareTo(markDeletePosition) <= 0) {
                // Make sure the newReadPosition comes after the mark delete position
                newReadPosition = ledger.getNextValidPosition(markDeletePosition);
            }
            readPosition = newReadPosition;
            ledger.onCursorReadPositionUpdated(ManagedCursorImpl.this, newReadPosition);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    boolean closeCursorLedger() throws BKException, InterruptedException {
        if (cursorLedger != null) {
            cursorLedger.close();
            return true;
        }
        return false;
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                log.debug("Successfully closed ledger");
                latch.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                log.warn().exception(exception).log("Closing ledger failed");
                result.exception = exception;
                latch.countDown();
            }
        }, null);

        if (!latch.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during close operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    /**
     * Persist given markDelete position to cursor-ledger or zk-metaStore based on max number of allowed unack-range
     * that can be persist in zk-metastore. If current unack-range is higher than configured threshold then broker
     * persists mark-delete into cursor-ledger else into zk-metastore.
     *
     * @param position
     * @param properties
     * @param callback
     * @param ctx
     */
    void persistPositionWhenClosing(Position position, Map<String, Long> properties,
            final AsyncCallbacks.CloseCallback callback, final Object ctx) {

        if (shouldPersistUnackRangesToLedger()) {
            persistPositionToLedger(cursorLedger, new MarkDeleteEntry(position, properties, null, null),
                    new VoidCallback() {
                        @Override
                        public void operationComplete() {
                            log.info()
                                    .attr("markDeletePosition", markDeletePosition)
                                    .attr("ledgerId", cursorLedger.getId())
                                    .log("Updated mark-delete position into cursor-ledger");
                            asyncCloseCursorLedger(callback, ctx);
                        }

                        @Override
                        public void operationFailed(ManagedLedgerException e) {
                            log.warn()
                                    .attr("ledgerId", cursorLedger.getId())
                                    .attr("errorMessage", e.getMessage())
                                    .log("Failed to persist mark-delete position into cursor-ledger");
                            callback.closeFailed(e, ctx);
                        }
                    }, true);
        } else {
            persistPositionMetaStore(-1, position, properties, new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void result, Stat stat) {
                    log.info().attr("markDeletePosition", markDeletePosition).log("Closed cursor");
                    // At this point the position had already been safely stored in the cursor z-node
                    callback.closeComplete(ctx);
                    asyncDeleteLedger(cursorLedger);
                }

                @Override
                public void operationFailed(MetaStoreException e) {
                    log.warn().attr("errorMessage", e.getMessage()).log("Failed to update cursor info when closing");
                    callback.closeFailed(e, ctx);
                }
            }, true);
        }
    }

    private boolean shouldPersistUnackRangesToLedger() {
        lock.readLock().lock();
        try {
            return cursorLedger != null
                    && !isCursorLedgerReadOnly
                    && getConfig().getMaxUnackedRangesToPersist() > 0
                    && individualDeletedMessages.size() > getConfig().getMaxUnackedRangesToPersistInMetadataStore();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void persistPositionMetaStore(long cursorsLedgerId, Position position, Map<String, Long> properties,
            MetaStoreCallback<Void> callback, boolean persistIndividualDeletedMessageRanges) {
        if (state == State.Closed) {
            ledger.getExecutor().execute(() -> callback.operationFailed(new MetaStoreException(
                    new CursorAlreadyClosedException(name + " cursor already closed"))));
            return;
        }

        final Stat lastCursorLedgerStat = cursorLedgerStat;

        // When closing we store the last mark-delete position in the z-node itself, so we won't need the cursor ledger,
        // hence we write it as -1. The cursor ledger is deleted once the z-node write is confirmed.
        ManagedCursorInfo info = new ManagedCursorInfo()
                .setCursorsLedgerId(cursorsLedgerId)
                .setMarkDeleteLedgerId(position.getLedgerId())
                .setMarkDeleteEntryId(position.getEntryId())
                .setLastActive(lastActive);

        info.addAllProperties(buildPropertiesMap(properties));
        info.addAllCursorProperties(buildStringPropertiesMap(cursorProperties));
        if (persistIndividualDeletedMessageRanges) {
            info.addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges());
            if (getConfig().isDeletionAtBatchIndexLevelEnabled()) {
                info.addAllBatchedEntryDeletionIndexInfos(buildBatchEntryDeletionIndexInfoList());
            }
        }

        log.debug().attr("position", position).log("Closing cursor");

        ManagedCursorInfo cursorInfo = info;
        ledger.getStore().asyncUpdateCursorInfo(ledger.getName(), name, cursorInfo, lastCursorLedgerStat,
                new MetaStoreCallback<Void>() {
                    @Override
                    public void operationComplete(Void result, Stat stat) {
                        updateCursorLedgerStat(cursorInfo, stat);
                        callback.operationComplete(result, stat);
                    }

                    @Override
                    public void operationFailed(MetaStoreException topLevelException) {
                        if (topLevelException instanceof MetaStoreException.BadVersionException) {
                            log.warn()
                                    .exceptionMessage(topLevelException)
                                    .log("Failed to update cursor metadata due to version conflict");
                            // it means previous owner of the ml might have updated the version incorrectly. So, check
                            // the ownership and refresh the version again.
                            if (ledger.mlOwnershipChecker != null) {
                                ledger.mlOwnershipChecker.get().whenComplete((hasOwnership, t) -> {
                                    if (t == null && hasOwnership) {
                                        ledger.getStore().asyncGetCursorInfo(ledger.getName(), name,
                                                new MetaStoreCallback<>() {
                                                    @Override
                                                    public void operationComplete(ManagedCursorInfo info, Stat stat) {
                                                        updateCursorLedgerStat(info, stat);
                                                        // fail the top level call so that the caller can retry
                                                        callback.operationFailed(topLevelException);
                                                    }

                                                    @Override
                                                    public void operationFailed(MetaStoreException e) {
                                                        log.debug()
                                                                .attr("errorMessage", e.getMessage())
                                                                .log("Failed to refresh cursor metadata-version");
                                                        // fail the top level call so that the caller can retry
                                                        callback.operationFailed(topLevelException);
                                                    }
                                                });
                                    } else {
                                        // fail the top level call so that the caller can retry
                                        callback.operationFailed(topLevelException);
                                    }
                                });
                            } else {
                                callback.operationFailed(topLevelException);
                            }
                        } else {
                            callback.operationFailed(topLevelException);
                        }
                    }
                });
    }

    @Override
    public void asyncClose(final AsyncCallbacks.CloseCallback callback, final Object ctx) {
        boolean alreadyClosing = !trySetStateToClosing();
        if (alreadyClosing) {
            log.info("State is already closed");
            callback.closeComplete(ctx);
            return;
        }
        closeWaitingCursor();
        setInactive();
        persistPositionWhenClosing(lastMarkDeleteEntry.newPosition, lastMarkDeleteEntry.properties,
                new AsyncCallbacks.CloseCallback(){

                    @Override
                    public void closeComplete(Object ctx) {
                        if (!STATE_UPDATER.compareAndSet(ManagedCursorImpl.this, State.Closing, State.Closed)) {
                            log.warn().attr("state", state).log("State was modified from closing while closing");
                            state = State.Closed;
                        }
                        callback.closeComplete(ctx);
                    }

                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        log.warn("Persistent position failure when closing,"
                                + " the state will remain in state-closing"
                                + " and will no longer work");
                        callback.closeFailed(exception, ctx);
                    }
                }, ctx);
    }

    protected void closeWaitingCursor() {
        synchronized (registerToWaitingCursorsLock) {
            if (registeredToWaitingCursors) {
                ledger.removeWaitingCursor(this);
            }
        }
        OpReadEntry opReadEntry = WAITING_READ_OP_UPDATER.getAndSet(this,
                OpReadEntry.WAITING_READ_OP_FOR_CLOSED_CURSOR);
        if (opReadEntry != null && opReadEntry != OpReadEntry.WAITING_READ_OP_FOR_CLOSED_CURSOR) {
            opReadEntry.readEntriesFailed(new CursorAlreadyClosedException("Cursor is closing"), opReadEntry.ctx);
        }
    }

    /**
     * Internal version of seek that doesn't do the validation check.
     *
     * @param newReadPositionInt
     */
    void setReadPosition(Position newReadPositionInt) {
        if (this.markDeletePosition == null
                || newReadPositionInt.compareTo(this.markDeletePosition) > 0) {
            this.readPosition = newReadPositionInt;
            ledger.onCursorReadPositionUpdated(this, newReadPositionInt);
        }
    }

    /**
     * Manually acknowledge all entries in the lost ledger.
     * - Since this is an uncommon event, we focus on maintainability. So we do not modify
     *   {@link #individualDeletedMessages} and {@link #batchDeletedIndexes}, but call
     *   {@link #asyncDelete(Position, AsyncCallbacks.DeleteCallback, Object)}.
     * - This method is valid regardless of the consumer ACK type.
     * - If there is a consumer ack request after this event, it will also work.
     */
    @Override
    public void skipNonRecoverableLedger(final long ledgerId){
        LedgerInfo ledgerInfo = ledger.getLedgersInfo().get(ledgerId);
        if (ledgerInfo == null) {
            return;
        }
        log.warn()
                .attr("ledgerId", ledgerId)
                .log("Ledger is lost, auto-acknowledging in subscription (autoSkipNonRecoverableData=true)");
        asyncDelete(() -> LongStream.range(0, ledgerInfo.getEntries())
                        .mapToObj(i -> PositionFactory.create(ledgerId, i)).iterator(),
                new AsyncCallbacks.DeleteCallback() {
                    @Override
                    public void deleteComplete(Object ctx) {
                        // ignore.
                    }

                    @Override
                    public void deleteFailed(ManagedLedgerException ex, Object ctx) {
                        // The method internalMarkDelete already handled the failure operation. We only need to
                        // make sure the memory state is updated.
                        // If the broker crashed, the non-recoverable ledger will be detected again.
                    }
                }, null);
    }

    /**
     * Manually acknowledge all entries from startPosition to endPosition.
     * - Since this is an uncommon event, we focus on maintainability. So we do not modify
     *   {@link #individualDeletedMessages} and {@link #batchDeletedIndexes}, but call
     *   {@link #asyncDelete(Position, AsyncCallbacks.DeleteCallback, Object)}.
     * - This method is valid regardless of the consumer ACK type.
     * - If there is a consumer ack request after this event, it will also work.
     */
    public void skipNonRecoverableEntries(Position startPosition, Position endPosition){
        long ledgerId = startPosition.getLedgerId();
        LedgerInfo ledgerInfo = ledger.getLedgersInfo().get(ledgerId);
        if (ledgerInfo == null) {
            return;
        }

        long startEntryId = Math.max(0, startPosition.getEntryId());
        long endEntryId = ledgerId != endPosition.getLedgerId() ? ledgerInfo.getEntries() : endPosition.getEntryId();
        if (startEntryId >= endEntryId) {
            return;
        }

        lock.writeLock().lock();
        log.warn()
                .attr("ledgerId", ledgerId)
                .attr("startEntryId", startEntryId)
                .attr("endEntryId", endEntryId)
                .log("Entries are lost, auto-acknowledging in subscription (autoSkipNonRecoverableData=true)");
        try {
            for (long i = startEntryId; i < endEntryId; i++) {
                if (!individualDeletedMessages.contains(ledgerId, i)) {
                    asyncDelete(PositionFactory.create(ledgerId, i), new AsyncCallbacks.DeleteCallback() {
                        @Override
                        public void deleteComplete(Object ctx) {
                            // ignore.
                        }

                        @Override
                        public void deleteFailed(ManagedLedgerException ex, Object ctx) {
                            // The method internalMarkDelete already handled the failure operation. We only need to
                            // make sure the memory state is updated.
                            // If the broker crashed, the non-recoverable ledger will be detected again.
                        }
                    }, null);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // //////////////////////////////////////////////////

    void startCreatingNewMetadataLedger() {
        // Change the state so that new mark-delete ops will be queued and not immediately submitted
        State oldState = changeStateIfNotClosed(State.SwitchingLedger);
        if (oldState == State.SwitchingLedger || oldState.isClosed()) {
            // Ignore double request
            return;
        }

        // Check if we can immediately switch to a new metadata ledger
        if (PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.get(this) == 0) {
            createNewMetadataLedger();
        }
    }

    void createNewMetadataLedger() {
        createNewMetadataLedger(new VoidCallback() {
            @Override
            public void operationComplete() {
                // We now have a new ledger where we can write
                synchronized (pendingMarkDeleteOps) {
                    flushPendingMarkDeletes();

                    // Resume normal mark-delete operations
                    changeStateIfNotClosed(State.Open);
                }
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                log.error().attr("error", exception).log("Metadata ledger creation failed");
                synchronized (pendingMarkDeleteOps) {
                    // At this point we don't have a ledger ready
                    changeStateIfNotClosed(State.NoLedger);
                    // There are two case may cause switch ledger fails.
                    // 1. No enough BKs; BKs are in read-only mode...
                    // 2. Write ZK fails.
                    // Regarding the case "No enough BKs", try to persist the position in the metadata store before
                    // giving up.
                    if (!(exception instanceof MetaStoreException)) {
                        flushPendingMarkDeletes();
                    } else {
                        while (!pendingMarkDeleteOps.isEmpty()) {
                            MarkDeleteEntry entry = pendingMarkDeleteOps.poll();
                            entry.callback.markDeleteFailed(exception, entry.ctx);
                        }
                    }
                }
            }
        });
    }

    /**
     * Try set {@link #state} to {@link State#Closing}.
     * @return false if the {@link #state} already is {@link State#Closing} or {@link State#Closed}.
     */
    private boolean trySetStateToClosing() {
        State previousState = changeStateIfNotClosed(State.Closing);
        return !previousState.isClosed();
    }

    private void flushPendingMarkDeletes() {
        if (!pendingMarkDeleteOps.isEmpty()) {
            internalFlushPendingMarkDeletes();
        }
    }

    void internalFlushPendingMarkDeletes() {
        MarkDeleteEntry lastEntry = pendingMarkDeleteOps.getLast();
        lastEntry.callbackGroup = Lists.newArrayList(pendingMarkDeleteOps);
        pendingMarkDeleteOps.clear();

        internalMarkDelete(lastEntry);
    }

    void createNewMetadataLedger(final VoidCallback callback) {
        ledger.mbean.startCursorLedgerCreateOp();
        doCreateNewMetadataLedger().thenAccept(newLedgerHandle -> {
            if (newLedgerHandle == null) {
                return;
            }
            MarkDeleteEntry mdEntry = lastMarkDeleteEntry;
            // Created the ledger, now write the last position content
            persistPositionToLedger(newLedgerHandle, mdEntry, new VoidCallback() {
                @Override
                public void operationComplete() {
                    log.debug().attr("position", mdEntry.newPosition).log("Persisted position");
                    switchToNewLedger(newLedgerHandle, callback);
                }

                @Override
                public void operationFailed(ManagedLedgerException exception) {
                    log.warn().attr("position", mdEntry.newPosition).log("Failed to persist position");

                    deleteLedgerAsync(newLedgerHandle);
                    callback.operationFailed(exception);
                }
            }, false);
        }).whenComplete((result, e) -> {
            ledger.mbean.endCursorLedgerCreateOp();
            if (e != null) {
                callback.operationFailed(createManagedLedgerException(e));
            }
        });
    }

    private CompletableFuture<LedgerHandle> doCreateNewMetadataLedger() {
        CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        ledger.asyncCreateLedger(bookkeeper, getConfig(), digestType, (rc, lh, ctx) -> {

            if (ledger.checkAndCompleteLedgerOpTask(rc, lh, ctx)) {
                future.complete(null);
                return;
            }

            ledger.getExecutor().execute(() -> {
                ledger.mbean.endCursorLedgerCreateOp();
                if (rc != BKException.Code.OK) {
                    log.warn().attr("errorMessage", BKException.getMessage(rc)).log("Error creating cursor ledger");
                    future.completeExceptionally(new ManagedLedgerException(BKException.getMessage(rc)));
                    return;
                }

                log.debug().attr("ledgerId", lh.getId()).log("Created cursor ledger");
                future.complete(lh);
            });
        }, LedgerMetadataUtils.buildAdditionalMetadataForCursor(name));

        return future;
    }

    private CompletableFuture<Void> deleteLedgerAsync(LedgerHandle ledgerHandle) {
        ledger.mbean.startCursorLedgerDeleteOp();
        CompletableFuture<Void> future = new CompletableFuture<>();
        bookkeeper.asyncDeleteLedger(ledgerHandle.getId(), (int rc, Object ctx) -> {
            future.complete(null);
            ledger.mbean.endCursorLedgerDeleteOp();
            if (rc != BKException.Code.OK) {
                log.warn().attr("ledgerId", ledgerHandle.getId()).log("Failed to delete orphan ledger");
            }
        }, null);
        return future;
    }


    private static List<LongProperty> buildPropertiesMap(Map<String, Long> properties) {
        if (properties.isEmpty()) {
            return Collections.emptyList();
        }

        List<LongProperty> longProperties = new ArrayList<>();
        properties.forEach((name, value) -> {
            LongProperty lp = new LongProperty().setName(name).setValue(value);
            longProperties.add(lp);
        });

        return longProperties;
    }

    private static List<StringProperty> buildStringPropertiesMap(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return Collections.emptyList();
        }

        List<StringProperty> stringProperties = new ArrayList<>();
        properties.forEach((name, value) -> {
            StringProperty sp = new StringProperty().setName(name).setValue(value);
            stringProperties.add(sp);
        });

        return stringProperties;
    }

    private List<MessageRange> buildIndividualDeletedMessageRanges() {
        lock.writeLock().lock();
        try {
            if (individualDeletedMessages.isEmpty()) {
                this.individualDeletedMessagesSerializedSize = 0;
                return Collections.emptyList();
            }

            AtomicInteger acksSerializedSize = new AtomicInteger(0);
            List<MessageRange> rangeList = new ArrayList<>();
            final int maxRanges = getConfig().getMaxUnackedRangesToPersist();
            final MutableBoolean truncated = new MutableBoolean(false);

            individualDeletedMessages.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
                if (rangeList.size() >= maxRanges) {
                    truncated.setTrue();
                    return false;
                }
                MessageRange messageRange = new MessageRange();
                messageRange.setLowerEndpoint()
                        .setLedgerId(lowerKey)
                        .setEntryId(lowerValue);
                messageRange.setUpperEndpoint()
                        .setLedgerId(upperKey)
                        .setEntryId(upperValue);

                acksSerializedSize.addAndGet(messageRange.getSerializedSize());
                rangeList.add(messageRange);

                return true;
            });

            this.individualDeletedMessagesSerializedSize = acksSerializedSize.get();
            individualDeletedMessages.resetDirtyKeys();

            if (truncated.booleanValue()) {
                ledger.getFactory().getOpenTelemetryManagedCursorStats()
                        .incrementPersistUnackedRangesTruncated(this);
                if (lastCursorDataFullyPersistable.compareAndSet(true, false)) {
                    int totalRanges = individualDeletedMessages.size();
                    log.warn()
                        .attr("totalRanges", totalRanges)
                        .attr("maxRanges", maxRanges)
                        .attr("truncated", totalRanges - rangeList.size())
                        .log("Individually deleted message ranges exceed"
                            + " managedLedgerMaxUnackedRangesToPersist."
                            + " Acknowledged messages beyond this limit are not persisted"
                            + " and will be replayed on broker restart."
                            + " Consider raising managedLedgerMaxUnackedRangesToPersist,"
                            + " verifying managedLedgerPersistIndividualAckAsLongArray=true (the default),"
                            + " and setting managedCursorInfoCompressionType=LZ4 to reduce the persisted size.");
                }
            } else {
                lastCursorDataFullyPersistable.compareAndSet(false, true);
            }

            return rangeList;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private List<BatchedEntryDeletionIndexInfo> buildBatchEntryDeletionIndexInfoList() {
        lock.readLock().lock();
        try {
            if (batchDeletedIndexes == null || batchDeletedIndexes.isEmpty()) {
                return Collections.emptyList();
            }
            List<BatchedEntryDeletionIndexInfo> result = new ArrayList<>();
            final var iterator = batchDeletedIndexes.entrySet().iterator();
            int maxIndexes = getConfig().getMaxBatchDeletedIndexToPersist();
            while (iterator.hasNext() && result.size() < maxIndexes) {
                final var entry = iterator.next();
                BatchedEntryDeletionIndexInfo batchDeletedIndexInfo = new BatchedEntryDeletionIndexInfo();
                batchDeletedIndexInfo.setPosition()
                        .setLedgerId(entry.getKey().getLedgerId())
                        .setEntryId(entry.getKey().getEntryId());
                long[] array = entry.getValue().toLongArray();
                for (long l : array) {
                    batchDeletedIndexInfo.addDeleteSet(l);
                }
                result.add(batchDeletedIndexInfo);
            }

            if (iterator.hasNext()) {
                ledger.getFactory().getOpenTelemetryManagedCursorStats()
                        .incrementPersistBatchDeletedIndexesTruncated(this);
                if (lastBatchDeletedIndexFullyPersistable.compareAndSet(true, false)) {
                    int totalIndexes = batchDeletedIndexes.size();
                    log.warn()
                        .attr("totalIndexes", totalIndexes)
                        .attr("maxIndexes", maxIndexes)
                        .attr("truncated", totalIndexes - result.size())
                        .log("Batch deleted indexes exceed"
                            + " managedLedgerMaxBatchDeletedIndexToPersist."
                            + " Partially acknowledged batch messages beyond this limit are not persisted"
                            + " and will be replayed on broker restart."
                            + " Consider raising managedLedgerMaxBatchDeletedIndexToPersist"
                            + " and setting managedCursorInfoCompressionType=LZ4 to reduce the persisted size.");
                }
            } else {
                lastBatchDeletedIndexFullyPersistable.compareAndSet(false, true);
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    void persistPositionToLedger(final LedgerHandle lh, MarkDeleteEntry mdEntry, final VoidCallback callback,
                                 boolean ignoreClosedStateAfterFailure) {
        Position position = mdEntry.newPosition;
        PositionInfo pi = reusablePositionInfo;
        pi.clear();
        pi.setLedgerId(position.getLedgerId())
                .setEntryId(position.getEntryId())
                .addAllBatchedEntryDeletionIndexInfos(buildBatchEntryDeletionIndexInfoList())
                .addAllProperties(buildPropertiesMap(mdEntry.properties));

        Map<Long, long[]> internalRanges = null;
        /**
         * Cursor will create the {@link #individualDeletedMessages} typed {@link LongPairRangeSet.DefaultRangeSet} if
         * disabled the config {@link ManagedLedgerConfig#unackedRangesOpenCacheSetEnabled}.
         * {@link LongPairRangeSet.DefaultRangeSet} never implemented the methods below:
         *   - {@link LongPairRangeSet#toRanges(int)}, which is used to serialize cursor metadata.
         *   - {@link LongPairRangeSet#build(Map)}, which is used to deserialize cursor metadata.
         * Do not enable the feature that https://github.com/apache/pulsar/pull/9292 introduced, to avoid serialization
         * and deserialization error.
         */
        if (getConfig().isUnackedRangesOpenCacheSetEnabled() && getConfig().isPersistIndividualAckAsLongArray()) {
            lock.readLock().lock();
            try {
                internalRanges = individualDeletedMessages.toRanges(getConfig().getMaxUnackedRangesToPersist());
            } catch (Exception e) {
                log.warn().exception(e).log("Failed to serialize individualDeletedMessages");
            } finally {
                lock.readLock().unlock();
            }
        }
        if (internalRanges != null && !internalRanges.isEmpty()) {
            pi.addAllIndividualDeletedMessageRanges(buildLongPropertiesMap(internalRanges));
        } else {
            pi.addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges());
        }

        log.debug().attr("ledgerId", lh.getId()).attr("position", position).log("Appending to cursor ledger");

        requireNonNull(lh);
        byte[] data = pi.toByteArray();
        lh.asyncAddEntry(data, (rc, lh1, entryId, ctx) -> {
            if (rc == BKException.Code.OK) {
                log.debug()
                        .attr("position", position)
                        .attr("ledgerId", lh1.getId())
                        .log("Updated position in meta-ledger");

                rolloverLedgerIfNeeded(lh1);

                mbean.persistToLedger(true);
                mbean.addWriteCursorLedgerSize(data.length);
                callback.operationComplete();
            } else {
                if (!ignoreClosedStateAfterFailure && state.isClosed()) {
                    // After closed the cursor, the in-progress persistence task will get a
                    // BKException.Code.LedgerClosedException.
                    callback.operationFailed(new CursorAlreadyClosedException(String.format("%s %s skipped this"
                            + " persistence, because the cursor already closed", ledger.getName(), name)));
                    return;
                }
                log.warn()
                        .attr("position", position)
                        .attr("ledgerId", lh1.getId())
                        .attr("errorMessage", BKException.getMessage(rc))
                        .log("Error updating position in meta-ledger");
                // If we've had a write error, the ledger will be automatically closed, we need to create a new one,
                // in the meantime the mark-delete will be queued.
                STATE_UPDATER.compareAndSet(ManagedCursorImpl.this, State.Open, State.NoLedger);

                // Before giving up, try to persist the position in the metadata store.
                persistPositionToMetaStore(mdEntry, callback);
            }
        }, null);
    }

    public boolean periodicRollover() {
        LedgerHandle lh = cursorLedger;
        if (state == State.Open && lh != null && lh.getLength() > 0) {
            boolean triggered = rolloverLedgerIfNeeded(lh);
            if (triggered) {
                log.info().attr("length", lh.getLength()).log("Periodic rollover triggered");
            } else {
                log.debug().attr("length", lh.getLength()).log("Periodic rollover skipped");

            }
            return triggered;
        }
        return false;
    }

    boolean rolloverLedgerIfNeeded(LedgerHandle lh1) {
        if (shouldCloseLedger(lh1)) {
            log.debug("Need to create new metadata ledger");
            startCreatingNewMetadataLedger();
            return true;
        }
        return false;
    }

    void persistPositionToMetaStore(MarkDeleteEntry mdEntry, final VoidCallback callback) {
        final Position newPosition = mdEntry.newPosition;
        STATE_UPDATER.compareAndSet(ManagedCursorImpl.this, State.Open, State.NoLedger);
        mbean.persistToLedger(false);
        // Before giving up, try to persist the position in the metadata store
        persistPositionMetaStore(-1, newPosition, mdEntry.properties, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                log.debug()
                        .attr("position", newPosition)
                        .log("Updated cursor in meta store after previous failure in ledger");
                mbean.persistToZookeeper(true);
                callback.operationComplete();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn()
                        .attr("errorMessage", e.getMessage())
                        .log("Failed to update cursor in meta store after previous failure in ledger");
                mbean.persistToZookeeper(false);
                callback.operationFailed(createManagedLedgerException(e));
            }
        }, true);
    }

    boolean shouldCloseLedger(LedgerHandle lh) {
        long now = clock.millis();
        if (ledger.getFactory().isMetadataServiceAvailable()
                && (lh.getLastAddConfirmed() >= getConfig().getMetadataMaxEntriesPerLedger()
                || lastLedgerSwitchTimestamp < (now - getConfig().getLedgerRolloverTimeout() * 1000))
                && !state.isClosed()) {
            // It's safe to modify the timestamp since this method will be only called from a callback, implying that
            // calls will be serialized on one single thread
            lastLedgerSwitchTimestamp = now;
            return true;
        } else {
            return false;
        }
    }

    void switchToNewLedger(final LedgerHandle lh, final VoidCallback callback) {
        log.debug().attr("ledgerId", lh.getId()).log("Switching to new metadata ledger");
        persistPositionMetaStore(lh.getId(), lastMarkDeleteEntry.newPosition, lastMarkDeleteEntry.properties,
                new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                log.info()
                        .attr("ledgerId", lh.getId())
                        .attr("markDeletePosition", markDeletePosition)
                        .attr("readPosition", readPosition)
                        .log("Updated cursor with new ledger");
                final LedgerHandle oldLedger = cursorLedger;
                cursorLedger = lh;
                isCursorLedgerReadOnly = false;

                // At this point the position had already been safely markdeleted
                callback.operationComplete();

                asyncDeleteLedger(oldLedger);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn().exception(e).log("Failed to update cursor metadata");
                // it means it failed to switch the newly created ledger so, it should be
                // deleted to prevent leak
                deleteLedgerAsync(lh).thenRun(() -> callback.operationFailed(e));
            }
        }, false);
    }

    /**
     *
     * @return Whether the cursor responded to the notification
     */
    void notifyEntriesAvailable() {
        log.debug("Received managed ledger notification");

        OpReadEntry opReadEntry = WAITING_READ_OP_UPDATER.getAndUpdate(this, current -> {
            // if the waitingReadOp is WAITING_READ_OP_FOR_CLOSED_CURSOR, keep it as is
            if (current == OpReadEntry.WAITING_READ_OP_FOR_CLOSED_CURSOR) {
                return current;
            } else {
                // Otherwise, clear the waiting read operation
                return null;
            }
        });

        // ignore the notification if the cursor is already closed
        if (opReadEntry == OpReadEntry.WAITING_READ_OP_FOR_CLOSED_CURSOR) {
            return;
        }

        if (opReadEntry != null) {
            log.debug()
                    .attr("readPosition", opReadEntry.readPosition)
                    .attr("lastConfirmedEntry", ledger.lastConfirmedEntry)
                    .log("Received notification of new messages persisted");
            log.debug()
                    .attr("messagesConsumedCounter", messagesConsumedCounter)
                    .attr("markDeletePosition", markDeletePosition)
                    .attr("readPosition", readPosition)
                    .log("Cursor notification counters");
            if (isClosed()) {
                // If the cursor is closed, we should not read any more entries
                log.debug("Cursor is already closed, ignoring notification");
                opReadEntry.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException(
                        "Cursor was already closed"), opReadEntry.ctx);
                return;
            }
            PENDING_READ_OPS_UPDATER.incrementAndGet(this);
            opReadEntry.readPosition = getReadPosition();
            ledger.asyncReadEntries(opReadEntry);
        } else {
            // No one is waiting to be notified. Ignore
            log.debug("Received notification but had no pending read operation");
        }
    }

    void asyncCloseCursorLedger(final AsyncCallbacks.CloseCallback callback, final Object ctx) {
        LedgerHandle lh = cursorLedger;
        ledger.mbean.startCursorLedgerCloseOp();
        log.info().attr("ledgerId", lh.getId()).log("Closing metadata ledger");
        lh.asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                ledger.mbean.endCursorLedgerCloseOp();
                if (rc == BKException.Code.OK) {
                    log.info().attr("ledgerId", cursorLedger.getId()).log("Closed cursor-ledger");
                    callback.closeComplete(ctx);
                } else {
                    log.warn()
                            .attr("ledgerId", cursorLedger.getId())
                            .attr("errorMessage", BKException.getMessage(rc))
                            .log("Failed to close cursor-ledger");
                    callback.closeFailed(createManagedLedgerException(rc), ctx);
                }
            }
        }, ctx);
    }

    void decrementPendingMarkDeleteCount() {
        if (PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.decrementAndGet(this) == 0) {
            if (state == State.SwitchingLedger) {
                // A metadata ledger switch was pending and now we can do it since we don't have any more
                // outstanding mark-delete requests
                createNewMetadataLedger();
            }
        }
    }

    void readOperationCompleted() {
        if (PENDING_READ_OPS_UPDATER.decrementAndGet(this) == 0) {
            synchronized (pendingMarkDeleteOps) {
                if (state == State.Open) {
                    // Flush the pending writes only if the state is open.
                    flushPendingMarkDeletes();
                } else if (PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.get(this) != 0) {
                    log.info()
                            .log("Read operation completed and cursor was closed,"
                                    + " need to call any queued cursor close");
                }
            }
        }
    }

    void asyncDeleteLedger(final LedgerHandle lh) {
        asyncDeleteLedger(lh, DEFAULT_LEDGER_DELETE_RETRIES);
    }

    private void asyncDeleteLedger(final LedgerHandle lh, int retry) {
        if (lh == null || retry <= 0) {
            if (lh != null) {
                log.warn().attr("ledgerId", lh.getId()).log("Failed to delete cursor ledger after retries");
            }
            return;
        }

        ledger.mbean.startCursorLedgerDeleteOp();
        bookkeeper.asyncDeleteLedger(lh.getId(), (rc, ctx) -> {
            ledger.mbean.endCursorLedgerDeleteOp();
            if (rc != BKException.Code.OK) {
                log.warn()
                        .attr("ledgerId", lh.getId())
                        .attr("errorMessage", BKException.getMessage(rc))
                        .log("Failed to delete cursor ledger");
                if (!isNoSuchLedgerExistsException(rc)) {
                    ledger.getScheduledExecutor().schedule(() -> asyncDeleteLedger(lh, retry - 1),
                        DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC, TimeUnit.SECONDS);
                }
                return;
            } else {
                log.info().attr("ledgerId", lh.getId()).log("Successfully closed and deleted cursor ledger");
            }
        }, null);
    }

    void asyncDeleteCursorLedger() {
        asyncDeleteCursorLedger(DEFAULT_LEDGER_DELETE_RETRIES);
    }

    private void asyncDeleteCursorLedger(int retry) {
        State beforeChangingState = changeStateToDeletingIfNotDeleted();
        if (beforeChangingState == State.Deleted) {
            log.warn().attr("state", beforeChangingState).log("Cursor ledger is already deleted");
            return;
        }

        closeWaitingCursor();

        if (cursorLedger == null) {
            log.warn("There's no cursor ledger available for deletion.");
            state = State.DeletingFailed;
            return;
        }

        if (retry <= 0) {
            log.warn().attr("ledgerId", cursorLedger.getId()).log("Failed to delete cursor ledger after retries");
            state = State.DeletingFailed;
            return;
        }

        ledger.mbean.startCursorLedgerDeleteOp();
        bookkeeper.asyncDeleteLedger(cursorLedger.getId(), (rc, ctx) -> {
            ledger.mbean.endCursorLedgerDeleteOp();
            if (rc == BKException.Code.OK) {
                state = State.Deleted;
                log.info().attr("ledgerId", cursorLedger.getId()).log("Deleted cursor ledger");
            } else {
                log.warn()
                        .attr("ledgerId", cursorLedger.getId())
                        .attr("errorMessage", BKException.getMessage(rc))
                        .log("Failed to delete cursor ledger");
                if (!isNoSuchLedgerExistsException(rc)) {
                    state = State.DeletingFailed;
                    ledger.getScheduledExecutor().schedule(() -> asyncDeleteCursorLedger(retry - 1),
                            DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC, TimeUnit.SECONDS);
                } else {
                    state = State.Deleted;
                }
            }
        }, null);
    }

    /**
     * Change the state to {@link State#Deleting} if the current state is not {@link State#Deleted}.
     * @return The state before changing.
     */
    State changeStateToDeletingIfNotDeleted() {
        return STATE_UPDATER.getAndUpdate(this, current -> {
            // don't change the state if it's already deleted
            if (current == State.Deleted) {
                return current;
            }
            return State.Deleting;
        });
    }

    /**
     * return BK error codes that are considered not likely to be recoverable.
     */
    public static boolean isBkErrorNotRecoverable(int rc) {
        switch (rc) {
        case Code.NoSuchLedgerExistsException:
        case Code.NoSuchLedgerExistsOnMetadataServerException:
        case Code.ReadException:
        case Code.LedgerRecoveryException:
        case Code.NoSuchEntryException:
            return true;

        default:
            return false;
        }
    }

    /**
     * If we fail to recover the cursor ledger, we want to still open the ML and rollback.
     *
     * @param info
     */
    private Position getRollbackPosition(ManagedCursorInfo info) {
        Position firstPosition = ledger.getFirstPosition();
        Position snapshottedPosition =
                PositionFactory.create(info.getMarkDeleteLedgerId(), info.getMarkDeleteEntryId());
        if (firstPosition == null) {
            // There are no ledgers in the ML, any position is good
            return snapshottedPosition;
        } else if (snapshottedPosition.compareTo(firstPosition) < 0) {
            // The snapshotted position might be pointing to a ledger that was already deleted
            return firstPosition;
        } else {
            return snapshottedPosition;
        }
    }

    // / Expose internal values for debugging purpose
    public int getPendingReadOpsCount() {
        return PENDING_READ_OPS_UPDATER.get(this);
    }

    public long getMessagesConsumedCounter() {
        return messagesConsumedCounter;
    }

    public long getCursorLedger() {
        LedgerHandle lh = cursorLedger;
        return lh != null ? lh.getId() : -1;
    }

    public long getCursorLedgerLastEntry() {
        LedgerHandle lh = cursorLedger;
        return lh != null ? lh.getLastAddConfirmed() : -1;
    }

    public String getIndividuallyDeletedMessages() {
        lock.readLock().lock();
        try {
            return individualDeletedMessages.toString();
        } finally {
            lock.readLock().unlock();
        }
    }

    @VisibleForTesting
    public LongPairRangeSet<Position> getIndividuallyDeletedMessagesSet() {
        return individualDeletedMessages;
    }

    public Position processIndividuallyDeletedMessagesAndGetMarkDeletedPosition(
            LongPairRangeSet.RangeProcessor<Position> processor) {
        final Position mdp;
        lock.readLock().lock();
        try {
            mdp = markDeletePosition;
            individualDeletedMessages.forEach(processor);
        } finally {
            lock.readLock().unlock();
        }
        return mdp;
    }

    @Override
    public boolean isMessageDeleted(Position position) {
        lock.readLock().lock();
        try {
            return internalIsMessageDeleted(position);
        } finally {
            lock.readLock().unlock();
        }
    }

    // When this method is called while the external has already acquired a write lock or a read lock,
    // it avoids unnecessary lock nesting.
    private boolean internalIsMessageDeleted(Position position) {
        return position.compareTo(markDeletePosition) <= 0
                || individualDeletedMessages.contains(position.getLedgerId(), position.getEntryId());
    }

    //this method will return a copy of the position's ack set
    @Override
    public long[] getBatchPositionAckSet(Position position) {
        if (batchDeletedIndexes != null) {
            final var bitSet = batchDeletedIndexes.get(position);
            if (bitSet == null) {
                return null;
            } else {
                return bitSet.toLongArray();
            }
        } else {
            return null;
        }
    }

    /**
     * Checks given position is part of deleted-range and returns next position of upper-end as all the messages are
     * deleted up to that point.
     *
     * @param position
     * @return next available position
     */
    public Position getNextAvailablePosition(Position position) {
        lock.readLock().lock();
        try {
            if (individualDeletedMessages.isEmpty()) {
                return ledger.getNextValidPosition(position);
            }
            Range<Position> range = individualDeletedMessages.rangeContaining(position.getLedgerId(),
                    position.getEntryId());
            if (range != null) {
                Position nextPosition = range.upperEndpoint().getNext();
                return (nextPosition != null && nextPosition.compareTo(position) > 0)
                        ? nextPosition : position.getNext();
            }
            return ledger.getNextValidPosition(position);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Position getNextLedgerPosition(long currentLedgerId) {
        Long nextExistingLedger = ledger.getNextValidLedger(currentLedgerId);
        return nextExistingLedger != null ? PositionFactory.create(nextExistingLedger, 0) : null;
    }

    public boolean isIndividuallyDeletedEntriesEmpty() {
        lock.readLock().lock();
        try {
            return individualDeletedMessages.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getLastLedgerSwitchTimestamp() {
        return lastLedgerSwitchTimestamp;
    }

    public String getState() {
        return state.toString();
    }

    @Override
    public double getThrottleMarkDelete() {
        return this.markDeleteLimiter.getRate();
    }

    @Override
    public void setThrottleMarkDelete(double throttleMarkDelete) {
        if (throttleMarkDelete > 0.0) {
            if (markDeleteLimiter == null) {
                markDeleteLimiter = RateLimiter.create(throttleMarkDelete);
            } else {
                this.markDeleteLimiter.setRate(throttleMarkDelete);
            }
        } else {
            // Disable mark-delete rate limiter
            markDeleteLimiter = null;
        }
    }

    @Override
    public ManagedLedger getManagedLedger() {
        return this.ledger;
    }

    @Override
    public Range<Position> getLastIndividualDeletedRange() {
        lock.readLock().lock();
        try {
            return individualDeletedMessages.lastRange();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void trimDeletedEntries(List<Entry> entries) {
        entries.removeIf(entry -> {
            boolean isDeleted = isMessageDeleted(entry.getPosition());
            if (isDeleted) {
                entry.release();
            }
            return isDeleted;
        });
    }

    private ManagedCursorImpl cursorImpl() {
        return this;
    }

    @Override
    public long[] getDeletedBatchIndexesAsLongArray(Position position) {
        if (batchDeletedIndexes != null) {
            final var bitSet = batchDeletedIndexes.get(position);
            return bitSet == null ? null : bitSet.toLongArray();
        } else {
            return null;
        }
    }

    @Override
    public ManagedCursorMXBean getStats() {
        return this.mbean;
    }

    @Override
    public void updateReadStats(int readEntriesCount, long readEntriesSize) {
        this.entriesReadCount += readEntriesCount;
        this.entriesReadSize += readEntriesSize;
    }

    void flush() {
        if (!isDirty) {
            return;
        }

        isDirty = false;
        asyncMarkDelete(lastMarkDeleteEntry.newPosition, lastMarkDeleteEntry.properties, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                log.debug("Flushed dirty mark-delete position");
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                if (exception.getCause() instanceof MarkDeletingMarkedPosition) {
                    // this is not actually a problem, we should not log a stacktrace
                    log.info()
                            .exceptionMessage(exception.getCause())
                            .log("Cannot flush mark-delete position");
                } else {
                    log.warn().exception(exception).log("Failed to flush mark-delete position");
                }
            }
        }, null);
    }

    @Override
    public int applyMaxSizeCap(int maxEntries, long maxSizeBytes) {
        if (maxSizeBytes == NO_MAX_SIZE_LIMIT) {
            return maxEntries;
        }
        int estimatedEntryCount = estimateEntryCountByBytesSize(maxEntries, maxSizeBytes, readPosition, ledger);
        return Math.min(estimatedEntryCount, maxEntries);
    }

    @Override
    public boolean checkAndUpdateReadPositionChanged() {
        Position lastEntry = ledger.lastConfirmedEntry;
        boolean isReadPositionOnTail = lastEntry == null || readPosition == null
                || (lastEntry.compareTo(readPosition) <= 0);
        boolean isReadPositionChanged = readPosition != null && !readPosition.equals(statsLastReadPosition);
        statsLastReadPosition = readPosition;
        return isReadPositionOnTail || isReadPositionChanged;
    }

    private boolean isCompactionCursor() {
        return COMPACTION_CURSOR_NAME.equals(name);
    }

    @VisibleForTesting
    public State getAndSetState(State state) {
        return STATE_UPDATER.getAndSet(this, state);
    }

    public void setCacheReadEntry(boolean cacheReadEntry) {
        this.cacheReadEntry = cacheReadEntry;
    }

    public boolean isCacheReadEntry() {
        return cacheReadEntry;
    }

    public ManagedLedgerConfig getConfig() {
        return getManagedLedger().getConfig();
    }

    /***
     * Create a non-durable cursor and copy the ack stats.
     */
    @Override
    public ManagedCursor duplicateNonDurableCursor(String nonDurableCursorName) throws ManagedLedgerException {
        NonDurableCursorImpl newNonDurableCursor =
                (NonDurableCursorImpl) ledger.newNonDurableCursor(getMarkDeletedPosition(), nonDurableCursorName);
        lock.readLock().lock();
        try {
            if (individualDeletedMessages != null) {
                this.individualDeletedMessages.forEach(range -> {
                    newNonDurableCursor.individualDeletedMessages.addOpenClosed(
                            range.lowerEndpoint().getLedgerId(),
                            range.lowerEndpoint().getEntryId(),
                            range.upperEndpoint().getLedgerId(),
                            range.upperEndpoint().getEntryId());
                    return true;
                });
            }
        } finally {
            lock.readLock().unlock();
        }
        if (batchDeletedIndexes != null) {
            Objects.requireNonNull(newNonDurableCursor.batchDeletedIndexes);
            for (final var entry : this.batchDeletedIndexes.entrySet()) {
                newNonDurableCursor.batchDeletedIndexes.put(entry.getKey(), (BitSet) entry.getValue().clone());
            }
        }
        return newNonDurableCursor;
    }

    @Override
    public ManagedCursorAttributes getManagedCursorAttributes() {
        if (managedCursorAttributes != null) {
            return managedCursorAttributes;
        }
        return ATTRIBUTES_UPDATER.updateAndGet(this, old -> old != null ? old : new ManagedCursorAttributes(this));
    }

    @Override
    public ManagedLedgerInternalStats.CursorStats getCursorStats() {
        ManagedLedgerInternalStats.CursorStats cs = new ManagedLedgerInternalStats.CursorStats();
        cs.markDeletePosition = getMarkDeletedPosition().toString();
        cs.readPosition = getReadPosition().toString();
        cs.waitingReadOp = hasPendingReadRequest();
        cs.pendingReadOps = getPendingReadOpsCount();
        cs.messagesConsumedCounter = getMessagesConsumedCounter();
        cs.cursorLedger = getCursorLedger();
        cs.cursorLedgerLastEntry = getCursorLedgerLastEntry();
        cs.individuallyDeletedMessages = getIndividuallyDeletedMessages();
        cs.lastLedgerSwitchTimestamp = DateFormatter.format(getLastLedgerSwitchTimestamp());
        cs.state = getState();
        cs.active = isActive();
        cs.numberOfEntriesSinceFirstNotAckedMessage = getNumberOfEntriesSinceFirstNotAckedMessage();
        cs.totalNonContiguousDeletedMessagesRange = getTotalNonContiguousDeletedMessagesRange();
        cs.properties = getProperties();
        return cs;
    }

    /**
     * Called by ManagedLedgerImpl to execute the Runnable inside the lock to remove the cursor from it's
     * waiting cursors list.
     * The cursor state is set to unregistered, and it can be registered again for waiting in ManagedLedgerImpl.
     */
    void removeWaitingCursorRequested(Runnable removeWaitingCursorRunnable) {
        synchronized (registerToWaitingCursorsLock) {
            if (!registeredToWaitingCursors) {
                // The cursor hasn't been registered, do not attempt to remove
                log.debug("Skipping removing from waiting cursors since it's not registered.");
                return;
            }
            log.debug("Removing from waiting cursors");
            removeWaitingCursorRunnable.run();
            registeredToWaitingCursors = false;
        }
    }

    /**
     * Called by ManagedLedgerImpl to notify that the cursor has been dequeued from the waiting cursors list.
     */
    void notifyWaitingCursorDequeued() {
        synchronized (registerToWaitingCursorsLock) {
            registeredToWaitingCursors = false;
        }
    }

    /**
     * Called by ManagedLedgerImpl to execute the Runnable inside the lock to remove the cursor from it's
     * waiting cursors list.
     * This method is used to ensure that the cursor is not already registered, resulting in duplicates.
     */
    void addWaitingCursorRequested(Runnable addWaitingCursorRunnable) {
        synchronized (registerToWaitingCursorsLock) {
            if (registeredToWaitingCursors || isClosed()) {
                // The cursor is already registered or closed, do not register again.
                return;
            }
            log.debug("Adding to waiting cursors");
            addWaitingCursorRunnable.run();
            registeredToWaitingCursors = true;
        }
    }

    /**
     * When cache eviction by expected read count is enabled, this method returns the number of cursors
     * that are at the same position or before this cursor.
     *
     * @return the number of cursors at the same position or before
     */
    public int getNumberOfCursorsAtSamePositionOrBefore() {
        if (ledger.getConfig().isCacheEvictionByExpectedReadCount()) {
            return ledger.getNumberOfCursorsAtSamePositionOrBefore(this);
        } else if (isCacheReadEntry()) {
            return 1;
        } else {
            return 0;
        }
    }
}
