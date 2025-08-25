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
import com.google.protobuf.InvalidProtocolBufferException;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.annotation.Nullable;
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
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ScanOutcome;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.PositionBound;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.LongListMap;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.LongProperty;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.MessageRange;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo.Builder;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.StringProperty;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.LongPairConsumer;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.RangeBoundConsumer;
import org.apache.pulsar.metadata.api.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:javadoctype")
public class ManagedCursorImpl implements ManagedCursor {
    private static final Comparator<Entry> ENTRY_COMPARATOR = (e1, e2) -> {
        if (e1.getLedgerId() != e2.getLedgerId()) {
            return e1.getLedgerId() < e2.getLedgerId() ? -1 : 1;
        }

        if (e1.getEntryId() != e2.getEntryId()) {
            return e1.getEntryId() < e2.getEntryId() ? -1 : 1;
        }

        return 0;
    };
    protected final BookKeeper bookkeeper;
    protected final ManagedLedgerImpl ledger;
    private final String name;

    public static final String CURSOR_INTERNAL_PROPERTY_PREFIX = "#pulsar.internal.";

    private volatile Map<String, String> cursorProperties;
    private final BookKeeper.DigestType digestType;

    protected volatile PositionImpl markDeletePosition;

    // this position is have persistent mark delete position
    protected volatile PositionImpl persistentMarkDeletePosition;
    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, PositionImpl>
            INPROGRESS_MARKDELETE_PERSIST_POSITION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, PositionImpl.class,
                    "inProgressMarkDeletePersistPosition");
    protected volatile PositionImpl inProgressMarkDeletePersistPosition;

    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, PositionImpl> READ_POSITION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, PositionImpl.class, "readPosition");
    protected volatile PositionImpl readPosition;
    // keeps sample of last read-position for validation and monitoring if read-position is not moving forward.
    protected volatile PositionImpl statsLastReadPosition;

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

    // Stat of the cursor z-node
    // NOTE: Don't update cursorLedgerStat alone,
    // please use updateCursorLedgerStat method to update cursorLedgerStat and managedCursorInfo at the same time.
    private volatile Stat cursorLedgerStat;
    private volatile ManagedCursorInfo managedCursorInfo;

    private static final LongPairConsumer<PositionImpl> positionRangeConverter = PositionImpl::new;

    private static final RangeBoundConsumer<PositionImpl> positionRangeReverseConverter =
            (position) -> new LongPairRangeSet.LongPair(position.ledgerId, position.entryId);

    private static final LongPairConsumer<PositionImplRecyclable> recyclePositionRangeConverter = (key, value) -> {
        PositionImplRecyclable position = PositionImplRecyclable.create();
        position.ledgerId = key;
        position.entryId = value;
        position.ackSet = null;
        return position;
    };
    protected final RangeSetWrapper<PositionImpl> individualDeletedMessages;

    // Maintain the deletion status for batch messages
    // (ledgerId, entryId) -> deletion indexes
    @Getter
    @VisibleForTesting
    @Nullable protected final ConcurrentSkipListMap<PositionImpl, BitSet> batchDeletedIndexes;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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

    // This is a lock used to update the registration state of the cursor in the managed ledger.
    private final Object registerToWaitingCursorsLock = new Object();
    // This is used to track if the cursor is registered in the managed ledger's waitingCursors queue
    boolean registeredToWaitingCursors = false;

    class MarkDeleteEntry {
        final PositionImpl newPosition;
        final MarkDeleteCallback callback;
        final Object ctx;
        final Map<String, Long> properties;

        // If the callbackGroup is set, it means this mark-delete request was done on behalf of a group of request (just
        // persist the last one in the chain). In this case we need to trigger the callbacks for every request in the
        // group.
        List<MarkDeleteEntry> callbackGroup;

        public MarkDeleteEntry(PositionImpl newPosition, Map<String, Long> properties,
                MarkDeleteCallback callback, Object ctx) {
            this.newPosition = newPosition;
            this.properties = properties;
            this.callback = callback;
            this.ctx = ctx;
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

    @SuppressWarnings("checkstyle:javadoctype")
    public interface VoidCallback {
        void operationComplete();

        void operationFailed(ManagedLedgerException exception);
    }

    ManagedCursorImpl(BookKeeper bookkeeper, ManagedLedgerImpl ledger, String cursorName) {
        this.bookkeeper = bookkeeper;
        this.cursorProperties = Collections.emptyMap();
        this.ledger = ledger;
        this.name = cursorName;
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
    public Map<String, String> getCursorProperties() {
        return cursorProperties;
    }

    private CompletableFuture<Void> computeCursorProperties(
            final Function<Map<String, String>, Map<String, String>> updateFunction) {
        CompletableFuture<Void> updateCursorPropertiesResult = new CompletableFuture<>();

        Map<String, String> newProperties = updateFunction.apply(ManagedCursorImpl.this.cursorProperties);
        if (!isDurable()) {
            this.cursorProperties = Collections.unmodifiableMap(newProperties);
            updateCursorPropertiesResult.complete(null);
            return updateCursorPropertiesResult;
        }

        ManagedCursorInfo copy = ManagedCursorInfo
                .newBuilder(ManagedCursorImpl.this.managedCursorInfo)
                .clearCursorProperties()
                .addAllCursorProperties(buildStringPropertiesMap(newProperties))
                .build();
        final Stat lastCursorLedgerStat = ManagedCursorImpl.this.cursorLedgerStat;
        ledger.getStore().asyncUpdateCursorInfo(ledger.getName(),
                name, copy, lastCursorLedgerStat, new MetaStoreCallback<>() {
                    @Override
                    public void operationComplete(Void result, Stat stat) {
                        log.info("[{}] Updated ledger cursor: {}", ledger.getName(), name);
                        ManagedCursorImpl.this.cursorProperties = Collections.unmodifiableMap(newProperties);
                        updateCursorLedgerStat(copy, stat);
                        updateCursorPropertiesResult.complete(result);
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        log.error("[{}] Error while updating ledger cursor: {} properties {}", ledger.getName(),
                                name, newProperties, e);
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
                    properties.remove(key);
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
        log.info("[{}] Recovering from bookkeeper ledger cursor: {}", ledger.getName(), name);
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), name, new MetaStoreCallback<ManagedCursorInfo>() {
            @Override
            public void operationComplete(ManagedCursorInfo info, Stat stat) {

                updateCursorLedgerStat(info, stat);
                lastActive = info.getLastActive() != 0 ? info.getLastActive() : lastActive;

                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Recover cursor last active to [{}]", ledger.getName(), name, lastActive);
                }

                Map<String, String> recoveredCursorProperties = Collections.emptyMap();
                if (info.getCursorPropertiesCount() > 0) {
                    // Recover properties map
                    recoveredCursorProperties = new HashMap<>();
                    for (int i = 0; i < info.getCursorPropertiesCount(); i++) {
                        StringProperty property = info.getCursorProperties(i);
                        recoveredCursorProperties.put(property.getName(), property.getValue());
                    }
                }
                cursorProperties = recoveredCursorProperties;

                if (info.getCursorsLedgerId() == -1L) {
                    // There is no cursor ledger to read the last position from. It means the cursor has been properly
                    // closed and the last mark-delete position is stored in the ManagedCursorInfo itself.
                    PositionImpl recoveredPosition = new PositionImpl(info.getMarkDeleteLedgerId(),
                            info.getMarkDeleteEntryId());
                    if (info.getIndividualDeletedMessagesCount() > 0) {
                        recoverIndividualDeletedMessages(info.getIndividualDeletedMessagesList());
                    }

                    Map<String, Long> recoveredProperties = Collections.emptyMap();
                    if (info.getPropertiesCount() > 0) {
                        // Recover properties map
                        recoveredProperties = new HashMap<>();
                        for (int i = 0; i < info.getPropertiesCount(); i++) {
                            LongProperty property = info.getProperties(i);
                            recoveredProperties.put(property.getName(), property.getValue());
                        }
                    }

                    recoveredCursor(recoveredPosition, recoveredProperties, recoveredCursorProperties, null);
                    callback.operationComplete();
                } else {
                    // Need to proceed and read the last entry in the specified ledger to find out the last position
                    log.info("[{}] Cursor {} meta-data recover from ledger {}", ledger.getName(), name,
                            info.getCursorsLedgerId());
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
            if (log.isInfoEnabled()) {
                log.info("[{}] Opened ledger {} for cursor {}. rc={}", ledger.getName(), ledgerId, name, rc);
            }
            if (isBkErrorNotRecoverable(rc)) {
                log.error("[{}] Error opening metadata ledger {} for cursor {}: {}", ledger.getName(), ledgerId, name,
                        BKException.getMessage(rc));
                // Rewind to oldest entry available
                initialize(getRollbackPosition(info), Collections.emptyMap(), cursorProperties, callback);
                return;
            } else if (rc != BKException.Code.OK) {
                log.warn("[{}] Error opening metadata ledger {} for cursor {}: {}", ledger.getName(), ledgerId, name,
                        BKException.getMessage(rc));
                callback.operationFailed(new ManagedLedgerException(BKException.getMessage(rc)));
                return;
            }

            // Read the last entry in the ledger
            long lastEntryInLedger = lh.getLastAddConfirmed();

            if (lastEntryInLedger < 0) {
                log.warn("[{}] Error reading from metadata ledger {} for cursor {}: No entries in ledger",
                        ledger.getName(), ledgerId, name);
                // Rewind to last cursor snapshot available
                initialize(getRollbackPosition(info), Collections.emptyMap(), cursorProperties, callback);
                return;
            }

            lh.asyncReadEntries(lastEntryInLedger, lastEntryInLedger, (rc1, lh1, seq, ctx1) -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}} readComplete rc={} entryId={}", ledger.getName(), rc1, lh1.getLastAddConfirmed());
                }
                if (isBkErrorNotRecoverable(rc1)) {
                    log.error("[{}] Error reading from metadata ledger {} for cursor {}: {}", ledger.getName(),
                            ledgerId, name, BKException.getMessage(rc1));
                    // Rewind to oldest entry available
                    initialize(getRollbackPosition(info), Collections.emptyMap(), cursorProperties, callback);
                    return;
                } else if (rc1 != BKException.Code.OK) {
                    log.warn("[{}] Error reading from metadata ledger {} for cursor {}: {}", ledger.getName(),
                            ledgerId, name, BKException.getMessage(rc1));

                    callback.operationFailed(createManagedLedgerException(rc1));
                    return;
                }

                LedgerEntry entry = seq.nextElement();
                mbean.addReadCursorLedgerSize(entry.getLength());
                PositionInfo positionInfo;
                try {
                    positionInfo = PositionInfo.parseFrom(entry.getEntry());
                } catch (InvalidProtocolBufferException e) {
                    callback.operationFailed(new ManagedLedgerException(e));
                    return;
                }

                Map<String, Long> recoveredProperties = Collections.emptyMap();
                if (positionInfo.getPropertiesCount() > 0) {
                    // Recover properties map
                    recoveredProperties = new HashMap<>();
                    for (int i = 0; i < positionInfo.getPropertiesCount(); i++) {
                        LongProperty property = positionInfo.getProperties(i);
                        recoveredProperties.put(property.getName(), property.getValue());
                    }
                }

                PositionImpl position = new PositionImpl(positionInfo);
                recoverIndividualDeletedMessages(positionInfo);
                if (getConfig().isDeletionAtBatchIndexLevelEnabled()
                    && positionInfo.getBatchedEntryDeletionIndexInfoCount() > 0) {
                    recoverBatchDeletedIndexes(positionInfo.getBatchedEntryDeletionIndexInfoList());
                }
                recoveredCursor(position, recoveredProperties, cursorProperties, lh);
                callback.operationComplete();
            }, null);
        };
        try {
            bookkeeper.asyncOpenLedger(ledgerId, digestType, getConfig().getPassword(), openCallback,
                    null);
        } catch (Throwable t) {
            log.error("[{}] Encountered error on opening cursor ledger {} for cursor {}",
                ledger.getName(), ledgerId, name, t);
            openCallback.openComplete(BKException.Code.UnexpectedConditionException, null, null);
        }
    }

    public void recoverIndividualDeletedMessages(PositionInfo positionInfo) {
        if (positionInfo.getIndividualDeletedMessagesCount() > 0) {
            recoverIndividualDeletedMessages(positionInfo.getIndividualDeletedMessagesList());
        } else if (positionInfo.getIndividualDeletedMessageRangesCount() > 0) {
            List<LongListMap> rangeList = positionInfo.getIndividualDeletedMessageRangesList();
            lock.writeLock().lock();
            try {
                Map<Long, long[]> rangeMap = rangeList.stream().collect(Collectors.toMap(LongListMap::getKey,
                        list -> list.getValuesList().stream().mapToLong(i -> i).toArray()));
                // Guarantee compatability for the config "unackedRangesOpenCacheSetEnabled".
                if (getConfig().isUnackedRangesOpenCacheSetEnabled()) {
                    individualDeletedMessages.build(rangeMap);
                } else {
                    RangeSetWrapper<PositionImpl> rangeSetWrapperV2 = new RangeSetWrapper<>(positionRangeConverter,
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
                log.warn("[{}]-{} Failed to recover individualDeletedMessages from serialized data", ledger.getName(),
                        name, e);
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
            org.apache.bookkeeper.mledger.proto.MLDataFormats.LongListMap.Builder lmBuilder = LongListMap.newBuilder()
                    .setKey(id);
            for (long range : ranges) {
                lmBuilder.addValues(range);
            }
            LongListMap lm = lmBuilder.build();
            longListMap.add(lm);
            serializedSize.add(lm.getSerializedSize());
        });
        individualDeletedMessagesSerializedSize = serializedSize.toInteger();
        return longListMap;
    }

    private void recoverIndividualDeletedMessages(List<MLDataFormats.MessageRange> individualDeletedMessagesList) {
        lock.writeLock().lock();
        try {
            individualDeletedMessages.clear();
            individualDeletedMessagesList.forEach(messageRange -> {
                MLDataFormats.NestedPositionInfo lowerEndpoint = messageRange.getLowerEndpoint();
                MLDataFormats.NestedPositionInfo upperEndpoint = messageRange.getUpperEndpoint();

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
                        log.warn("[{}][{}] No ledger info of lower endpoint {}:{}", ledger.getName(), name,
                                lowerEndpoint.getLedgerId(), lowerEndpoint.getEntryId());
                    }

                    for (LedgerInfo li : ledger.getLedgersInfo()
                            .subMap(lowerEndpoint.getLedgerId(), false, upperEndpoint.getLedgerId(), false).values()) {
                        individualDeletedMessages.addOpenClosed(li.getLedgerId(), -1, li.getLedgerId(),
                                li.getEntries() - 1);
                    }

                    individualDeletedMessages.addOpenClosed(upperEndpoint.getLedgerId(), -1,
                            upperEndpoint.getLedgerId(), upperEndpoint.getEntryId());
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void recoverBatchDeletedIndexes (
            List<MLDataFormats.BatchedEntryDeletionIndexInfo> batchDeletedIndexInfoList) {
        Objects.requireNonNull(batchDeletedIndexes);
        lock.writeLock().lock();
        try {
            this.batchDeletedIndexes.clear();
            batchDeletedIndexInfoList.forEach(batchDeletedIndexInfo -> {
                if (batchDeletedIndexInfo.getDeleteSetCount() > 0) {
                    long[] array = new long[batchDeletedIndexInfo.getDeleteSetCount()];
                    for (int i = 0; i < batchDeletedIndexInfo.getDeleteSetList().size(); i++) {
                        array[i] = batchDeletedIndexInfo.getDeleteSetList().get(i);
                    }
                    this.batchDeletedIndexes.put(PositionImpl.get(batchDeletedIndexInfo.getPosition().getLedgerId(),
                        batchDeletedIndexInfo.getPosition().getEntryId()), BitSet.valueOf(array));
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void recoveredCursor(PositionImpl position, Map<String, Long> properties,
                                 Map<String, String> cursorProperties,
                                 LedgerHandle recoveredFromCursorLedger) {
        // if the position was at a ledger that didn't exist (since it will be deleted if it was previously empty),
        // we need to move to the next existing ledger
        if (position.getEntryId() == -1L && !ledger.ledgerExists(position.getLedgerId())) {
            Long nextExistingLedger = ledger.getNextValidLedger(position.getLedgerId());
            if (nextExistingLedger == null) {
                log.info("[{}] [{}] Couldn't find next next valid ledger for recovery {}", ledger.getName(), name,
                        position);
            }
            position = nextExistingLedger != null ? PositionImpl.get(nextExistingLedger, -1) : position;
        }
        if (position.compareTo(ledger.getLastPosition()) > 0) {
            log.warn("[{}] [{}] Current position {} is ahead of last position {}", ledger.getName(), name, position,
                    ledger.getLastPosition());
            position = ledger.getLastPosition();
        }
        log.info("[{}] Cursor {} recovered to position {}", ledger.getName(), name, position);
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

    void initialize(PositionImpl position, Map<String, Long> properties, Map<String, String> cursorProperties,
                    final VoidCallback callback) {
        recoveredCursor(position, properties, cursorProperties, null);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Consumer {} cursor initialized with counters: consumed {} mdPos {} rdPos {}",
                    ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
        }
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

        }, null, PositionImpl.LATEST);

        counter.await();

        if (result.exception != null) {
            throw result.exception;
        }

        return result.entries;
    }

    @Override
    public void asyncReadEntries(final int numberOfEntriesToRead, final ReadEntriesCallback callback,
                                 final Object ctx, PositionImpl maxPosition) {
        asyncReadEntries(numberOfEntriesToRead, NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes, ReadEntriesCallback callback,
                                 Object ctx, PositionImpl maxPosition) {
        asyncReadEntriesWithSkip(numberOfEntriesToRead, maxSizeBytes, callback, ctx, maxPosition, null);
    }

    @Override
    public void asyncReadEntriesWithSkip(int numberOfEntriesToRead, long maxSizeBytes, ReadEntriesCallback callback,
                                 Object ctx, PositionImpl maxPosition, Predicate<PositionImpl> skipCondition) {
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

        PositionImpl startPosition = ledger.getNextValidPosition(markDeletePosition);
        PositionImpl endPosition = ledger.getLastPosition();
        if (startPosition.compareTo(endPosition) <= 0) {
            long numOfEntries = getNumberOfEntries(Range.closed(startPosition, endPosition));
            if (numOfEntries >= n) {
                long deletedMessages = 0;
                if (deletedEntries == IndividualDeletedEntries.Exclude) {
                    deletedMessages = getNumIndividualDeletedEntriesToSkip(n);
                }
                PositionImpl positionAfterN = ledger.getPositionAfterN(markDeletePosition, n + deletedMessages,
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

        }, null, PositionImpl.LATEST);

        counter.await();

        if (result.exception != null) {
            throw result.exception;
        }

        return result.entries;
    }

    @Override
    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx,
                                       PositionImpl maxPosition) {
        asyncReadEntriesOrWait(numberOfEntriesToRead, NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes, ReadEntriesCallback callback, Object ctx,
                                       PositionImpl maxPosition) {
        asyncReadEntriesWithSkipOrWait(maxEntries, maxSizeBytes, callback, ctx, maxPosition, null);
    }

    @Override
    public void asyncReadEntriesWithSkipOrWait(int maxEntries, ReadEntriesCallback callback,
                                               Object ctx, PositionImpl maxPosition,
                                               Predicate<PositionImpl> skipCondition) {
        asyncReadEntriesWithSkipOrWait(maxEntries, NO_MAX_SIZE_LIMIT, callback, ctx, maxPosition, skipCondition);
    }

    @Override
    public void asyncReadEntriesWithSkipOrWait(int maxEntries, long maxSizeBytes, ReadEntriesCallback callback,
                                               Object ctx, PositionImpl maxPosition,
                                               Predicate<PositionImpl> skipCondition) {
        checkArgument(maxEntries > 0);
        if (isClosed()) {
            callback.readEntriesFailed(new CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        int numberOfEntriesToRead = applyMaxSizeCap(maxEntries, maxSizeBytes);

        if (hasMoreEntries() && maxPosition.compareTo(readPosition) >= 0) {
            // If we have available entries, we can read them immediately
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Read entries immediately", ledger.getName(), name);
            }
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

            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Deferring retry of read at position {}", ledger.getName(), name, op.readPosition);
            }

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
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Re-trying the read for op id {}", ledger.getName(), name, opReadId);
            }

            if (isClosed()) {
                callback.readEntriesFailed(new CursorAlreadyClosedException("Cursor was already closed"), ctx);
                return;
            }

            if (!hasMoreEntries()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Still no entries available. Register for notification", ledger.getName(),
                            name);
                }
                // Let the managed ledger know we want to be notified whenever a new entry is published
                ledger.addWaitingCursor(this);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Skip notification registering since we do have entries available",
                            ledger.getName(), name);
                }
            }

            // Check again the entries count, since an entry could have been written between the time we
            // checked and the time we've asked to be notified by managed ledger
            if (hasMoreEntries()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Found more entries", ledger.getName(), name);
                }
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
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Cancelled notification and scheduled read at {}", ledger.getName(),
                                name, op.readPosition);
                    }
                    PENDING_READ_OPS_UPDATER.incrementAndGet(this);
                    ledger.asyncReadEntries(op);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] notification was already cancelled for op id {}", ledger.getName(), name,
                                opReadId);
                    }
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
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Cancel pending read request", ledger.getName(), name);
        }
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
        PositionImpl writerPosition = ledger.getLastPosition();
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
        if (readPosition.compareTo(ledger.getLastPosition().getNext()) > 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Read position {} is ahead of last position {}. There are no entries to read",
                        ledger.getName(), name, readPosition, ledger.getLastPosition());
            }
            return 0;
        } else {
            return getNumberOfEntries(Range.closedOpen(readPosition, ledger.getLastPosition().getNext()));
        }
    }

    @Override
    public long getNumberOfEntriesSinceFirstNotAckedMessage() {
        // sometimes for already caught up consumer: due to race condition markDeletePosition > readPosition. so,
        // validate it before preparing range
        PositionImpl markDeletePosition = this.markDeletePosition;
        PositionImpl readPosition = this.readPosition;
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
        return ledger.estimateBacklogFromPosition(markDeletePosition);
    }

    private long getNumberOfEntriesInBacklog() {
        if (markDeletePosition.compareTo(ledger.getLastPosition()) >= 0) {
            return 0;
        }
        return getNumberOfEntries(Range.openClosed(markDeletePosition, ledger.getLastPosition()));
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean isPrecise) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Consumer {} cursor ml-entries: {} -- deleted-counter: {} other counters: mdPos {} rdPos {}",
                    ledger.getName(), name, ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.get(ledger),
                    messagesConsumedCounter, markDeletePosition, readPosition);
        }
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
        PositionImpl startPosition = (PositionImpl) position.orElseGet(
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
                                        PositionImpl start, PositionImpl end, FindEntryCallback callback,
                                        Object ctx, boolean isFindFromLedger) {
        PositionImpl startPosition;
        switch (constraint) {
            case SearchAllAvailableEntries ->
                    startPosition = start == null ? (PositionImpl) getFirstPosition() : start;
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
        PositionImpl endPosition = end == null ? ledger.lastConfirmedEntry : end;
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
        return firstLedgerId == null ? null : new PositionImpl(firstLedgerId, 0);
    }

    protected void internalResetCursor(PositionImpl proposedReadPosition,
                                       AsyncCallbacks.ResetCursorCallback resetCursorCallback) {
        final PositionImpl newReadPosition;
        if (proposedReadPosition.equals(PositionImpl.EARLIEST)) {
            newReadPosition = ledger.getFirstPosition();
        } else if (proposedReadPosition.equals(PositionImpl.LATEST)) {
            newReadPosition = ledger.getNextValidPosition(ledger.getLastPosition());
        } else {
            newReadPosition = proposedReadPosition;
        }

        log.info("[{}] Initiate reset readPosition from {} to {} on cursor {}", ledger.getName(), readPosition,
                newReadPosition, name);

        synchronized (pendingMarkDeleteOps) {
            if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                log.error("[{}] reset requested - readPosition [{}], previous reset in progress - cursor {}",
                        ledger.getName(), newReadPosition, name);
                resetCursorCallback.resetFailed(
                        new ManagedLedgerException.ConcurrentFindCursorPositionException("reset already in progress"),
                        newReadPosition);
                return;
            }
        }

        final AsyncCallbacks.ResetCursorCallback callback = resetCursorCallback;

        final PositionImpl newMarkDeletePosition = ledger.getPreviousPosition(newReadPosition);

        VoidCallback finalCallback = new VoidCallback() {
            @Override
            public void operationComplete() {

                // modify mark delete and read position since we are able to persist new position for cursor
                lock.writeLock().lock();
                try {
                    if (markDeletePosition.compareTo(newMarkDeletePosition) >= 0) {
                        MSG_CONSUMED_COUNTER_UPDATER.addAndGet(cursorImpl(), -getNumberOfEntries(
                                Range.closedOpen(newMarkDeletePosition, markDeletePosition)));
                    } else {
                        MSG_CONSUMED_COUNTER_UPDATER.addAndGet(cursorImpl(), getNumberOfEntries(
                                Range.closedOpen(markDeletePosition, newMarkDeletePosition)));
                    }
                    markDeletePosition = newMarkDeletePosition;
                    lastMarkDeleteEntry = new MarkDeleteEntry(newMarkDeletePosition, isCompactionCursor()
                            ? getProperties() : Collections.emptyMap(), null, null);
                    individualDeletedMessages.clear();
                    if (batchDeletedIndexes != null) {
                        batchDeletedIndexes.clear();
                        long[] resetWords = newReadPosition.ackSet;
                        if (resetWords != null) {
                                batchDeletedIndexes.put(newReadPosition, BitSet.valueOf(resetWords));
                        }
                    }

                    PositionImpl oldReadPosition = readPosition;
                    if (oldReadPosition.compareTo(newReadPosition) >= 0) {
                        log.info("[{}] reset readPosition to {} before current read readPosition {} on cursor {}",
                                ledger.getName(), newReadPosition, oldReadPosition, name);
                    } else {
                        log.info("[{}] reset readPosition to {} skipping from current read readPosition {} on "
                                        + "cursor {}", ledger.getName(), newReadPosition, oldReadPosition, name);
                    }
                    readPosition = newReadPosition;
                    ledger.onCursorReadPositionUpdated(ManagedCursorImpl.this, newReadPosition);
                } finally {
                    lock.writeLock().unlock();
                }
                synchronized (pendingMarkDeleteOps) {
                    pendingMarkDeleteOps.clear();
                    if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(ManagedCursorImpl.this, TRUE, FALSE)) {
                        log.error("[{}] expected reset readPosition [{}], but another reset in progress on cursor {}",
                                ledger.getName(), newReadPosition, name);
                    }
                }
                updateLastActive();
                callback.resetComplete(newReadPosition);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                synchronized (pendingMarkDeleteOps) {
                    if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(ManagedCursorImpl.this, TRUE, FALSE)) {
                        log.error("[{}] expected reset readPosition [{}], but another reset in progress on cursor {}",
                                ledger.getName(), newReadPosition, name);
                    }
                }
                callback.resetFailed(new ManagedLedgerException.InvalidCursorPositionException(
                        "unable to persist readPosition for cursor reset " + newReadPosition), newReadPosition);
            }

        };

        persistentMarkDeletePosition = null;
        inProgressMarkDeletePersistPosition = null;
        lastMarkDeleteEntry = new MarkDeleteEntry(newMarkDeletePosition, getProperties(), null, null);
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
        }, null);
    }

    @Override
    public void asyncResetCursor(Position newPos, boolean forceReset, AsyncCallbacks.ResetCursorCallback callback) {
        checkArgument(newPos instanceof PositionImpl);
        final PositionImpl newPosition = (PositionImpl) newPos;

        // order trim and reset operations on a ledger
        ledger.getExecutor().execute(() -> {
            PositionImpl actualPosition = newPosition;

            if (!ledger.isValidPosition(actualPosition)
                    && !actualPosition.equals(PositionImpl.EARLIEST)
                    && !actualPosition.equals(PositionImpl.LATEST)
                    && !forceReset) {
                actualPosition = ledger.getNextValidPosition(actualPosition);

                if (actualPosition == null) {
                    // next valid position would only return null when newPos
                    // is larger than all available positions, then it's latest in effect.
                    actualPosition = PositionImpl.LATEST;
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
                log.warn("[{}] Reset cursor to {} on cursor {} timed out with exception {}", ledger.getName(), newPos,
                        name, result.exception);
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
                log.warn("[{}][{}] Error while replaying entries", ledger.getName(), name, mle);
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
                    if (((PositionImpl) p).compareTo(this.readPosition) == 0) {
                        this.setReadPosition(this.readPosition.getNext());
                        log.warn("[{}][{}] replayPosition{} equals readPosition{}," + " need set next readPosition",
                                ledger.getName(), name, p, this.readPosition);
                    }
                    ledger.asyncReadEntry((PositionImpl) p, cb, ctx);
                });

        return alreadyAcknowledgedPositions;
    }

    protected long getNumberOfEntries(Range<PositionImpl> range) {
        long allEntries = ledger.getNumberOfEntries(range);

        if (log.isDebugEnabled()) {
            log.debug("[{}] getNumberOfEntries. {} allEntries: {}", ledger.getName(), range, allEntries);
        }

        AtomicLong deletedEntries = new AtomicLong(0);

        lock.readLock().lock();
        try {
            if (getConfig().isUnackedRangesOpenCacheSetEnabled()) {
                int cardinality = individualDeletedMessages.cardinality(
                        range.lowerEndpoint().ledgerId, range.lowerEndpoint().entryId,
                        range.upperEndpoint().ledgerId, range.upperEndpoint().entryId);
                deletedEntries.addAndGet(cardinality);
            } else {
                individualDeletedMessages.forEach((r) -> {
                    try {
                        if (r.isConnected(range)) {
                            Range<PositionImpl> commonEntries = r.intersection(range);
                            long commonCount = ledger.getNumberOfEntries(commonEntries);
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] [{}] Discounting {} entries for already deleted range {}",
                                        ledger.getName(), name, commonCount, commonEntries);
                            }
                            deletedEntries.addAndGet(commonCount);
                        }
                        return true;
                    } finally {
                        if (r.lowerEndpoint() instanceof PositionImplRecyclable) {
                            ((PositionImplRecyclable) r.lowerEndpoint()).recycle();
                            ((PositionImplRecyclable) r.upperEndpoint()).recycle();
                        }
                    }
                }, recyclePositionRangeConverter);
            }
        } finally {
            lock.readLock().unlock();
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Found {} entries - deleted: {}", ledger.getName(), allEntries - deletedEntries.get(),
                    deletedEntries);
        }
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
        checkArgument(position instanceof PositionImpl);

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
        log.info("[{}] Skipping {} entries on cursor {}", ledger.getName(), numEntriesToSkip, name);
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
                            log.error("[{}] Skip {} entries failed for cursor {}", ledger.getName(), numEntriesToSkip,
                                    name, exception);
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
        PositionImpl startPosition;
        PositionImpl endPosition;

        InvidualDeletedMessagesHandlingState(PositionImpl startPosition) {
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
                        Range<PositionImpl> range = Range.openClosed(state.startPosition, state.endPosition);
                        long entries = ledger.getNumberOfEntries(range);
                        if (state.totalEntriesToSkip + entries >= numEntries) {
                            // do not process further
                            return false;
                        }
                        state.totalEntriesToSkip += entries;
                        state.deletedMessages += ledger.getNumberOfEntries(r);
                        state.startPosition = r.upperEndpoint();
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] deletePosition {} moved ahead without clearing deleteMsgs {} for cursor {}",
                                    ledger.getName(), markDeletePosition, r.lowerEndpoint(), name);
                        }
                    }
                    return true;
                } finally {
                    if (r.lowerEndpoint() instanceof PositionImplRecyclable) {
                        ((PositionImplRecyclable) r.lowerEndpoint()).recycle();
                    }
                }
            }, recyclePositionRangeConverter);
            return state.deletedMessages;
        } finally {
            lock.readLock().unlock();
        }
    }

    boolean hasMoreEntries(PositionImpl position) {
        PositionImpl lastPositionInLedger = ledger.getLastPosition();
        if (position.compareTo(lastPositionInLedger) <= 0) {
            return getNumberOfEntries(Range.closed(position, lastPositionInLedger)) > 0;
        }
        return false;
    }

    void initializeCursorPosition(Pair<PositionImpl, Long> lastPositionCounter) {
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
    PositionImpl setAcknowledgedPosition(PositionImpl newMarkDeletePosition) {
        if (newMarkDeletePosition.compareTo(markDeletePosition) < 0) {
            throw new MarkDeletingMarkedPosition(
                    "Mark deleting an already mark-deleted position. Current mark-delete: " + markDeletePosition
                            + " -- attempted mark delete: " + newMarkDeletePosition);
        }

        PositionImpl oldMarkDeletePosition = markDeletePosition;

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

            PositionImpl positionAfterNewMarkDelete = ledger.getNextValidPosition(newMarkDeletePosition);
            // sometime ranges are connected but belongs to different ledgers so, they are placed sequentially
            // eg: (2:10..3:15] can be returned as (2:10..2:15],[3:0..3:15]. So, try to iterate over connected range and
            // found the last non-connected range which gives new markDeletePosition
            while (positionAfterNewMarkDelete.compareTo(ledger.lastConfirmedEntry) <= 0) {
                if (individualDeletedMessages.contains(positionAfterNewMarkDelete.getLedgerId(),
                        positionAfterNewMarkDelete.getEntryId())) {
                    Range<PositionImpl> rangeToBeMarkDeleted = individualDeletedMessages.rangeContaining(
                            positionAfterNewMarkDelete.getLedgerId(), positionAfterNewMarkDelete.getEntryId());
                    newMarkDeletePosition = rangeToBeMarkDeleted.upperEndpoint();
                    positionAfterNewMarkDelete = ledger.getNextValidPosition(newMarkDeletePosition);
                    // check if next valid position is also deleted and part of the deleted-range
                    continue;
                }
                break;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Moved ack position from: {} to: {} -- skipped: {}", ledger.getName(),
                        oldMarkDeletePosition, newMarkDeletePosition, skippedEntries);
            }
            MSG_CONSUMED_COUNTER_UPDATER.addAndGet(this, skippedEntries);
        }

        // markDelete-position and clear out deletedMsgSet
        markDeletePosition = newMarkDeletePosition;
        individualDeletedMessages.removeAtMost(markDeletePosition.getLedgerId(), markDeletePosition.getEntryId());

        READ_POSITION_UPDATER.updateAndGet(this, currentReadPosition -> {
            if (currentReadPosition.compareTo(markDeletePosition) <= 0) {
                // If the position that is mark-deleted is past the read position, it
                // means that the client has skipped some entries. We need to move
                // read position forward
                PositionImpl newReadPosition = ledger.getNextValidPosition(markDeletePosition);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Moved read position from: {} to: {}, and new mark-delete position {}",
                            ledger.getName(), currentReadPosition, newReadPosition, markDeletePosition);
                }
                ledger.onCursorReadPositionUpdated(ManagedCursorImpl.this, newReadPosition);
                return newReadPosition;
            } else {
                return currentReadPosition;
            }
        });

        return newMarkDeletePosition;
    }

    @Override
    public void asyncMarkDelete(final Position position, final MarkDeleteCallback callback, final Object ctx) {
        asyncMarkDelete(position, Collections.emptyMap(), callback, ctx);
    }

    private final class MarkDeletingMarkedPosition extends IllegalArgumentException {
        public MarkDeletingMarkedPosition(String s) {
            super(s);
        }
    }

    @Override
    public void asyncMarkDelete(final Position position, Map<String, Long> properties,
            final MarkDeleteCallback callback, final Object ctx) {
        requireNonNull(position);
        checkArgument(position instanceof PositionImpl);

        if (isClosed()) {
            callback.markDeleteFailed(new ManagedLedgerException
                    .CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        if (RESET_CURSOR_IN_PROGRESS_UPDATER.get(this) == TRUE) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] cursor reset in progress - ignoring mark delete on position [{}] for cursor [{}]",
                        ledger.getName(), position, name);
            }
            callback.markDeleteFailed(
                    new ManagedLedgerException("Reset cursor in progress - unable to mark delete position "
                            + position.toString()),
                    ctx);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Mark delete cursor {} up to position: {}", ledger.getName(), name, position);
        }

        PositionImpl newPosition = ackBatchPosition((PositionImpl) position);
        if (((PositionImpl) ledger.getLastConfirmedEntry()).compareTo(newPosition) < 0) {
            boolean shouldCursorMoveForward = false;
            try {
                long ledgerEntries = ledger.getLedgerInfo(markDeletePosition.getLedgerId()).get().getEntries();
                Long nextValidLedger = ledger.getNextValidLedger(ledger.getLastConfirmedEntry().getLedgerId());
                shouldCursorMoveForward = nextValidLedger != null
                        && (markDeletePosition.getEntryId() + 1 >= ledgerEntries)
                        && (newPosition.getLedgerId() == nextValidLedger);
            } catch (Exception e) {
                log.warn("Failed to get ledger entries while setting mark-delete-position", e);
            }

            if (shouldCursorMoveForward) {
                log.info("[{}] move mark-delete-position from {} to {} since all the entries have been consumed",
                        ledger.getName(), markDeletePosition, newPosition);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Failed mark delete due to invalid markDelete {} is ahead of last-confirmed-entry {}"
                             + " for cursor [{}]", ledger.getName(), position, ledger.getLastConfirmedEntry(), name);
                }
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
        internalAsyncMarkDelete(newPosition, properties, callback, ctx);
    }

    private PositionImpl ackBatchPosition(PositionImpl position) {
        return Optional.ofNullable(position.getAckSet())
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
                    batchDeletedIndexes.subMap(PositionImpl.EARLIEST, newPosition).clear();
                    return newPosition;
                })
                .orElse(position);
    }

    protected void internalAsyncMarkDelete(final PositionImpl newPosition, Map<String, Long> properties,
            final MarkDeleteCallback callback, final Object ctx) {
        ledger.mbean.addMarkDeleteOp();

        MarkDeleteEntry mdEntry = new MarkDeleteEntry(newPosition, properties, callback, ctx);

        // We cannot write to the ledger during the switch, need to wait until the new metadata ledger is available
        synchronized (pendingMarkDeleteOps) {
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
                log.error("[{}][{}] Invalid cursor state: {}", ledger.getName(), name, state);
                callback.markDeleteFailed(new ManagedLedgerException("Cursor was in invalid state: " + state), ctx);
                break;
            }
        }
    }

    void internalMarkDelete(final MarkDeleteEntry mdEntry) {
        if (persistentMarkDeletePosition != null
                && mdEntry.newPosition.compareTo(persistentMarkDeletePosition) < 0) {
            if (log.isInfoEnabled()) {
                log.info("Skipping updating mark delete position to {}. The persisted mark delete position {} "
                        + "is later.", mdEntry.newPosition, persistentMarkDeletePosition);
            }
            // run with executor to prevent deadlock
            ledger.getExecutor().execute(() -> mdEntry.triggerComplete());
            return;
        }

        PositionImpl inProgressLatest = INPROGRESS_MARKDELETE_PERSIST_POSITION_UPDATER.updateAndGet(this, current -> {
            if (current != null && current.compareTo(mdEntry.newPosition) > 0) {
                return current;
            } else {
                return mdEntry.newPosition;
            }
        });

        // if there's a newer or equal mark delete update in progress, skip it.
        if (inProgressLatest != mdEntry.newPosition) {
            if (log.isInfoEnabled()) {
                log.info("Skipping updating mark delete position to {}. The mark delete position update "
                        + "in progress {} is later.", mdEntry.newPosition, inProgressLatest);
            }
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
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Mark delete cursor {} to position {} succeeded", ledger.getName(), name,
                            mdEntry.newPosition);
                }

                INPROGRESS_MARKDELETE_PERSIST_POSITION_UPDATER.compareAndSet(ManagedCursorImpl.this,
                        mdEntry.newPosition, null);

                // Remove from the individual deleted messages all the entries before the new mark delete
                // point.
                lock.writeLock().lock();
                try {
                    individualDeletedMessages.removeAtMost(mdEntry.newPosition.getLedgerId(),
                            mdEntry.newPosition.getEntryId());
                    if (batchDeletedIndexes != null) {
                        batchDeletedIndexes.subMap(PositionImpl.EARLIEST,
                                false, PositionImpl.get(mdEntry.newPosition.getLedgerId(),
                                mdEntry.newPosition.getEntryId()), true).clear();
                    }
                    persistentMarkDeletePosition = mdEntry.newPosition;
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
                log.warn("[{}] Failed to mark delete position for cursor={} position={}", ledger.getName(),
                        ManagedCursorImpl.this, mdEntry.newPosition);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Consumer {} cursor mark delete failed with counters: consumed {} mdPos {} rdPos {}",
                            ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
                }

                decrementPendingMarkDeleteCount();

                mdEntry.triggerFailed(exception);
            }
        };

        if (state == State.NoLedger) {
            if (ledger.isNoMessagesAfterPos(mdEntry.newPosition)) {
                log.error("[{}][{}] Metadata ledger creation failed, try to persist the position in the metadata"
                        + " store.", ledger.getName(), name);
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
                    log.warn("[{}] [{}] Delete operation timeout. Callback deleteComplete at position {}",
                            ledger.getName(), name, positions);
                }

                counter.countDown();
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;

                if (timeout.get()) {
                    log.warn("[{}] [{}] Delete operation timeout. Callback deleteFailed at position {}",
                            ledger.getName(), name, positions);
                }

                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            timeout.set(true);
            log.warn("[{}] [{}] Delete operation timeout. No callback was triggered at position {}", ledger.getName(),
                    name, positions);
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

        PositionImpl newMarkDeletePosition = null;

        lock.writeLock().lock();
        boolean skipMarkDeleteBecauseAckedNothing = false;
        try {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Deleting individual messages at {}. Current status: {} - md-position: {}",
                        ledger.getName(), name, positions, individualDeletedMessages, markDeletePosition);
            }

            for (Position pos : positions) {
                PositionImpl position  = (PositionImpl) requireNonNull(pos);
                if (((PositionImpl) ledger.getLastConfirmedEntry()).compareTo(position) < 0) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                            "[{}] Failed mark delete due to invalid markDelete {} is ahead of last-confirmed-entry {} "
                            + "for cursor [{}]", ledger.getName(), position, ledger.getLastConfirmedEntry(), name);
                    }
                    callback.deleteFailed(new ManagedLedgerException("Invalid mark deleted position"), ctx);
                    return;
                }

                if (internalIsMessageDeleted(position)) {
                    if (batchDeletedIndexes != null) {
                        batchDeletedIndexes.remove(position);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Position was already deleted {}", ledger.getName(), name, position);
                    }
                    continue;
                }
                if (position.ackSet == null || position.ackSet.length == 0) {
                    if (batchDeletedIndexes != null) {
                        batchDeletedIndexes.remove(position);
                    }
                    // Add a range (prev, pos] to the set. Adding the previous entry as an open limit to the range will
                    // make the RangeSet recognize the "continuity" between adjacent Positions.
                    // Before https://github.com/apache/pulsar/pull/21105 is merged, the range does not support crossing
                    // multi ledgers, so the first position's entryId maybe "-1".
                    PositionImpl previousPosition;
                    if (position.getEntryId() == 0) {
                        previousPosition = new PositionImpl(position.getLedgerId(), -1);
                    } else {
                        previousPosition = ledger.getPreviousPosition(position);
                    }
                    individualDeletedMessages.addOpenClosed(previousPosition.getLedgerId(),
                        previousPosition.getEntryId(), position.getLedgerId(), position.getEntryId());
                    MSG_CONSUMED_COUNTER_UPDATER.incrementAndGet(this);

                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Individually deleted messages: {}", ledger.getName(), name,
                            individualDeletedMessages);
                    }
                } else if (batchDeletedIndexes != null) {
                    final var givenBitSet = BitSet.valueOf(position.ackSet);
                    final var bitSet = batchDeletedIndexes.computeIfAbsent(position, __ -> givenBitSet);
                    if (givenBitSet != bitSet) {
                        bitSet.and(givenBitSet);
                    }
                    if (bitSet.isEmpty()) {
                        PositionImpl previousPosition = ledger.getPreviousPosition(position);
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
            Range<PositionImpl> range = individualDeletedMessages.firstRange();

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

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Found a position range to mark delete for cursor {}: {} ", ledger.getName(),
                            name, range);
                }

                newMarkDeletePosition = range.upperEndpoint();
            }

            if (newMarkDeletePosition != null) {
                newMarkDeletePosition = setAcknowledgedPosition(newMarkDeletePosition);
            } else {
                newMarkDeletePosition = markDeletePosition;
            }
        } catch (Exception e) {
            log.warn("[{}] [{}] Error while updating individualDeletedMessages [{}]", ledger.getName(), name,
                    e.getMessage(), e);
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
            Map<String, Long> properties = lastMarkDeleteEntry != null ? lastMarkDeleteEntry.properties
                    : Collections.emptyMap();

            internalAsyncMarkDelete(newMarkDeletePosition, properties, new MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    callback.deleteComplete(ctx);
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    callback.deleteFailed(exception, ctx);
                }

            }, ctx);

        } catch (Exception e) {
            log.warn("[{}] [{}] Error doing asyncDelete [{}]", ledger.getName(), name, e.getMessage(), e);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Consumer {} cursor asyncDelete error, counters: consumed {} mdPos {} rdPos {}",
                        ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
            }
            callback.deleteFailed(new ManagedLedgerException(e), ctx);
        }
    }

    // update lastMarkDeleteEntry field if newPosition is later than the current lastMarkDeleteEntry.newPosition
    private void updateLastMarkDeleteEntryToLatest(final PositionImpl newPosition,
                                                   final Map<String, Long> properties) {
        LAST_MARK_DELETE_ENTRY_UPDATER.updateAndGet(this, last -> {
            if (last != null && last.newPosition.compareTo(newPosition) > 0) {
                // keep current value, don't update
                return last;
            } else {
                // use given properties or when missing, use the properties from the previous field value
                Map<String, Long> propertiesToUse =
                        properties != null ? properties : (last != null ? last.properties : Collections.emptyMap());
                return new MarkDeleteEntry(newPosition, propertiesToUse, null, null);
            }
        });
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
            Range<PositionImpl> entriesRange = Range.closed((PositionImpl) entries.get(0).getPosition(),
                    (PositionImpl) entries.get(entries.size() - 1).getPosition());
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Filtering entries {} - alreadyDeleted: {}", ledger.getName(), name, entriesRange,
                        individualDeletedMessages);
            }
            Range<PositionImpl> span = individualDeletedMessages.isEmpty() ? null : individualDeletedMessages.span();
            if (span == null || !entriesRange.isConnected(span)) {
                // There are no individually deleted messages in this entry list, no need to perform filtering
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] No filtering needed for entries {}", ledger.getName(), name, entriesRange);
                }
                return entries;
            } else {
                // Remove from the entry list all the entries that were already marked for deletion
                return Lists.newArrayList(Collections2.filter(entries, entry -> {
                    boolean includeEntry = !individualDeletedMessages.contains(entry.getLedgerId(), entry.getEntryId());
                    if (!includeEntry) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] [{}] Filtering entry at {} - already deleted", ledger.getName(), name,
                                    entry.getPosition());
                        }

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
            PositionImpl newReadPosition =
                    readCompacted ? markDeletePosition.getNext() : ledger.getNextValidPosition(markDeletePosition);
            PositionImpl oldReadPosition = readPosition;

            log.info("[{}-{}] Rewind from {} to {}", ledger.getName(), name, oldReadPosition, newReadPosition);

            readPosition = newReadPosition;
            ledger.onCursorReadPositionUpdated(ManagedCursorImpl.this, newReadPosition);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void seek(Position newReadPositionInt, boolean force) {
        checkArgument(newReadPositionInt instanceof PositionImpl);
        PositionImpl newReadPosition = (PositionImpl) newReadPositionInt;

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
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Successfully closed ledger for cursor {}", ledger.getName(), name);
                }
                latch.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("[{}] Closing ledger failed for cursor {}", ledger.getName(), name, exception);
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
    void persistPositionWhenClosing(PositionImpl position, Map<String, Long> properties,
            final AsyncCallbacks.CloseCallback callback, final Object ctx) {

        if (shouldPersistUnackRangesToLedger()) {
            persistPositionToLedger(cursorLedger, new MarkDeleteEntry(position, properties, null, null),
                    new VoidCallback() {
                        @Override
                        public void operationComplete() {
                            log.info("[{}][{}] Updated md-position={} into cursor-ledger {}", ledger.getName(), name,
                                    markDeletePosition, cursorLedger.getId());
                            asyncCloseCursorLedger(callback, ctx);
                        }

                        @Override
                        public void operationFailed(ManagedLedgerException e) {
                            log.warn("[{}][{}] Failed to persist mark-delete position into cursor-ledger{}: {}",
                                    ledger.getName(), name, cursorLedger.getId(), e.getMessage());
                            callback.closeFailed(e, ctx);
                        }
                    }, true);
        } else {
            persistPositionMetaStore(-1, position, properties, new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void result, Stat stat) {
                    log.info("[{}][{}] Closed cursor at md-position={}", ledger.getName(), name, markDeletePosition);
                    // At this point the position had already been safely stored in the cursor z-node
                    callback.closeComplete(ctx);
                    asyncDeleteLedger(cursorLedger);
                }

                @Override
                public void operationFailed(MetaStoreException e) {
                    log.warn("[{}][{}] Failed to update cursor info when closing: {}", ledger.getName(), name,
                            e.getMessage());
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

    private void persistPositionMetaStore(long cursorsLedgerId, PositionImpl position, Map<String, Long> properties,
            MetaStoreCallback<Void> callback, boolean persistIndividualDeletedMessageRanges) {
        if (state == State.Closed) {
            ledger.getExecutor().execute(() -> callback.operationFailed(new MetaStoreException(
                    new CursorAlreadyClosedException(name + " cursor already closed"))));
            return;
        }

        final Stat lastCursorLedgerStat = cursorLedgerStat;

        // When closing we store the last mark-delete position in the z-node itself, so we won't need the cursor ledger,
        // hence we write it as -1. The cursor ledger is deleted once the z-node write is confirmed.
        ManagedCursorInfo.Builder info = ManagedCursorInfo.newBuilder() //
                .setCursorsLedgerId(cursorsLedgerId) //
                .setMarkDeleteLedgerId(position.getLedgerId()) //
                .setMarkDeleteEntryId(position.getEntryId()) //
                .setLastActive(lastActive); //

        info.addAllProperties(buildPropertiesMap(properties));
        info.addAllCursorProperties(buildStringPropertiesMap(cursorProperties));
        if (persistIndividualDeletedMessageRanges) {
            info.addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges());
            if (getConfig().isDeletionAtBatchIndexLevelEnabled()) {
                info.addAllBatchedEntryDeletionIndexInfo(buildBatchEntryDeletionIndexInfoList());
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}]  Closing cursor at md-position: {}", ledger.getName(), name, position);
        }

        ManagedCursorInfo cursorInfo = info.build();
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
                            log.warn("[{}] Failed to update cursor metadata for {} due to version conflict {}",
                                    ledger.name, name, topLevelException.getMessage());
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
                                                        if (log.isDebugEnabled()) {
                                                            log.debug(
                                                                    "[{}] Failed to refresh cursor metadata-version "
                                                                            + "for {} due to {}", ledger.name, name,
                                                                    e.getMessage());
                                                        }
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
            log.info("[{}] [{}] State is already closed", ledger.getName(), name);
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
                            log.warn("[{}] [{}] State was modified from closing to {} while closing", ledger.getName(),
                                    name, state);
                            state = State.Closed;
                        }
                        callback.closeComplete(ctx);
                    }

                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        log.warn("[{}] [{}] persistent position failure when closing, the state will remain in"
                                + " state-closing and will no longer work", ledger.getName(), name);
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
        checkArgument(newReadPositionInt instanceof PositionImpl);
        if (this.markDeletePosition == null
                || ((PositionImpl) newReadPositionInt).compareTo(this.markDeletePosition) > 0) {
            this.readPosition = (PositionImpl) newReadPositionInt;
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
        log.warn("[{}] [{}] Since the ledger [{}] is lost and the autoSkipNonRecoverableData is true, this ledger will"
                + " be auto acknowledge in subscription", ledger.getName(), name, ledgerId);
        asyncDelete(() -> LongStream.range(0, ledgerInfo.getEntries())
                        .mapToObj(i -> (Position) PositionImpl.get(ledgerId, i)).iterator(),
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
                log.error("[{}][{}] Metadata ledger creation failed {}", ledger.getName(), name, exception);
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
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Persisted position {} for cursor {}", ledger.getName(),
                                mdEntry.newPosition, name);
                    }
                    switchToNewLedger(newLedgerHandle, callback);
                }

                @Override
                public void operationFailed(ManagedLedgerException exception) {
                    log.warn("[{}] Failed to persist position {} for cursor {}", ledger.getName(),
                            mdEntry.newPosition, name);

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
                    log.warn("[{}] Error creating ledger for cursor {}: {}", ledger.getName(), name,
                            BKException.getMessage(rc));
                    future.completeExceptionally(new ManagedLedgerException(BKException.getMessage(rc)));
                    return;
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Created ledger {} for cursor {}", ledger.getName(), lh.getId(), name);
                }
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
                log.warn("[{}] Failed to delete orphan ledger {}", ledger.getName(),
                        ledgerHandle.getId());
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
            LongProperty lp = LongProperty.newBuilder().setName(name).setValue(value).build();
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
            StringProperty sp = StringProperty.newBuilder().setName(name).setValue(value).build();
            stringProperties.add(sp);
        });

        return stringProperties;
    }

    private List<MLDataFormats.MessageRange> buildIndividualDeletedMessageRanges() {
        lock.writeLock().lock();
        try {
            if (individualDeletedMessages.isEmpty()) {
                this.individualDeletedMessagesSerializedSize = 0;
                return Collections.emptyList();
            }

            MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo
                    .newBuilder();

            MLDataFormats.MessageRange.Builder messageRangeBuilder = MLDataFormats.MessageRange
                    .newBuilder();

            AtomicInteger acksSerializedSize = new AtomicInteger(0);
            List<MessageRange> rangeList = new ArrayList<>();

            individualDeletedMessages.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
                MLDataFormats.NestedPositionInfo lowerPosition = nestedPositionBuilder
                        .setLedgerId(lowerKey)
                        .setEntryId(lowerValue)
                        .build();

                MLDataFormats.NestedPositionInfo upperPosition = nestedPositionBuilder
                        .setLedgerId(upperKey)
                        .setEntryId(upperValue)
                        .build();

                MessageRange messageRange = messageRangeBuilder
                        .setLowerEndpoint(lowerPosition)
                        .setUpperEndpoint(upperPosition)
                        .build();

                acksSerializedSize.addAndGet(messageRange.getSerializedSize());
                rangeList.add(messageRange);

                return rangeList.size() <= getConfig().getMaxUnackedRangesToPersist();
            });

            this.individualDeletedMessagesSerializedSize = acksSerializedSize.get();
            individualDeletedMessages.resetDirtyKeys();
            return rangeList;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private List<MLDataFormats.BatchedEntryDeletionIndexInfo> buildBatchEntryDeletionIndexInfoList() {
        lock.readLock().lock();
        try {
            if (batchDeletedIndexes == null || batchDeletedIndexes.isEmpty()) {
                return Collections.emptyList();
            }
            MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo
                    .newBuilder();
            MLDataFormats.BatchedEntryDeletionIndexInfo.Builder batchDeletedIndexInfoBuilder = MLDataFormats
                    .BatchedEntryDeletionIndexInfo.newBuilder();
            List<MLDataFormats.BatchedEntryDeletionIndexInfo> result = new ArrayList<>();
            final var iterator = batchDeletedIndexes.entrySet().iterator();
            while (iterator.hasNext() && result.size() < getConfig().getMaxBatchDeletedIndexToPersist()) {
                final var entry = iterator.next();
                nestedPositionBuilder.setLedgerId(entry.getKey().getLedgerId());
                nestedPositionBuilder.setEntryId(entry.getKey().getEntryId());
                batchDeletedIndexInfoBuilder.setPosition(nestedPositionBuilder.build());
                long[] array = entry.getValue().toLongArray();
                List<Long> deleteSet = new ArrayList<>(array.length);
                for (long l : array) {
                    deleteSet.add(l);
                }
                batchDeletedIndexInfoBuilder.clearDeleteSet();
                batchDeletedIndexInfoBuilder.addAllDeleteSet(deleteSet);
                result.add(batchDeletedIndexInfoBuilder.build());
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    void persistPositionToLedger(final LedgerHandle lh, MarkDeleteEntry mdEntry, final VoidCallback callback,
                                 boolean ignoreClosedStateAfterFailure) {
        PositionImpl position = mdEntry.newPosition;
        Builder piBuilder = PositionInfo.newBuilder().setLedgerId(position.getLedgerId())
                .setEntryId(position.getEntryId())
                .addAllBatchedEntryDeletionIndexInfo(buildBatchEntryDeletionIndexInfoList())
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
                log.warn("[{}]-{} Failed to serialize individualDeletedMessages", ledger.getName(), name, e);
            } finally {
                lock.readLock().unlock();
            }
        }
        if (internalRanges != null && !internalRanges.isEmpty()) {
            piBuilder.addAllIndividualDeletedMessageRanges(buildLongPropertiesMap(internalRanges));
        } else {
            piBuilder.addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges());
        }
        PositionInfo pi = piBuilder.build();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Cursor {} Appending to ledger={} position={}", ledger.getName(), name, lh.getId(),
                    position);
        }

        requireNonNull(lh);
        byte[] data = pi.toByteArray();
        lh.asyncAddEntry(data, (rc, lh1, entryId, ctx) -> {
            if (rc == BKException.Code.OK) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Updated cursor {} position {} in meta-ledger {}", ledger.getName(), name, position,
                            lh1.getId());
                }

                if (shouldCloseLedger(lh1)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Need to create new metadata ledger for cursor {}", ledger.getName(), name);
                    }
                    startCreatingNewMetadataLedger();
                }

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
                log.warn("[{}] Error updating cursor {} position {} in meta-ledger {}: {}", ledger.getName(), name,
                        position, lh1.getId(), BKException.getMessage(rc));
                // If we've had a write error, the ledger will be automatically closed, we need to create a new one,
                // in the meantime the mark-delete will be queued.
                STATE_UPDATER.compareAndSet(ManagedCursorImpl.this, State.Open, State.NoLedger);

                // Before giving up, try to persist the position in the metadata store.
                persistPositionToMetaStore(mdEntry, callback);
            }
        }, null);
    }

    void persistPositionToMetaStore(MarkDeleteEntry mdEntry, final VoidCallback callback) {
        final PositionImpl newPosition = mdEntry.newPosition;
        STATE_UPDATER.compareAndSet(ManagedCursorImpl.this, State.Open, State.NoLedger);
        mbean.persistToLedger(false);
        // Before giving up, try to persist the position in the metadata store
        persistPositionMetaStore(-1, newPosition, mdEntry.properties, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "[{}][{}] Updated cursor in meta store after previous failure in ledger at position"
                            + " {}", ledger.getName(), name, newPosition);
                }
                mbean.persistToZookeeper(true);
                callback.operationComplete();
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn("[{}][{}] Failed to update cursor in meta store after previous failure in ledger: {}",
                        ledger.getName(), name, e.getMessage());
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
        if (log.isDebugEnabled()) {
            log.debug("[{}] Switching cursor {} to ledger {}", ledger.getName(), name, lh.getId());
        }
        persistPositionMetaStore(lh.getId(), lastMarkDeleteEntry.newPosition, lastMarkDeleteEntry.properties,
                new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                log.info("[{}] Updated cursor {} with ledger id {} md-position={} rd-position={}", ledger.getName(),
                        name, lh.getId(), markDeletePosition, readPosition);
                final LedgerHandle oldLedger = cursorLedger;
                cursorLedger = lh;
                isCursorLedgerReadOnly = false;

                // At this point the position had already been safely markdeleted
                callback.operationComplete();

                asyncDeleteLedger(oldLedger);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn("[{}] Failed to update cursor metadata {}", ledger.getName(), name, e);
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
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Received ml notification", ledger.getName(), name);
        }

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
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received notification of new messages persisted, reading at {} -- last: {}",
                        ledger.getName(), name, opReadEntry.readPosition, ledger.lastConfirmedEntry);
                log.debug("[{}] Consumer {} cursor notification: other counters: consumed {} mdPos {} rdPos {}",
                        ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
            }
            if (isClosed()) {
                // If the cursor is closed, we should not read any more entries
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Cursor is already closed, ignoring notification", ledger.getName(), name);
                }
                opReadEntry.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException(
                        "Cursor was already closed"), opReadEntry.ctx);
                return;
            }
            PENDING_READ_OPS_UPDATER.incrementAndGet(this);
            opReadEntry.readPosition = (PositionImpl) getReadPosition();
            ledger.asyncReadEntries(opReadEntry);
        } else {
            // No one is waiting to be notified. Ignore
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received notification but had no pending read operation", ledger.getName(), name);
            }
        }
    }

    void asyncCloseCursorLedger(final AsyncCallbacks.CloseCallback callback, final Object ctx) {
        LedgerHandle lh = cursorLedger;
        ledger.mbean.startCursorLedgerCloseOp();
        log.info("[{}] [{}] Closing metadata ledger {}", ledger.getName(), name, lh.getId());
        lh.asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                ledger.mbean.endCursorLedgerCloseOp();
                if (rc == BKException.Code.OK) {
                    log.info("[{}][{}] Closed cursor-ledger {}", ledger.getName(), name,
                            cursorLedger.getId());
                    callback.closeComplete(ctx);
                } else {
                    log.warn("[{}][{}] Failed to close cursor-ledger {}: {}", ledger.getName(), name,
                            cursorLedger.getId(), BKException.getMessage(rc));
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
                    log.info(
                            "[{}] read operation completed and cursor was closed. need to call any queued cursor close",
                            name);
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
                log.warn("[{}-{}] Failed to delete ledger after retries {}", ledger.getName(), name, lh.getId());
            }
            return;
        }

        ledger.mbean.startCursorLedgerDeleteOp();
        bookkeeper.asyncDeleteLedger(lh.getId(), (rc, ctx) -> {
            ledger.mbean.endCursorLedgerDeleteOp();
            if (rc != BKException.Code.OK) {
                log.warn("[{}] Failed to delete ledger {}: {}", ledger.getName(), lh.getId(),
                        BKException.getMessage(rc));
                if (!isNoSuchLedgerExistsException(rc)) {
                    ledger.getScheduledExecutor().schedule(() -> asyncDeleteLedger(lh, retry - 1),
                        DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC, TimeUnit.SECONDS);
                }
                return;
            } else {
                log.info("[{}][{}] Successfully closed & deleted ledger {} in cursor", ledger.getName(), name,
                        lh.getId());
            }
        }, null);
    }

    void asyncDeleteCursorLedger() {
        asyncDeleteCursorLedger(DEFAULT_LEDGER_DELETE_RETRIES);
    }

    private void asyncDeleteCursorLedger(int retry) {
        State beforeChangingState = changeStateToDeletingIfNotDeleted();
        if (beforeChangingState == State.Deleted) {
            log.warn("[{}-{}] Cursor ledger is already deleted. state={}", ledger.getName(), name,
                    beforeChangingState);
            return;
        }

        closeWaitingCursor();

        if (cursorLedger == null) {
            log.warn("[{}-{}] There's no cursor ledger available for deletion.", ledger.getName(), name);
            state = State.DeletingFailed;
            return;
        }

        if (retry <= 0) {
            log.warn("[{}-{}] Failed to delete ledger after retries {}", ledger.getName(), name,
                    cursorLedger.getId());
            state = State.DeletingFailed;
            return;
        }

        ledger.mbean.startCursorLedgerDeleteOp();
        bookkeeper.asyncDeleteLedger(cursorLedger.getId(), (rc, ctx) -> {
            ledger.mbean.endCursorLedgerDeleteOp();
            if (rc == BKException.Code.OK) {
                state = State.Deleted;
                log.info("[{}][{}] Deleted cursor ledger {}", ledger.getName(), name, cursorLedger.getId());
            } else {
                log.warn("[{}][{}] Failed to delete ledger {}: {}", ledger.getName(), name, cursorLedger.getId(),
                        BKException.getMessage(rc));
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
    private PositionImpl getRollbackPosition(ManagedCursorInfo info) {
        PositionImpl firstPosition = ledger.getFirstPosition();
        PositionImpl snapshottedPosition = new PositionImpl(info.getMarkDeleteLedgerId(), info.getMarkDeleteEntryId());
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
    public LongPairRangeSet<PositionImpl> getIndividuallyDeletedMessagesSet() {
        return individualDeletedMessages;
    }

    public boolean isMessageDeleted(Position position) {
        checkArgument(position instanceof PositionImpl);
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
        return ((PositionImpl) position).compareTo(markDeletePosition) <= 0
                || individualDeletedMessages.contains(position.getLedgerId(), position.getEntryId());
    }

    //this method will return a copy of the position's ack set
    public long[] getBatchPositionAckSet(Position position) {
        if (!(position instanceof PositionImpl)) {
            return null;
        }

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
    public PositionImpl getNextAvailablePosition(PositionImpl position) {
        lock.readLock().lock();
        try {
            Range<PositionImpl> range = individualDeletedMessages.rangeContaining(position.getLedgerId(),
                    position.getEntryId());
            if (range != null) {
                PositionImpl nextPosition = range.upperEndpoint().getNext();
                return (nextPosition != null && nextPosition.compareTo(position) > 0)
                        ? nextPosition : position.getNext();
            }
            return position.getNext();
        } finally {
            lock.readLock().unlock();
        }
    }

    public Position getNextLedgerPosition(long currentLedgerId) {
        Long nextExistingLedger = ledger.getNextValidLedger(currentLedgerId);
        return nextExistingLedger != null ? PositionImpl.get(nextExistingLedger, 0) : null;
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
    public Range<PositionImpl> getLastIndividualDeletedRange() {
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
    public long[] getDeletedBatchIndexesAsLongArray(PositionImpl position) {
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
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Flushed dirty mark-delete position", ledger.getName(), name);
                }
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                if (exception.getCause() instanceof MarkDeletingMarkedPosition) {
                    // this is not actually a problem, we should not log a stacktrace
                    log.info("[{}][{}] Cannot flush mark-delete position: {}", ledger.getName(),
                            name, exception.getCause().getMessage());
                } else {
                    log.warn("[{}][{}] Failed to flush mark-delete position", ledger.getName(), name, exception);
                }
            }
        }, null);
    }

    public int applyMaxSizeCap(int maxEntries, long maxSizeBytes) {
        if (maxSizeBytes == NO_MAX_SIZE_LIMIT) {
            return maxEntries;
        }
        int estimatedEntryCount = estimateEntryCountByBytesSize(maxEntries, maxSizeBytes, readPosition, ledger);
        return Math.min(estimatedEntryCount, maxEntries);
    }

    @Override
    public boolean checkAndUpdateReadPositionChanged() {
        PositionImpl lastEntry = ledger.lastConfirmedEntry;
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

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorImpl.class);

    public ManagedLedgerConfig getConfig() {
        return getManagedLedger().getConfig();
    }

    /***
     * Create a non-durable cursor and copy the ack stats.
     */
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

    /**
     * Called by ManagedLedgerImpl to execute the Runnable inside the lock to remove the cursor from it's
     * waiting cursors list.
     * The cursor state is set to unregistered, and it can be registered again for waiting in ManagedLedgerImpl.
     */
    void removeWaitingCursorRequested(Runnable removeWaitingCursorRunnable) {
        synchronized (registerToWaitingCursorsLock) {
            if (!registeredToWaitingCursors) {
                // The cursor hasn't been registered, do not attempt to remove
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Skipping removing cursor {} from waiting cursors since it's not registered.",
                            ledger.getName(), name);
                }
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] Removing cursor {} from waiting cursors", ledger.getName(), name);
            }
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
            if (log.isDebugEnabled()) {
                log.debug("[{}] Adding cursor {} to waiting cursors", ledger.getName(), name);
            }
            addWaitingCursorRunnable.run();
            registeredToWaitingCursors = true;
        }
    }
}
