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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.ManagedLedgerException.getManagedLedgerException;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import static org.apache.bookkeeper.mledger.util.Errors.isNoSuchLedgerExistsException;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.InvalidProtocolBufferException;

import io.netty.util.concurrent.FastThreadLocal;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
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
import org.apache.bookkeeper.mledger.AsyncCallbacks.SkipEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.PositionBound;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.LongProperty;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.MessageRange;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentOpenLongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.LongPairConsumer;
import org.apache.pulsar.metadata.api.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:javadoctype")
public class ManagedCursorImpl implements ManagedCursor {

    protected final BookKeeper bookkeeper;
    protected final ManagedLedgerConfig config;
    protected final ManagedLedgerImpl ledger;
    private final String name;
    private final BookKeeper.DigestType digestType;

    protected volatile PositionImpl markDeletePosition;

    // this position is have persistent mark delete position
    protected volatile PositionImpl persistentMarkDeletePosition;

    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, PositionImpl> READ_POSITION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, PositionImpl.class, "readPosition");
    protected volatile PositionImpl readPosition;
    // keeps sample of last read-position for validation and monitoring if read-position is not moving forward.
    protected volatile PositionImpl statsLastReadPosition;

    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, MarkDeleteEntry> LAST_MARK_DELETE_ENTRY_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, MarkDeleteEntry.class, "lastMarkDeleteEntry");
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
    private volatile LedgerHandle cursorLedger;

    // Wether the current cursorLedger is read-only or writable
    private boolean isCursorLedgerReadOnly = true;

    // Stat of the cursor z-node
    private volatile Stat cursorLedgerStat;

    private static final LongPairConsumer<PositionImpl> positionRangeConverter = PositionImpl::new;
    private static final LongPairConsumer<PositionImplRecyclable> recyclePositionRangeConverter = (key, value) -> {
        PositionImplRecyclable position = PositionImplRecyclable.create();
        position.ledgerId = key;
        position.entryId = value;
        position.ackSet = null;
        return position;
    };
    private final LongPairRangeSet<PositionImpl> individualDeletedMessages;

    // Maintain the deletion status for batch messages
    // (ledgerId, entryId) -> deletion indexes
    private final ConcurrentSkipListMap<PositionImpl, BitSetRecyclable> batchDeletedIndexes;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private RateLimiter markDeleteLimiter;
    // The cursor is considered "dirty" when there are mark-delete updates that are only applied in memory,
    // because of the rate limiting.
    private volatile boolean isDirty = false;

    private boolean alwaysInactive = false;

    /** used temporary variables to {@link #getNumIndividualDeletedEntriesToSkip(long)} **/
    private static final FastThreadLocal<Long> tempTotalEntriesToSkip = new FastThreadLocal<>();
    private static final FastThreadLocal<Long> tempDeletedMessages = new FastThreadLocal<>();
    private static final FastThreadLocal<PositionImpl> tempStartPosition = new FastThreadLocal<>();
    private static final FastThreadLocal<PositionImpl> tempEndPosition = new FastThreadLocal<>();

    private static final long NO_MAX_SIZE_LIMIT = -1L;

    private long entriesReadCount;
    private long entriesReadSize;
    private int individualDeletedMessagesSerializedSize;

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
    }

    protected final ArrayDeque<MarkDeleteEntry> pendingMarkDeleteOps = new ArrayDeque<>();
    private static final AtomicIntegerFieldUpdater<ManagedCursorImpl> PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ManagedCursorImpl.class, "pendingMarkDeletedSubmittedCount");
    @SuppressWarnings("unused")
    private volatile int pendingMarkDeletedSubmittedCount = 0;
    private long lastLedgerSwitchTimestamp;
    private final Clock clock;

    // The last active time (Unix time, milliseconds) of the cursor
    private long lastActive;

    enum State {
        Uninitialized, // Cursor is being initialized
        NoLedger, // There is no metadata ledger open for writing
        Open, // Metadata ledger is ready
        SwitchingLedger, // The metadata ledger is being switched
        Closing, // The managed cursor is closing
        Closed // The managed cursor has been closed
    }

    private static final AtomicReferenceFieldUpdater<ManagedCursorImpl, State> STATE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, State.class, "state");
    protected volatile State state = null;

    protected final ManagedCursorMXBean mbean;

    @SuppressWarnings("checkstyle:javadoctype")
    public interface VoidCallback {
        void operationComplete();

        void operationFailed(ManagedLedgerException exception);
    }

    ManagedCursorImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger, String cursorName) {
        this.bookkeeper = bookkeeper;
        this.config = config;
        this.ledger = ledger;
        this.name = cursorName;
        this.individualDeletedMessages = config.isUnackedRangesOpenCacheSetEnabled()
                ? new ConcurrentOpenLongPairRangeSet<>(4096, positionRangeConverter)
                : new LongPairRangeSet.DefaultRangeSet<>(positionRangeConverter);
        if (config.isDeletionAtBatchIndexLevelEnabled()) {
            this.batchDeletedIndexes = new ConcurrentSkipListMap<>();
        } else {
            this.batchDeletedIndexes = null;
        }
        this.digestType = BookKeeper.DigestType.fromApiDigestType(config.getDigestType());
        STATE_UPDATER.set(this, State.Uninitialized);
        PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.set(this, 0);
        PENDING_READ_OPS_UPDATER.set(this, 0);
        RESET_CURSOR_IN_PROGRESS_UPDATER.set(this, FALSE);
        WAITING_READ_OP_UPDATER.set(this, null);
        this.clock = config.getClock();
        this.lastActive = this.clock.millis();
        this.lastLedgerSwitchTimestamp = this.clock.millis();

        if (config.getThrottleMarkDelete() > 0.0) {
            markDeleteLimiter = RateLimiter.create(config.getThrottleMarkDelete());
        } else {
            // Disable mark-delete rate limiter
            markDeleteLimiter = null;
        }
        this.mbean = new ManagedCursorMXBeanImpl(this);
    }

    @Override
    public Map<String, Long> getProperties() {
        return lastMarkDeleteEntry != null ? lastMarkDeleteEntry.properties : Collections.emptyMap();
    }

    @Override
    public boolean putProperty(String key, Long value) {
        if (lastMarkDeleteEntry != null) {
            LAST_MARK_DELETE_ENTRY_UPDATER.updateAndGet(this, last -> {
                Map<String, Long> properties = last.properties;
                Map<String, Long> newProperties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
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

                cursorLedgerStat = stat;
                lastActive = info.getLastActive() != 0 ? info.getLastActive() : lastActive;

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
                        recoveredProperties = Maps.newHashMap();
                        for (int i = 0; i < info.getPropertiesCount(); i++) {
                            LongProperty property = info.getProperties(i);
                            recoveredProperties.put(property.getName(), property.getValue());
                        }
                    }

                    recoveredCursor(recoveredPosition, recoveredProperties, null);
                    callback.operationComplete();
                } else {
                    // Need to proceed and read the last entry in the specified ledger to find out the last position
                    log.info("[{}] Consumer {} meta-data recover from ledger {}", ledger.getName(), name,
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
                log.info("[{}] Opened ledger {} for consumer {}. rc={}", ledger.getName(), ledgerId, name, rc);
            }
            if (isBkErrorNotRecoverable(rc)) {
                log.error("[{}] Error opening metadata ledger {} for consumer {}: {}", ledger.getName(), ledgerId, name,
                        BKException.getMessage(rc));
                // Rewind to oldest entry available
                initialize(getRollbackPosition(info), Collections.emptyMap(), callback);
                return;
            } else if (rc != BKException.Code.OK) {
                log.warn("[{}] Error opening metadata ledger {} for consumer {}: {}", ledger.getName(), ledgerId, name,
                        BKException.getMessage(rc));
                callback.operationFailed(new ManagedLedgerException(BKException.getMessage(rc)));
                return;
            }

            // Read the last entry in the ledger
            long lastEntryInLedger = lh.getLastAddConfirmed();

            if (lastEntryInLedger < 0) {
                log.warn("[{}] Error reading from metadata ledger {} for consumer {}: No entries in ledger",
                        ledger.getName(), ledgerId, name);
                // Rewind to last cursor snapshot available
                initialize(getRollbackPosition(info), Collections.emptyMap(), callback);
                return;
            }

            lh.asyncReadEntries(lastEntryInLedger, lastEntryInLedger, (rc1, lh1, seq, ctx1) -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}} readComplete rc={} entryId={}", ledger.getName(), rc1, lh1.getLastAddConfirmed());
                }
                if (isBkErrorNotRecoverable(rc1)) {
                    log.error("[{}] Error reading from metadata ledger {} for consumer {}: {}", ledger.getName(),
                            ledgerId, name, BKException.getMessage(rc1));
                    // Rewind to oldest entry available
                    initialize(getRollbackPosition(info), Collections.emptyMap(), callback);
                    return;
                } else if (rc1 != BKException.Code.OK) {
                    log.warn("[{}] Error reading from metadata ledger {} for consumer {}: {}", ledger.getName(),
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
                    recoveredProperties = Maps.newHashMap();
                    for (int i = 0; i < positionInfo.getPropertiesCount(); i++) {
                        LongProperty property = positionInfo.getProperties(i);
                        recoveredProperties.put(property.getName(), property.getValue());
                    }
                }

                PositionImpl position = new PositionImpl(positionInfo);
                if (positionInfo.getIndividualDeletedMessagesCount() > 0) {
                    recoverIndividualDeletedMessages(positionInfo.getIndividualDeletedMessagesList());
                }
                if (config.isDeletionAtBatchIndexLevelEnabled() && batchDeletedIndexes != null
                    && positionInfo.getBatchedEntryDeletionIndexInfoCount() > 0) {
                    recoverBatchDeletedIndexes(positionInfo.getBatchedEntryDeletionIndexInfoList());
                }
                recoveredCursor(position, recoveredProperties, lh);
                callback.operationComplete();
            }, null);
        };
        try {
            bookkeeper.asyncOpenLedger(ledgerId, digestType, config.getPassword(), openCallback, null);
        } catch (Throwable t) {
            log.error("[{}] Encountered error on opening cursor ledger {} for cursor {}",
                ledger.getName(), ledgerId, name, t);
            openCallback.openComplete(BKException.Code.UnexpectedConditionException, null, null);
        }
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

    private void recoverBatchDeletedIndexes (List<MLDataFormats.BatchedEntryDeletionIndexInfo> batchDeletedIndexInfoList) {
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
                        batchDeletedIndexInfo.getPosition().getEntryId()), BitSetRecyclable.create().resetWords(array));
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void recoveredCursor(PositionImpl position, Map<String, Long> properties,
                                 LedgerHandle recoveredFromCursorLedger) {
        // if the position was at a ledger that didn't exist (since it will be deleted if it was previously empty),
        // we need to move to the next existing ledger
        if (!ledger.ledgerExists(position.getLedgerId())) {
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

        messagesConsumedCounter = -getNumberOfEntries(Range.openClosed(position, ledger.getLastPosition()));
        markDeletePosition = position;
        persistentMarkDeletePosition = position;
        readPosition = ledger.getNextValidPosition(position);
        lastMarkDeleteEntry = new MarkDeleteEntry(markDeletePosition, properties, null, null);
        // assign cursor-ledger so, it can be deleted when new ledger will be switched
        this.cursorLedger = recoveredFromCursorLedger;
        this.isCursorLedgerReadOnly = true;
        STATE_UPDATER.set(this, State.NoLedger);
    }

    void initialize(PositionImpl position, Map<String, Long> properties, final VoidCallback callback) {
        recoveredCursor(position, properties, null);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Consumer {} cursor initialized with counters: consumed {} mdPos {} rdPos {}",
                    ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
        }

        createNewMetadataLedger(new VoidCallback() {
            @Override
            public void operationComplete() {
                STATE_UPDATER.set(ManagedCursorImpl.this, State.Open);
                callback.operationComplete();
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                callback.operationFailed(exception);
            }
        });
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

        }, null, PositionImpl.latest);

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
        checkArgument(numberOfEntriesToRead > 0);
        if (isClosed()) {
            callback.readEntriesFailed(new ManagedLedgerException
                    .CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        int numOfEntriesToRead = applyMaxSizeCap(numberOfEntriesToRead, maxSizeBytes);

        PENDING_READ_OPS_UPDATER.incrementAndGet(this);
        OpReadEntry op = OpReadEntry.create(this, readPosition, numOfEntriesToRead, callback, ctx, maxPosition);
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

        }, null, PositionImpl.latest);

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
        checkArgument(maxEntries > 0);
        if (isClosed()) {
            callback.readEntriesFailed(new CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        int numberOfEntriesToRead = applyMaxSizeCap(maxEntries, maxSizeBytes);

        if (hasMoreEntries()) {
            // If we have available entries, we can read them immediately
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Read entries immediately", ledger.getName(), name);
            }
            asyncReadEntries(numberOfEntriesToRead, callback, ctx, maxPosition);
        } else {
            OpReadEntry op = OpReadEntry.create(this, readPosition, numberOfEntriesToRead, callback,
                    ctx, maxPosition);

            if (!WAITING_READ_OP_UPDATER.compareAndSet(this, null, op)) {
                callback.readEntriesFailed(new ManagedLedgerException("We can only have a single waiting callback"),
                        ctx);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Deferring retry of read at position {}", ledger.getName(), name, op.readPosition);
            }

            // Check again for new entries after the configured time, then if still no entries are available register
            // to be notified
            if (config.getNewEntriesCheckDelayInMillis() > 0) {
                ledger.getScheduledExecutor()
                        .schedule(() -> checkForNewEntries(op, callback, ctx),
                                config.getNewEntriesCheckDelayInMillis(), TimeUnit.MILLISECONDS);
            } else {
                // If there's no delay, check directly from the same thread
                checkForNewEntries(op, callback, ctx);
            }
        }
    }

    private void checkForNewEntries(OpReadEntry op, ReadEntriesCallback callback, Object ctx) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Re-trying the read at position {}", ledger.getName(), name, op.readPosition);
            }

            if (!hasMoreEntries()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Still no entries available. Register for notification", ledger.getName(),
                            name);
                }
                // Let the managed ledger know we want to be notified whenever a new entry is published
                ledger.waitingCursors.add(this);
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
                if (WAITING_READ_OP_UPDATER.compareAndSet(this, op, null)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Cancelled notification and scheduled read at {}", ledger.getName(),
                                name, op.readPosition);
                    }
                    PENDING_READ_OPS_UPDATER.incrementAndGet(this);
                    ledger.asyncReadEntries(op);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] notification was already cancelled", ledger.getName(), name);
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

    public boolean isClosed() {
        return state == State.Closed || state == State.Closing;
    }

    @Override
    public boolean cancelPendingReadRequest() {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Cancel pending read request", ledger.getName(), name);
        }
        return WAITING_READ_OP_UPDATER.getAndSet(this, null) != null;
    }

    public boolean hasPendingReadRequest() {
        return WAITING_READ_OP_UPDATER.get(this) != null;
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
        return individualDeletedMessages.size();
    }

    @Override
    public int getNonContiguousDeletedMessagesRangeSerializedSize() {
        return this.individualDeletedMessagesSerializedSize;
    }

    @Override
    public long getEstimatedSizeSinceMarkDeletePosition() {
        return ledger.estimateBacklogFromPosition(markDeletePosition);
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean isPrecise) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Consumer {} cursor ml-entries: {} -- deleted-counter: {} other counters: mdPos {} rdPos {}",
                    ledger.getName(), name, ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.get(ledger),
                    messagesConsumedCounter, markDeletePosition, readPosition);
        }
        if (isPrecise) {
            return getNumberOfEntries(Range.openClosed(markDeletePosition, ledger.getLastPosition()));
        }

        long backlog = ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.get(ledger) - messagesConsumedCounter;
        if (backlog < 0) {
            // In some case the counters get incorrect values, fall back to the precise backlog count
            backlog = getNumberOfEntries(Range.openClosed(markDeletePosition, ledger.getLastPosition()));
        }

        return backlog;
    }

    public long getNumberOfEntriesInStorage() {
        return ledger.getNumberOfEntries(Range.openClosed(markDeletePosition, ledger.getLastPosition().getNext()));
    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
        return findNewestMatching(FindPositionConstraint.SearchActiveEntries, condition);
    }

    @Override
    public Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
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
        OpFindNewest op;
        PositionImpl startPosition = null;
        long max = 0;
        switch (constraint) {
        case SearchAllAvailableEntries:
            startPosition = (PositionImpl) getFirstPosition();
            max = ledger.getNumberOfEntries() - 1;
            break;
        case SearchActiveEntries:
            startPosition = ledger.getNextValidPosition(markDeletePosition);
            max = getNumberOfEntriesInStorage();
            break;
        default:
            callback.findEntryFailed(new ManagedLedgerException("Unknown position constraint"), Optional.empty(), ctx);
            return;
        }
        if (startPosition == null) {
            callback.findEntryFailed(new ManagedLedgerException("Couldn't find start position"),
                    Optional.empty(), ctx);
            return;
        }
        op = new OpFindNewest(this, startPosition, condition, max, callback, ctx);
        op.find();
    }

    @Override
    public void setActive() {
        if (!alwaysInactive) {
            ledger.activateCursor(this);
        }
    }

    @Override
    public boolean isActive() {
        return ledger.isCursorActive(this);
    }

    @Override
    public void setInactive() {
        ledger.deactivateCursor(this);
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

    protected void internalResetCursor(PositionImpl position, AsyncCallbacks.ResetCursorCallback resetCursorCallback) {
        if (position.equals(PositionImpl.earliest)) {
            position = ledger.getFirstPosition();
        } else if (position.equals(PositionImpl.latest)) {
            position = ledger.getLastPosition().getNext();
        }

        log.info("[{}] Initiate reset position to {} on cursor {}", ledger.getName(), position, name);

        synchronized (pendingMarkDeleteOps) {
            if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                log.error("[{}] reset requested - position [{}], previous reset in progress - cursor {}",
                        ledger.getName(), position, name);
                resetCursorCallback.resetFailed(
                        new ManagedLedgerException.ConcurrentFindCursorPositionException("reset already in progress"),
                        position);
            }
        }

        final AsyncCallbacks.ResetCursorCallback callback = resetCursorCallback;

        final PositionImpl newPosition = position;

        VoidCallback finalCallback = new VoidCallback() {
            @Override
            public void operationComplete() {

                // modify mark delete and read position since we are able to persist new position for cursor
                lock.writeLock().lock();
                try {
                    PositionImpl newMarkDeletePosition = ledger.getPreviousPosition(newPosition);

                    if (markDeletePosition.compareTo(newMarkDeletePosition) >= 0) {
                        MSG_CONSUMED_COUNTER_UPDATER.addAndGet(cursorImpl(), -getNumberOfEntries(
                                Range.closedOpen(newMarkDeletePosition, markDeletePosition)));
                    } else {
                        MSG_CONSUMED_COUNTER_UPDATER.addAndGet(cursorImpl(), getNumberOfEntries(
                                Range.closedOpen(markDeletePosition, newMarkDeletePosition)));
                    }
                    markDeletePosition = newMarkDeletePosition;
                    lastMarkDeleteEntry = new MarkDeleteEntry(newMarkDeletePosition, Collections.emptyMap(),
                            null, null);
                    individualDeletedMessages.clear();
                    if (config.isDeletionAtBatchIndexLevelEnabled() && batchDeletedIndexes != null) {
                        batchDeletedIndexes.values().forEach(BitSetRecyclable::recycle);
                        batchDeletedIndexes.clear();
                        long[] resetWords = newPosition.ackSet;
                        if (resetWords != null) {
                            BitSetRecyclable ackSet = BitSetRecyclable.create().resetWords(resetWords);
                            batchDeletedIndexes.put(newPosition, ackSet);
                        }
                    }

                    PositionImpl oldReadPosition = readPosition;
                    if (oldReadPosition.compareTo(newPosition) >= 0) {
                        log.info("[{}] reset position to {} before current read position {} on cursor {}",
                                ledger.getName(), newPosition, oldReadPosition, name);
                    } else {
                        log.info("[{}] reset position to {} skipping from current read position {} on cursor {}",
                                ledger.getName(), newPosition, oldReadPosition, name);
                    }
                    readPosition = newPosition;
                } finally {
                    lock.writeLock().unlock();
                }
                synchronized (pendingMarkDeleteOps) {
                    pendingMarkDeleteOps.clear();
                    if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(ManagedCursorImpl.this, TRUE, FALSE)) {
                        log.error("[{}] expected reset position [{}], but another reset in progress on cursor {}",
                                ledger.getName(), newPosition, name);
                    }
                }
                callback.resetComplete(newPosition);

            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                synchronized (pendingMarkDeleteOps) {
                    if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(ManagedCursorImpl.this, TRUE, FALSE)) {
                        log.error("[{}] expected reset position [{}], but another reset in progress on cursor {}",
                                ledger.getName(), newPosition, name);
                    }
                }
                callback.resetFailed(new ManagedLedgerException.InvalidCursorPositionException(
                        "unable to persist position for cursor reset " + newPosition.toString()), newPosition);
            }

        };

        internalAsyncMarkDelete(newPosition, Collections.emptyMap(), new MarkDeleteCallback() {
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
    public void asyncResetCursor(Position newPos, AsyncCallbacks.ResetCursorCallback callback) {
        checkArgument(newPos instanceof PositionImpl);
        final PositionImpl newPosition = (PositionImpl) newPos;

        // order trim and reset operations on a ledger
        ledger.getExecutor().executeOrdered(ledger.getName(), safeRun(() -> {
            PositionImpl actualPosition = newPosition;

            if (!ledger.isValidPosition(actualPosition) &&
                !actualPosition.equals(PositionImpl.earliest) &&
                !actualPosition.equals(PositionImpl.latest)) {
                actualPosition = ledger.getNextValidPosition(actualPosition);

                if (actualPosition == null) {
                    // next valid position would only return null when newPos
                    // is larger than all available positions, then it's latest in effect.
                    actualPosition = PositionImpl.latest;
                }
            }

            internalResetCursor(actualPosition, callback);
        }));
    }

    @Override
    public void resetCursor(Position newPos) throws ManagedLedgerException, InterruptedException {
        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncResetCursor(newPos, new AsyncCallbacks.ResetCursorCallback() {
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
        Set<Position> alreadyAcknowledgedPositions = Sets.newHashSet();
        lock.readLock().lock();
        try {
            positions.stream()
                    .filter(position -> individualDeletedMessages.contains(((PositionImpl) position).getLedgerId(),
                            ((PositionImpl) position).getEntryId())
                            || ((PositionImpl) position).compareTo(markDeletePosition) <= 0)
                    .forEach(alreadyAcknowledgedPositions::add);
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
                            entries.sort((e1, e2) -> ComparisonChain.start()
                                    .compare(e1.getLedgerId(), e2.getLedgerId())
                                    .compare(e1.getEntryId(), e2.getEntryId()).result());
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
        };

        positions.stream().filter(position -> !alreadyAcknowledgedPositions.contains(position))
                .forEach(p ->{
                    if (((PositionImpl) p).compareTo(this.readPosition) == 0) {
                        this.setReadPosition(this.readPosition.getNext());
                        log.warn("[{}][{}] replayPosition{} equals readPosition{}," + " need set next readPositio",
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
            individualDeletedMessages.forEach((r) -> {
                try {
                    if (r.isConnected(range)) {
                        Range<PositionImpl> commonEntries = r.intersection(range);
                        long commonCount = ledger.getNumberOfEntries(commonEntries);
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] [{}] Discounting {} entries for already deleted range {}", ledger.getName(),
                                    name, commonCount, commonEntries);
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
        checkNotNull(position);
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

    long getNumIndividualDeletedEntriesToSkip(long numEntries) {
        tempTotalEntriesToSkip.set(0L);
        tempDeletedMessages.set(0L);
        lock.readLock().lock();
        try {
            tempStartPosition.set(markDeletePosition);
            tempEndPosition.set(null);
            individualDeletedMessages.forEach((r) -> {
                try {
                    tempEndPosition.set(r.lowerEndpoint());
                    if (tempStartPosition.get().compareTo(tempEndPosition.get()) <= 0) {
                        Range<PositionImpl> range = Range.openClosed(tempStartPosition.get(), tempEndPosition.get());
                        long entries = ledger.getNumberOfEntries(range);
                        if (tempTotalEntriesToSkip.get() + entries >= numEntries) {
                            // do not process further
                            return false;
                        }
                        tempTotalEntriesToSkip.set(tempTotalEntriesToSkip.get() + entries);
                        tempDeletedMessages.set(tempDeletedMessages.get() + ledger.getNumberOfEntries(r));
                        tempStartPosition.set(r.upperEndpoint());
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
                        ((PositionImplRecyclable) r.upperEndpoint()).recycle();
                    }
                }
            }, recyclePositionRangeConverter);
        } finally {
            lock.readLock().unlock();
        }
        return tempDeletedMessages.get();
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
        markDeletePosition = lastPositionCounter.getLeft();

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
            throw new IllegalArgumentException("Mark deleting an already mark-deleted position");
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
                    log.debug("[{}] Moved read position from: {} to: {}, and new mark-delete position {}", ledger.getName(),
                            currentReadPosition, newReadPosition, markDeletePosition);
                }
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

    @Override
    public void asyncMarkDelete(final Position position, Map<String, Long> properties,
            final MarkDeleteCallback callback, final Object ctx) {
        checkNotNull(position);
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

        PositionImpl newPosition = (PositionImpl) position;

        if (config.isDeletionAtBatchIndexLevelEnabled() && batchDeletedIndexes != null) {
            if (newPosition.ackSet != null) {
                batchDeletedIndexes.put(newPosition, BitSetRecyclable.create().resetWords(newPosition.ackSet));
                newPosition = ledger.getPreviousPosition(newPosition);
            }
            Map<PositionImpl, BitSetRecyclable> subMap = batchDeletedIndexes.subMap(PositionImpl.earliest, newPosition);
            subMap.values().forEach(BitSetRecyclable::recycle);
            subMap.clear();
        } else if (newPosition.ackSet != null) {
            newPosition = ledger.getPreviousPosition(newPosition);
            newPosition.ackSet = null;
        }

        if (((PositionImpl) ledger.getLastConfirmedEntry()).compareTo(newPosition) < 0) {
            boolean shouldCursorMoveForward = false;
            try {
                long ledgerEntries = ledger.getLedgerInfo(markDeletePosition.getLedgerId()).get().getEntries();
                Long nextValidLedger = ledger.getNextValidLedger(ledger.getLastConfirmedEntry().getLedgerId());
                shouldCursorMoveForward = (markDeletePosition.getEntryId() + 1 >= ledgerEntries)
                        && (newPosition.getLedgerId() == nextValidLedger);
            } catch (Exception e) {
                log.warn("Failed to get ledger entries while setting mark-delete-position", e);
            }

            if (shouldCursorMoveForward) {
                log.info("[{}] move mark-delete-position from {} to {} since all the entries have been consumed",
                        ledger.getName(), markDeletePosition, newPosition);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "[{}] Failed mark delete due to invalid markDelete {} is ahead of last-confirmed-entry {} for cursor [{}]",
                            ledger.getName(), position, ledger.getLastConfirmedEntry(), name);
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
            lastMarkDeleteEntry = new MarkDeleteEntry(newPosition, properties, null, null);
            callback.markDeleteComplete(ctx);
            return;
        }
        internalAsyncMarkDelete(newPosition, properties, callback, ctx);
    }

    protected void internalAsyncMarkDelete(final PositionImpl newPosition, Map<String, Long> properties,
            final MarkDeleteCallback callback, final Object ctx) {
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
                // We need to create a new ledger to write into
                startCreatingNewMetadataLedger();
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
        // The counter is used to mark all the pending mark-delete request that were submitted to BK and that are not
        // yet finished. While we have outstanding requests we cannot close the current ledger, so the switch to new
        // ledger is postponed to when the counter goes to 0.
        PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.incrementAndGet(this);

        lastMarkDeleteEntry = mdEntry;

        persistPositionToLedger(cursorLedger, mdEntry, new VoidCallback() {
            @Override
            public void operationComplete() {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Mark delete cursor {} to position {} succeeded", ledger.getName(), name,
                            mdEntry.newPosition);
                }

                // Remove from the individual deleted messages all the entries before the new mark delete
                // point.
                lock.writeLock().lock();
                try {
                    individualDeletedMessages.removeAtMost(mdEntry.newPosition.getLedgerId(),
                            mdEntry.newPosition.getEntryId());
                    if (config.isDeletionAtBatchIndexLevelEnabled() && batchDeletedIndexes != null) {
                        Map<PositionImpl, BitSetRecyclable> subMap = batchDeletedIndexes.subMap(PositionImpl.earliest, false, PositionImpl.get(mdEntry.newPosition.getLedgerId(), mdEntry.newPosition.getEntryId()), true);
                        subMap.values().forEach(BitSetRecyclable::recycle);
                        subMap.clear();
                    }
                    if (persistentMarkDeletePosition == null
                            || mdEntry.newPosition.compareTo(persistentMarkDeletePosition) > 0) {
                        persistentMarkDeletePosition = mdEntry.newPosition;
                    }

                } finally {
                    lock.writeLock().unlock();
                }

                ledger.updateCursor(ManagedCursorImpl.this, mdEntry.newPosition);

                decrementPendingMarkDeleteCount();

                // Trigger the final callback after having (eventually) triggered the switchin-ledger operation. This
                // will ensure that no race condition will happen between the next mark-delete and the switching
                // operation.
                if (mdEntry.callbackGroup != null) {
                    // Trigger the callback for every request in the group
                    for (MarkDeleteEntry e : mdEntry.callbackGroup) {
                        e.callback.markDeleteComplete(e.ctx);
                    }
                } else {
                    // Only trigger the callback for the current request
                    mdEntry.callback.markDeleteComplete(mdEntry.ctx);
                }
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                log.warn("[{}] Failed to mark delete position for cursor={} position={}", ledger.getName(),
                        ManagedCursorImpl.this, mdEntry.newPosition);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Consumer {} cursor mark delete failed with counters: consumed {} mdPos {} rdPos {}",
                            ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
                }

                decrementPendingMarkDeleteCount();

                if (mdEntry.callbackGroup != null) {
                    for (MarkDeleteEntry e : mdEntry.callbackGroup) {
                        e.callback.markDeleteFailed(exception, e.ctx);
                    }
                } else {
                    mdEntry.callback.markDeleteFailed(exception, mdEntry.ctx);
                }
            }
        });
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
        checkNotNull(positions);

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

        try {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Deleting individual messages at {}. Current status: {} - md-position: {}",
                        ledger.getName(), name, positions, individualDeletedMessages, markDeletePosition);
            }

            for (Position pos : positions) {
                PositionImpl position  = (PositionImpl) checkNotNull(pos);
                if (((PositionImpl) ledger.getLastConfirmedEntry()).compareTo(position) < 0) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                            "[{}] Failed mark delete due to invalid markDelete {} is ahead of last-confirmed-entry {} for cursor [{}]",
                            ledger.getName(), position, ledger.getLastConfirmedEntry(), name);
                    }
                    callback.deleteFailed(new ManagedLedgerException("Invalid mark deleted position"), ctx);
                    return;
                }

                if (individualDeletedMessages.contains(position.getLedgerId(), position.getEntryId())
                    || position.compareTo(markDeletePosition) <= 0) {
                    if (config.isDeletionAtBatchIndexLevelEnabled() && batchDeletedIndexes != null) {
                        BitSetRecyclable bitSetRecyclable = batchDeletedIndexes.remove(position);
                        if (bitSetRecyclable != null) {
                            bitSetRecyclable.recycle();
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Position was already deleted {}", ledger.getName(), name, position);
                    }
                    continue;
                }
                if (position.ackSet == null) {
                    if (config.isDeletionAtBatchIndexLevelEnabled() && batchDeletedIndexes != null) {
                        BitSetRecyclable bitSetRecyclable = batchDeletedIndexes.remove(position);
                        if (bitSetRecyclable != null) {
                            bitSetRecyclable.recycle();
                        }
                    }
                    // Add a range (prev, pos] to the set. Adding the previous entry as an open limit to the range will make
                    // the RangeSet recognize the "continuity" between adjacent Positions
                    PositionImpl previousPosition = ledger.getPreviousPosition(position);
                    individualDeletedMessages.addOpenClosed(previousPosition.getLedgerId(), previousPosition.getEntryId(),
                        position.getLedgerId(), position.getEntryId());
                    MSG_CONSUMED_COUNTER_UPDATER.incrementAndGet(this);

                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Individually deleted messages: {}", ledger.getName(), name,
                            individualDeletedMessages);
                    }
                } else if (config.isDeletionAtBatchIndexLevelEnabled() && batchDeletedIndexes != null) {
                    BitSetRecyclable bitSet = batchDeletedIndexes.computeIfAbsent(position, (v) -> BitSetRecyclable.create().resetWords(position.ackSet));
                    BitSetRecyclable givenBitSet = BitSetRecyclable.create().resetWords(position.ackSet);
                    bitSet.and(givenBitSet);
                    givenBitSet.recycle();
                    if (bitSet.isEmpty()) {
                        PositionImpl previousPosition = ledger.getPreviousPosition(position);
                        individualDeletedMessages.addOpenClosed(previousPosition.getLedgerId(), previousPosition.getEntryId(),
                            position.getLedgerId(), position.getEntryId());
                        MSG_CONSUMED_COUNTER_UPDATER.incrementAndGet(this);
                        BitSetRecyclable bitSetRecyclable = batchDeletedIndexes.remove(position);
                        if (bitSetRecyclable != null) {
                            bitSetRecyclable.recycle();
                        }
                    }
                }
            }

            if (individualDeletedMessages.isEmpty()) {
                // No changes to individually deleted messages, so nothing to do at this point
                callback.deleteComplete(ctx);
                return;
            }

            // If the lower bound of the range set is the current mark delete position, then we can trigger a new
            // mark-delete to the upper bound of the first range segment
            Range<PositionImpl> range = individualDeletedMessages.firstRange();

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
        }

        // Apply rate limiting to mark-delete operations
        if (markDeleteLimiter != null && !markDeleteLimiter.tryAcquire()) {
            isDirty = true;
            PositionImpl finalNewMarkDeletePosition = newMarkDeletePosition;
            LAST_MARK_DELETE_ENTRY_UPDATER.updateAndGet(this,
                    last -> new MarkDeleteEntry(finalNewMarkDeletePosition, last.properties, null, null));
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
            if (individualDeletedMessages.isEmpty() || individualDeletedMessages.span() == null ||
                    !entriesRange.isConnected(individualDeletedMessages.span())) {
                // There are no individually deleted messages in this entry list, no need to perform filtering
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] No filtering needed for entries {}", ledger.getName(), name, entriesRange);
                }
                return entries;
            } else {
                // Remove from the entry list all the entries that were already marked for deletion
                return Lists.newArrayList(Collections2.filter(entries, entry -> {
                    boolean includeEntry = !individualDeletedMessages.contains(
                            ((PositionImpl) entry.getPosition()).getLedgerId(),
                            ((PositionImpl) entry.getPosition()).getEntryId());
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
        return MoreObjects.toStringHelper(this).add("ledger", ledger.getName()).add("name", name)
                .add("ackPos", markDeletePosition).add("readPos", readPosition).toString();
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
        lock.writeLock().lock();
        try {
            PositionImpl newReadPosition = ledger.getNextValidPosition(markDeletePosition);
            PositionImpl oldReadPosition = readPosition;

            log.info("[{}-{}] Rewind from {} to {}", ledger.getName(), name, oldReadPosition, newReadPosition);

            readPosition = newReadPosition;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void seek(Position newReadPositionInt) {
        checkArgument(newReadPositionInt instanceof PositionImpl);
        PositionImpl newReadPosition = (PositionImpl) newReadPositionInt;

        lock.writeLock().lock();
        try {
            if (newReadPosition.compareTo(markDeletePosition) <= 0) {
                // Make sure the newReadPosition comes after the mark delete position
                newReadPosition = ledger.getNextValidPosition(markDeletePosition);
            }

            PositionImpl oldReadPosition = readPosition;
            readPosition = newReadPosition;
        } finally {
            lock.writeLock().unlock();
        }
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
                    });
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
        return cursorLedger != null
                && !isCursorLedgerReadOnly
                && config.getMaxUnackedRangesToPersist() > 0
                && individualDeletedMessages.size() > config.getMaxUnackedRangesToPersistInZk();
    }

    private void persistPositionMetaStore(long cursorsLedgerId, PositionImpl position, Map<String, Long> properties,
            MetaStoreCallback<Void> callback, boolean persistIndividualDeletedMessageRanges) {
        if (state == State.Closed) {
            ledger.getExecutor().execute(safeRun(() -> callback.operationFailed(new MetaStoreException(
                    new CursorAlreadyClosedException(name + " cursor already closed")))));
            return;
        }

        // When closing we store the last mark-delete position in the z-node itself, so we won't need the cursor ledger,
        // hence we write it as -1. The cursor ledger is deleted once the z-node write is confirmed.
        ManagedCursorInfo.Builder info = ManagedCursorInfo.newBuilder() //
                .setCursorsLedgerId(cursorsLedgerId) //
                .setMarkDeleteLedgerId(position.getLedgerId()) //
                .setMarkDeleteEntryId(position.getEntryId()) //
                .setLastActive(lastActive); //

        info.addAllProperties(buildPropertiesMap(properties));
        if (persistIndividualDeletedMessageRanges) {
            info.addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges());
            if (config.isDeletionAtBatchIndexLevelEnabled()) {
                info.addAllBatchedEntryDeletionIndexInfo(buildBatchEntryDeletionIndexInfoList());
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}]  Closing cursor at md-position: {}", ledger.getName(), name, position);
        }

        ledger.getStore().asyncUpdateCursorInfo(ledger.getName(), name, info.build(), cursorLedgerStat,
                new MetaStoreCallback<Void>() {
                    @Override
                    public void operationComplete(Void result, Stat stat) {
                        cursorLedgerStat = stat;
                        callback.operationComplete(result, stat);
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        if (e instanceof MetaStoreException.BadVersionException) {
                            log.warn("[{}] Failed to update cursor metadata for {} due to version conflict {}",
                                    ledger.name, name, e.getMessage());
                            // it means previous owner of the ml might have updated the version incorrectly. So, check
                            // the ownership and refresh the version again.
                            if (ledger.mlOwnershipChecker != null && ledger.mlOwnershipChecker.get()) {
                                ledger.getStore().asyncGetCursorInfo(ledger.getName(), name,
                                        new MetaStoreCallback<ManagedCursorInfo>() {
                                            @Override
                                            public void operationComplete(ManagedCursorInfo info, Stat stat) {
                                                cursorLedgerStat = stat;
                                            }

                                            @Override
                                            public void operationFailed(MetaStoreException e) {
                                                if (log.isDebugEnabled()) {
                                                    log.debug(
                                                            "[{}] Failed to refresh cursor metadata-version for {} due to {}",
                                                            ledger.name, name, e.getMessage());
                                                }
                                            }
                                        });
                            }
                        }
                        callback.operationFailed(e);
                    }
                });
    }

    @Override
    public void asyncClose(final AsyncCallbacks.CloseCallback callback, final Object ctx) {
        State oldState = STATE_UPDATER.getAndSet(this, State.Closing);
        if (oldState == State.Closed || oldState == State.Closing) {
            log.info("[{}] [{}] State is already closed", ledger.getName(), name);
            callback.closeComplete(ctx);
            return;
        }
        persistPositionWhenClosing(lastMarkDeleteEntry.newPosition, lastMarkDeleteEntry.properties, callback, ctx);
        STATE_UPDATER.set(this, State.Closed);
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
        }
    }

    // //////////////////////////////////////////////////

    void startCreatingNewMetadataLedger() {
        // Change the state so that new mark-delete ops will be queued and not immediately submitted
        State oldState = STATE_UPDATER.getAndSet(this, State.SwitchingLedger);
        if (oldState == State.SwitchingLedger) {
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
                    STATE_UPDATER.set(ManagedCursorImpl.this, State.Open);
                }
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                log.error("[{}][{}] Metadata ledger creation failed", ledger.getName(), name, exception);

                synchronized (pendingMarkDeleteOps) {
                    while (!pendingMarkDeleteOps.isEmpty()) {
                        MarkDeleteEntry entry = pendingMarkDeleteOps.poll();
                        entry.callback.markDeleteFailed(exception, entry.ctx);
                    }

                    // At this point we don't have a ledger ready
                    STATE_UPDATER.set(ManagedCursorImpl.this, State.NoLedger);
                }
            }
        });
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

        ledger.asyncCreateLedger(bookkeeper, config, digestType, (rc, lh, ctx) -> {

            if (ledger.checkAndCompleteLedgerOpTask(rc, lh, ctx)) {
                return;
            }

            ledger.getExecutor().execute(safeRun(() -> {
                ledger.mbean.endCursorLedgerCreateOp();
                if (rc != BKException.Code.OK) {
                    log.warn("[{}] Error creating ledger for cursor {}: {}", ledger.getName(), name,
                            BKException.getMessage(rc));
                    callback.operationFailed(new ManagedLedgerException(BKException.getMessage(rc)));
                    return;
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}] Created ledger {} for cursor {}", ledger.getName(), lh.getId(), name);
                }
                // Created the ledger, now write the last position
                // content
                MarkDeleteEntry mdEntry = lastMarkDeleteEntry;
                persistPositionToLedger(lh, mdEntry, new VoidCallback() {
                    @Override
                    public void operationComplete() {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Persisted position {} for cursor {}", ledger.getName(),
                                    mdEntry.newPosition, name);
                        }
                        switchToNewLedger(lh, new VoidCallback() {
                            @Override
                            public void operationComplete() {
                                callback.operationComplete();
                            }

                            @Override
                            public void operationFailed(ManagedLedgerException exception) {
                                // it means it failed to switch the newly created ledger so, it should be
                                // deleted to prevent leak
                                bookkeeper.asyncDeleteLedger(lh.getId(), (int rc, Object ctx) -> {
                                    if (rc != BKException.Code.OK) {
                                        log.warn("[{}] Failed to delete orphan ledger {}", ledger.getName(),
                                                lh.getId());
                                    }
                                }, null);
                                callback.operationFailed(exception);
                            }
                        });
                    }

                    @Override
                    public void operationFailed(ManagedLedgerException exception) {
                        log.warn("[{}] Failed to persist position {} for cursor {}", ledger.getName(),
                                mdEntry.newPosition, name);

                        ledger.mbean.startCursorLedgerDeleteOp();
                        bookkeeper.asyncDeleteLedger(lh.getId(), new DeleteCallback() {
                            @Override
                            public void deleteComplete(int rc, Object ctx) {
                                ledger.mbean.endCursorLedgerDeleteOp();
                            }
                        }, null);
                        callback.operationFailed(exception);
                    }
                });
            }));
        }, LedgerMetadataUtils.buildAdditionalMetadataForCursor(name));
    }

    private List<LongProperty> buildPropertiesMap(Map<String, Long> properties) {
        if (properties.isEmpty()) {
            return Collections.emptyList();
        }

        List<LongProperty> longProperties = Lists.newArrayList();
        properties.forEach((name, value) -> {
            LongProperty lp = LongProperty.newBuilder().setName(name).setValue(value).build();
            longProperties.add(lp);
        });

        return longProperties;
    }

    private List<MLDataFormats.MessageRange> buildIndividualDeletedMessageRanges() {
        lock.readLock().lock();
        try {
            if (individualDeletedMessages.isEmpty()) {
                return Collections.emptyList();
            }

            MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo
                    .newBuilder();
            MLDataFormats.MessageRange.Builder messageRangeBuilder = MLDataFormats.MessageRange.newBuilder();
            AtomicInteger acksSerializedSize = new AtomicInteger(0);
            List<MessageRange> rangeList = Lists.newArrayList();
            individualDeletedMessages.forEach((positionRange) -> {
                PositionImpl p = positionRange.lowerEndpoint();
                nestedPositionBuilder.setLedgerId(p.getLedgerId());
                nestedPositionBuilder.setEntryId(p.getEntryId());
                messageRangeBuilder.setLowerEndpoint(nestedPositionBuilder.build());
                p = positionRange.upperEndpoint();
                nestedPositionBuilder.setLedgerId(p.getLedgerId());
                nestedPositionBuilder.setEntryId(p.getEntryId());
                messageRangeBuilder.setUpperEndpoint(nestedPositionBuilder.build());
                MessageRange messageRange = messageRangeBuilder.build();
                acksSerializedSize.addAndGet(messageRange.getSerializedSize());
                rangeList.add(messageRange);
                return rangeList.size() <= config.getMaxUnackedRangesToPersist();
            });
            this.individualDeletedMessagesSerializedSize = acksSerializedSize.get();
            return rangeList;
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<MLDataFormats.BatchedEntryDeletionIndexInfo> buildBatchEntryDeletionIndexInfoList() {
        lock.readLock().lock();
        try {
            if (!config.isDeletionAtBatchIndexLevelEnabled() || batchDeletedIndexes == null || batchDeletedIndexes.isEmpty()) {
                return Collections.emptyList();
            }
            MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo
                    .newBuilder();
            MLDataFormats.BatchedEntryDeletionIndexInfo.Builder batchDeletedIndexInfoBuilder = MLDataFormats.BatchedEntryDeletionIndexInfo
                    .newBuilder();
            List<MLDataFormats.BatchedEntryDeletionIndexInfo> result = Lists.newArrayList();
            Iterator<Map.Entry<PositionImpl, BitSetRecyclable>> iterator = batchDeletedIndexes.entrySet().iterator();
            while (iterator.hasNext() && result.size() < config.getMaxBatchDeletedIndexToPersist()) {
                Map.Entry<PositionImpl, BitSetRecyclable> entry = iterator.next();
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

    void persistPositionToLedger(final LedgerHandle lh, MarkDeleteEntry mdEntry, final VoidCallback callback) {
        PositionImpl position = mdEntry.newPosition;
        PositionInfo pi = PositionInfo.newBuilder().setLedgerId(position.getLedgerId())
                .setEntryId(position.getEntryId())
                .addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges())
                .addAllBatchedEntryDeletionIndexInfo(buildBatchEntryDeletionIndexInfoList())
                .addAllProperties(buildPropertiesMap(mdEntry.properties)).build();


        if (log.isDebugEnabled()) {
            log.debug("[{}] Cursor {} Appending to ledger={} position={}", ledger.getName(), name, lh.getId(),
                    position);
        }

        checkNotNull(lh);
        byte[] data = pi.toByteArray();
        lh.asyncAddEntry(data, (rc, lh1, entryId, ctx) -> {
            if (rc == BKException.Code.OK) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Updated cursor {} position {} in meta-ledger {}", ledger.getName(), name, position,
                            lh1.getId());
                }

                if (shouldCloseLedger(lh1)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Need to create new metadata ledger for consumer {}", ledger.getName(), name);
                    }
                    startCreatingNewMetadataLedger();
                }

                mbean.persistToLedger(true);
                mbean.addWriteCursorLedgerSize(data.length);
                callback.operationComplete();
            } else {
                log.warn("[{}] Error updating cursor {} position {} in meta-ledger {}: {}", ledger.getName(), name,
                        position, lh1.getId(), BKException.getMessage(rc));
                // If we've had a write error, the ledger will be automatically closed, we need to create a new one,
                // in the meantime the mark-delete will be queued.
                STATE_UPDATER.compareAndSet(ManagedCursorImpl.this, State.Open, State.NoLedger);

                mbean.persistToLedger(false);
                // Before giving up, try to persist the position in the metadata store
                persistPositionMetaStore(-1, position, mdEntry.properties, new MetaStoreCallback<Void>() {
                    @Override
                    public void operationComplete(Void result, Stat stat) {
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "[{}][{}] Updated cursor in meta store after previous failure in ledger at position {}",
                                    ledger.getName(), name, position);
                        }
                        mbean.persistToZookeeper(true);
                        callback.operationComplete();
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        log.warn("[{}][{}] Failed to update cursor in meta store after previous failure in ledger: {}",
                                ledger.getName(), name, e.getMessage());
                        mbean.persistToZookeeper(false);
                        callback.operationFailed(createManagedLedgerException(rc));
                    }
                }, true);
            }
        }, null);
    }

    boolean shouldCloseLedger(LedgerHandle lh) {
        long now = clock.millis();
        if (ledger.factory.isMetadataServiceAvailable() &&
                (lh.getLastAddConfirmed() >= config.getMetadataMaxEntriesPerLedger()
                || lastLedgerSwitchTimestamp < (now - config.getLedgerRolloverTimeout() * 1000))
                && (STATE_UPDATER.get(this) != State.Closed && STATE_UPDATER.get(this) != State.Closing)) {
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
                cursorLedgerStat = stat;

                // At this point the position had already been safely markdeleted
                callback.operationComplete();

                asyncDeleteLedger(oldLedger);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn("[{}] Failed to update consumer {}", ledger.getName(), name, e);
                callback.operationFailed(e);
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
        OpReadEntry opReadEntry = WAITING_READ_OP_UPDATER.getAndSet(this, null);

        if (opReadEntry != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Received notification of new messages persisted, reading at {} -- last: {}",
                        ledger.getName(), name, opReadEntry.readPosition, ledger.lastConfirmedEntry);
                log.debug("[{}] Consumer {} cursor notification: other counters: consumed {} mdPos {} rdPos {}",
                        ledger.getName(), name, messagesConsumedCounter, markDeletePosition, readPosition);
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
            final State state = STATE_UPDATER.get(this);
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
                if (STATE_UPDATER.get(this) == State.Open) {
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
                    ledger.getScheduledExecutor().schedule(safeRun(() -> asyncDeleteLedger(lh, retry - 1)),
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
        STATE_UPDATER.set(this, State.Closed);

        if (cursorLedger == null || retry <= 0) {
            if (cursorLedger != null) {
                log.warn("[{}-{}] Failed to delete ledger after retries {}", ledger.getName(), name,
                        cursorLedger.getId());
            }
            return;
        }

        ledger.mbean.startCursorLedgerDeleteOp();
        bookkeeper.asyncDeleteLedger(cursorLedger.getId(), (rc, ctx) -> {
            ledger.mbean.endCursorLedgerDeleteOp();
            if (rc == BKException.Code.OK) {
                log.info("[{}][{}] Deleted cursor ledger {}", ledger.getName(), name, cursorLedger.getId());
            } else {
                log.warn("[{}][{}] Failed to delete ledger {}: {}", ledger.getName(), name, cursorLedger.getId(),
                        BKException.getMessage(rc));
                if (!isNoSuchLedgerExistsException(rc)) {
                    ledger.getScheduledExecutor().schedule(safeRun(() -> asyncDeleteCursorLedger(retry - 1)),
                            DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC, TimeUnit.SECONDS);
                }
            }
        }, null);
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
        return individualDeletedMessages.contains(((PositionImpl) position).getLedgerId(),
                ((PositionImpl) position).getEntryId()) || ((PositionImpl) position).compareTo(markDeletePosition) <= 0 ;
    }

    //this method will return a copy of the position's ack set
    public long[] getBatchPositionAckSet(Position position) {
        if (!(position instanceof PositionImpl)) {
            return null;
        }

        if (batchDeletedIndexes != null) {
            BitSetRecyclable bitSetRecyclable = batchDeletedIndexes.get(position);
            if (bitSetRecyclable == null) {
                return null;
            } else {
                return bitSetRecyclable.toLongArray();
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
        Range<PositionImpl> range = individualDeletedMessages.rangeContaining(position.getLedgerId(), position.getEntryId());
        if (range != null) {
            PositionImpl nextPosition = range.upperEndpoint().getNext();
            return (nextPosition != null && nextPosition.compareTo(position) > 0) ? nextPosition : position.getNext();
        }
        return position.getNext();
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
        return STATE_UPDATER.get(this).toString();
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
        return individualDeletedMessages.lastRange();
    }

    @Override
    public void trimDeletedEntries(List<Entry> entries) {
        entries.removeIf(entry -> ((PositionImpl) entry.getPosition()).compareTo(markDeletePosition) <= 0
                || individualDeletedMessages.contains(entry.getLedgerId(), entry.getEntryId()));
    }

    private ManagedCursorImpl cursorImpl() {
        return this;
    }

    @Override
    public long[] getDeletedBatchIndexesAsLongArray(PositionImpl position) {
        if (config.isDeletionAtBatchIndexLevelEnabled() && batchDeletedIndexes != null) {
            BitSetRecyclable bitSet = batchDeletedIndexes.get(position);
            return bitSet == null ? null : bitSet.toLongArray();
        } else {
            return null;
        }
    }

    @Override
    public ManagedCursorMXBean getStats() {
        return this.mbean;
    }

    void updateReadStats(int readEntriesCount, long readEntriesSize) {
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
                log.warn("[{}][{}] Failed to flush mark-delete position", ledger.getName(), name, exception);
            }
        }, null);
    }

    private int applyMaxSizeCap(int maxEntries, long maxSizeBytes) {
        if (maxSizeBytes == NO_MAX_SIZE_LIMIT) {
            return maxEntries;
        }

        double avgEntrySize = ledger.getStats().getEntrySizeAverage();
        if (!Double.isFinite(avgEntrySize)) {
            // We don't have yet any stats on the topic entries. Let's try to use the cursor avg size stats
            avgEntrySize = (double) entriesReadSize / (double) entriesReadCount;
        }

        if (!Double.isFinite(avgEntrySize)) {
            // If we still don't have any information, it means this is the first time we attempt reading
            // and there are no writes. Let's start with 1 to avoid any overflow and start the avg stats
            return 1;
        }

        int maxEntriesBasedOnSize = (int)(maxSizeBytes / avgEntrySize);
        if (maxEntriesBasedOnSize < 1) {
            // We need to read at least one entry
            return 1;
        }

        return Math.min(maxEntriesBasedOnSize, maxEntries);
    }

    @Override
    public boolean checkAndUpdateReadPositionChanged() {
        PositionImpl lastEntry = ledger.lastConfirmedEntry;
        boolean isReadPositionOnTail = lastEntry == null || readPosition == null
                || !(lastEntry.compareTo(readPosition) > 0);
        boolean isReadPositionChanged = readPosition != null && !readPosition.equals(statsLastReadPosition);
        statsLastReadPosition = readPosition;
        return isReadPositionOnTail || isReadPositionChanged;
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorImpl.class);
}
