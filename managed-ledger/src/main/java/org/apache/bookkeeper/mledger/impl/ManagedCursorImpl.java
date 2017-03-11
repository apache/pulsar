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
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.SkipEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.PositionBound;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.Stat;
import org.apache.bookkeeper.mledger.impl.MetaStoreImplZookeeper.ZNodeProtobufFormat;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.bookkeeper.mledger.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.InvalidProtocolBufferException;

public class ManagedCursorImpl implements ManagedCursor {

    protected final BookKeeper bookkeeper;
    protected final ManagedLedgerConfig config;
    protected final ManagedLedgerImpl ledger;
    private final String name;

    private volatile PositionImpl markDeletePosition;
    private volatile PositionImpl readPosition;

    protected static final AtomicReferenceFieldUpdater<ManagedCursorImpl, OpReadEntry> WAITING_READ_OP_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, OpReadEntry.class, "waitingReadOp");
    private volatile OpReadEntry waitingReadOp = null;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<ManagedCursorImpl> RESET_CURSOR_IN_PROGRESS_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(ManagedCursorImpl.class, "resetCursorInProgress");
    private volatile int resetCursorInProgress = FALSE;
    private static final AtomicIntegerFieldUpdater<ManagedCursorImpl> PENDING_READ_OPS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ManagedCursorImpl.class, "pendingReadOps");
    private volatile int pendingReadOps = 0;

    // This counters are used to compute the numberOfEntries and numberOfEntriesInBacklog values, without having to look
    // at the list of ledgers in the ml. They are initialized to (-backlog) at opening, and will be incremented each
    // time a message is read or deleted.
    private volatile long messagesConsumedCounter;

    // Current ledger used to append the mark-delete position
    private volatile LedgerHandle cursorLedger;
    // Stat of the cursor z-node
    private volatile Stat cursorLedgerStat;

    private final RangeSet<PositionImpl> individualDeletedMessages = TreeRangeSet.create();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final RateLimiter markDeleteLimiter;
    
    private final ZNodeProtobufFormat protobufFormat;

    class PendingMarkDeleteEntry {
        final PositionImpl newPosition;
        final MarkDeleteCallback callback;
        final Object ctx;

        // If the callbackGroup is set, it means this mark-delete request was done on behalf of a group of request (just
        // persist the last one in the chain). In this case we need to trigger the callbacks for every request in the
        // group.
        List<PendingMarkDeleteEntry> callbackGroup;

        public PendingMarkDeleteEntry(PositionImpl newPosition, MarkDeleteCallback callback, Object ctx) {
            this.newPosition = PositionImpl.get(newPosition);
            this.callback = callback;
            this.ctx = ctx;
        }
    }

    private final ArrayDeque<PendingMarkDeleteEntry> pendingMarkDeleteOps = new ArrayDeque<>();
    private static final AtomicIntegerFieldUpdater<ManagedCursorImpl> PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ManagedCursorImpl.class, "pendingMarkDeletedSubmittedCount");
    private volatile int pendingMarkDeletedSubmittedCount = 0;
    private long lastLedgerSwitchTimestamp;

    enum State {
        Uninitialized, // Cursor is being initialized
        NoLedger, // There is no metadata ledger open for writing
        Open, // Metadata ledger is ready
        SwitchingLedger, // The metadata ledger is being switched
        Closed // The managed cursor has been closed
    }

    private static final AtomicReferenceFieldUpdater<ManagedCursorImpl, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ManagedCursorImpl.class, State.class, "state");
    private volatile State state = null;

    public interface VoidCallback {
        void operationComplete();

        void operationFailed(ManagedLedgerException exception);
    }

    ManagedCursorImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger, String cursorName) {
        this.bookkeeper = bookkeeper;
        this.config = config;
        this.ledger = ledger;
        this.name = cursorName;
        STATE_UPDATER.set(this, State.Uninitialized);
        PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.set(this, 0);
        PENDING_READ_OPS_UPDATER.set(this, 0);
        RESET_CURSOR_IN_PROGRESS_UPDATER.set(this, FALSE);
        WAITING_READ_OP_UPDATER.set(this, null);
        this.lastLedgerSwitchTimestamp = System.currentTimeMillis();
        this.protobufFormat = ledger.factory.getConfig().useProtobufBinaryFormatInZK() ? //
                ZNodeProtobufFormat.Binary : //
                ZNodeProtobufFormat.Text;

        if (config.getThrottleMarkDelete() > 0.0) {
            markDeleteLimiter = RateLimiter.create(config.getThrottleMarkDelete());
        } else {
            // Disable mark-delete rate limiter
            markDeleteLimiter = null;
        }
    }

    /**
     * Performs the initial recovery, reading the mark-deleted position from the ledger and then calling initialize to
     * have a new opened ledger
     */
    void recover(final VoidCallback callback) {
        // Read the meta-data ledgerId from the store
        log.info("[{}] Recovering from bookkeeper ledger cursor: {}", ledger.getName(), name);
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), name, new MetaStoreCallback<ManagedCursorInfo>() {
            @Override
            public void operationComplete(ManagedCursorInfo info, Stat stat) {

                cursorLedgerStat = stat;

                if (info.getCursorsLedgerId() == -1L) {
                    // There is no cursor ledger to read the last position from. It means the cursor has been properly
                    // closed and the last mark-delete position is stored in the ManagedCursorInfo itself.s
                    PositionImpl recoveredPosition = new PositionImpl(info.getMarkDeleteLedgerId(),
                            info.getMarkDeleteEntryId());
                    if (info.getIndividualDeletedMessagesCount() > 0) {
                        recoverIndividualDeletedMessages(info.getIndividualDeletedMessagesList());
                    }
                    recoveredCursor(recoveredPosition);
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
        bookkeeper.asyncOpenLedger(ledgerId, config.getDigestType(), config.getPassword(), (rc, lh, ctx) -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Opened ledger {} for consumer {}. rc={}", ledger.getName(), ledgerId, name, rc);
            }
            if (isBkErrorNotRecoverable(rc)) {
                log.error("[{}] Error opening metadata ledger {} for consumer {}: {}", ledger.getName(), ledgerId, name,
                        BKException.getMessage(rc));
                // Rewind to oldest entry available
                initialize(getRollbackPosition(info), callback);
                return;
            } else if (rc != BKException.Code.OK) {
                log.warn("[{}] Error opening metadata ledger {} for consumer {}: {}", ledger.getName(), ledgerId, name,
                        BKException.getMessage(rc));
                callback.operationFailed(new ManagedLedgerException(BKException.getMessage(rc)));
                return;
            }

            // Read the last entry in the ledger
            cursorLedger = lh;
            lh.asyncReadLastEntry((rc1, lh1, seq, ctx1) -> {
                if (log.isDebugEnabled()) {
                    log.debug("readComplete rc={} entryId={}", rc1, lh1.getLastAddConfirmed());
                }
                if (isBkErrorNotRecoverable(rc1)) {
                    log.error("[{}] Error reading from metadata ledger {} for consumer {}: {}", ledger.getName(),
                            ledgerId, name, BKException.getMessage(rc1));
                    // Rewind to oldest entry available
                    initialize(getRollbackPosition(info), callback);
                    return;
                } else if (rc1 != BKException.Code.OK) {
                    log.warn("[{}] Error reading from metadata ledger {} for consumer {}: {}", ledger.getName(),
                            ledgerId, name, BKException.getMessage(rc1));

                    callback.operationFailed(new ManagedLedgerException(BKException.getMessage(rc1)));
                    return;
                }

                LedgerEntry entry = seq.nextElement();
                PositionInfo positionInfo;
                try {
                    positionInfo = PositionInfo.parseFrom(entry.getEntry());
                } catch (InvalidProtocolBufferException e) {
                    callback.operationFailed(new ManagedLedgerException(e));
                    return;
                }

                PositionImpl position = new PositionImpl(positionInfo);
                if (positionInfo.getIndividualDeletedMessagesCount() > 0) {
                    recoverIndividualDeletedMessages(positionInfo.getIndividualDeletedMessagesList());
                }
                recoveredCursor(position);
                callback.operationComplete();
            }, null);
        }, null);
    }

    private void recoverIndividualDeletedMessages(List<MLDataFormats.MessageRange> individualDeletedMessagesList) {
        lock.writeLock().lock();
        try {
            individualDeletedMessages.clear();
            individualDeletedMessagesList
                    .forEach(messageRange -> individualDeletedMessages.add(
                            Range.openClosed(
                                    new PositionImpl(messageRange.getLowerEndpoint()),
                                    new PositionImpl(messageRange.getUpperEndpoint())
                            )
                    ));
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void recoveredCursor(PositionImpl position) {
        // if the position was at a ledger that didn't exist (since it will be deleted if it was previously empty),
        // we need to move to the next existing ledger
        if (!ledger.ledgerExists(position.getLedgerId())) {
            long nextExistingLedger = ledger.getNextValidLedger(position.getLedgerId());
            position = PositionImpl.get(nextExistingLedger, -1);
        }
        log.info("[{}] Cursor {} recovered to position {}", ledger.getName(), name, position);

        messagesConsumedCounter = -getNumberOfEntries(Range.openClosed(position, ledger.getLastPosition()));

        markDeletePosition = position;
        readPosition = ledger.getNextValidPosition(position);
        STATE_UPDATER.set(this, State.NoLedger);
    }

    void initialize(PositionImpl position, final VoidCallback callback) {
        recoveredCursor(position);
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

        }, null);

        counter.await();

        if (result.exception != null)
            throw result.exception;

        return result.entries;
    }

    @Override
    public void asyncReadEntries(final int numberOfEntriesToRead, final ReadEntriesCallback callback,
            final Object ctx) {
        checkArgument(numberOfEntriesToRead > 0);
        if (STATE_UPDATER.get(this) == State.Closed) {
            callback.readEntriesFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
            return;
        }

        PENDING_READ_OPS_UPDATER.incrementAndGet(this);
        OpReadEntry op = OpReadEntry.create(this, PositionImpl.get(readPosition), numberOfEntriesToRead, callback, ctx);
        ledger.asyncReadEntries(op);
    }

    @Override
    public Entry getNthEntry(int N, IndividualDeletedEntries deletedEntries)
            throws InterruptedException, ManagedLedgerException {

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            Entry entry = null;
        }

        final Result result = new Result();

        asyncGetNthEntry(N, deletedEntries, new ReadEntryCallback() {

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

        counter.await();

        if (result.exception != null)
            throw result.exception;

        return result.entry;
    }

    @Override
    public void asyncGetNthEntry(int N, IndividualDeletedEntries deletedEntries, ReadEntryCallback callback,
            Object ctx) {
        checkArgument(N > 0);
        if (STATE_UPDATER.get(this) == State.Closed) {
            callback.readEntryFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
            return;
        }

        PositionImpl startPosition = ledger.getNextValidPosition(markDeletePosition);
        PositionImpl endPosition = ledger.getLastPosition();
        if (startPosition.compareTo(endPosition) <= 0) {
            long numOfEntries = getNumberOfEntries(Range.closed(startPosition, endPosition));
            if (numOfEntries >= N) {
                long deletedMessages = 0;
                if (deletedEntries == IndividualDeletedEntries.Exclude) {
                    deletedMessages = getNumIndividualDeletedEntriesToSkip(N);
                }
                PositionImpl positionAfterN = ledger.getPositionAfterN(markDeletePosition, N + deletedMessages,
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
        checkArgument(numberOfEntriesToRead > 0);

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            List<Entry> entries = null;
        }

        final Result result = new Result();

        asyncReadEntriesOrWait(numberOfEntriesToRead, new ReadEntriesCallback() {
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

        if (result.exception != null)
            throw result.exception;

        return result.entries;
    }

    @Override
    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx) {
        checkArgument(numberOfEntriesToRead > 0);
        if (STATE_UPDATER.get(this) == State.Closed) {
            callback.readEntriesFailed(new CursorAlreadyClosedException("Cursor was already closed"), ctx);
            return;
        }

        if (hasMoreEntries()) {
            // If we have available entries, we can read them immediately
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Read entries immediately", ledger.getName(), name);
            }
            asyncReadEntries(numberOfEntriesToRead, callback, ctx);
        } else {
            OpReadEntry op = OpReadEntry.create(this, PositionImpl.get(readPosition), numberOfEntriesToRead, callback,
                    ctx);

            if (!WAITING_READ_OP_UPDATER.compareAndSet(this, null, op)) {
                callback.readEntriesFailed(new ManagedLedgerException("We can only have a single waiting callback"),
                        ctx);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Deferring retry of read at position {}", ledger.getName(), name, op.readPosition);
            }

            // Check again for new entries again in 10ms, then if still no entries are available register to be notified
            ledger.getScheduledExecutor().schedule(safeRun(() -> {
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
                }
            }), 10, TimeUnit.MILLISECONDS);
        }
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
        return getNumberOfEntries(Range.closedOpen(readPosition, ledger.getLastPosition().getNext()));
    }

    public long getNumberOfEntriesSinceFirstNotAckedMessage() {
        return ledger.getNumberOfEntries(Range.openClosed(markDeletePosition, readPosition));
    }

    @Override
    public long getNumberOfEntriesInBacklog() {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Consumer {} cursor ml-entries: {} -- deleted-counter: {} other counters: mdPos {} rdPos {}",
                    ledger.getName(), name, ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.get(ledger), messagesConsumedCounter,
                    markDeletePosition, readPosition);
        }
        long backlog = ManagedLedgerImpl.ENTRIES_ADDED_COUNTER_UPDATER.get(ledger) - messagesConsumedCounter;
        if (backlog < 0) {
            // In some case the counters get incorrect values, fall back to the precise backlog count
            backlog = getNumberOfEntries(Range.closed(markDeletePosition, ledger.getLastPosition()));
        }

        return backlog;
    }

    public long getNumberOfEntriesInStorage() {
        return ledger.getNumberOfEntries(Range.openClosed(markDeletePosition, ledger.getLastPosition().getNext()));
    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            Position position = null;
        }

        final Result result = new Result();
        asyncFindNewestMatching(FindPositionConstraint.SearchActiveEntries, condition, new FindEntryCallback() {
            @Override
            public void findEntryComplete(Position position, Object ctx) {
                result.position = position;
                counter.countDown();
            }

            @Override
            public void findEntryFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        counter.await();
        if (result.exception != null)
            throw result.exception;

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
            callback.findEntryFailed(new ManagedLedgerException("Unknown position constraint"), ctx);
            return;
        }
        if (startPosition == null) {
            callback.findEntryFailed(new ManagedLedgerException("Couldn't find start position"), ctx);
            return;
        }
        op = new OpFindNewest(this, startPosition, condition, max, callback, ctx);
        op.find();
    }

    @Override
    public void setActive() {
        ledger.activateCursor(this);
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
    public Position getFirstPosition() {
        Long firstLedgerId = ledger.getLedgersInfo().firstKey();
        return firstLedgerId == null ? null : new PositionImpl(firstLedgerId, 0);
    }

    protected void internalResetCursor(final PositionImpl newPosition,
            AsyncCallbacks.ResetCursorCallback resetCursorCallback) {
        log.info("[{}] Initiate reset position to {} on cursor {}", ledger.getName(), newPosition, name);

        synchronized (pendingMarkDeleteOps) {
            if (!RESET_CURSOR_IN_PROGRESS_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                log.error("[{}] reset requested - position [{}], previous reset in progress - cursor {}",
                        ledger.getName(), newPosition, name);
                resetCursorCallback.resetFailed(
                        new ManagedLedgerException.ConcurrentFindCursorPositionException("reset already in progress"),
                        newPosition);
            }
        }

        final AsyncCallbacks.ResetCursorCallback callback = resetCursorCallback;

        VoidCallback finalCallback = new VoidCallback() {
            @Override
            public void operationComplete() {

                // modify mark delete and read position since we are able to persist new position for cursor
                lock.writeLock().lock();
                try {
                    PositionImpl newMarkDeletePosition = ledger.getPreviousPosition(newPosition);

                    if (markDeletePosition.compareTo(newMarkDeletePosition) >= 0) {
                        messagesConsumedCounter -= getNumberOfEntries(
                                Range.closedOpen(newMarkDeletePosition, markDeletePosition));
                    } else {
                        messagesConsumedCounter += getNumberOfEntries(
                                Range.closedOpen(markDeletePosition, newMarkDeletePosition));
                    }
                    markDeletePosition = newMarkDeletePosition;
                    individualDeletedMessages.clear();

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

        if (cursorLedger == null) {
            persistPositionMetaStore(-1, newPosition, new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void result, Stat stat) {
                    log.info("[{}] Updated cursor {} with ledger id {} md-position={} rd-position={}",
                            ledger.getName(), name, -1, markDeletePosition, readPosition);
                    cursorLedgerStat = stat;
                    finalCallback.operationComplete();
                }

                @Override
                public void operationFailed(MetaStoreException e) {
                    finalCallback.operationFailed(e);
                }
            });
        } else {
            persistPosition(cursorLedger, newPosition, finalCallback);
        }
    }

    @Override
    public void asyncResetCursor(Position newPos, AsyncCallbacks.ResetCursorCallback callback) {
        checkArgument(newPos instanceof PositionImpl);
        final PositionImpl newPosition = (PositionImpl) newPos;

        // order trim and reset operations on a ledger
        ledger.getExecutor().submitOrdered(ledger.getName(), safeRun(() -> {
            if (ledger.isValidPosition(newPosition)) {
                internalResetCursor(newPosition, callback);
            } else {
                // caller (replay) should handle this error and retry cursor reset
                callback.resetFailed(new ManagedLedgerException.InvalidCursorPositionException(newPosition.toString()),
                        newPosition);
            }
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

        if (result.exception != null)
            throw result.exception;

        return result.entries;
    }

    /**
     * Async replays given positions: 
     * a. before reading it filters out already-acked messages 
     * b. reads remaining entries async and gives it to given ReadEntriesCallback
     * c. returns all already-acked messages which are not replayed so, those messages can be removed by
     * caller(Dispatcher)'s replay-list and it won't try to replay it again
     * 
     */
    @Override
    public Set<? extends Position> asyncReplayEntries(final Set<? extends Position> positions, ReadEntriesCallback callback, Object ctx) {
        List<Entry> entries = Lists.newArrayListWithExpectedSize(positions.size());
        if (positions.isEmpty()) {
            callback.readEntriesComplete(entries, ctx);
        }

        // filters out messages which are already acknowledged
        Set<Position> alreadyAcknowledgedPositions = Sets.newHashSet();
        lock.readLock().lock();
        try {
            positions.stream().filter(position -> individualDeletedMessages.contains((PositionImpl) position)
                    || ((PositionImpl) position).compareTo(markDeletePosition) < 0).forEach(alreadyAcknowledgedPositions::add);
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

        positions.stream()
                .filter(position -> !alreadyAcknowledgedPositions.contains(position))
                .forEach(p -> ledger.asyncReadEntry((PositionImpl) p, cb, ctx));
        
        return alreadyAcknowledgedPositions;
    }

    private long getNumberOfEntries(Range<PositionImpl> range) {
        long allEntries = ledger.getNumberOfEntries(range);

        if (log.isDebugEnabled()) {
            log.debug("getNumberOfEntries. {} allEntries: {}", range, allEntries);
        }

        long deletedEntries = 0;

        lock.readLock().lock();
        try {
            for (Range<PositionImpl> r : individualDeletedMessages.asRanges()) {
                if (r.isConnected(range)) {
                    Range<PositionImpl> commonEntries = r.intersection(range);
                    long commonCount = ledger.getNumberOfEntries(commonEntries);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Discounting {} entries for already deleted range {}", ledger.getName(),
                                name, commonCount, commonEntries);
                    }
                    deletedEntries += commonCount;
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        if (log.isDebugEnabled()) {
            log.debug("Found {} entries - deleted: {}", allEntries - deletedEntries, deletedEntries);
        }
        return allEntries - deletedEntries;
    }

    @Override
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {
        checkNotNull(position);
        checkArgument(position instanceof PositionImpl);

        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncMarkDelete(position, new MarkDeleteCallback() {
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
        long totalEntriesToSkip = 0;
        long deletedMessages = 0;
        lock.readLock().lock();
        try {
            PositionImpl startPosition = markDeletePosition;
            PositionImpl endPosition = null;
            for (Range<PositionImpl> r : individualDeletedMessages.asRanges()) {
                endPosition = r.lowerEndpoint();
                if (startPosition.compareTo(endPosition) <= 0) {
                    Range<PositionImpl> range = Range.openClosed(startPosition, endPosition);
                    long entries = ledger.getNumberOfEntries(range);
                    if (totalEntriesToSkip + entries >= numEntries) {
                        break;
                    }
                    totalEntriesToSkip += entries;
                    deletedMessages += ledger.getNumberOfEntries(r);
                    startPosition = r.upperEndpoint();
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] deletePosition {} moved ahead without clearing deleteMsgs {} for cursor {}",
                                ledger.getName(), markDeletePosition, r.lowerEndpoint(), name);
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return deletedMessages;
    }

    boolean hasMoreEntries(PositionImpl position) {
        PositionImpl lastPositionInLedger = ledger.getLastPosition();
        if (position.compareTo(lastPositionInLedger) <= 0) {
            return getNumberOfEntries(Range.closed(position, lastPositionInLedger)) > 0;
        }
        return false;
    }

    void initializeCursorPosition(Pair<PositionImpl, Long> lastPositionCounter) {
        readPosition = ledger.getNextValidPosition(lastPositionCounter.first);
        markDeletePosition = PositionImpl.get(lastPositionCounter.first);

        // Initialize the counter such that the difference between the messages written on the ML and the
        // messagesConsumed is 0, to ensure the initial backlog count is 0.
        messagesConsumedCounter = lastPositionCounter.second;
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

        if (readPosition.compareTo(newMarkDeletePosition) <= 0) {
            // If the position that is mark-deleted is past the read position, it
            // means that the client has skipped some entries. We need to move
            // read position forward
            PositionImpl oldReadPosition = readPosition;
            readPosition = ledger.getNextValidPosition(newMarkDeletePosition);

            if (log.isDebugEnabled()) {
                log.debug("Moved read position from: {} to: {}", oldReadPosition, readPosition);
            }

            oldReadPosition.recycle();
        }

        PositionImpl oldMarkDeletePosition = markDeletePosition;

        if (!newMarkDeletePosition.equals(oldMarkDeletePosition)) {
            long skippedEntries = 0;
            if (newMarkDeletePosition.getLedgerId() == oldMarkDeletePosition.getLedgerId()
                    && newMarkDeletePosition.getEntryId() == oldMarkDeletePosition.getEntryId() + 1) {
                // Mark-deleting the position next to current one
                skippedEntries = individualDeletedMessages.contains(newMarkDeletePosition) ? 0 : 1;
            } else {
                skippedEntries = getNumberOfEntries(Range.openClosed(oldMarkDeletePosition, newMarkDeletePosition));
            }
            PositionImpl positionAfterNewMarkDelete = ledger.getNextValidPosition(newMarkDeletePosition);
            if (individualDeletedMessages.contains(positionAfterNewMarkDelete)) {
                Range<PositionImpl> rangeToBeMarkDeleted = individualDeletedMessages
                        .rangeContaining(positionAfterNewMarkDelete);
                newMarkDeletePosition = rangeToBeMarkDeleted.upperEndpoint();
            }

            if (log.isDebugEnabled()) {
                log.debug("Moved ack position from: {} to: {} -- skipped: {}", oldMarkDeletePosition,
                        newMarkDeletePosition, skippedEntries);
            }
            messagesConsumedCounter += skippedEntries;
        }

        // markDelete-position and clear out deletedMsgSet
        markDeletePosition = PositionImpl.get(newMarkDeletePosition);
        individualDeletedMessages.remove(Range.atMost(markDeletePosition));
        oldMarkDeletePosition.recycle();

        return newMarkDeletePosition;
    }

    @Override
    public void asyncMarkDelete(final Position position, final MarkDeleteCallback callback, final Object ctx) {
        checkNotNull(position);
        checkArgument(position instanceof PositionImpl);

        if (STATE_UPDATER.get(this) == State.Closed) {
            callback.markDeleteFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
            return;
        }

        if (RESET_CURSOR_IN_PROGRESS_UPDATER.get(this) == TRUE) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] cursor reset in progress - ignoring mark delete on position [{}] for cursor [{}]",
                        ledger.getName(), (PositionImpl) position, name);
            }
            callback.markDeleteFailed(
                    new ManagedLedgerException("Reset cursor in progress - unable to mark delete position "
                            + ((PositionImpl) position).toString()),
                    ctx);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Mark delete cursor {} up to position: {}", ledger.getName(), name, position);
        }
        PositionImpl newPosition = (PositionImpl) position;

        lock.writeLock().lock();
        try {
            newPosition = setAcknowledgedPosition(newPosition);
        } catch (IllegalArgumentException e) {
            callback.markDeleteFailed(new ManagedLedgerException(e), ctx);
            return;
        } finally {
            lock.writeLock().unlock();
        }

        // Apply rate limiting to mark-delete operations
        if (markDeleteLimiter != null && !markDeleteLimiter.tryAcquire()) {
            callback.markDeleteComplete(ctx);
            return;
        }
        internalAsyncMarkDelete(newPosition, callback, ctx);
    }

    private void internalAsyncMarkDelete(final PositionImpl newPosition, final MarkDeleteCallback callback,
            final Object ctx) {
        ledger.mbean.addMarkDeleteOp();

        PendingMarkDeleteEntry mdEntry = new PendingMarkDeleteEntry(newPosition, callback, ctx);

        // We cannot write to the ledger during the switch, need to wait until the new metadata ledger is available
        synchronized (pendingMarkDeleteOps) {
            // The state might have changed while we were waiting on the queue mutex
            switch (STATE_UPDATER.get(this)) {
            case Closed:
                callback.markDeleteFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
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

    void internalMarkDelete(final PendingMarkDeleteEntry mdEntry) {
        // The counter is used to mark all the pending mark-delete request that were submitted to BK and that are not
        // yet finished. While we have outstanding requests we cannot close the current ledger, so the switch to new
        // ledger is postponed to when the counter goes to 0.
        PENDING_MARK_DELETED_SUBMITTED_COUNT_UPDATER.incrementAndGet(this);

        persistPosition(cursorLedger, mdEntry.newPosition, new VoidCallback() {
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
                    individualDeletedMessages.remove(Range.atMost(mdEntry.newPosition));
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
                    for (PendingMarkDeleteEntry e : mdEntry.callbackGroup) {
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
                    for (PendingMarkDeleteEntry e : mdEntry.callbackGroup) {
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
        checkNotNull(position);
        checkArgument(position instanceof PositionImpl);

        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);
        final AtomicBoolean timeout = new AtomicBoolean(false);

        asyncDelete(position, new AsyncCallbacks.DeleteCallback() {
            @Override
            public void deleteComplete(Object ctx) {
                if (timeout.get()) {
                    log.warn("[{}] [{}] Delete operation timeout. Callback deleteComplete at position {}",
                            ledger.getName(), name, position);
                }

                counter.countDown();
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;

                if (timeout.get()) {
                    log.warn("[{}] [{}] Delete operation timeout. Callback deleteFailed at position {}",
                            ledger.getName(), name, position);
                }

                counter.countDown();
            }
        }, null);

        if (!counter.await(ManagedLedgerImpl.AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            timeout.set(true);
            log.warn("[{}] [{}] Delete operation timeout. No callback was triggered at position {}", ledger.getName(),
                    name, position);
            throw new ManagedLedgerException("Timeout during delete operation");
        }

        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncDelete(Position pos, final AsyncCallbacks.DeleteCallback callback, Object ctx) {
        checkArgument(pos instanceof PositionImpl);

        if (STATE_UPDATER.get(this) == State.Closed) {
            callback.deleteFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
            return;
        }

        PositionImpl position = (PositionImpl) pos;

        PositionImpl previousPosition = ledger.getPreviousPosition(position);
        PositionImpl newMarkDeletePosition = null;

        lock.writeLock().lock();

        try {
            if (log.isDebugEnabled()) {
                log.debug(
                        "[{}] [{}] Deleting single message at {}. Current status: {} - md-position: {}  - previous-position: {}",
                        ledger.getName(), name, pos, individualDeletedMessages, markDeletePosition, previousPosition);
            }

            if (individualDeletedMessages.contains(position) || position.compareTo(markDeletePosition) <= 0) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Position was already deleted {}", ledger.getName(), name, position);
                }
                callback.deleteComplete(ctx);
                return;
            }

            if (previousPosition.compareTo(markDeletePosition) == 0 && individualDeletedMessages.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Immediately mark-delete to position {}", ledger.getName(), name, position);
                }

                newMarkDeletePosition = position;
            } else {
                // Add a range (prev, pos] to the set. Adding the previous entry as an open limit to the range will make
                // the RangeSet recognize the "continuity" between adjacent Positions
                individualDeletedMessages.add(Range.openClosed(previousPosition, position));
                ++messagesConsumedCounter;

                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Individually deleted messages: {}", ledger.getName(), name,
                            individualDeletedMessages);
                }

                // If the lower bound of the range set is the current mark delete position, then we can trigger a new
                // mark
                // delete to the upper bound of the first range segment
                Range<PositionImpl> range = individualDeletedMessages.asRanges().iterator().next();

                // Bug:7062188 - markDeletePosition can sometimes be stuck at the beginning of an empty ledger.
                // If the lowerBound is ahead of MarkDelete, verify if there are any entries in-between
                if (range.lowerEndpoint().compareTo(markDeletePosition) <= 0 || ledger
                        .getNumberOfEntries(Range.openClosed(markDeletePosition, range.lowerEndpoint())) <= 0) {

                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Found a position range to mark delete for cursor {}: {} ", ledger.getName(),
                                name, range);
                    }

                    newMarkDeletePosition = range.upperEndpoint();
                }
            }

            if (newMarkDeletePosition != null) {
                newMarkDeletePosition = setAcknowledgedPosition(newMarkDeletePosition);
            } else {
                newMarkDeletePosition = markDeletePosition;
            }
        } catch (Exception e) {
            log.warn("[{}] [{}] Error while updating individualDeletedMessages [{}]", ledger.getName(), name,
                    e.getMessage(), e);
            callback.deleteFailed(new ManagedLedgerException(e), ctx);
            return;
        } finally {
            lock.writeLock().unlock();
        }

        // Apply rate limiting to mark-delete operations
        if (markDeleteLimiter != null && !markDeleteLimiter.tryAcquire()) {
            callback.deleteComplete(ctx);
            return;
        }

        try {
            internalAsyncMarkDelete(newMarkDeletePosition, new MarkDeleteCallback() {
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

            if (individualDeletedMessages.isEmpty() || !entriesRange.isConnected(individualDeletedMessages.span())) {
                // There are no individually deleted messages in this entry list, no need to perform filtering
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] No filtering needed for entries {}", ledger.getName(), name, entriesRange);
                }
                return entries;
            } else {
                // Remove from the entry list all the entries that were already marked for deletion
                return Lists.newArrayList(Collections2.filter(entries, entry -> {
                    boolean includeEntry = !individualDeletedMessages.contains((PositionImpl) entry.getPosition());
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
        return Objects.toStringHelper(this).add("ledger", ledger.getName()).add("name", name)
                .add("ackPos", markDeletePosition).add("readPos", readPosition).toString();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Position getReadPosition() {
        return PositionImpl.get(readPosition);
    }

    @Override
    public Position getMarkDeletedPosition() {
        return PositionImpl.get(markDeletePosition);
    }

    @Override
    public void rewind() {
        lock.writeLock().lock();
        try {
            PositionImpl newReadPosition = ledger.getNextValidPosition(markDeletePosition);
            PositionImpl oldReadPosition = readPosition;

            if (log.isDebugEnabled()) {
                log.debug("Rewind from {} to {}", oldReadPosition, newReadPosition);
            }

            readPosition = newReadPosition;
            oldReadPosition.recycle();
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
            oldReadPosition.recycle();
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

    private void persistPositionMetaStore(long cursorsLedgerId, PositionImpl position,
            MetaStoreCallback<Void> callback) {
        // When closing we store the last mark-delete position in the z-node itself, so we won't need the cursor ledger,
        // hence we write it as -1. The cursor ledger is deleted once the z-node write is confirmed.
        ManagedCursorInfo.Builder info = ManagedCursorInfo.newBuilder() //
                .setCursorsLedgerId(cursorsLedgerId) //
                .setMarkDeleteLedgerId(position.getLedgerId()) //
                .setMarkDeleteEntryId(position.getEntryId()); //

        if (protobufFormat == ZNodeProtobufFormat.Binary) {
            info.addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges());
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}][{}]  Closing cursor at md-position: {}", ledger.getName(), name, markDeletePosition);
        }

        ledger.getStore().asyncUpdateCursorInfo(ledger.getName(), name, info.build(), cursorLedgerStat,
                new MetaStoreCallback<Void>() {
                    @Override
                    public void operationComplete(Void result, Stat stat) {
                        callback.operationComplete(result, stat);
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        callback.operationFailed(e);
                    }
                });
    }

    @Override
    public void asyncClose(final AsyncCallbacks.CloseCallback callback, final Object ctx) {
        State oldState = STATE_UPDATER.getAndSet(this, State.Closed);
        if (oldState == State.Closed) {
            log.info("[{}] [{}] State is already closed", ledger.getName(), name);
            callback.closeComplete(ctx);
            return;
        }

        lock.readLock().lock();
        try {
            if (cursorLedger != null && protobufFormat == ZNodeProtobufFormat.Text
                    && !individualDeletedMessages.isEmpty()) {
                // To save individualDeletedMessages status, we don't want to dump the information in text format into
                // the z-node. Until we switch to binary format, just flush the mark-delete + the
                // individualDeletedMessages into the ledger.
                persistPosition(cursorLedger, markDeletePosition, new VoidCallback() {
                    @Override
                    public void operationComplete() {
                        cursorLedger.asyncClose(new CloseCallback() {
                            @Override
                            public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                                callback.closeComplete(ctx);
                            }
                        }, ctx);
                    }

                    @Override
                    public void operationFailed(ManagedLedgerException exception) {
                        callback.closeFailed(exception, ctx);
                    }
                });

                return;
            }
        } finally {
            lock.readLock().unlock();
        }

        persistPositionMetaStore(-1, markDeletePosition, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                log.info("[{}][{}] Closed cursor at md-position={}", ledger.getName(), name,
                        markDeletePosition);

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
        });
    }

    /**
     * Internal version of seek that doesn't do the validation check
     *
     * @param newReadPositionInt
     */
    void setReadPosition(Position newReadPositionInt) {
        checkArgument(newReadPositionInt instanceof PositionImpl);

        this.readPosition = (PositionImpl) newReadPositionInt;
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
                        PendingMarkDeleteEntry entry = pendingMarkDeleteOps.poll();
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
            if (RESET_CURSOR_IN_PROGRESS_UPDATER.get(this) == TRUE) {
                failPendingMarkDeletes();
            } else {
                internalFlushPendingMarkDeletes();
            }
        }
    }

    private void failPendingMarkDeletes() {
        for (PendingMarkDeleteEntry e : pendingMarkDeleteOps) {
            e.callback.markDeleteFailed(new ManagedLedgerException("reset cursor in progress"), e.ctx);
        }
        pendingMarkDeleteOps.clear();
    }

    void internalFlushPendingMarkDeletes() {
        PendingMarkDeleteEntry lastEntry = pendingMarkDeleteOps.getLast();
        lastEntry.callbackGroup = Lists.newArrayList(pendingMarkDeleteOps);
        pendingMarkDeleteOps.clear();

        internalMarkDelete(lastEntry);
    }

    void createNewMetadataLedger(final VoidCallback callback) {
        ledger.mbean.startCursorLedgerCreateOp();
        bookkeeper.asyncCreateLedger(config.getMetadataEnsemblesize(), config.getMetadataWriteQuorumSize(),
                config.getMetadataAckQuorumSize(), config.getDigestType(), config.getPassword(), (rc, lh, ctx) -> {
                    ledger.getExecutor().submit(safeRun(() -> {
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
                        final PositionImpl position = (PositionImpl) getMarkDeletedPosition();
                        persistPosition(lh, position, new VoidCallback() {
                            @Override
                            public void operationComplete() {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Persisted position {} for cursor {}", ledger.getName(), position,
                                            name);
                                }
                                switchToNewLedger(lh, callback);
                            }

                            @Override
                            public void operationFailed(ManagedLedgerException exception) {
                                log.warn("[{}] Failed to persist position {} for cursor {}", ledger.getName(), position,
                                        name);

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
                }, null);
    }

    private List<MLDataFormats.MessageRange> buildIndividualDeletedMessageRanges() {
        lock.readLock().lock();
        try {
            if (individualDeletedMessages.isEmpty()) {
                return Collections.emptyList();
            }

            MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo.newBuilder();
            MLDataFormats.MessageRange.Builder messageRangeBuilder = MLDataFormats.MessageRange.newBuilder();
            return individualDeletedMessages.asRanges().stream()
                    .limit(config.getMaxUnackedRangesToPersist())
                    .map(positionRange -> {
                        PositionImpl p = positionRange.lowerEndpoint();
                        nestedPositionBuilder.setLedgerId(p.getLedgerId());
                        nestedPositionBuilder.setEntryId(p.getEntryId());
                        messageRangeBuilder.setLowerEndpoint(nestedPositionBuilder.build());
                        p = positionRange.upperEndpoint();
                        nestedPositionBuilder.setLedgerId(p.getLedgerId());
                        nestedPositionBuilder.setEntryId(p.getEntryId());
                        messageRangeBuilder.setUpperEndpoint(nestedPositionBuilder.build());
                        return messageRangeBuilder.build();
                    })
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    void persistPosition(final LedgerHandle lh, final PositionImpl position, final VoidCallback callback) {
        PositionInfo pi = PositionInfo.newBuilder()
                .setLedgerId(position.getLedgerId())
                .setEntryId(position.getEntryId())
                .addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges()).build();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Cursor {} Appending to ledger={} position={}", ledger.getName(), name, lh.getId(),
                    position);
        }

        checkNotNull(lh);
        lh.asyncAddEntry(pi.toByteArray(), (rc, lh1, entryId, ctx) -> {
            if (rc == BKException.Code.OK) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Updated cursor {} position {} in meta-ledger {}", ledger.getName(), name,
                            position, lh1.getId());
                }

                if (shouldCloseLedger(lh1)) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Need to create new metadata ledger for consumer {}", ledger.getName(),
                                name);
                    }
                    startCreatingNewMetadataLedger();
                }

                callback.operationComplete();
            } else {
                log.warn("[{}] Error updating cursor {} position {} in meta-ledger {}: {}", ledger.getName(), name,
                        position, lh1.getId(), BKException.getMessage(rc));
                // If we've had a write error, the ledger will be automatically closed, we need to create a new one,
                // in the meantime the mark-delete will be queued.
                STATE_UPDATER.compareAndSet(ManagedCursorImpl.this, State.Open, State.NoLedger);
                callback.operationFailed(new ManagedLedgerException(BKException.getMessage(rc)));
            }
        }, null);
    }

    boolean shouldCloseLedger(LedgerHandle lh) {
        long now = System.currentTimeMillis();
        if ((lh.getLastAddConfirmed() >= config.getMetadataMaxEntriesPerLedger()
                || lastLedgerSwitchTimestamp < (now - config.getLedgerRolloverTimeout() * 1000))
                && STATE_UPDATER.get(this) != State.Closed) {
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
        persistPositionMetaStore(lh.getId(), markDeletePosition, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                log.info("[{}] Updated cursor {} with ledger id {} md-position={} rd-position={}",
                        ledger.getName(), name, lh.getId(), markDeletePosition, readPosition);
                final LedgerHandle oldLedger = cursorLedger;
                cursorLedger = lh;
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
        });
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
                    callback.closeComplete(ctx);
                } else {
                    callback.closeFailed(new ManagedLedgerException(BKException.getMessage(rc)), ctx);
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
        if (lh == null) {
            return;
        }

        ledger.mbean.startCursorLedgerDeleteOp();
        bookkeeper.asyncDeleteLedger(lh.getId(), (rc, ctx) -> {
            ledger.getExecutor().submit(safeRun(() -> {
                ledger.mbean.endCursorLedgerDeleteOp();
                if (rc != BKException.Code.OK) {
                    log.warn("[{}] Failed to delete ledger {}: {}", ledger.getName(), lh.getId(),
                            BKException.getMessage(rc));
                    return;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Successfully closed&deleted ledger {} in cursor", ledger.getName(), name,
                                lh.getId());
                    }
                }
            }));
        }, null);
    }

    void asyncDeleteCursorLedger() {
        STATE_UPDATER.set(this, State.Closed);

        if (cursorLedger == null) {
            // No ledger was created
            return;
        }

        ledger.mbean.startCursorLedgerDeleteOp();
        bookkeeper.asyncDeleteLedger(cursorLedger.getId(), (rc, ctx) -> {
            ledger.getExecutor().submit(safeRun(() -> {
                ledger.mbean.endCursorLedgerDeleteOp();
                if (rc == BKException.Code.OK) {
                    log.debug("[{}][{}] Deleted cursor ledger", cursorLedger.getId());
                } else {
                    log.warn("[{}][{}] Failed to delete ledger {}: {}", ledger.getName(), name, cursorLedger.getId(),
                            BKException.getMessage(rc));
                }
            }));
        }, null);
    }

    /**
     * return BK error codes that are considered not likely to be recoverable
     */
    private static boolean isBkErrorNotRecoverable(int rc) {
        switch (rc) {
        case BKException.Code.NoSuchLedgerExistsException:
        case BKException.Code.ReadException:
        case BKException.Code.LedgerRecoveryException:
            return true;

        default:
            return false;
        }
    }

    /**
     * If we fail to recover the cursor ledger, we want to still open the ML and rollback
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

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorImpl.class);
}
