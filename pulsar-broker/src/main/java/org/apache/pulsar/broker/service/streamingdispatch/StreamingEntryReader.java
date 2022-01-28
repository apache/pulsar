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
package org.apache.pulsar.broker.service.streamingdispatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.WaitingEntryCallBack;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.exception.buffer.TransactionBufferException;
import org.apache.pulsar.client.impl.Backoff;

/**
 * Entry reader that fulfill read request by streamline the read instead of reading with micro batch.
 */
@Slf4j
@RequiredArgsConstructor
public class StreamingEntryReader implements AsyncCallbacks.ReadEntryCallback, WaitingEntryCallBack {

    private final int maxRetry = 3;

    // Queue for read request issued yet waiting for complete from managed ledger.
    private ConcurrentLinkedQueue<PendingReadEntryRequest> issuedReads = new ConcurrentLinkedQueue<>();

    // Queue for read request that's wait for new entries from managed ledger.
    private ConcurrentLinkedQueue<PendingReadEntryRequest> pendingReads = new ConcurrentLinkedQueue<>();

    private final ManagedCursorImpl cursor;

    private final StreamingDispatcher dispatcher;

    private final PersistentTopic topic;

    private AtomicInteger currentReadSizeByte = new AtomicInteger(0);

    private volatile State state;

    private static final AtomicReferenceFieldUpdater<StreamingEntryReader, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(StreamingEntryReader.class, State.class, "state");

    private volatile long maxReadSizeByte;

    private final Backoff readFailureBackoff = new Backoff(10, TimeUnit.MILLISECONDS,
            1, TimeUnit.SECONDS, 0, TimeUnit.MILLISECONDS);

    /**
     * Read entries in streaming way, that said instead of reading with micro batch and send entries to consumer after
     * all entries in the batch are read from ledger, this method will fire numEntriesToRead requests to managedLedger
     * and send entry to consumer whenever it is read && all entries before it have been sent to consumer.
     * @param numEntriesToRead number of entry to read from ledger.
     * @param maxReadSizeByte maximum byte will be read from ledger.
     * @param ctx Context send along with read request.
     */
    public synchronized void asyncReadEntries(int numEntriesToRead, long maxReadSizeByte, Object ctx) {
        if (STATE_UPDATER.compareAndSet(this, State.Canceling, State.Canceled)) {
            internalCancelReadRequests();
        }

        if (!issuedReads.isEmpty() || !pendingReads.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] There's pending streaming read not completed yet. Not scheduling next read request.",
                        cursor.getName());
            }
            return;
        }

        PositionImpl nextReadPosition = (PositionImpl) cursor.getReadPosition();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) cursor.getManagedLedger();
        // Edge case, when a old ledger is full and new ledger is not yet opened, position can point to next
        // position of the last confirmed position, but it'll be an invalid position. So try to update the position.
        if (!managedLedger.isValidPosition(nextReadPosition)) {
            nextReadPosition = managedLedger.getNextValidPosition(nextReadPosition);
        }
        boolean hasEntriesToRead = managedLedger.hasMoreEntries(nextReadPosition);
        currentReadSizeByte.set(0);
        STATE_UPDATER.set(this, State.Issued);
        this.maxReadSizeByte = maxReadSizeByte;
        for (int c = 0; c < numEntriesToRead; c++) {
            PendingReadEntryRequest pendingReadEntryRequest = PendingReadEntryRequest.create(ctx, nextReadPosition);
            // Make sure once we start putting request into pending requests queue, we won't put any following request
            // to issued requests queue in order to guarantee the order.
            if (hasEntriesToRead && managedLedger.hasMoreEntries(nextReadPosition)) {
                issuedReads.offer(pendingReadEntryRequest);
            } else {
                pendingReads.offer(pendingReadEntryRequest);
            }
            nextReadPosition = managedLedger.getNextValidPosition(nextReadPosition);
        }

        // Issue requests.
        for (PendingReadEntryRequest request : issuedReads) {
            managedLedger.asyncReadEntry(request.position, this, request);
        }

        if (!pendingReads.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}} Streaming entry reader has {} pending read requests waiting on new entry."
                        , cursor.getName(), pendingReads.size());
            }
            // If new entries are available after we put request into pending queue, fire read.
            // Else register callback with managed ledger to get notify when new entries are available.
            if (managedLedger.hasMoreEntries(pendingReads.peek().position)) {
                entriesAvailable();
            } else if (managedLedger.isTerminated()) {
                dispatcher.notifyConsumersEndOfTopic();
                cleanQueue(pendingReads);
                if (issuedReads.size() == 0) {
                    dispatcher.canReadMoreEntries(true);
                }
            } else {
                managedLedger.addWaitingEntryCallBack(this);
            }
        }
    }

    @Override
    public void readEntryComplete(Entry entry, Object ctx) {
        // Don't block caller thread, complete read entry with dispatcher dedicated thread.
        topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(dispatcher.getName(), SafeRun.safeRun(() -> {
            internalReadEntryComplete(entry, ctx);
        }));
    }

    private void internalReadEntryComplete(Entry entry, Object ctx) {
        PendingReadEntryRequest pendingReadEntryRequest = (PendingReadEntryRequest) ctx;
        pendingReadEntryRequest.entry = entry;
        readFailureBackoff.reduceToHalf();
        Entry readEntry;
        // If we have entry to send to dispatcher.
        if (!issuedReads.isEmpty() && issuedReads.peek() == pendingReadEntryRequest) {
            while (!issuedReads.isEmpty() && issuedReads.peek().entry != null) {
                PendingReadEntryRequest firstPendingReadEntryRequest = issuedReads.poll();
                readEntry = firstPendingReadEntryRequest.entry;
                currentReadSizeByte.addAndGet(readEntry.getLength());
                //Cancel remaining requests and reset cursor if maxReadSizeByte exceeded.
                if (currentReadSizeByte.get() > maxReadSizeByte) {
                    cancelReadRequests(readEntry.getPosition());
                    dispatcher.canReadMoreEntries(false);
                    STATE_UPDATER.set(this, State.Completed);
                    return;
                } else {
                    // All request has been completed, mark returned entry as last.
                    if (issuedReads.isEmpty() && pendingReads.isEmpty()) {
                        firstPendingReadEntryRequest.isLast = true;
                        STATE_UPDATER.set(this, State.Completed);
                    }
                    dispatcher.readEntryComplete(readEntry, firstPendingReadEntryRequest);
                }
            }
        } else if (!issuedReads.isEmpty() && issuedReads.peek().retry > maxRetry) {
            cancelReadRequests(issuedReads.peek().position);
            dispatcher.canReadMoreEntries(true);
            STATE_UPDATER.set(this, State.Completed);
        }
    }

    @Override
    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
        // Don't block caller thread, complete read entry fail with dispatcher dedicated thread.
        topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(dispatcher.getName(), SafeRun.safeRun(() -> {
            internalReadEntryFailed(exception, ctx);
        }));
    }

    private void internalReadEntryFailed(ManagedLedgerException exception, Object ctx) {
        PendingReadEntryRequest pendingReadEntryRequest = (PendingReadEntryRequest) ctx;
        PositionImpl readPosition = pendingReadEntryRequest.position;
        pendingReadEntryRequest.retry++;
        long waitTimeMillis = readFailureBackoff.next();
        if (exception.getCause() instanceof TransactionBufferException.TransactionNotSealedException) {
            waitTimeMillis = 1;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Error reading transaction entries : {}, - Retrying to read in {} seconds",
                        cursor.getName(), exception.getMessage(), waitTimeMillis / 1000.0);
            }
        } else if (!(exception instanceof ManagedLedgerException.TooManyRequestsException)) {
            log.error("[{} Error reading entries at {} : {} - Retrying to read in {} seconds", cursor.getName(),
                    readPosition, exception.getMessage(), waitTimeMillis / 1000.0);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Got throttled by bookies while reading at {} : {} - Retrying to read in {} seconds",
                        cursor.getName(), readPosition, exception.getMessage(), waitTimeMillis / 1000.0);
            }
        }
        if (!issuedReads.isEmpty()) {
            if (issuedReads.peek().retry > maxRetry) {
                cancelReadRequests(issuedReads.peek().position);
                dispatcher.canReadMoreEntries(true);
                STATE_UPDATER.set(this, State.Completed);
                return;
            }
            if (pendingReadEntryRequest.retry <= maxRetry) {
                retryReadRequest(pendingReadEntryRequest, waitTimeMillis);
            }
        }
    }

    // Cancel all issued and pending request and update cursor's read position.
    private void cancelReadRequests(Position position) {
        if (!issuedReads.isEmpty()) {
            cleanQueue(issuedReads);
            cursor.seek(position);
        }

        if (!pendingReads.isEmpty()) {
            cleanQueue(pendingReads);
        }
    }

    private void internalCancelReadRequests() {
        Position readPosition = !issuedReads.isEmpty() ? issuedReads.peek().position : pendingReads.peek().position;
        cancelReadRequests(readPosition);
    }

    public boolean cancelReadRequests() {
        if (STATE_UPDATER.compareAndSet(this, State.Issued, State.Canceling)) {
            // Don't block caller thread, complete cancel read with dispatcher dedicated thread.
            topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(topic.getName(), SafeRun.safeRun(() -> {
                synchronized (StreamingEntryReader.this) {
                    if (STATE_UPDATER.compareAndSet(this, State.Canceling, State.Canceled)) {
                        internalCancelReadRequests();
                    }
                }
            }));
            return true;
        }
        return false;
    }

    private void cleanQueue(Queue<PendingReadEntryRequest> queue) {
        while (!queue.isEmpty()) {
            PendingReadEntryRequest pendingReadEntryRequest = queue.poll();
            if (pendingReadEntryRequest.entry != null) {
                pendingReadEntryRequest.entry.release();
                pendingReadEntryRequest.recycle();
            }
        }
    }

    private void retryReadRequest(PendingReadEntryRequest pendingReadEntryRequest, long delay) {
        topic.getBrokerService().executor().schedule(() -> {
            // Jump again into dispatcher dedicated thread
            topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(dispatcher.getName(),
                    SafeRun.safeRun(() -> {
                ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) cursor.getManagedLedger();
                managedLedger.asyncReadEntry(pendingReadEntryRequest.position, this, pendingReadEntryRequest);
            }));
        }, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void entriesAvailable() {
        topic.getBrokerService().getTopicOrderedExecutor().executeOrdered(dispatcher.getName(), SafeRun.safeRun(() -> {
            internalEntriesAvailable();
        }));
    }

    private synchronized void internalEntriesAvailable() {
        if (log.isDebugEnabled()) {
            log.debug("[{}} Streaming entry reader get notification of newly added entries from managed ledger,"
                    + " trying to issued pending read requests.", cursor.getName());
        }
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) cursor.getManagedLedger();
        List<PendingReadEntryRequest> newlyIssuedRequests = new ArrayList<>();
        if (!pendingReads.isEmpty()) {
            // Edge case, when a old ledger is full and new ledger is not yet opened, position can point to next
            // position of the last confirmed position, but it'll be an invalid position. So try to update the position.
            if (!managedLedger.isValidPosition(pendingReads.peek().position)) {
                pendingReads.peek().position = managedLedger.getNextValidPosition(pendingReads.peek().position);
            }
            while (!pendingReads.isEmpty() && managedLedger.hasMoreEntries(pendingReads.peek().position)) {
                PendingReadEntryRequest next = pendingReads.poll();
                issuedReads.offer(next);
                newlyIssuedRequests.add(next);
                // Need to update the position because when the PendingReadEntryRequest is created, the position could
                // be all set to managed ledger's last confirmed position.
                if (!pendingReads.isEmpty()) {
                    pendingReads.peek().position = managedLedger.getNextValidPosition(next.position);
                }
            }

            for (PendingReadEntryRequest request : newlyIssuedRequests) {
                managedLedger.asyncReadEntry(request.position, this, request);
            }

            if (!pendingReads.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}} Streaming entry reader has {} pending read requests waiting on new entry."
                            , cursor.getName(), pendingReads.size());
                }
                if (managedLedger.hasMoreEntries(pendingReads.peek().position)) {
                    entriesAvailable();
                } else {
                    managedLedger.addWaitingEntryCallBack(this);
                }
            }
        }
    }

    protected State getState() {
        return STATE_UPDATER.get(this);
    }

    enum State {
        Issued, Canceling, Canceled, Completed;
    }

}
