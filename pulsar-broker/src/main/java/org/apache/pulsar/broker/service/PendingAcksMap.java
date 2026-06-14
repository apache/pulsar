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
package org.apache.pulsar.broker.service;

import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.pulsar.common.util.collections.IntIntPair;
import org.apache.pulsar.common.util.collections.Long2LongOpenHashMap;

/**
 * A thread-safe map to store pending acks in the consumer.
 *
 * The locking solution is used for the draining hashes solution
 * to ensure that there's a consistent view of the pending acks. This is needed in the DrainingHashesTracker
 * to ensure that the reference counts are consistent at all times.
 * Calling forEachAndClose will ensure that no more entries can be added,
 * therefore no other thread cannot send out entries while the forEachAndClose is being called.
 * remove is also locked to ensure that there aren't races in the removal of entries while forEachAndClose is
 * running.
 */
public class PendingAcksMap {
    /**
     * Callback interface for handling the addition of pending acknowledgments.
     */
    public interface PendingAcksAddHandler {
        /**
         * Handle the addition of a pending acknowledgment.
         *
         * @param consumer      the consumer
         * @param ledgerId      the ledger ID
         * @param entryId       the entry ID
         * @param stickyKeyHash the sticky key hash
         * @return true if the addition is allowed, false otherwise
         */
        boolean handleAdding(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash);
    }

    /**
     * Callback interface for handling the removal of pending acknowledgments.
     */
    public interface PendingAcksRemoveHandler {
        /**
         * Handle the removal of a pending acknowledgment.
         *
         * @param consumer      the consumer
         * @param ledgerId      the ledger ID
         * @param entryId       the entry ID
         * @param stickyKeyHash the sticky key hash
         * @param closing       true if the pending ack is being removed because the map is being closed, false
         *                      otherwise
         */
        void handleRemoving(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash, boolean closing);
        /**
         * Start a batch of pending acknowledgment removals.
         */
        void startBatch();
        /**
         * End a batch of pending acknowledgment removals.
         */
        void endBatch();
    }

    /**
     * Callback interface for processing pending acknowledgments.
     */
    public interface PendingAcksConsumer {
        /**
         * Accept a pending acknowledgment.
         *
         * @param ledgerId          the ledger ID
         * @param entryId           the entry ID
         * @param remainingUnacked  the number of remaining unacked messages in this entry
         *                          (accounts for batch index level acknowledgments)
         * @param stickyKeyHash     the sticky key hash
         */
        void accept(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash);
    }

    private final Consumer consumer;
    private final TreeMap<Long, LedgerPendingAcks> pendingAcks;
    private final Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier;
    private final Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier;
    private final Lock readLock;
    private final Lock writeLock;
    private static final int PENDING_ACK_NOT_FOUND = -1;
    /*
     * Pending ack values are stored as a packed long to avoid allocating an IntIntPair per entry.
     * The high 32 bits contain remainingUnacked and the low 32 bits contain stickyKeyHash.
     * Long.MIN_VALUE is reserved for missing entries; remainingUnacked is a non-negative count in normal use, so
     * the packed representation cannot collide with this sentinel.
     */
    private static final long PACKED_PENDING_ACK_NOT_FOUND = Long.MIN_VALUE;
    private boolean closed = false;

    PendingAcksMap(Consumer consumer, Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier,
                   Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier) {
        this.consumer = consumer;
        this.pendingAcks = new TreeMap<>();
        this.pendingAcksAddHandlerSupplier = pendingAcksAddHandlerSupplier;
        this.pendingAcksRemoveHandlerSupplier = pendingAcksRemoveHandlerSupplier;
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.writeLock = readWriteLock.writeLock();
        this.readLock = readWriteLock.readLock();
    }

    /**
     * Add a pending ack to the map if it's allowed to send a message with the given sticky key hash.
     * If this method returns false, it means that the pending ack was not added, and it's not allowed to send a
     * message. In that case, the caller should not send a message and skip the entry.
     * The sending could be disallowed if the sticky key hash is blocked in the Key_Shared subscription.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param remainingUnacked the number of remaining unacked messages in this entry
     *                         (for batch entries with some indexes already acked, this may be less than batchSize)
     * @param stickyKeyHash the sticky key hash
     * @return true if the pending ack was added, and it's allowed to send a message, false otherwise
     */
    public boolean addPendingAckIfAllowed(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
        try {
            writeLock.lock();
            // prevent adding sticky hash to pending acks if the PendingAcksMap has already been closed
            // and there's a race condition between closing the consumer and sending new messages
            if (closed) {
                return false;
            }
            // prevent adding sticky hash to pending acks if it's already in draining hashes
            // to avoid any race conditions that would break consistency
            PendingAcksAddHandler pendingAcksAddHandler = pendingAcksAddHandlerSupplier.get();
            if (pendingAcksAddHandler != null
                    && !pendingAcksAddHandler.handleAdding(consumer, ledgerId, entryId, stickyKeyHash)) {
                return false;
            }
            LedgerPendingAcks ledgerPendingAcks =
                    pendingAcks.computeIfAbsent(ledgerId, k -> new LedgerPendingAcks());
            ledgerPendingAcks.put(entryId, packPendingAckValue(remainingUnacked, stickyKeyHash));
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Get the size of the pending acks map.
     *
     * @return the size of the pending acks map
     */
    public long size() {
        try {
            readLock.lock();
            return pendingAcks.values().stream().mapToInt(LedgerPendingAcks::size).sum();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Iterate over all the pending acks and process them using the given processor.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEach(PendingAcksConsumer processor) {
        try {
            readLock.lock();
            processPendingAcks(processor);
        } finally {
            readLock.unlock();
        }
    }

    // iterate all pending acks and process them
    private void processPendingAcks(PendingAcksConsumer processor) {
        // this code uses for loops intentionally, don't refactor to use forEach
        // iterate the outer map
        for (Map.Entry<Long, LedgerPendingAcks> entry : pendingAcks.entrySet()) {
            long ledgerId = entry.getKey();
            LedgerPendingAcks ledgerPendingAcks = entry.getValue();
            // iterate the inner map
            ledgerPendingAcks.forEach((entryId, packedValue) ->
                    processor.accept(ledgerId, entryId, unpackRemainingUnacked(packedValue),
                            unpackStickyKeyHash(packedValue)));
        }
    }

    /**
     * Iterate over all the pending acks and close the map so that no more entries can be added.
     * All entries are removed.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEachAndClose(PendingAcksConsumer processor) {
        internalForEachAndClear(processor, true);
    }

    /**
     * Iterate over all the pending acks and clear the map.
     * Unlike {@link #forEachAndClose(PendingAcksConsumer)}, this method does not close the map,
     * so new entries can still be added after this method returns.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEachAndClear(PendingAcksConsumer processor) {
        internalForEachAndClear(processor, false);
    }

    private void internalForEachAndClear(PendingAcksConsumer processor, boolean close) {
        try {
            writeLock.lock();
            if (close) {
                closed = true;
            }
            PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
            if (pendingAcksRemoveHandler != null) {
                try {
                    pendingAcksRemoveHandler.startBatch();
                    processPendingAcks((ledgerId, entryId, batchSize, stickyKeyHash) -> {
                        processor.accept(ledgerId, entryId, batchSize, stickyKeyHash);
                        pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
                    });
                } finally {
                    pendingAcksRemoveHandler.endBatch();
                }
            } else {
                processPendingAcks(processor);
            }
            pendingAcks.clear();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Check if the map contains a pending ack for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return true if the map contains the pending ack, false otherwise
     */
    public boolean contains(long ledgerId, long entryId) {
        try {
            readLock.lock();
            LedgerPendingAcks ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            return ledgerMap.containsKey(entryId);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get the pending ack for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return the pending ack, or null if not found
     */
    public IntIntPair get(long ledgerId, long entryId) {
        try {
            readLock.lock();
            LedgerPendingAcks ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return null;
            }
            long packedValue = getPackedPendingAckOrNotFound(ledgerMap, entryId);
            return isPackedPendingAckNotFound(packedValue) ? null : unpackPendingAckValue(packedValue);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get the remaining unacked count for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return the remaining unacked count, or -1 if not found
     */
    public int getRemainingUnacked(long ledgerId, long entryId) {
        try {
            readLock.lock();
            LedgerPendingAcks ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return PENDING_ACK_NOT_FOUND;
            }
            long packedValue = getPackedPendingAckOrNotFound(ledgerMap, entryId);
            return isPackedPendingAckNotFound(packedValue)
                    ? PENDING_ACK_NOT_FOUND : unpackRemainingUnacked(packedValue);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Remove the pending ack for the given ledger ID, entry ID, batch size, and sticky key hash.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param batchSize the batch size
     * @param stickyKeyHash the sticky key hash
     * @return true if the pending ack was removed, false otherwise
     */
    public boolean remove(long ledgerId, long entryId, int batchSize, int stickyKeyHash) {
        try {
            writeLock.lock();
            LedgerPendingAcks ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            long packedValue = getPackedPendingAckOrNotFound(ledgerMap, entryId);
            if (isPackedPendingAckNotFound(packedValue)) {
                return false;
            }
            boolean removed = unpackRemainingUnacked(packedValue) == batchSize
                    && unpackStickyKeyHash(packedValue) == stickyKeyHash;
            if (removed) {
                ledgerMap.removePresent(entryId);
                handleRemovePendingAck(ledgerId, entryId, stickyKeyHash);
            }
            if (removed && ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically update the remaining unacked count for a pending ack entry by subtracting the given delta.
     * Called from the ack handler after computing the number of batch indexes acknowledged in a partial ack.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param ackedDelta the number of batch indexes that were just acknowledged
     * @return true if the entry was found and updated, false otherwise
     */
    public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
        try {
            writeLock.lock();
            LedgerPendingAcks ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            long packedValue = getPackedPendingAckOrNotFound(ledgerMap, entryId);
            if (isPackedPendingAckNotFound(packedValue)) {
                return false;
            }
            int newRemaining = unpackRemainingUnacked(packedValue) - ackedDelta;
            ledgerMap.put(entryId, packPendingAckValue(newRemaining, unpackStickyKeyHash(packedValue)));
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Remove the pending ack for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return true if the pending ack was removed, false otherwise
     */
    public boolean remove(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            LedgerPendingAcks ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            long removedEntry = getPackedPendingAckOrNotFound(ledgerMap, entryId);
            if (isPackedPendingAckNotFound(removedEntry)) {
                return false;
            }
            ledgerMap.removePresent(entryId);
            handleRemovePendingAck(ledgerId, entryId, unpackStickyKeyHash(removedEntry));
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically remove and return the pending ack for the given ledger ID and entry ID.
     * Unlike {@link #remove(long, long)}, this method returns the removed entry so the caller
     * can access the batch size and sticky key hash without a separate get operation.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return the removed entry as an IntIntPair (batchSize, stickyKeyHash), or null if not found
     */
    public IntIntPair removeAndGet(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            LedgerPendingAcks ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return null;
            }
            long removedEntry = getPackedPendingAckOrNotFound(ledgerMap, entryId);
            if (isPackedPendingAckNotFound(removedEntry)) {
                return null;
            }
            ledgerMap.removePresent(entryId);
            handleRemovePendingAck(ledgerId, entryId, unpackStickyKeyHash(removedEntry));
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return unpackPendingAckValue(removedEntry);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically remove and return the remaining unacked count for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return the remaining unacked count, or -1 if not found
     */
    public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            LedgerPendingAcks ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return PENDING_ACK_NOT_FOUND;
            }
            long removedEntry = getPackedPendingAckOrNotFound(ledgerMap, entryId);
            if (isPackedPendingAckNotFound(removedEntry)) {
                return PENDING_ACK_NOT_FOUND;
            }
            ledgerMap.removePresent(entryId);
            handleRemovePendingAck(ledgerId, entryId, unpackStickyKeyHash(removedEntry));
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return unpackRemainingUnacked(removedEntry);
        } finally {
            writeLock.unlock();
        }
    }


    /**
     * Remove all pending acks up to the given ledger ID and entry ID, invoking a callback for each removed entry.
     *
     * @param markDeleteLedgerId the ledger ID up to which to remove pending acks
     * @param markDeleteEntryId the entry ID up to which to remove pending acks
     * @param removedEntryCallback optional callback invoked for each removed entry (within the write lock),
     *                             receiving ledgerId, entryId, batchSize, and stickyKeyHash
     */
    public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId,
                             PendingAcksConsumer removedEntryCallback) {
        internalRemoveAllUpTo(markDeleteLedgerId, markDeleteEntryId, false, removedEntryCallback);
    }

    /**
     * Removes all pending acknowledgments up to the specified ledger ID and entry ID.
     *
     * ReadWriteLock doesn't support upgrading from read lock to write lock.
     * This method first checks if there's anything to remove using a read lock and if there is, exits
     * and retries with a write lock to make the removals.
     *
     * @param markDeleteLedgerId the ledger ID up to which to remove pending acks
     * @param markDeleteEntryId the entry ID up to which to remove pending acks
     * @param useWriteLock true if the method should use a write lock, false otherwise
     * @param removedEntryCallback optional callback invoked for each removed entry (within the write lock)
     */
    private void internalRemoveAllUpTo(long markDeleteLedgerId, long markDeleteEntryId, boolean useWriteLock,
                                      PendingAcksConsumer removedEntryCallback) {
        PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
        // track if the write lock was acquired
        boolean acquiredWriteLock = false;
        // track if a batch was started
        boolean batchStarted = false;
        // track if the method should retry with a write lock
        boolean retryWithWriteLock = false;
        try {
            if (useWriteLock) {
                writeLock.lock();
                acquiredWriteLock = true;
            } else {
                readLock.lock();
            }
            Iterator<Map.Entry<Long, LedgerPendingAcks>> ledgerMapIterator =
                    pendingAcks.headMap(markDeleteLedgerId, true).entrySet().iterator();
            while (ledgerMapIterator.hasNext()) {
                Map.Entry<Long, LedgerPendingAcks> entry = ledgerMapIterator.next();
                long ledgerId = entry.getKey();
                LedgerPendingAcks ledgerMap = entry.getValue();
                if (!acquiredWriteLock) {
                    if (ledgerId < markDeleteLedgerId && !ledgerMap.isEmpty()) {
                        retryWithWriteLock = true;
                        return;
                    }
                    if (ledgerId == markDeleteLedgerId && ledgerMap.hasEntryUpTo(markDeleteEntryId)) {
                        retryWithWriteLock = true;
                        return;
                    }
                    continue;
                }
                if (ledgerId < markDeleteLedgerId) {
                    MutableBoolean batchStartedHolder = new MutableBoolean(batchStarted);
                    ledgerMap.forEach((entryId, packedValue) -> {
                        handleRemovedEntry(ledgerId, entryId, packedValue, pendingAcksRemoveHandler,
                                batchStartedHolder, removedEntryCallback);
                    });
                    batchStarted = batchStartedHolder.booleanValue();
                    ledgerMapIterator.remove();
                } else {
                    MutableBoolean batchStartedHolder = new MutableBoolean(batchStarted);
                    int removed = ledgerMap.removeUpTo(markDeleteEntryId, (entryId, packedValue) ->
                            handleRemovedEntry(ledgerId, entryId, packedValue, pendingAcksRemoveHandler,
                                    batchStartedHolder, removedEntryCallback));
                    batchStarted = batchStartedHolder.booleanValue();
                    if (removed > 0 && ledgerMap.isEmpty()) {
                        ledgerMapIterator.remove();
                    }
                }
            }
        } finally {
            if (batchStarted) {
                pendingAcksRemoveHandler.endBatch();
            }
            if (acquiredWriteLock) {
                writeLock.unlock();
            } else {
                readLock.unlock();
                if (retryWithWriteLock) {
                    internalRemoveAllUpTo(markDeleteLedgerId, markDeleteEntryId, true, removedEntryCallback);
                }
            }
        }
    }

    // A packed value can legitimately be 0, so do not use Long2LongOpenHashMap.get() for lookups here.
    private static long getPackedPendingAckOrNotFound(LedgerPendingAcks ledgerMap, long entryId) {
        return ledgerMap.getOrDefault(entryId, PACKED_PENDING_ACK_NOT_FOUND);
    }

    private static boolean isPackedPendingAckNotFound(long packedValue) {
        return packedValue == PACKED_PENDING_ACK_NOT_FOUND;
    }

    private static long packPendingAckValue(int remainingUnacked, int stickyKeyHash) {
        return ((long) remainingUnacked << Integer.SIZE) | (stickyKeyHash & 0xFFFF_FFFFL);
    }

    private static IntIntPair unpackPendingAckValue(long packedValue) {
        return IntIntPair.of(unpackRemainingUnacked(packedValue), unpackStickyKeyHash(packedValue));
    }

    private static int unpackRemainingUnacked(long packedValue) {
        return (int) (packedValue >> Integer.SIZE);
    }

    private static int unpackStickyKeyHash(long packedValue) {
        return (int) packedValue;
    }

    private void handleRemovePendingAck(long ledgerId, long entryId, int stickyKeyHash) {
        PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
        if (pendingAcksRemoveHandler != null) {
            pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
        }
    }

    private void handleRemovedEntry(long ledgerId, long entryId, long packedValue,
                                    PendingAcksRemoveHandler pendingAcksRemoveHandler,
                                    MutableBoolean batchStartedHolder,
                                    PendingAcksConsumer removedEntryCallback) {
        int stickyKeyHash = unpackStickyKeyHash(packedValue);
        if (pendingAcksRemoveHandler != null) {
            if (!batchStartedHolder.booleanValue()) {
                pendingAcksRemoveHandler.startBatch();
                batchStartedHolder.setTrue();
            }
            pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
        }
        if (removedEntryCallback != null) {
            removedEntryCallback.accept(ledgerId, entryId, unpackRemainingUnacked(packedValue), stickyKeyHash);
        }
    }

    private static final class LedgerPendingAcks {
        private static final int MAX_INDEXED_ENTRY_ID = 1 << 20;
        private final Long2LongOpenHashMap entries = new Long2LongOpenHashMap();
        private final BitSet entryIdIndex = new BitSet();
        private boolean entryIdIndexEnabled = true;
        private long maxEntryId = Long.MIN_VALUE;

        private void put(long entryId, long packedValue) {
            entries.put(entryId, packedValue);
            maxEntryId = Math.max(maxEntryId, entryId);
            if (!entryIdIndexEnabled) {
                return;
            }
            if (canIndexEntryId(entryId)) {
                entryIdIndex.set((int) entryId);
            } else {
                entryIdIndex.clear();
                entryIdIndexEnabled = false;
            }
        }

        private long getOrDefault(long entryId, long defaultValue) {
            return entries.getOrDefault(entryId, defaultValue);
        }

        private boolean containsKey(long entryId) {
            return entries.containsKey(entryId);
        }

        private boolean isEmpty() {
            return entries.isEmpty();
        }

        private int size() {
            return entries.size();
        }

        private void forEach(Long2LongOpenHashMap.EntryConsumer consumer) {
            entries.forEach(consumer);
        }

        private void removePresent(long entryId) {
            entries.remove(entryId);
            if (entryIdIndexEnabled && canIndexEntryId(entryId)) {
                entryIdIndex.clear((int) entryId);
            }
        }

        private boolean hasEntryUpTo(long markDeleteEntryId) {
            if (entries.isEmpty() || markDeleteEntryId < 0) {
                return false;
            }
            if (markDeleteEntryId >= maxEntryId) {
                return true;
            }
            if (!entryIdIndexEnabled) {
                return true;
            }
            int firstEntryId = entryIdIndex.nextSetBit(0);
            return firstEntryId >= 0 && firstEntryId <= markDeleteEntryId;
        }

        private int removeUpTo(long markDeleteEntryId, RemovedEntryConsumer removedEntryConsumer) {
            if (!hasEntryUpTo(markDeleteEntryId)) {
                return 0;
            }
            if (markDeleteEntryId >= maxEntryId) {
                entries.forEach(removedEntryConsumer::accept);
                int removed = entries.size();
                entries.clear();
                entryIdIndex.clear();
                return removed;
            }
            if (entryIdIndexEnabled && canIndexEntryId(markDeleteEntryId)) {
                return removeIndexedPrefix((int) markDeleteEntryId, removedEntryConsumer);
            }
            return entries.removeIf((entryId, packedValue) -> {
                if (entryId > markDeleteEntryId) {
                    return false;
                }
                removedEntryConsumer.accept(entryId, packedValue);
                return true;
            });
        }

        private int removeIndexedPrefix(int markDeleteEntryId, RemovedEntryConsumer removedEntryConsumer) {
            int removed = 0;
            int indexedEntryId = entryIdIndex.nextSetBit(0);
            while (indexedEntryId >= 0 && indexedEntryId <= markDeleteEntryId) {
                long entryId = indexedEntryId;
                long packedValue = entries.getOrDefault(entryId, PACKED_PENDING_ACK_NOT_FOUND);
                if (!isPackedPendingAckNotFound(packedValue)) {
                    entries.remove(entryId);
                    removedEntryConsumer.accept(entryId, packedValue);
                    removed++;
                }
                indexedEntryId = entryIdIndex.nextSetBit(indexedEntryId + 1);
            }
            entryIdIndex.clear(0, markDeleteEntryId + 1);
            return removed;
        }

        private static boolean canIndexEntryId(long entryId) {
            return entryId >= 0 && entryId <= MAX_INDEXED_ENTRY_ID;
        }
    }

    @FunctionalInterface
    private interface RemovedEntryConsumer {
        void accept(long entryId, long packedValue);
    }
}
