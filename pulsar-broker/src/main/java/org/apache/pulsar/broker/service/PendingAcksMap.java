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

import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

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
         */
        void handleRemoving(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash);
    }

    /**
     * Callback interface for processing pending acknowledgments.
     */
    public interface PendingAcksConsumer {
        /**
         * Accept a pending acknowledgment.
         *
         * @param ledgerId      the ledger ID
         * @param entryId       the entry ID
         * @param batchSize     the batch size
         * @param stickyKeyHash the sticky key hash
         */
        void accept(long ledgerId, long entryId, int batchSize, int stickyKeyHash);
    }

    private final Consumer consumer;
    private final Long2ObjectSortedMap<Long2ObjectSortedMap<IntIntPair>> pendingAcks;
    private final Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier;
    private final Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier;
    private final Lock readLock;
    private final Lock writeLock;
    private boolean closed = false;

    PendingAcksMap(Consumer consumer, Supplier<PendingAcksAddHandler> pendingAcksAddHandlerSupplier,
                   Supplier<PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier, boolean useExclusiveReadLock) {
        this.consumer = consumer;
        this.pendingAcks = new Long2ObjectRBTreeMap<>();
        this.pendingAcksAddHandlerSupplier = pendingAcksAddHandlerSupplier;
        this.pendingAcksRemoveHandlerSupplier = pendingAcksRemoveHandlerSupplier;
        if (useExclusiveReadLock) {
            this.writeLock = new ReentrantLock();
            this.readLock = this.writeLock;
        } else {
            ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
            this.writeLock = readWriteLock.writeLock();
            this.readLock = readWriteLock.readLock();
        }
    }

    /**
     * Add a pending ack to the map if it's allowed to send a message with the given sticky key hash.
     * If this method returns false, it means that the pending ack was not added, and it's not allowed to send a
     * message. In that case, the caller should not send a message and skip the entry.
     * The sending could be disallowed if the sticky key hash is blocked in the Key_Shared subscription.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @param batchSize the batch size
     * @param stickyKeyHash the sticky key hash
     * @return true if the pending ack was added, and it's allowed to send a message, false otherwise
     */
    public boolean addPendingAckIfAllowed(long ledgerId, long entryId, int batchSize, int stickyKeyHash) {
        try {
            writeLock.lock();
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
            Long2ObjectSortedMap<IntIntPair> ledgerPendingAcks =
                    pendingAcks.computeIfAbsent(ledgerId, k -> new Long2ObjectRBTreeMap<>());
            ledgerPendingAcks.put(entryId, IntIntPair.of(batchSize, stickyKeyHash));
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
        return pendingAcks.size();
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
        for (Map.Entry<Long, Long2ObjectSortedMap<IntIntPair>> entry : pendingAcks.entrySet()) {
            Long ledgerId = entry.getKey();
            Long2ObjectSortedMap<IntIntPair> ledgerPendingAcks = entry.getValue();
            // iterate the inner map
            for (Map.Entry<Long, IntIntPair> e : ledgerPendingAcks.entrySet()) {
                Long entryId = e.getKey();
                IntIntPair batchSizeAndStickyKeyHash = e.getValue();
                processor.accept(ledgerId, entryId, batchSizeAndStickyKeyHash.leftInt(),
                        batchSizeAndStickyKeyHash.rightInt());
            }
        }
    }

    /**
     * Iterate over all the pending acks and close the map so that no more entries can be added.
     * All entries are removed.
     *
     * @param processor the processor to handle each pending ack
     */
    public void forEachAndClose(PendingAcksConsumer processor) {
        try {
            writeLock.lock();
            closed = true;
            PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
            if (pendingAcksRemoveHandler != null) {
                processPendingAcks((ledgerId, entryId, batchSize, stickyKeyHash) -> {
                    processor.accept(ledgerId, entryId, batchSize, stickyKeyHash);
                    pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash);
                });
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
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
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
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return null;
            }
            return ledgerMap.get(entryId);
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
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            boolean removed = ledgerMap.remove(entryId, IntIntPair.of(batchSize, stickyKeyHash));
            if (removed) {
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
     * Remove the pending ack for the given ledger ID and entry ID.
     *
     * @param ledgerId the ledger ID
     * @param entryId the entry ID
     * @return true if the pending ack was removed, false otherwise
     */
    public boolean remove(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            IntIntPair removedEntry = ledgerMap.remove(entryId);
            boolean removed = removedEntry != null;
            if (removed) {
                int stickyKeyHash = removedEntry.rightInt();
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
     * Remove all pending acks up to the given ledger ID and entry ID.
     *
     * @param markDeleteLedgerId the ledger ID up to which to remove pending acks
     * @param markDeleteEntryId the entry ID up to which to remove pending acks
     */
    public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
        boolean acquiredWriteLock = false;
        try {
            readLock.lock();
            PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
            ObjectBidirectionalIterator<Long2ObjectMap.Entry<Long2ObjectSortedMap<IntIntPair>>> ledgerMapIterator =
                    pendingAcks.headMap(markDeleteLedgerId + 1).long2ObjectEntrySet().iterator();
            while (ledgerMapIterator.hasNext()) {
                Long2ObjectMap.Entry<Long2ObjectSortedMap<IntIntPair>> entry = ledgerMapIterator.next();
                long ledgerId = entry.getLongKey();
                Long2ObjectSortedMap<IntIntPair> ledgerMap = entry.getValue();
                Long2ObjectSortedMap<IntIntPair> ledgerMapHead;
                if (ledgerId == markDeleteLedgerId) {
                    ledgerMapHead = ledgerMap.headMap(markDeleteEntryId + 1);
                } else {
                    ledgerMapHead = ledgerMap;
                }
                ObjectBidirectionalIterator<Long2ObjectMap.Entry<IntIntPair>> entryMapIterator =
                        ledgerMapHead.long2ObjectEntrySet().iterator();
                while (entryMapIterator.hasNext()) {
                    Long2ObjectMap.Entry<IntIntPair> intIntPairEntry = entryMapIterator.next();
                    long entryId = intIntPairEntry.getLongKey();
                    if (!acquiredWriteLock && writeLock != readLock) {
                        readLock.unlock();
                        writeLock.lock();
                        acquiredWriteLock = true;
                    }
                    if (pendingAcksRemoveHandler != null) {
                        int stickyKeyHash = intIntPairEntry.getValue().rightInt();
                        pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash);
                    }
                    entryMapIterator.remove();
                }
                if (ledgerMap.isEmpty()) {
                    if (!acquiredWriteLock && writeLock != readLock) {
                        readLock.unlock();
                        writeLock.lock();
                        acquiredWriteLock = true;
                    }
                    ledgerMapIterator.remove();
                }
            }
        } finally {
            if (acquiredWriteLock) {
                writeLock.unlock();
            } else {
                readLock.unlock();
            }
        }
    }

    private void handleRemovePendingAck(long ledgerId, long entryId, int stickyKeyHash) {
        PendingAcksRemoveHandler pendingAcksRemoveHandler = pendingAcksRemoveHandlerSupplier.get();
        if (pendingAcksRemoveHandler != null) {
            pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash);
        }
    }
}