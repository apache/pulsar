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
    public interface PendingAcksAddHandler {
        boolean handleAdding(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash);
    }

    public interface PendingAcksRemoveHandler {
        void handleRemoving(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash);
    }

    public interface PendingAcksConsumer {
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

    public boolean put(long ledgerId, long entryId, int batchSize, int stickyKeyHash) {
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

    public long size() {
        return pendingAcks.size();
    }

    public void forEach(PendingAcksConsumer processor) {
        try {
            readLock.lock();
            processPendingAcks(processor);
        } finally {
            readLock.unlock();
        }
    }

    private void processPendingAcks(PendingAcksConsumer processor) {
        pendingAcks.forEach((ledgerId, ledgerPendingAcks) -> {
            ledgerPendingAcks.forEach((entryId, batchSizeAndStickyKeyHash) -> {
                processor.accept(ledgerId, entryId, batchSizeAndStickyKeyHash.leftInt(),
                        batchSizeAndStickyKeyHash.rightInt());
            });
        });
    }

    /**
     * Iterate over all the pending acks and close the map so that no more entries can be added.
     * @param processor
     */
    public void forEachAndClose(PendingAcksConsumer processor) {
        try {
            writeLock.lock();
            closed = true;
            processPendingAcks(processor);
            pendingAcks.clear();
        } finally {
            writeLock.unlock();
        }
    }

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

    public boolean remove(long ledgerId, long entryId, int batchSize, int stickyKeyHash) {
        try {
            writeLock.lock();
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            boolean removed = ledgerMap.remove(entryId, IntIntPair.of(batchSize, stickyKeyHash));
            if (removed && ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean remove(long ledgerId, long entryId) {
        try {
            writeLock.lock();
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            boolean removed = ledgerMap.remove(entryId) != null;
            if (removed && ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

    public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
        boolean acquiredWriteLock = false;
        try {
            readLock.lock();
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
                    IntIntPair batchSizeAndStickyKeyHash = intIntPairEntry.getValue();
                    handleRemovePendingAck(ledgerId, entryId, batchSizeAndStickyKeyHash.rightInt());
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
