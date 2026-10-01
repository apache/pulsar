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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
public class PendingAcksMapPackingBenchmark {
    private static final int PENDING_ACK_NOT_FOUND = -1;

    @Benchmark
    public boolean addOrReplace(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        return state.store.addOrReplace(state.ledgerIds[index], state.entryIds[index],
                remainingUnacked(index), stickyKeyHash(index));
    }

    @Benchmark
    public boolean containsHit(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        return state.store.contains(state.ledgerIds[index], state.entryIds[index]);
    }

    @Benchmark
    public long forEachScan(MapState state) {
        return state.store.forEachScan();
    }

    @Benchmark
    public int getRemainingUnackedHit(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        return state.store.getRemainingUnacked(state.ledgerIds[index], state.entryIds[index]);
    }

    @Benchmark
    public long removeAllUpToBeforeFirstEntry(MapState state) {
        return state.store.removeAllUpTo(0, -1);
    }

    @Benchmark
    public long removeAllUpToSmallPrefixAndRefill(PrefixRemoveState state) {
        long removed = state.store.removeAllUpTo(0, state.prefixEntries - 1L);
        for (int i = 0; i < state.prefixEntries; i++) {
            state.store.addOrReplace(0, i, remainingUnacked(i), stickyKeyHash(i));
        }
        return removed;
    }

    @Benchmark
    public int removeAndGetRemainingAndAdd(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        long ledgerId = state.ledgerIds[index];
        long entryId = state.entryIds[index];
        int remainingUnacked = state.store.removeAndGetRemainingUnacked(ledgerId, entryId);
        state.store.addOrReplace(ledgerId, entryId,
                remainingUnacked == PENDING_ACK_NOT_FOUND ? remainingUnacked(index) : remainingUnacked,
                stickyKeyHash(index));
        return remainingUnacked;
    }

    @Benchmark
    public boolean removeWithValueAndAdd(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        long ledgerId = state.ledgerIds[index];
        long entryId = state.entryIds[index];
        int remainingUnacked = remainingUnacked(index);
        int stickyKeyHash = stickyKeyHash(index);
        boolean removed = state.store.remove(ledgerId, entryId, remainingUnacked, stickyKeyHash);
        state.store.addOrReplace(ledgerId, entryId, remainingUnacked, stickyKeyHash);
        return removed;
    }

    @Benchmark
    public boolean updateRemainingUnacked(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        return state.store.updateRemainingUnacked(state.ledgerIds[index], state.entryIds[index], 1);
    }

    @State(Scope.Benchmark)
    public static class MapState {
        @Param({"FORMER_PENDING_ACKS_MAP", "CURRENT_PENDING_ACKS_MAP"})
        private Implementation implementation;

        @Param({"50000"})
        private int entries;

        @Param({"1"})
        private int ledgers;

        private PendingAcksStore store;
        private long[] ledgerIds;
        private long[] entryIds;

        @Setup(Level.Trial)
        public void setup() {
            store = implementation.createStore();
            ledgerIds = new long[entries];
            entryIds = new long[entries];
            populate(store, entries, ledgers, ledgerIds, entryIds);
        }
    }

    @State(Scope.Thread)
    public static class PrefixRemoveState {
        @Param({"FORMER_PENDING_ACKS_MAP", "CURRENT_PENDING_ACKS_MAP"})
        private Implementation implementation;

        @Param({"50000"})
        private int entries;

        private PendingAcksStore store;
        private int prefixEntries;

        @Setup(Level.Trial)
        public void setup() {
            store = implementation.createStore();
            prefixEntries = Math.max(1, entries / 50);
            populate(store, entries, 1, null, null);
        }
    }

    @State(Scope.Thread)
    public static class CursorState {
        private int cursor;

        int next(int bound) {
            int next = cursor++;
            if (cursor == bound) {
                cursor = 0;
            }
            return next;
        }
    }

    public enum Implementation {
        FORMER_PENDING_ACKS_MAP {
            @Override
            PendingAcksStore createStore() {
                return new FormerPendingAcksMapStore();
            }
        },
        CURRENT_PENDING_ACKS_MAP {
            @Override
            PendingAcksStore createStore() {
                return new CurrentPendingAcksMapStore();
            }
        };

        abstract PendingAcksStore createStore();
    }

    private interface PendingAcksStore {
        boolean addOrReplace(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash);

        boolean contains(long ledgerId, long entryId);

        int getRemainingUnacked(long ledgerId, long entryId);

        boolean remove(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash);

        int removeAndGetRemainingUnacked(long ledgerId, long entryId);

        boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta);

        long forEachScan();

        long removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId);
    }

    private static final class FormerPendingAcksMapStore implements PendingAcksStore {
        private final Consumer consumer = null;
        private final Long2ObjectSortedMap<Long2ObjectSortedMap<IntIntPair>> pendingAcks =
                new Long2ObjectRBTreeMap<>();
        private final Supplier<PendingAcksMap.PendingAcksAddHandler> pendingAcksAddHandlerSupplier = () -> null;
        private final Supplier<PendingAcksMap.PendingAcksRemoveHandler> pendingAcksRemoveHandlerSupplier = () -> null;
        private final Lock readLock;
        private final Lock writeLock;
        private boolean closed = false;
        private volatile long size;
        private long forEachSum;
        private long removedCount;
        private final PendingAcksMap.PendingAcksConsumer pendingAckScanner =
                (ledgerId, entryId, remainingUnacked, stickyKeyHash) ->
                        forEachSum += ledgerId + entryId + remainingUnacked + stickyKeyHash;
        private final PendingAcksMap.PendingAcksConsumer removedCounter =
                (ledgerId, entryId, remainingUnacked, stickyKeyHash) -> removedCount++;

        private FormerPendingAcksMapStore() {
            ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
            this.writeLock = readWriteLock.writeLock();
            this.readLock = readWriteLock.readLock();
        }

        @Override
        public boolean addOrReplace(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            try {
                writeLock.lock();
                if (closed) {
                    return false;
                }
                PendingAcksMap.PendingAcksAddHandler pendingAcksAddHandler = pendingAcksAddHandlerSupplier.get();
                if (pendingAcksAddHandler != null
                        && !pendingAcksAddHandler.handleAdding(consumer, ledgerId, entryId, stickyKeyHash)) {
                    return false;
                }
                Long2ObjectSortedMap<IntIntPair> ledgerPendingAcks =
                        pendingAcks.computeIfAbsent(ledgerId, k -> new Long2ObjectRBTreeMap<>());
                IntIntPair previous = ledgerPendingAcks.put(entryId, IntIntPair.of(remainingUnacked, stickyKeyHash));
                if (previous == null) {
                    size++;
                }
                return true;
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public boolean contains(long ledgerId, long entryId) {
            try {
                readLock.lock();
                Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                return ledgerMap != null && ledgerMap.containsKey(entryId);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public int getRemainingUnacked(long ledgerId, long entryId) {
            try {
                readLock.lock();
                Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                IntIntPair value = ledgerMap == null ? null : ledgerMap.get(entryId);
                return value == null ? PENDING_ACK_NOT_FOUND : value.leftInt();
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public boolean remove(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            try {
                writeLock.lock();
                Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                if (ledgerMap == null) {
                    return false;
                }
                boolean removed = ledgerMap.remove(entryId, IntIntPair.of(remainingUnacked, stickyKeyHash));
                if (removed) {
                    size--;
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

        @Override
        public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
            try {
                writeLock.lock();
                Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                if (ledgerMap == null) {
                    return PENDING_ACK_NOT_FOUND;
                }
                IntIntPair removed = ledgerMap.remove(entryId);
                if (removed == null) {
                    return PENDING_ACK_NOT_FOUND;
                }
                size--;
                handleRemovePendingAck(ledgerId, entryId, removed.rightInt());
                if (ledgerMap.isEmpty()) {
                    pendingAcks.remove(ledgerId);
                }
                return removed.leftInt();
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
            try {
                writeLock.lock();
                Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                IntIntPair value = ledgerMap == null ? null : ledgerMap.get(entryId);
                if (value == null) {
                    return false;
                }
                ledgerMap.put(entryId, IntIntPair.of(value.leftInt() - ackedDelta, value.rightInt()));
                return true;
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public long forEachScan() {
            forEachSum = 0;
            forEach(pendingAckScanner);
            return forEachSum;
        }

        @Override
        public long removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
            removedCount = 0;
            internalRemoveAllUpTo(markDeleteLedgerId, markDeleteEntryId, false, removedCounter);
            return removedCount;
        }

        private void forEach(PendingAcksMap.PendingAcksConsumer processor) {
            try {
                readLock.lock();
                processPendingAcks(processor);
            } finally {
                readLock.unlock();
            }
        }

        private void processPendingAcks(PendingAcksMap.PendingAcksConsumer processor) {
            for (Long2ObjectMap.Entry<Long2ObjectSortedMap<IntIntPair>> entry : pendingAcks.long2ObjectEntrySet()) {
                long ledgerId = entry.getLongKey();
                Long2ObjectSortedMap<IntIntPair> ledgerPendingAcks = entry.getValue();
                for (Long2ObjectMap.Entry<IntIntPair> e : ledgerPendingAcks.long2ObjectEntrySet()) {
                    long entryId = e.getLongKey();
                    IntIntPair batchSizeAndStickyKeyHash = e.getValue();
                    processor.accept(ledgerId, entryId, batchSizeAndStickyKeyHash.leftInt(),
                            batchSizeAndStickyKeyHash.rightInt());
                }
            }
        }

        private void internalRemoveAllUpTo(long markDeleteLedgerId, long markDeleteEntryId, boolean useWriteLock,
                                          PendingAcksMap.PendingAcksConsumer removedEntryCallback) {
            PendingAcksMap.PendingAcksRemoveHandler pendingAcksRemoveHandler =
                    pendingAcksRemoveHandlerSupplier.get();
            boolean acquiredWriteLock = false;
            boolean batchStarted = false;
            boolean retryWithWriteLock = false;
            try {
                if (useWriteLock) {
                    writeLock.lock();
                    acquiredWriteLock = true;
                } else {
                    readLock.lock();
                }
                ObjectBidirectionalIterator<Long2ObjectMap.Entry<Long2ObjectSortedMap<IntIntPair>>> ledgerIterator =
                        pendingAcks.headMap(markDeleteLedgerId + 1).long2ObjectEntrySet().iterator();
                while (ledgerIterator.hasNext()) {
                    Long2ObjectMap.Entry<Long2ObjectSortedMap<IntIntPair>> entry = ledgerIterator.next();
                    long ledgerId = entry.getLongKey();
                    Long2ObjectSortedMap<IntIntPair> ledgerMap = entry.getValue();
                    Long2ObjectSortedMap<IntIntPair> ledgerMapHead =
                            ledgerId == markDeleteLedgerId ? ledgerMap.headMap(markDeleteEntryId + 1) : ledgerMap;
                    ObjectBidirectionalIterator<Long2ObjectMap.Entry<IntIntPair>> entryIterator =
                            ledgerMapHead.long2ObjectEntrySet().iterator();
                    while (entryIterator.hasNext()) {
                        Long2ObjectMap.Entry<IntIntPair> pendingAckEntry = entryIterator.next();
                        long entryId = pendingAckEntry.getLongKey();
                        if (!acquiredWriteLock) {
                            retryWithWriteLock = true;
                            return;
                        }
                        IntIntPair value = pendingAckEntry.getValue();
                        int remainingUnacked = value.leftInt();
                        int stickyKeyHash = value.rightInt();
                        if (pendingAcksRemoveHandler != null) {
                            if (!batchStarted) {
                                pendingAcksRemoveHandler.startBatch();
                                batchStarted = true;
                            }
                            pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash,
                                    closed);
                        }
                        if (removedEntryCallback != null) {
                            removedEntryCallback.accept(ledgerId, entryId, remainingUnacked, stickyKeyHash);
                        }
                        entryIterator.remove();
                        size--;
                    }
                    if (ledgerMap.isEmpty()) {
                        if (!acquiredWriteLock) {
                            retryWithWriteLock = true;
                            return;
                        }
                        ledgerIterator.remove();
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

        private void handleRemovePendingAck(long ledgerId, long entryId, int stickyKeyHash) {
            PendingAcksMap.PendingAcksRemoveHandler pendingAcksRemoveHandler =
                    pendingAcksRemoveHandlerSupplier.get();
            if (pendingAcksRemoveHandler != null) {
                pendingAcksRemoveHandler.handleRemoving(consumer, ledgerId, entryId, stickyKeyHash, closed);
            }
        }
    }

    private static final class CurrentPendingAcksMapStore implements PendingAcksStore {
        private final PendingAcksMap pendingAcks = new PendingAcksMap(null, () -> null, () -> null);
        private long forEachSum;
        private long removedCount;
        private final PendingAcksMap.PendingAcksConsumer pendingAckScanner =
                (ledgerId, entryId, remainingUnacked, stickyKeyHash) ->
                        forEachSum += ledgerId + entryId + remainingUnacked + stickyKeyHash;
        private final PendingAcksMap.PendingAcksConsumer removedCounter =
                (ledgerId, entryId, remainingUnacked, stickyKeyHash) -> removedCount++;

        @Override
        public boolean addOrReplace(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            return pendingAcks.addPendingAckIfAllowed(ledgerId, entryId, remainingUnacked, stickyKeyHash);
        }

        @Override
        public boolean contains(long ledgerId, long entryId) {
            return pendingAcks.contains(ledgerId, entryId);
        }

        @Override
        public int getRemainingUnacked(long ledgerId, long entryId) {
            return pendingAcks.getRemainingUnacked(ledgerId, entryId);
        }

        @Override
        public boolean remove(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            return pendingAcks.remove(ledgerId, entryId, remainingUnacked, stickyKeyHash);
        }

        @Override
        public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
            return pendingAcks.removeAndGetRemainingUnacked(ledgerId, entryId);
        }

        @Override
        public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
            return pendingAcks.updateRemainingUnacked(ledgerId, entryId, ackedDelta);
        }

        @Override
        public long forEachScan() {
            forEachSum = 0;
            pendingAcks.forEach(pendingAckScanner);
            return forEachSum;
        }

        @Override
        public long removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
            removedCount = 0;
            pendingAcks.removeAllUpTo(markDeleteLedgerId, markDeleteEntryId, removedCounter);
            return removedCount;
        }
    }

    private static void populate(PendingAcksStore store, int entries, int ledgers, long[] ledgerIds, long[] entryIds) {
        int index = 0;
        for (int ledger = 0; ledger < ledgers; ledger++) {
            int entriesInLedger = entries / ledgers + (ledger < entries % ledgers ? 1 : 0);
            for (int entry = 0; entry < entriesInLedger; entry++) {
                if (ledgerIds != null) {
                    ledgerIds[index] = ledger;
                    entryIds[index] = entry;
                }
                store.addOrReplace(ledger, entry, remainingUnacked(index), stickyKeyHash(index));
                index++;
            }
        }
    }

    private static int remainingUnacked(int index) {
        return 1_000_000_000 - (index & 1023);
    }

    private static int stickyKeyHash(int index) {
        return index * 0x9E3779B9;
    }
}
