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
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongSortedMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import java.util.concurrent.TimeUnit;
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
    private static final long STICKY_KEY_HASH_MASK = 0xFFFF_FFFFL;
    private static final long PACKED_PENDING_ACK_NOT_FOUND = packPendingAckValueUnchecked(PENDING_ACK_NOT_FOUND, 0);

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
    public IntIntPair getPairHit(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        return state.store.get(state.ledgerIds[index], state.entryIds[index]);
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
        @Param({"FASTUTIL_OBJECT", "FASTUTIL_PACKED", "FASTUTIL_PACKED_SENTINEL"})
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
        @Param({"FASTUTIL_OBJECT", "FASTUTIL_PACKED", "FASTUTIL_PACKED_SENTINEL"})
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
        FASTUTIL_OBJECT {
            @Override
            PendingAcksStore createStore() {
                return new FastutilObjectStore();
            }
        },
        FASTUTIL_PACKED {
            @Override
            PendingAcksStore createStore() {
                return new FastutilPackedStore(false);
            }
        },
        FASTUTIL_PACKED_SENTINEL {
            @Override
            PendingAcksStore createStore() {
                return new FastutilPackedStore(true);
            }
        };

        abstract PendingAcksStore createStore();
    }

    private interface PendingAcksStore {
        boolean addOrReplace(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash);

        boolean contains(long ledgerId, long entryId);

        IntIntPair get(long ledgerId, long entryId);

        int getRemainingUnacked(long ledgerId, long entryId);

        boolean remove(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash);

        int removeAndGetRemainingUnacked(long ledgerId, long entryId);

        boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta);

        long forEachScan();

        long removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId);
    }

    private static final class FastutilObjectStore implements PendingAcksStore {
        private final Long2ObjectSortedMap<Long2ObjectSortedMap<IntIntPair>> pendingAcks =
                new Long2ObjectRBTreeMap<>();

        @Override
        public boolean addOrReplace(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            Long2ObjectSortedMap<IntIntPair> ledgerPendingAcks =
                    pendingAcks.computeIfAbsent(ledgerId, k -> new Long2ObjectRBTreeMap<>());
            ledgerPendingAcks.put(entryId, IntIntPair.of(remainingUnacked, stickyKeyHash));
            return true;
        }

        @Override
        public boolean contains(long ledgerId, long entryId) {
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            return ledgerMap != null && ledgerMap.containsKey(entryId);
        }

        @Override
        public IntIntPair get(long ledgerId, long entryId) {
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            return ledgerMap == null ? null : ledgerMap.get(entryId);
        }

        @Override
        public int getRemainingUnacked(long ledgerId, long entryId) {
            IntIntPair value = get(ledgerId, entryId);
            return value == null ? PENDING_ACK_NOT_FOUND : value.leftInt();
        }

        @Override
        public boolean remove(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            IntIntPair value = ledgerMap == null ? null : ledgerMap.get(entryId);
            if (value == null || value.leftInt() != remainingUnacked || value.rightInt() != stickyKeyHash) {
                return false;
            }
            ledgerMap.remove(entryId);
            removeLedgerIfEmpty(ledgerId, ledgerMap);
            return true;
        }

        @Override
        public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return PENDING_ACK_NOT_FOUND;
            }
            IntIntPair removed = ledgerMap.remove(entryId);
            if (removed == null) {
                return PENDING_ACK_NOT_FOUND;
            }
            removeLedgerIfEmpty(ledgerId, ledgerMap);
            return removed.leftInt();
        }

        @Override
        public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
            Long2ObjectSortedMap<IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
            IntIntPair value = ledgerMap == null ? null : ledgerMap.get(entryId);
            if (value == null) {
                return false;
            }
            int newRemaining = value.leftInt() - ackedDelta;
            if (newRemaining < 0) {
                return false;
            }
            ledgerMap.put(entryId, IntIntPair.of(newRemaining, value.rightInt()));
            return true;
        }

        @Override
        public long forEachScan() {
            long sum = 0;
            for (Long2ObjectMap.Entry<Long2ObjectSortedMap<IntIntPair>> entry : pendingAcks.long2ObjectEntrySet()) {
                long ledgerId = entry.getLongKey();
                for (Long2ObjectMap.Entry<IntIntPair> pendingAckEntry : entry.getValue().long2ObjectEntrySet()) {
                    IntIntPair value = pendingAckEntry.getValue();
                    sum += ledgerId + pendingAckEntry.getLongKey() + value.leftInt() + value.rightInt();
                }
            }
            return sum;
        }

        @Override
        public long removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
            long removed = 0;
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
                    entryIterator.next();
                    entryIterator.remove();
                    removed++;
                }
                if (ledgerMap.isEmpty()) {
                    ledgerIterator.remove();
                }
            }
            return removed;
        }

        private void removeLedgerIfEmpty(long ledgerId, Long2ObjectSortedMap<IntIntPair> ledgerMap) {
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
        }
    }

    private static final class FastutilPackedStore implements PendingAcksStore {
        private final Long2ObjectSortedMap<Long2LongSortedMap> pendingAcks = new Long2ObjectRBTreeMap<>();
        private final boolean sentinelDefault;

        private FastutilPackedStore(boolean sentinelDefault) {
            this.sentinelDefault = sentinelDefault;
        }

        @Override
        public boolean addOrReplace(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            Long2LongSortedMap ledgerPendingAcks = pendingAcks.computeIfAbsent(ledgerId, k -> newLedgerPendingAcks());
            long packedValue = packPendingAckValue(remainingUnacked, stickyKeyHash);
            ledgerPendingAcks.put(entryId, packedValue);
            return true;
        }

        @Override
        public boolean contains(long ledgerId, long entryId) {
            Long2LongSortedMap ledgerMap = pendingAcks.get(ledgerId);
            return ledgerMap != null && ledgerMap.containsKey(entryId);
        }

        @Override
        public IntIntPair get(long ledgerId, long entryId) {
            Long2LongSortedMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return null;
            }
            if (sentinelDefault) {
                long packedValue = ledgerMap.get(entryId);
                return packedValue == PACKED_PENDING_ACK_NOT_FOUND ? null : unpackPendingAckValue(packedValue);
            }
            if (!ledgerMap.containsKey(entryId)) {
                return null;
            }
            return unpackPendingAckValue(ledgerMap.get(entryId));
        }

        @Override
        public int getRemainingUnacked(long ledgerId, long entryId) {
            Long2LongSortedMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return PENDING_ACK_NOT_FOUND;
            }
            if (sentinelDefault) {
                long packedValue = ledgerMap.get(entryId);
                return packedValue == PACKED_PENDING_ACK_NOT_FOUND
                        ? PENDING_ACK_NOT_FOUND : unpackRemainingUnacked(packedValue);
            }
            if (!ledgerMap.containsKey(entryId)) {
                return PENDING_ACK_NOT_FOUND;
            }
            return unpackRemainingUnacked(ledgerMap.get(entryId));
        }

        @Override
        public boolean remove(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash) {
            if (remainingUnacked < 0) {
                return false;
            }
            Long2LongSortedMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            long expectedValue = packPendingAckValue(remainingUnacked, stickyKeyHash);
            if (sentinelDefault) {
                if (ledgerMap.get(entryId) != expectedValue) {
                    return false;
                }
            } else if (!ledgerMap.containsKey(entryId) || ledgerMap.get(entryId) != expectedValue) {
                return false;
            }
            ledgerMap.remove(entryId);
            removeLedgerIfEmpty(ledgerId, ledgerMap);
            return true;
        }

        @Override
        public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
            Long2LongSortedMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return PENDING_ACK_NOT_FOUND;
            }
            if (sentinelDefault) {
                long removed = ledgerMap.remove(entryId);
                if (removed == PACKED_PENDING_ACK_NOT_FOUND) {
                    return PENDING_ACK_NOT_FOUND;
                }
                removeLedgerIfEmpty(ledgerId, ledgerMap);
                return unpackRemainingUnacked(removed);
            }
            if (!ledgerMap.containsKey(entryId)) {
                return PENDING_ACK_NOT_FOUND;
            }
            long removed = ledgerMap.remove(entryId);
            removeLedgerIfEmpty(ledgerId, ledgerMap);
            return unpackRemainingUnacked(removed);
        }

        @Override
        public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
            Long2LongSortedMap ledgerMap = pendingAcks.get(ledgerId);
            if (ledgerMap == null) {
                return false;
            }
            long packedValue;
            if (sentinelDefault) {
                packedValue = ledgerMap.get(entryId);
                if (packedValue == PACKED_PENDING_ACK_NOT_FOUND) {
                    return false;
                }
            } else {
                if (!ledgerMap.containsKey(entryId)) {
                    return false;
                }
                packedValue = ledgerMap.get(entryId);
            }
            int newRemaining = unpackRemainingUnacked(packedValue) - ackedDelta;
            if (newRemaining < 0) {
                return false;
            }
            ledgerMap.put(entryId, packPendingAckValue(newRemaining, unpackStickyKeyHash(packedValue)));
            return true;
        }

        @Override
        public long forEachScan() {
            long sum = 0;
            for (Long2ObjectMap.Entry<Long2LongSortedMap> entry : pendingAcks.long2ObjectEntrySet()) {
                long ledgerId = entry.getLongKey();
                for (Long2LongMap.Entry pendingAckEntry : entry.getValue().long2LongEntrySet()) {
                    long packedValue = pendingAckEntry.getLongValue();
                    sum += ledgerId + pendingAckEntry.getLongKey()
                            + unpackRemainingUnacked(packedValue) + unpackStickyKeyHash(packedValue);
                }
            }
            return sum;
        }

        @Override
        public long removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
            long removed = 0;
            ObjectBidirectionalIterator<Long2ObjectMap.Entry<Long2LongSortedMap>> ledgerIterator =
                    pendingAcks.headMap(markDeleteLedgerId + 1).long2ObjectEntrySet().iterator();
            while (ledgerIterator.hasNext()) {
                Long2ObjectMap.Entry<Long2LongSortedMap> entry = ledgerIterator.next();
                long ledgerId = entry.getLongKey();
                Long2LongSortedMap ledgerMap = entry.getValue();
                Long2LongSortedMap ledgerMapHead =
                        ledgerId == markDeleteLedgerId ? ledgerMap.headMap(markDeleteEntryId + 1) : ledgerMap;
                ObjectBidirectionalIterator<Long2LongMap.Entry> entryIterator =
                        ledgerMapHead.long2LongEntrySet().iterator();
                while (entryIterator.hasNext()) {
                    entryIterator.next();
                    entryIterator.remove();
                    removed++;
                }
                if (ledgerMap.isEmpty()) {
                    ledgerIterator.remove();
                }
            }
            return removed;
        }

        private Long2LongSortedMap newLedgerPendingAcks() {
            Long2LongRBTreeMap ledgerPendingAcks = new Long2LongRBTreeMap();
            if (sentinelDefault) {
                ledgerPendingAcks.defaultReturnValue(PACKED_PENDING_ACK_NOT_FOUND);
            }
            return ledgerPendingAcks;
        }

        private void removeLedgerIfEmpty(long ledgerId, Long2LongSortedMap ledgerMap) {
            if (ledgerMap.isEmpty()) {
                pendingAcks.remove(ledgerId);
            }
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

    private static long packPendingAckValue(int remainingUnacked, int stickyKeyHash) {
        if (remainingUnacked < 0) {
            throw new IllegalArgumentException("remainingUnacked must be non-negative");
        }
        return packPendingAckValueUnchecked(remainingUnacked, stickyKeyHash);
    }

    private static long packPendingAckValueUnchecked(int remainingUnacked, int stickyKeyHash) {
        return ((long) remainingUnacked << Integer.SIZE) | (stickyKeyHash & STICKY_KEY_HASH_MASK);
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
}
