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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.common.util.collections.IntIntPair;
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
import org.openjdk.jmh.infra.Blackhole;

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
public class PendingAcksMapBenchmark {
    private static final int PENDING_ACK_NOT_FOUND = -1;

    @Benchmark
    public int getRemainingUnackedHit(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        return state.store.getRemainingUnacked(state.ledgerIds[index], state.entryIds[index]);
    }

    @Benchmark
    public boolean containsHit(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        return state.store.contains(state.ledgerIds[index], state.entryIds[index]);
    }

    @Benchmark
    public boolean addOrReplace(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        return state.store.addPendingAckIfAllowed(state.ledgerIds[index], state.entryIds[index],
                remaining(index), stickyKeyHash(index));
    }

    @Benchmark
    public boolean updateRemainingUnacked(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        return state.store.updateRemainingUnacked(state.ledgerIds[index], state.entryIds[index], 1);
    }

    @Benchmark
    public int removeAndAddRemaining(MapState state, CursorState cursor) {
        int index = cursor.next(state.entries);
        long ledgerId = state.ledgerIds[index];
        long entryId = state.entryIds[index];
        int removed = state.store.removeAndGetRemainingUnacked(ledgerId, entryId);
        state.store.addPendingAckIfAllowed(ledgerId, entryId,
                removed == PENDING_ACK_NOT_FOUND ? remaining(index) : removed, stickyKeyHash(index));
        return removed;
    }

    @Benchmark
    public long forEachAll(MapState state) {
        return state.store.forEachAll();
    }

    @Benchmark
    public long removeAllUpTo(RangeState state) {
        return state.store.removeAllUpTo(state.markDeleteLedgerId, state.markDeleteEntryId);
    }

    @Benchmark
    public long removeAllUpToSameLedger(CleanupState state) {
        return state.store.removeAllUpTo(state.markDeleteLedgerId, state.markDeleteEntryId);
    }

    @Benchmark
    public void populate(PopulateState state, Blackhole blackhole) {
        PendingAckStore store = createStore(state.implementation);
        populate(store, state.parsedDataset, null, null);
        blackhole.consume(store);
    }

    @Benchmark
    public long dispatchAndAckCycle(RollingWindowState state) {
        return state.dispatchAndAckCycle();
    }

    @Benchmark
    public long dispatchAckAndPartialAckCycle(RollingWindowState state) {
        long result = state.dispatchAndAckCycle();
        if (state.shouldApplyPartialAck()) {
            result += state.applyPartialAck();
        }
        return result;
    }

    @State(Scope.Benchmark)
    public static class MapState {
        @Param({"oldProduction", "production"})
        private String implementation;

        @Param({"receiverQueue1kEntries1Ledger", "batchedReceiverQueue100Entries1Ledger",
                "defaultUnacked50kEntries1Ledger", "defaultUnacked50kEntries2Ledgers",
                "defaultUnacked50kEntries5Ledgers", "defaultUnacked50kEntries10Ledgers",
                "defaultUnacked50kEntries20Ledgers", "residual1kEntries5Ledgers", "residual1kEntries100Ledgers",
                "64kEntries1kLedgers", "1mEntries16kLedgers"})
        private String dataset;

        private PendingAckStore store;
        private long[] ledgerIds;
        private long[] entryIds;
        private int entries;
        private int ledgers;

        @Setup(Level.Trial)
        public void setup() {
            Dataset parsedDataset = Dataset.from(dataset);
            entries = parsedDataset.entries;
            ledgers = parsedDataset.ledgers;
            ledgerIds = new long[entries];
            entryIds = new long[entries];
            store = createStore(implementation);
            populate(store, parsedDataset, ledgerIds, entryIds);
        }
    }

    @State(Scope.Thread)
    public static class RangeState {
        @Param({"oldProduction", "production"})
        private String implementation;

        @Param({"receiverQueue1kEntries1Ledger", "batchedReceiverQueue100Entries1Ledger",
                "defaultUnacked50kEntries1Ledger", "defaultUnacked50kEntries2Ledgers",
                "defaultUnacked50kEntries5Ledgers", "defaultUnacked50kEntries10Ledgers",
                "defaultUnacked50kEntries20Ledgers", "residual1kEntries5Ledgers", "residual1kEntries100Ledgers",
                "64kEntries1kLedgers", "1mEntries16kLedgers"})
        private String dataset;

        private PendingAckStore store;
        private long markDeleteLedgerId;
        private long markDeleteEntryId;

        @Setup(Level.Invocation)
        public void setup() {
            Dataset parsedDataset = Dataset.from(dataset);
            store = createStore(implementation);
            populate(store, parsedDataset, null, null);
            markDeleteLedgerId = parsedDataset.ledgers / 2L;
            markDeleteEntryId = parsedDataset.entriesInLedger(markDeleteLedgerId) / 2L;
        }
    }

    @State(Scope.Thread)
    public static class PopulateState {
        @Param({"oldProduction", "production"})
        private String implementation;

        @Param({"receiverQueue1kEntries1Ledger", "batchedReceiverQueue100Entries1Ledger",
                "defaultUnacked50kEntries1Ledger", "defaultUnacked50kEntries2Ledgers",
                "defaultUnacked50kEntries5Ledgers", "defaultUnacked50kEntries10Ledgers",
                "defaultUnacked50kEntries20Ledgers", "residual1kEntries5Ledgers", "residual1kEntries100Ledgers",
                "64kEntries1kLedgers", "1mEntries16kLedgers"})
        private String dataset;

        private Dataset parsedDataset;

        @Setup(Level.Trial)
        public void setup() {
            parsedDataset = Dataset.from(dataset);
        }
    }

    @State(Scope.Thread)
    public static class CleanupState {
        @Param({"oldProduction", "production"})
        private String implementation;

        @Param({"receiverQueue1kEntries1Ledger", "defaultUnacked50kEntries1Ledger"})
        private String dataset;

        @Param({"beforePendingWindow", "smallPrefix"})
        private String scenario;

        private PendingAckStore store;
        private long markDeleteLedgerId;
        private long markDeleteEntryId;

        @Setup(Level.Invocation)
        public void setup() {
            Dataset parsedDataset = Dataset.from(dataset);
            int prefixEntries = Math.max(1, parsedDataset.entries / 50);
            store = createStore(implementation);
            markDeleteLedgerId = 0;
            switch (scenario) {
                case "beforePendingWindow" -> {
                    populateSingleLedger(store, prefixEntries, parsedDataset.entries);
                    markDeleteEntryId = prefixEntries - 1L;
                }
                case "smallPrefix" -> {
                    populateSingleLedger(store, 0, parsedDataset.entries);
                    markDeleteEntryId = prefixEntries - 1L;
                }
                default -> throw new IllegalArgumentException("Unknown cleanup scenario: " + scenario);
            }
        }
    }

    @State(Scope.Thread)
    public static class RollingWindowState {
        private static final int PARTIAL_ACK_INTERVAL = 16;

        @Param({"oldProduction", "production"})
        private String implementation;

        @Param({"receiverQueue1kEntries1Ledger", "batchedReceiverQueue100Entries1Ledger",
                "defaultUnacked50kEntries1Ledger", "defaultUnacked50kEntries2Ledgers",
                "defaultUnacked50kEntries5Ledgers", "defaultUnacked50kEntries10Ledgers",
                "defaultUnacked50kEntries20Ledgers"})
        private String dataset;

        private PendingAckStore store;
        private long[] ledgerIds;
        private long[] entryIds;
        private int[] remainingUnacked;
        private int entries;
        private long entriesPerLedger;
        private int cursor;
        private long nextSequence;
        private long operations;

        @Setup(Level.Trial)
        public void setup() {
            Dataset parsedDataset = Dataset.from(dataset);
            entries = parsedDataset.entries;
            entriesPerLedger = Math.max(1, (entries + parsedDataset.ledgers - 1L) / parsedDataset.ledgers);
            ledgerIds = new long[entries];
            entryIds = new long[entries];
            remainingUnacked = new int[entries];
            store = createStore(implementation);
            for (int i = 0; i < entries; i++) {
                setSlot(i, i);
                store.addPendingAckIfAllowed(ledgerIds[i], entryIds[i], remainingUnacked[i], stickyKeyHash(i));
            }
            nextSequence = entries;
        }

        private long dispatchAndAckCycle() {
            int slot = cursor;
            cursor = nextCursor(slot);

            long ledgerIdToAck = ledgerIds[slot];
            long entryIdToAck = entryIds[slot];
            int removed = store.removeAndGetRemainingUnacked(ledgerIdToAck, entryIdToAck);

            long sequence = nextSequence++;
            setSlot(slot, sequence);
            store.addPendingAckIfAllowed(ledgerIds[slot], entryIds[slot],
                    remainingUnacked[slot], stickyKeyHash(sequence));
            return removed + remainingUnacked[slot];
        }

        private boolean shouldApplyPartialAck() {
            return (operations++ & (PARTIAL_ACK_INTERVAL - 1)) == 0;
        }

        private long applyPartialAck() {
            int slot = cursor + entries / 2;
            if (slot >= entries) {
                slot -= entries;
            }
            int remaining = remainingUnacked[slot];
            if (remaining <= 1) {
                return 0;
            }
            boolean updated = store.updateRemainingUnacked(ledgerIds[slot], entryIds[slot], 1);
            if (!updated) {
                return 0;
            }
            remainingUnacked[slot] = remaining - 1;
            return remaining;
        }

        private void setSlot(int slot, long sequence) {
            ledgerIds[slot] = sequence / entriesPerLedger;
            entryIds[slot] = sequence % entriesPerLedger;
            remainingUnacked[slot] = remaining(sequence);
        }

        private int nextCursor(int current) {
            int next = current + 1;
            return next == entries ? 0 : next;
        }
    }

    @State(Scope.Thread)
    public static class CursorState {
        private int index;

        private int next(int entries) {
            int current = index;
            int next = current + 1;
            index = next == entries ? 0 : next;
            return current;
        }
    }

    private enum Dataset {
        RECEIVER_QUEUE_1K_ENTRIES_1_LEDGER("receiverQueue1kEntries1Ledger", 1_000, 1),
        BATCHED_RECEIVER_QUEUE_100_ENTRIES_1_LEDGER("batchedReceiverQueue100Entries1Ledger", 100, 1),
        DEFAULT_UNACKED_50K_ENTRIES_1_LEDGER("defaultUnacked50kEntries1Ledger", 50_000, 1),
        DEFAULT_UNACKED_50K_ENTRIES_2_LEDGERS("defaultUnacked50kEntries2Ledgers", 50_000, 2),
        DEFAULT_UNACKED_50K_ENTRIES_5_LEDGERS("defaultUnacked50kEntries5Ledgers", 50_000, 5),
        DEFAULT_UNACKED_50K_ENTRIES_10_LEDGERS("defaultUnacked50kEntries10Ledgers", 50_000, 10),
        DEFAULT_UNACKED_50K_ENTRIES_20_LEDGERS("defaultUnacked50kEntries20Ledgers", 50_000, 20),
        RESIDUAL_1K_ENTRIES_5_LEDGERS("residual1kEntries5Ledgers", 1_000, 5),
        RESIDUAL_1K_ENTRIES_100_LEDGERS("residual1kEntries100Ledgers", 1_000, 100),
        ENTRIES_64K_LEDGERS_1K("64kEntries1kLedgers", 65_536, 1_024),
        ENTRIES_1M_LEDGERS_16K("1mEntries16kLedgers", 1_048_576, 16_384);

        private final String name;
        private final int entries;
        private final int ledgers;

        Dataset(String name, int entries, int ledgers) {
            this.name = name;
            this.entries = entries;
            this.ledgers = ledgers;
        }

        private static Dataset from(String name) {
            for (Dataset dataset : values()) {
                if (dataset.name.equals(name)) {
                    return dataset;
                }
            }
            throw new IllegalArgumentException("Unknown dataset: " + name);
        }

        private int entriesInLedger(long ledgerId) {
            int baseEntries = entries / ledgers;
            int extraEntries = entries % ledgers;
            return baseEntries + (ledgerId < extraEntries ? 1 : 0);
        }
    }

    private interface PendingAckStore {
        boolean addPendingAckIfAllowed(long ledgerId, long entryId, int remainingUnacked, int stickyKeyHash);

        boolean contains(long ledgerId, long entryId);

        int getRemainingUnacked(long ledgerId, long entryId);

        boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta);

        int removeAndGetRemainingUnacked(long ledgerId, long entryId);

        long forEachAll();

        long removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId);
    }

    private static PendingAckStore createStore(String implementation) {
        return switch (implementation) {
            case "oldProduction" -> new OldProductionPendingAckStore();
            case "production" -> new ProductionPendingAckStore();
            default -> throw new IllegalArgumentException("Unknown implementation: " + implementation);
        };
    }

    private static void populate(PendingAckStore store, Dataset dataset,
                                 long[] ledgerIds, long[] entryIds) {
        int index = 0;
        // Managed ledger entries are appended sequentially inside one ledger before rollover.
        for (long ledgerId = 0; ledgerId < dataset.ledgers; ledgerId++) {
            int entriesInLedger = dataset.entriesInLedger(ledgerId);
            for (long entryId = 0; entryId < entriesInLedger; entryId++) {
                if (ledgerIds != null) {
                    ledgerIds[index] = ledgerId;
                    entryIds[index] = entryId;
                }
                store.addPendingAckIfAllowed(ledgerId, entryId, remaining(index), stickyKeyHash(index));
                index++;
            }
        }
    }

    private static void populateSingleLedger(PendingAckStore store, long firstEntryId, int entries) {
        for (int index = 0; index < entries; index++) {
            long entryId = firstEntryId + index;
            store.addPendingAckIfAllowed(0, entryId, remaining(index), stickyKeyHash(index));
        }
    }

    private static int remaining(long index) {
        return (int) (index & 15) + 1;
    }

    private static int stickyKeyHash(long index) {
        return (int) (index * 31);
    }

    private static final class ProductionPendingAckStore implements PendingAckStore {
        private final PendingAcksMap pendingAcks = new PendingAcksMap(null, () -> null, () -> null);
        private long total;
        private final PendingAcksMap.PendingAcksConsumer sumAllConsumer =
                (ledgerId, entryId, remainingUnacked, stickyKeyHash) -> total += remainingUnacked + stickyKeyHash;
        private final PendingAcksMap.PendingAcksConsumer sumRemainingConsumer =
                (ledgerId, entryId, remainingUnacked, stickyKeyHash) -> total += remainingUnacked;

        @Override
        public boolean addPendingAckIfAllowed(long ledgerId, long entryId, int remainingUnacked,
                                              int stickyKeyHash) {
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
        public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
            return pendingAcks.updateRemainingUnacked(ledgerId, entryId, ackedDelta);
        }

        @Override
        public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
            return pendingAcks.removeAndGetRemainingUnacked(ledgerId, entryId);
        }

        @Override
        public long forEachAll() {
            total = 0;
            pendingAcks.forEach(sumAllConsumer);
            return total;
        }

        @Override
        public long removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
            total = 0;
            pendingAcks.removeAllUpTo(markDeleteLedgerId, markDeleteEntryId, sumRemainingConsumer);
            return total;
        }
    }

    private static final class OldProductionPendingAckStore implements PendingAckStore {
        private final TreeMap<Long, TreeMap<Long, IntIntPair>> pendingAcks = new TreeMap<>();
        private final Lock readLock;
        private final Lock writeLock;

        private OldProductionPendingAckStore() {
            ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
            writeLock = readWriteLock.writeLock();
            readLock = readWriteLock.readLock();
        }

        @Override
        public boolean addPendingAckIfAllowed(long ledgerId, long entryId, int remainingUnacked,
                                              int stickyKeyHash) {
            try {
                writeLock.lock();
                TreeMap<Long, IntIntPair> ledgerPendingAcks =
                        pendingAcks.computeIfAbsent(ledgerId, k -> new TreeMap<>());
                ledgerPendingAcks.put(entryId, IntIntPair.of(remainingUnacked, stickyKeyHash));
                return true;
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public boolean contains(long ledgerId, long entryId) {
            try {
                readLock.lock();
                TreeMap<Long, IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                return ledgerMap != null && ledgerMap.containsKey(entryId);
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public int getRemainingUnacked(long ledgerId, long entryId) {
            try {
                readLock.lock();
                TreeMap<Long, IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                IntIntPair value = ledgerMap == null ? null : ledgerMap.get(entryId);
                return value == null ? PENDING_ACK_NOT_FOUND : value.leftInt();
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public boolean updateRemainingUnacked(long ledgerId, long entryId, int ackedDelta) {
            try {
                writeLock.lock();
                TreeMap<Long, IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
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
        public int removeAndGetRemainingUnacked(long ledgerId, long entryId) {
            try {
                writeLock.lock();
                TreeMap<Long, IntIntPair> ledgerMap = pendingAcks.get(ledgerId);
                if (ledgerMap == null) {
                    return PENDING_ACK_NOT_FOUND;
                }
                IntIntPair value = ledgerMap.remove(entryId);
                if (value == null) {
                    return PENDING_ACK_NOT_FOUND;
                }
                if (ledgerMap.isEmpty()) {
                    pendingAcks.remove(ledgerId);
                }
                return value.leftInt();
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public long forEachAll() {
            try {
                readLock.lock();
                long total = 0;
                for (Map.Entry<Long, TreeMap<Long, IntIntPair>> ledgerEntry : pendingAcks.entrySet()) {
                    TreeMap<Long, IntIntPair> ledgerPendingAcks = ledgerEntry.getValue();
                    for (IntIntPair value : ledgerPendingAcks.values()) {
                        total += value.leftInt() + value.rightInt();
                    }
                }
                return total;
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public long removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
            try {
                writeLock.lock();
                long total = 0;
                Iterator<Map.Entry<Long, TreeMap<Long, IntIntPair>>> ledgerIterator =
                        pendingAcks.headMap(markDeleteLedgerId, true).entrySet().iterator();
                while (ledgerIterator.hasNext()) {
                    Map.Entry<Long, TreeMap<Long, IntIntPair>> ledgerEntry = ledgerIterator.next();
                    long ledgerId = ledgerEntry.getKey();
                    TreeMap<Long, IntIntPair> ledgerMap = ledgerEntry.getValue();
                    if (ledgerId < markDeleteLedgerId) {
                        for (IntIntPair value : ledgerMap.values()) {
                            total += value.leftInt();
                        }
                        ledgerIterator.remove();
                    } else {
                        Iterator<Map.Entry<Long, IntIntPair>> entryIterator =
                                ledgerMap.headMap(markDeleteEntryId, true).entrySet().iterator();
                        while (entryIterator.hasNext()) {
                            total += entryIterator.next().getValue().leftInt();
                            entryIterator.remove();
                        }
                        if (ledgerMap.isEmpty()) {
                            ledgerIterator.remove();
                        }
                    }
                }
                return total;
            } finally {
                writeLock.unlock();
            }
        }
    }
}
