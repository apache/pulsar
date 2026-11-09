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

import it.unimi.dsi.fastutil.longs.Long2LongAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares the per-ledger structures of a consumer's pending acks: the AVL tree that {@link PendingAcksMap} used
 * before, {@code Long2LongAVLTreeMap}, and {@link PendingAcksLedgerMap}, in the patterns of dispatch and acks. It
 * excludes {@link PendingAcksMap}'s lock and outer ledger map, which are the same for both and which
 * {@link PendingAckMapCycleBenchmark} includes.
 *
 * <p>The window is the number of pending acks. Every operation that removes an entry adds a new one after the others,
 * as a dispatch does, so that the window stays the same. The results are per entry, except for {@code traverse},
 * which is per traversal of the window.
 *
 * <p>{@link PendingAcksMap} reuses an emptied ledger map, where it created a new AVL tree before:
 * {@code oneEntryAtATime} does the same with each structure.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class PendingAcksLedgerMapBenchmark {
    // the entries that an ack command acknowledges together, and that a mark-delete removes together
    private static final int GROUP = 64;
    // one in this many acked entries is redelivered: removed and added again later, out of order
    private static final int REDELIVERY_INTERVAL = 16;
    // a redelivered entry is added again this many acks later
    private static final int REDELIVERY_DELAY = 64;
    // the number of maps in tinyMaps, each with a window of TINY_WINDOW entries
    private static final int TINY_MAPS = 8192;
    private static final int TINY_WINDOW = 8;
    // the length of the precomputed sequence of random indexes that ackShuffled acks
    private static final int RANDOM_INDEXES = 1 << 20;

    /** The operations of a ledger's pending acks that the patterns use. */
    interface LedgerMap {
        long get(long entryId);

        void put(long entryId, long value);

        long remove(long entryId);

        /** Removes the entries up to the entry ID, inclusive, as a mark-delete does; returns their count. */
        int removeUpTo(long entryId);

        void forEach(Blackhole blackhole);
    }

    /** The previous structure, as {@link PendingAcksMap} used it. */
    static final class AvlTreeLedgerMap implements LedgerMap {
        private final Long2LongAVLTreeMap map = new Long2LongAVLTreeMap();

        AvlTreeLedgerMap() {
            map.defaultReturnValue(PendingAckValues.PACKED_NOT_FOUND);
        }

        @Override
        public long get(long entryId) {
            return map.get(entryId);
        }

        @Override
        public void put(long entryId, long value) {
            map.put(entryId, value);
        }

        @Override
        public long remove(long entryId) {
            return map.remove(entryId);
        }

        @Override
        public int removeUpTo(long entryId) {
            int removed = 0;
            ObjectBidirectionalIterator<Long2LongMap.Entry> iterator =
                    map.headMap(entryId + 1).long2LongEntrySet().iterator();
            while (iterator.hasNext()) {
                iterator.next();
                iterator.remove();
                removed++;
            }
            return removed;
        }

        @Override
        public void forEach(Blackhole blackhole) {
            for (Long2LongMap.Entry entry : map.long2LongEntrySet()) {
                blackhole.consume(entry.getLongKey());
                blackhole.consume(entry.getLongValue());
            }
        }
    }

    /** The new structure, as {@link PendingAcksMap} uses it. */
    static final class BlockListLedgerMap implements LedgerMap {
        private final PendingAcksLedgerMap map = new PendingAcksLedgerMap();

        @Override
        public long get(long entryId) {
            return map.get(entryId);
        }

        @Override
        public void put(long entryId, long value) {
            map.put(entryId, value);
        }

        @Override
        public long remove(long entryId) {
            return map.remove(entryId);
        }

        @Override
        public int removeUpTo(long entryId) {
            // as PendingAcksMap.removeAllUpTo does, which tolerates callbacks that change the map
            int removed = 0;
            long next = map.hasEntriesUpTo(entryId) ? map.firstEntryId() : PendingAcksLedgerMap.NO_ENTRY;
            while (next != PendingAcksLedgerMap.NO_ENTRY && next <= entryId) {
                if (!PendingAckValues.isNotFound(map.get(next))) {
                    map.remove(next);
                    removed++;
                }
                next = map.higherEntryId(next);
            }
            return removed;
        }

        @Override
        public void forEach(Blackhole blackhole) {
            map.forEach((entryId, value) -> {
                blackhole.consume(entryId);
                blackhole.consume(value);
            });
        }
    }

    @Param({"AVL_TREE", "BLOCK_LIST"})
    public String implementation;

    @Param({"1024", "65536"})
    public int window;

    private final long packedValue = PendingAckValues.pack(1, 0);

    private LedgerMap map;
    // the window's entry IDs; an operation acks the entry at an index and replaces it with a new one
    private long[] entryIds;
    private long nextEntryId;
    private int cursor;
    // the negatively acked entry IDs, added again REDELIVERY_DELAY acks after their removal
    private long[] negativelyAcked;
    // the redelivered entry IDs, acked REDELIVERY_DELAY acks after they were added again
    private long[] redelivered;
    private int acks;

    private LedgerMap[] tinyMaps;
    private long[] tinyNextEntryIds;
    // the index of a random entry of the window, for each ack of ackShuffled
    private int[] randomIndexes;
    // the emptied map that oneEntryAtATime reuses with the block list
    private LedgerMap emptyMap;
    private long nextSingleEntryId;

    private LedgerMap newLedgerMap() {
        return "AVL_TREE".equals(implementation) ? new AvlTreeLedgerMap() : new BlockListLedgerMap();
    }

    @Setup(Level.Trial)
    public void setup() {
        map = newLedgerMap();
        entryIds = new long[window];
        for (int i = 0; i < window; i++) {
            entryIds[i] = i;
            map.put(i, packedValue);
        }
        nextEntryId = window;
        negativelyAcked = new long[REDELIVERY_DELAY];
        Arrays.fill(negativelyAcked, -1);
        redelivered = new long[REDELIVERY_DELAY];
        Arrays.fill(redelivered, -1);
        tinyMaps = new LedgerMap[TINY_MAPS];
        tinyNextEntryIds = new long[TINY_MAPS];
        for (int m = 0; m < TINY_MAPS; m++) {
            tinyMaps[m] = newLedgerMap();
            for (int i = 0; i < TINY_WINDOW; i++) {
                tinyMaps[m].put(i, packedValue);
            }
            tinyNextEntryIds[m] = TINY_WINDOW;
        }
        SplittableRandom random = new SplittableRandom(1);
        randomIndexes = new int[RANDOM_INDEXES];
        for (int i = 0; i < RANDOM_INDEXES; i++) {
            randomIndexes[i] = random.nextInt(window);
        }
        emptyMap = newLedgerMap();
    }

    /** The index of the next entry to ack: in order, or a random one of the window. */
    private int nextIndex(boolean shuffled) {
        int next = cursor++;
        return shuffled ? randomIndexes[next & (RANDOM_INDEXES - 1)] : next & (window - 1);
    }

    // Acks the entry at the index: looks it up and removes it, and dispatches a new entry in its place
    private long ackAndReplace(int index) {
        long entryId = entryIds[index];
        long value = map.get(entryId);
        map.remove(entryId);
        long added = nextEntryId++;
        map.put(added, packedValue);
        entryIds[index] = added;
        return value;
    }

    /** Acks arrive in the order of the entries, the common case: lookup, removal and a new entry. */
    @Benchmark
    public long ackInOrder() {
        return ackAndReplace(nextIndex(false));
    }

    /**
     * Acks arrive in a random order over the window, as when a consumer processes messages at different speeds: each
     * ack is for a random entry of the window, which is replaced by a new one.
     */
    @Benchmark
    public long ackShuffled() {
        return ackAndReplace(nextIndex(true));
    }

    /**
     * An ack command acknowledges a group of entries in order: they are all looked up before any of them is removed
     * after the ack is persisted, and new entries are dispatched after that.
     */
    @Benchmark
    @OperationsPerInvocation(GROUP)
    public long ackGroupInOrder() {
        int first = cursor;
        cursor += GROUP;
        long sum = 0;
        for (int i = 0; i < GROUP; i++) {
            sum += map.get(entryIds[(first + i) & (window - 1)]);
        }
        for (int i = 0; i < GROUP; i++) {
            map.remove(entryIds[(first + i) & (window - 1)]);
        }
        for (int i = 0; i < GROUP; i++) {
            long added = nextEntryId++;
            map.put(added, packedValue);
            entryIds[(first + i) & (window - 1)] = added;
        }
        return sum;
    }

    /**
     * Acks in order, where one in {@value #REDELIVERY_INTERVAL} entries is negatively acked: it is removed, added again
     * {@value #REDELIVERY_DELAY} acks later, older than the others, and acked another {@value #REDELIVERY_DELAY} acks
     * later. The result is per ack of a dispatched entry, including the redeliveries' share.
     */
    @Benchmark
    public long ackInOrderWithRedeliveries() {
        int slot = acks++ & (REDELIVERY_DELAY - 1);
        long value = 0;
        if (redelivered[slot] >= 0) {
            // the redelivered entry is acked
            value += map.get(redelivered[slot]);
            map.remove(redelivered[slot]);
            redelivered[slot] = -1;
        }
        if (negativelyAcked[slot] >= 0) {
            // the negatively acked entry is dispatched again, out of order
            map.put(negativelyAcked[slot], packedValue);
            redelivered[slot] = negativelyAcked[slot];
            negativelyAcked[slot] = -1;
        }
        int index = nextIndex(false);
        long entryId = entryIds[index];
        value += map.get(entryId);
        map.remove(entryId);
        if (acks % REDELIVERY_INTERVAL == 0) {
            // negatively acked instead
            negativelyAcked[slot] = entryId;
        }
        long added = nextEntryId++;
        map.put(added, packedValue);
        entryIds[index] = added;
        return value;
    }

    /** A mark-delete removes the oldest entries up to a position, and new entries are dispatched. */
    @Benchmark
    @OperationsPerInvocation(GROUP)
    public int removeUpToMarkDelete() {
        long upTo = nextEntryId - window + GROUP - 1;
        int removed = map.removeUpTo(upTo);
        for (int i = 0; i < GROUP; i++) {
            map.put(nextEntryId++, packedValue);
        }
        return removed;
    }

    /** A traversal of the window, as a redelivery of all pending acks or a consumer's close does. */
    @Benchmark
    public void traverse(Blackhole blackhole) {
        map.forEach(blackhole);
    }

    /**
     * A consumer whose pending acks drop to none between entries, as on a topic with a low rate: the ledger's map is
     * removed when it empties, and a map is taken for the next entry. {@link PendingAcksMap} created a new AVL tree
     * for it, and reuses the emptied block list.
     */
    @Benchmark
    public long oneEntryAtATime() {
        LedgerMap single = "AVL_TREE".equals(implementation) ? new AvlTreeLedgerMap() : emptyMap;
        long entryId = nextSingleEntryId++;
        single.put(entryId, packedValue);
        long value = single.get(entryId);
        single.remove(entryId);
        return value;
    }

    /** Many consumers with a few pending acks each, as with many slow or idle consumers: an ack on a random one. */
    @Benchmark
    public long tinyMaps() {
        int m = (cursor++ * 4051) & (TINY_MAPS - 1);
        LedgerMap tiny = tinyMaps[m];
        long next = tinyNextEntryIds[m]++;
        long entryId = next - TINY_WINDOW;
        long value = tiny.get(entryId);
        tiny.remove(entryId);
        tiny.put(next, packedValue);
        return value;
    }
}
