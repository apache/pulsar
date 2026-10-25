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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.testng.annotations.Test;

public class PendingAcksLedgerMapTest {

    @Test
    public void keepsEntriesInOrderWithAppendsInsertsAndRemovals() {
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        assertThat(map.put(5, value(1))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(map.put(7, value(2))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        // out of order, as a redelivery adds it
        assertThat(map.put(6, value(3))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(map.put(1, value(4))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(map.put(6, value(5))).isEqualTo(value(3));
        assertThat(entries(map)).containsExactly(1L, 5L, 6L, 7L);
        assertThat(map.get(6)).isEqualTo(value(5));
        assertThat(map.get(2)).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);

        assertThat(map.remove(5)).isEqualTo(value(1));
        assertThat(map.remove(5)).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(map.containsKey(5)).isFalse();
        // a removed entry can come back
        assertThat(map.put(5, value(6))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(entries(map)).containsExactly(1L, 5L, 6L, 7L);

        List<Long> removed = new ArrayList<>();
        assertThat(map.hasEntriesUpTo(0)).isFalse();
        assertThat(map.hasEntriesUpTo(6)).isTrue();
        while (map.hasEntriesUpTo(6)) {
            removed.add(map.firstEntryId());
            map.removeFirst();
        }
        assertThat(removed).containsExactly(1L, 5L, 6L);
        assertThat(entries(map)).containsExactly(7L);
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.remove(7)).isEqualTo(value(2));
        assertThat(map.isEmpty()).isTrue();
        assertThat(map.hasEntriesUpTo(Long.MAX_VALUE)).isFalse();
    }

    @Test
    public void findsAndRemovesEntriesAckedInOrder() {
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        int count = PendingAcksLedgerMap.BLOCK_CAPACITY * 3 + 5;
        for (int i = 0; i < count; i++) {
            map.put(i * 2, value(i));
        }
        // acked in the order they were sent: each is the first entry, across the blocks
        for (int i = 0; i < count; i++) {
            assertThat(map.get(i * 2)).isEqualTo(value(i));
            // not an entry, just after the first
            assertThat(map.get(i * 2 + 1)).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
            assertThat(map.remove(i * 2)).isEqualTo(value(i));
            assertThat(map.get(i * 2)).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
            assertThat(map.size()).isEqualTo(count - i - 1);
        }
        assertThat(map.isEmpty()).isTrue();
        assertThat(map.remove(0)).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(map.get(0)).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
    }

    @Test
    public void behavesAsASortedMap() {
        // random operations in the patterns of dispatch and acks: mostly increasing entry IDs, some redeliveries,
        // removals near the start and range removals up to a mark-delete position
        Random random = new Random(1);
        for (int round = 0; round < 50; round++) {
            PendingAcksLedgerMap map = new PendingAcksLedgerMap();
            TreeMap<Long, Long> reference = new TreeMap<>();
            long nextEntryId = 0;
            for (int step = 0; step < 5000; step++) {
                int operation = random.nextInt(100);
                if (operation < 45) {
                    long entryId = random.nextInt(10) == 0 && nextEntryId > 0
                            ? random.nextLong(nextEntryId) : nextEntryId++;
                    long v = value(random.nextInt(1000));
                    Long previous = reference.put(entryId, v);
                    assertThat(map.put(entryId, v))
                            .isEqualTo(previous != null ? previous : PendingAckValues.PACKED_NOT_FOUND);
                } else if (operation < 90) {
                    long entryId = reference.isEmpty() || random.nextInt(4) == 0
                            ? random.nextLong(nextEntryId + 1) : nthKey(reference, random.nextInt(
                                    Math.min(reference.size(), 20)));
                    Long previous = reference.remove(entryId);
                    assertThat(map.remove(entryId))
                            .isEqualTo(previous != null ? previous : PendingAckValues.PACKED_NOT_FOUND);
                } else if (operation < 95) {
                    long upTo = reference.isEmpty() ? nextEntryId : reference.firstKey() + random.nextInt(30);
                    Map<Long, Long> head = new TreeMap<>(reference.headMap(upTo, true));
                    List<Long> removedIds = new ArrayList<>();
                    List<Long> removedValues = new ArrayList<>();
                    assertThat(map.hasEntriesUpTo(upTo)).isEqualTo(!head.isEmpty());
                    while (map.hasEntriesUpTo(upTo)) {
                        removedIds.add(map.firstEntryId());
                        removedValues.add(map.firstValue());
                        map.removeFirst();
                    }
                    assertThat(removedIds).containsExactlyElementsOf(head.keySet());
                    assertThat(removedValues).containsExactlyElementsOf(head.values());
                    reference.headMap(upTo, true).clear();
                } else {
                    long entryId = random.nextLong(nextEntryId + 1);
                    Long expected = reference.get(entryId);
                    assertThat(map.get(entryId))
                            .isEqualTo(expected != null ? expected : PendingAckValues.PACKED_NOT_FOUND);
                    assertThat(map.containsKey(entryId)).isEqualTo(expected != null);
                }
                assertThat(map.size()).isEqualTo(reference.size());
                map.checkInvariants();
                if (step % 97 == 0) {
                    assertThat(contents(map)).isEqualTo(reference);
                }
            }
            List<Long> ids = new ArrayList<>();
            List<Long> values = new ArrayList<>();
            map.forEach((entryId, v) -> {
                ids.add(entryId);
                values.add(v);
            });
            assertThat(ids).containsExactlyElementsOf(reference.keySet());
            assertThat(values).containsExactlyElementsOf(reference.values());
        }
    }

    @Test(timeOut = 30_000)
    public void reinsertsARemovedPrefixInAscendingOrderWithoutCopyingTheRest() {
        // explicit redelivery removes a consumer's pending acks and dispatches them again, in the order of their
        // positions: re-adding them mustn't shift every following entry each time
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        int count = 200_000;
        for (int entryId = 0; entryId < count; entryId++) {
            map.put(entryId, value(1));
        }
        for (int entryId = 0; entryId < count / 2; entryId++) {
            assertThat(map.remove(entryId)).isEqualTo(value(1));
        }
        for (int entryId = 0; entryId < count / 2; entryId++) {
            assertThat(map.put(entryId, value(2))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        }
        assertThat(map.size()).isEqualTo(count);
        long[] expected = {0};
        map.forEach((entryId, v) -> {
            assertThat(entryId).isEqualTo(expected[0]++);
            assertThat(v).isEqualTo(entryId < count / 2 ? value(2) : value(1));
        });
    }

    @Test
    public void releasesTheMemoryOfRemovedEntries() {
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        int count = 64 * PendingAcksLedgerMap.BLOCK_CAPACITY;
        for (int entryId = 0; entryId < count; entryId++) {
            map.put(entryId, value(1));
        }
        assertThat(map.blockCount()).isEqualTo(64);
        // keep every 64th entry: the sparse blocks merge with their neighbors
        for (int entryId = 0; entryId < count; entryId++) {
            if (entryId % 64 != 0) {
                map.remove(entryId);
            }
        }
        // 128 entries, in blocks of at most half a block after the merges
        assertThat(map.size()).isEqualTo(128);
        assertThat(map.blockCount()).isLessThanOrEqualTo(4);
        for (int entryId = 0; entryId < count; entryId += 64) {
            assertThat(map.get(entryId)).isEqualTo(value(1));
        }
        while (!map.isEmpty()) {
            map.removeFirst();
        }
        assertThat(map.blockCount()).isZero();
    }

    @Test
    public void splitsAFullBlockForAnEntryInsertedInIt() {
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        for (int entryId = 0; entryId < PendingAcksLedgerMap.BLOCK_CAPACITY; entryId++) {
            map.put(entryId * 2L, value(1));
        }
        assertThat(map.blockCount()).isEqualTo(1);
        map.put(1, value(2));
        map.put(2L * PendingAcksLedgerMap.BLOCK_CAPACITY - 3, value(3));
        assertThat(map.blockCount()).isEqualTo(2);
        assertThat(map.get(1)).isEqualTo(value(2));
        assertThat(map.get(2L * PendingAcksLedgerMap.BLOCK_CAPACITY - 3)).isEqualTo(value(3));
        assertThat(map.size()).isEqualTo(PendingAcksLedgerMap.BLOCK_CAPACITY + 2);
        List<Long> ids = entries(map);
        assertThat(ids).isSorted().doesNotHaveDuplicates();
    }

    @Test
    public void insertsIntoTheUpperHalfOfAFullBlock() {
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        for (int entryId = 0; entryId < PendingAcksLedgerMap.BLOCK_CAPACITY; entryId++) {
            map.put(entryId * 2L, value(1));
        }
        // after the first block's middle, so that it goes into the new upper block of the split
        long entryId = 2L * PendingAcksLedgerMap.BLOCK_CAPACITY - 5;
        map.put(entryId, value(2));
        assertThat(map.blockCount()).isEqualTo(2);
        assertThat(map.get(entryId)).isEqualTo(value(2));
        assertThat(entries(map)).isSorted().doesNotHaveDuplicates().hasSize(PendingAcksLedgerMap.BLOCK_CAPACITY + 1);
    }

    @Test
    public void forEachContinuesAfterTheCurrentEntryWhenTheConsumerChangesTheMap() {
        // The consumer removes the current entry, removes or adds other entries before and after it, in blocks that
        // shift and split. The traversal visits the entries after the current one as they are after the consumer's
        // changes, as TreeMap.higherKey finds them
        for (int round = 0; round < 20; round++) {
            PendingAcksLedgerMap map = new PendingAcksLedgerMap();
            TreeMap<Long, Long> reference = new TreeMap<>();
            for (long entryId = 0; entryId < 1000; entryId += 2) {
                map.put(entryId, value(1));
                reference.put(entryId, value(1));
            }
            long seed = round;
            List<Long> visited = new ArrayList<>();
            map.forEach((entryId, v) -> {
                visited.add(entryId);
                changeAt(entryId, seed, (id, add) -> {
                    if (add) {
                        map.put(id, value(1));
                    } else {
                        map.remove(id);
                    }
                });
            });

            List<Long> expected = new ArrayList<>();
            for (Long entryId = reference.firstKey(); entryId != null; entryId = reference.higherKey(entryId)) {
                expected.add(entryId);
                changeAt(entryId, seed, (id, add) -> {
                    if (add) {
                        reference.put(id, value(1));
                    } else {
                        reference.remove(id);
                    }
                });
            }
            assertThat(visited).as("round %d", round).isEqualTo(expected);
            assertThat(contents(map)).as("round %d", round).isEqualTo(reference);
            map.checkInvariants();
        }
    }

    @Test
    public void forEachVisitsEveryEntryWhileTheConsumerDrainsTheMap() {
        // Removing the current entry, and every other one after it, empties blocks, which are removed, and leaves
        // sparse blocks, which merge: the traversal still visits each remaining entry once, in order
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        int count = 5 * PendingAcksLedgerMap.BLOCK_CAPACITY;
        for (long entryId = 0; entryId < count; entryId++) {
            map.put(entryId, value(1));
        }
        int blocksBefore = map.blockCount();
        List<Long> visited = new ArrayList<>();
        int[] minBlocks = {blocksBefore};
        map.forEach((entryId, v) -> {
            visited.add(entryId);
            map.remove(entryId);
            if (entryId % 64 == 0) {
                // thin out the entries ahead, which leaves sparse blocks that merge
                for (long ahead = entryId + 2; ahead < entryId + 64; ahead += 2) {
                    map.remove(ahead);
                }
            }
            minBlocks[0] = Math.min(minBlocks[0], map.blockCount());
        });

        List<Long> expected = new ArrayList<>();
        for (long entryId = 0; entryId < count; entryId++) {
            if (entryId % 64 == 0 || entryId % 2 == 1) {
                expected.add(entryId);
            }
        }
        assertThat(visited).isEqualTo(expected);
        assertThat(map.isEmpty()).isTrue();
        assertThat(map.blockCount()).isZero();
        assertThat(minBlocks[0]).isLessThan(blocksBefore);
        map.checkInvariants();
    }

    @Test
    public void startsWithASmallFirstBlockThatGrows() {
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        map.put(0, value(1));
        assertThat(map.blockCapacity(0)).isEqualTo(PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY);
        for (long entryId = 1; entryId <= PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY; entryId++) {
            map.put(entryId, value(1));
        }
        assertThat(map.blockCapacity(0)).isEqualTo(2 * PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY);
        for (long entryId = PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY + 1;
             entryId < PendingAcksLedgerMap.BLOCK_CAPACITY; entryId++) {
            map.put(entryId, value(1));
        }
        // one block of the full capacity, and then a new one of the full capacity
        assertThat(map.blockCount()).isEqualTo(1);
        assertThat(map.blockCapacity(0)).isEqualTo(PendingAcksLedgerMap.BLOCK_CAPACITY);
        map.put(PendingAcksLedgerMap.BLOCK_CAPACITY, value(1));
        assertThat(map.blockCount()).isEqualTo(2);
        assertThat(map.blockCapacity(1)).isEqualTo(PendingAcksLedgerMap.BLOCK_CAPACITY);
        map.checkInvariants();
    }

    @Test
    public void growsASmallFullBlockForAnEntryInsertedInIt() {
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        for (long entryId = 0; entryId < 2L * PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY; entryId += 2) {
            map.put(entryId, value(1));
        }
        assertThat(map.blockCapacity(0)).isEqualTo(PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY);
        // before the first entry, and between two entries
        map.put(-1, value(2));
        map.put(5, value(3));
        assertThat(map.blockCount()).isEqualTo(1);
        assertThat(map.blockCapacity(0)).isEqualTo(2 * PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY);
        assertThat(map.firstEntryId()).isEqualTo(-1);
        assertThat(map.get(-1)).isEqualTo(value(2));
        assertThat(map.get(5)).isEqualTo(value(3));
        assertThat(entries(map)).isSorted().doesNotHaveDuplicates()
                .hasSize(PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY + 2);
        map.checkInvariants();
    }

    @Test
    public void reusesTheLastEmptiedBlockForAWindowThatMoves() {
        // a consumer's window of pending acks moving through a ledger: acks remove the oldest entries and dispatches
        // append new ones
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        int window = 4 * PendingAcksLedgerMap.BLOCK_CAPACITY;
        for (long entryId = 0; entryId < window; entryId++) {
            map.put(entryId, value(1));
        }
        int allocated = map.allocatedBlocks();
        for (long entryId = 0; entryId < 100L * PendingAcksLedgerMap.BLOCK_CAPACITY; entryId++) {
            assertThat(map.remove(entryId)).isEqualTo(value(1));
            map.put(entryId + window, value(1));
        }
        // a block more than the window's, while the oldest block still has entries when a new one starts
        assertThat(map.allocatedBlocks()).isLessThanOrEqualTo(allocated + 1);
        assertThat(map.size()).isEqualTo(window);
        map.checkInvariants();
        // an empty map keeps a spare block of the smallest capacity only, which it starts again with
        while (!map.isEmpty()) {
            map.removeFirst();
            map.checkInvariants();
        }
        int allocatedWhenEmpty = map.allocatedBlocks();
        map.put(0, value(1));
        assertThat(map.blockCapacity(0)).isEqualTo(PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY);
        assertThat(map.allocatedBlocks()).isEqualTo(allocatedWhenEmpty);
        map.checkInvariants();
    }

    @Test
    public void shrinksALoneBlockAsItsEntriesAreRemoved() {
        // a map that held many entries and now holds a few keeps a small block, and no spare one
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        int count = 5 * PendingAcksLedgerMap.BLOCK_CAPACITY;
        for (long entryId = 0; entryId < count; entryId++) {
            map.put(entryId, value(1));
        }
        // ack holes: every 100th entry stays pending
        for (long entryId = 0; entryId < count; entryId++) {
            if (entryId % 100 != 0) {
                map.remove(entryId);
                map.checkInvariants();
            }
        }
        assertThat(map.size()).isEqualTo(7);
        assertThat(map.blockCount()).isEqualTo(1);
        assertThat(map.blockCapacity(0)).isEqualTo(PendingAcksLedgerMap.INITIAL_BLOCK_CAPACITY);
        assertThat(entries(map)).containsExactly(0L, 100L, 200L, 300L, 400L, 500L, 600L);
    }

    @Test
    public void staysASortedMapWithRemovalsAcrossTheWindowAndAppendsAfterThem() {
        // Removals anywhere in a large window empty, merge and shrink blocks, whose arrays are reused as spare
        // blocks for the appends, inserts and splits that follow
        Random random = new Random(2);
        for (int round = 0; round < 20; round++) {
            PendingAcksLedgerMap map = new PendingAcksLedgerMap();
            TreeMap<Long, Long> reference = new TreeMap<>();
            long nextEntryId = 0;
            for (; nextEntryId < 1000; nextEntryId++) {
                map.put(nextEntryId, value(1));
                reference.put(nextEntryId, value(1));
            }
            for (int step = 0; step < 4000; step++) {
                int operation = random.nextInt(10);
                if (operation < 5 && !reference.isEmpty()) {
                    // a removal anywhere in the window
                    long entryId = random.nextLong(nextEntryId);
                    Long previous = reference.remove(entryId);
                    assertThat(map.remove(entryId))
                            .isEqualTo(previous != null ? previous : PendingAckValues.PACKED_NOT_FOUND);
                } else if (operation < 8) {
                    long v = value(1 + random.nextInt(100));
                    reference.put(nextEntryId, v);
                    map.put(nextEntryId++, v);
                } else {
                    // a redelivery, out of order
                    long entryId = random.nextLong(nextEntryId);
                    long v = value(1 + random.nextInt(100));
                    Long previous = reference.put(entryId, v);
                    assertThat(map.put(entryId, v))
                            .isEqualTo(previous != null ? previous : PendingAckValues.PACKED_NOT_FOUND);
                }
                map.checkInvariants();
                assertThat(map.size()).isEqualTo(reference.size());
            }
            assertThat(contents(map)).as("round %d", round).isEqualTo(reference);
        }
    }

    @Test
    public void higherEntryIdFindsTheNextEntryAcrossBlocks() {
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        assertThat(map.higherEntryId(0)).isEqualTo(PendingAcksLedgerMap.NO_ENTRY);
        for (long entryId = 10; entryId < 10 + 3 * PendingAcksLedgerMap.BLOCK_CAPACITY; entryId += 2) {
            map.put(entryId, value(1));
        }
        assertThat(map.blockCount()).isGreaterThan(1);
        long last = 10 + 3 * PendingAcksLedgerMap.BLOCK_CAPACITY - 2;
        assertThat(map.higherEntryId(Long.MIN_VALUE + 1)).isEqualTo(10);
        assertThat(map.higherEntryId(10)).isEqualTo(12);
        assertThat(map.higherEntryId(11)).isEqualTo(12);
        for (long entryId = 10; entryId < last; entryId++) {
            assertThat(map.higherEntryId(entryId)).as("after %d", entryId).isEqualTo(entryId + 2 - entryId % 2);
        }
        assertThat(map.higherEntryId(last)).isEqualTo(PendingAcksLedgerMap.NO_ENTRY);
        assertThat(map.higherEntryId(Long.MAX_VALUE)).isEqualTo(PendingAcksLedgerMap.NO_ENTRY);
    }

    private interface Change {
        void apply(long entryId, boolean add);
    }

    // The changes that a consumer makes when it visits the entry, the same for the map and the reference
    private static void changeAt(long entryId, long seed, Change change) {
        Random random = new Random(seed * 1_000_003 + entryId);
        if (random.nextInt(3) == 0) {
            change.apply(entryId, false);
        }
        for (int i = random.nextInt(4); i > 0; i--) {
            change.apply(random.nextInt(1200), random.nextBoolean());
        }
    }

    private static TreeMap<Long, Long> contents(PendingAcksLedgerMap map) {
        TreeMap<Long, Long> contents = new TreeMap<>();
        map.forEach(contents::put);
        return contents;
    }

    private static long value(int remainingUnacked) {
        return PendingAckValues.pack(remainingUnacked, remainingUnacked * 31);
    }

    private static long nthKey(TreeMap<Long, Long> map, int n) {
        int i = 0;
        for (Long key : map.keySet()) {
            if (i++ == n) {
                return key;
            }
        }
        throw new IllegalArgumentException();
    }

    private static List<Long> entries(PendingAcksLedgerMap map) {
        List<Long> ids = new ArrayList<>();
        map.forEach((entryId, v) -> ids.add(entryId));
        return ids;
    }
}
