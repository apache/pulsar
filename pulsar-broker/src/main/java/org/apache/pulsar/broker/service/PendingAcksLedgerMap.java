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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * The pending acks of one ledger: a map from entry ID to a packed {@link PendingAckValues} value, in the order of the
 * entry IDs. Not thread-safe; {@link PendingAcksMap} guards it with its lock.
 *
 * <p>The entries are kept in a list of blocks, each a sorted array of up to {@value #BLOCK_CAPACITY} entries, in the
 * order of their entry IDs, with each block's entries in a window of its arrays. A consumer is sent a ledger's entries
 * mostly in increasing order and acks them mostly in the same order, so that adding an entry appends it to the last
 * block and removing one advances the first block's window; a lookup is a binary search over the blocks' first
 * entry IDs, kept in one array, and over one block's consecutive memory, without the pointer chasing of a tree. An
 * entry added out of order, as a redelivery adds it, shifts the entries of one block at most, which splits when it is
 * full. Empty blocks are dropped and a block that becomes sparse merges with its neighbor, so that the memory follows
 * the number of entries. A lone block starts small, grows as entries are added and shrinks as they are removed, so
 * that a map of a few entries stays small. While the map has more than one block, the last emptied block is kept for
 * the next one, so that a window of entries that moves through the map doesn't allocate.
 *
 * <p>The methods that read the map don't change it: {@link PendingAcksMap} calls them concurrently under its read
 * lock.
 */
final class PendingAcksLedgerMap {
    static final int BLOCK_CAPACITY = 128;
    // The smallest capacity of a block: a lone block starts with it, doubles up to BLOCK_CAPACITY as entries are added
    // and halves as they are removed
    static final int INITIAL_BLOCK_CAPACITY = 16;
    // A block with fewer entries than this merges with a neighbor when they fit in half a block together
    private static final int MERGE_THRESHOLD = BLOCK_CAPACITY / 4;

    private static final class Block {
        long[] entryIds;
        long[] values;
        // the entries are in [start, end)
        int start;
        int end;

        Block(int capacity) {
            entryIds = new long[capacity];
            values = new long[capacity];
        }

        int capacity() {
            return entryIds.length;
        }

        int size() {
            return end - start;
        }

        long firstEntryId() {
            return entryIds[start];
        }

        long lastEntryId() {
            return entryIds[end - 1];
        }

        int indexOf(long entryId) {
            return Arrays.binarySearch(entryIds, start, end, entryId);
        }

        /** Inserts at the insertion point, which keeps the order; the block's arrays mustn't be full. */
        void insertAt(int insertionPoint, long entryId, long value) {
            boolean roomBefore = start > 0;
            boolean roomAfter = end < capacity();
            if (roomBefore && (!roomAfter || insertionPoint - start < end - insertionPoint)) {
                // shift the entries before the insertion point to the left
                System.arraycopy(entryIds, start, entryIds, start - 1, insertionPoint - start);
                System.arraycopy(values, start, values, start - 1, insertionPoint - start);
                start--;
                entryIds[insertionPoint - 1] = entryId;
                values[insertionPoint - 1] = value;
            } else {
                System.arraycopy(entryIds, insertionPoint, entryIds, insertionPoint + 1, end - insertionPoint);
                System.arraycopy(values, insertionPoint, values, insertionPoint + 1, end - insertionPoint);
                end++;
                entryIds[insertionPoint] = entryId;
                values[insertionPoint] = value;
            }
        }

        void removeAt(int index) {
            if (index - start < end - 1 - index) {
                System.arraycopy(entryIds, start, entryIds, start + 1, index - start);
                System.arraycopy(values, start, values, start + 1, index - start);
                start++;
            } else {
                System.arraycopy(entryIds, index + 1, entryIds, index, end - 1 - index);
                System.arraycopy(values, index + 1, values, index, end - 1 - index);
                end--;
            }
            if (start == end) {
                start = 0;
                end = 0;
            }
        }

        /** Moves the entries to the start of the arrays. */
        void compact() {
            if (start > 0) {
                System.arraycopy(entryIds, start, entryIds, 0, size());
                System.arraycopy(values, start, values, 0, size());
                end -= start;
                start = 0;
            }
        }

        /** Grows the arrays to at least the capacity, doubling, up to {@link #BLOCK_CAPACITY}; compacts them. */
        void grow(int minCapacity) {
            int capacity = capacity();
            while (capacity < minCapacity) {
                capacity *= 2;
            }
            resize(Math.min(capacity, BLOCK_CAPACITY));
        }

        /** Moves the entries to new arrays of the capacity, which must hold them. */
        void resize(int capacity) {
            int size = size();
            entryIds = Arrays.copyOfRange(entryIds, start, start + capacity);
            values = Arrays.copyOfRange(values, start, start + capacity);
            start = 0;
            end = size;
        }
    }

    /** A callback for each entry, in the order of the entry IDs. */
    interface EntryConsumer {
        void accept(long entryId, long value);
    }

    /**
     * Returned by {@link #higherEntryId} when there's no entry after the entry ID. It can't be an entry ID, since
     * BookKeeper's entry IDs aren't negative.
     */
    static final long NO_ENTRY = Long.MIN_VALUE;

    private final ArrayList<Block> blocks = new ArrayList<>(1);
    // the first entry ID of each block, in step with the blocks, for the binary search over the blocks
    private long[] firstEntryIds = new long[1];
    // The last emptied block, reused for the next block: one of the full capacity while the map has more than one
    // block, so that a window that moves through the map doesn't allocate, or the lone block of the smallest capacity
    // that an empty map had, so that a map that empties between entries, which PendingAcksMap reuses, doesn't allocate
    private Block spareBlock;
    // changed when an entry is added or removed, so that a traversal notices when its callback changed the map
    private int modCount;
    // the most blocks since the list's capacity was last trimmed, to release the capacity after the map shrinks
    private int peakBlocks;
    private int size;
    // the blocks allocated by this map, for tests
    private int allocatedBlocks;

    int size() {
        return size;
    }

    boolean isEmpty() {
        return size == 0;
    }

    @VisibleForTesting
    int blockCount() {
        return blocks.size();
    }

    @VisibleForTesting
    int allocatedBlocks() {
        return allocatedBlocks;
    }

    @VisibleForTesting
    int blockCapacity(int blockIndex) {
        return blocks.get(blockIndex).capacity();
    }

    /** Checks the map's internal consistency, for tests. */
    @VisibleForTesting
    void checkInvariants() {
        int entries = 0;
        long previous = Long.MIN_VALUE;
        for (int b = 0; b < blocks.size(); b++) {
            Block block = blocks.get(b);
            if (block.size() == 0) {
                throw new IllegalStateException("empty block " + b);
            }
            if (firstEntryIds[b] != block.firstEntryId()) {
                throw new IllegalStateException("first entry ID of block " + b + " is " + block.firstEntryId()
                        + ", indexed as " + firstEntryIds[b]);
            }
            for (int i = block.start; i < block.end; i++) {
                if (block.entryIds[i] <= previous) {
                    throw new IllegalStateException("entry " + block.entryIds[i] + " after " + previous);
                }
                previous = block.entryIds[i];
            }
            entries += block.size();
        }
        if (entries != size) {
            throw new IllegalStateException(entries + " entries, size " + size);
        }
        if (spareBlock != null && spareBlock.size() != 0) {
            throw new IllegalStateException("a spare block with entries");
        }
        if (spareBlock != null && (blocks.size() == 1
                || blocks.isEmpty() && spareBlock.capacity() != INITIAL_BLOCK_CAPACITY)) {
            throw new IllegalStateException("a spare block of capacity " + spareBlock.capacity() + " with "
                    + blocks.size() + " blocks");
        }
        if (blocks.size() > 1) {
            for (Block block : blocks) {
                if (block.capacity() != BLOCK_CAPACITY) {
                    throw new IllegalStateException("a block of capacity " + block.capacity() + " among others");
                }
            }
        }
    }

    /** The entry's value, or {@link PendingAckValues#PACKED_NOT_FOUND}. */
    long get(long entryId) {
        if (isFirst(entryId)) {
            return firstValue();
        }
        int blockIndex = blockIndexFor(entryId);
        if (blockIndex < 0) {
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        Block block = blocks.get(blockIndex);
        int index = block.indexOf(entryId);
        return index >= 0 ? block.values[index] : PendingAckValues.PACKED_NOT_FOUND;
    }

    // Whether the entry is the first one: acks mostly come in the order of the entries, and both the ack's lookup and
    // its removal then find it without a search
    private boolean isFirst(long entryId) {
        return size > 0 && firstEntryId() == entryId;
    }

    boolean containsKey(long entryId) {
        return !PendingAckValues.isNotFound(get(entryId));
    }

    /**
     * Sets the entry's value, which mustn't be {@link PendingAckValues#PACKED_NOT_FOUND}.
     *
     * @return the previous value, or {@link PendingAckValues#PACKED_NOT_FOUND}
     */
    long put(long entryId, long value) {
        Block last = blocks.isEmpty() ? null : blocks.get(blocks.size() - 1);
        if (last == null || entryId > last.lastEntryId()) {
            // the common case: an entry after the others
            boolean newBlock = false;
            if (last == null || last.end == last.capacity()) {
                if (last != null && last.start >= last.capacity() / 4) {
                    // compacting frees a quarter of the block at least, so that it doesn't happen on every append
                    last.compact();
                } else if (last != null && last.capacity() < BLOCK_CAPACITY) {
                    last.grow(last.capacity() + 1);
                } else {
                    // a map's first block starts small, the blocks after it as full ones
                    last = newBlock(last == null ? INITIAL_BLOCK_CAPACITY : BLOCK_CAPACITY);
                    newBlock = true;
                }
            }
            last.entryIds[last.end] = entryId;
            last.values[last.end] = value;
            last.end++;
            if (newBlock) {
                addBlock(blocks.size(), last);
            }
            size++;
            modCount++;
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        // the first block whose entries reach the entry ID
        int blockIndex = Math.max(blockIndexFor(entryId), 0);
        Block block = blocks.get(blockIndex);
        int index = block.indexOf(entryId);
        if (index >= 0) {
            long previous = block.values[index];
            block.values[index] = value;
            return previous;
        }
        if (block.size() == BLOCK_CAPACITY) {
            Block upper = split(blockIndex);
            if (entryId > block.lastEntryId()) {
                block = upper;
                blockIndex++;
            }
            index = block.indexOf(entryId);
        } else if (block.size() == block.capacity()) {
            // a small first block that is full
            block.grow(block.capacity() + 1);
            index = block.indexOf(entryId);
        }
        block.insertAt(-index - 1, entryId, value);
        firstEntryIds[blockIndex] = block.firstEntryId();
        size++;
        modCount++;
        return PendingAckValues.PACKED_NOT_FOUND;
    }

    /**
     * Removes the entry.
     *
     * @return its value, or {@link PendingAckValues#PACKED_NOT_FOUND} if it wasn't in the map
     */
    long remove(long entryId) {
        if (isFirst(entryId)) {
            long previous = firstValue();
            removeFirst();
            return previous;
        }
        int blockIndex = blockIndexFor(entryId);
        if (blockIndex < 0) {
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        Block block = blocks.get(blockIndex);
        int index = block.indexOf(entryId);
        if (index < 0) {
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        long previous = block.values[index];
        removeAt(blockIndex, block, index);
        return previous;
    }

    /** The first entry's ID; the map mustn't be empty. */
    long firstEntryId() {
        return firstEntryIds[0];
    }

    /** The first entry's value; the map mustn't be empty. */
    long firstValue() {
        Block first = blocks.get(0);
        return first.values[first.start];
    }

    /** Removes the first entry; the map mustn't be empty. */
    void removeFirst() {
        Block first = blocks.get(0);
        removeAt(0, first, first.start);
    }

    /** Whether the map has an entry with an ID up to {@code maxEntryId}, inclusive. */
    boolean hasEntriesUpTo(long maxEntryId) {
        return size > 0 && firstEntryId() <= maxEntryId;
    }

    /**
     * Calls the consumer for each entry, in the order of the entry IDs. The consumer may add and remove entries: the
     * traversal continues with the first entry after the current one as the map is then, so it visits the entries
     * added after the current one and not those added before it, and never an entry that was removed.
     */
    void forEach(EntryConsumer consumer) {
        for (int b = 0; b < blocks.size(); b++) {
            Block block = blocks.get(b);
            for (int i = block.start; i < block.end; i++) {
                long entryId = block.entryIds[i];
                int expectedModCount = modCount;
                consumer.accept(entryId, block.values[i]);
                if (modCount != expectedModCount) {
                    // the entries may have moved: continue after the entry, wherever the next one is now
                    for (long next = higherEntryId(entryId); next != NO_ENTRY; next = higherEntryId(next)) {
                        consumer.accept(next, get(next));
                    }
                    return;
                }
            }
        }
    }

    /** The ID of the first entry after the entry ID, or {@link #NO_ENTRY}. */
    long higherEntryId(long entryId) {
        if (size == 0) {
            return NO_ENTRY;
        }
        if (firstEntryId() > entryId) {
            // the common case when entries are removed in order
            return firstEntryId();
        }
        int blockIndex = blockIndexFor(entryId);
        Block block = blocks.get(blockIndex);
        int index = block.indexOf(entryId);
        int next = index >= 0 ? index + 1 : -index - 1;
        if (next < block.end) {
            return block.entryIds[next];
        }
        return blockIndex + 1 < blocks.size() ? firstEntryIds[blockIndex + 1] : NO_ENTRY;
    }

    /** The index of the last block whose first entry ID is at most the entry ID, or -1. */
    private int blockIndexFor(long entryId) {
        int low = 0;
        int high = blocks.size() - 1;
        int found = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (firstEntryIds[mid] <= entryId) {
                found = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return found;
    }

    /** A block for the next entries: the spare one, or a new one of the capacity. */
    private Block newBlock(int capacity) {
        Block block = spareBlock;
        if (block != null && block.capacity() >= capacity) {
            spareBlock = null;
            return block;
        }
        allocatedBlocks++;
        return new Block(capacity);
    }

    /** Adds a block that has entries at the index, with its first entry ID. */
    private void addBlock(int blockIndex, Block block) {
        int count = blocks.size();
        if (count == firstEntryIds.length) {
            firstEntryIds = Arrays.copyOf(firstEntryIds, count * 2);
        }
        System.arraycopy(firstEntryIds, blockIndex, firstEntryIds, blockIndex + 1, count - blockIndex);
        firstEntryIds[blockIndex] = block.firstEntryId();
        blocks.add(blockIndex, block);
        peakBlocks = Math.max(peakBlocks, blocks.size());
    }

    /** Removes the block at the index, keeping it as the spare one. */
    private void removeBlock(int blockIndex) {
        Block block = blocks.remove(blockIndex);
        System.arraycopy(firstEntryIds, blockIndex + 1, firstEntryIds, blockIndex, blocks.size() - blockIndex);
        // a block merged into another one still has its window
        block.start = 0;
        block.end = 0;
        spareBlock = block;
    }

    /** Splits a full block in two halves; returns the new upper half, which follows it in the list. */
    private Block split(int blockIndex) {
        Block block = blocks.get(blockIndex);
        Block upper = newBlock(BLOCK_CAPACITY);
        int half = block.size() / 2;
        int from = block.start + half;
        int count = block.end - from;
        System.arraycopy(block.entryIds, from, upper.entryIds, 0, count);
        System.arraycopy(block.values, from, upper.values, 0, count);
        upper.end = count;
        block.end = from;
        addBlock(blockIndex + 1, upper);
        return upper;
    }

    private void removeAt(int blockIndex, Block block, int index) {
        block.removeAt(index);
        size--;
        modCount++;
        if (block.size() == 0) {
            // Removing the first block shifts the references to the others and the first entry IDs: draining N
            // entries in order moves about (N / BLOCK_CAPACITY)^2 references in all. That is a small part of the
            // entries' own handling up to about a million pending acks of one consumer in one ledger, and the AVL
            // tree's cost at about 8 million, far above the default maxUnackedMessagesPerConsumer of 50,000
            removeBlock(blockIndex);
        } else {
            firstEntryIds[blockIndex] = block.firstEntryId();
            if (block.size() < MERGE_THRESHOLD) {
                mergeWithNeighbor(blockIndex, block);
            }
        }
        if (blocks.size() == 1) {
            // a lone block shrinks with its entries, and the map keeps no spare block of the full capacity for it
            spareBlock = null;
            Block lone = blocks.get(0);
            if (lone.capacity() > INITIAL_BLOCK_CAPACITY && lone.size() < lone.capacity() / 4) {
                lone.resize(lone.capacity() / 2);
            }
        } else if (blocks.isEmpty() && (spareBlock == null || spareBlock.capacity() != INITIAL_BLOCK_CAPACITY)) {
            // an empty map keeps a spare block of the smallest capacity only
            spareBlock = null;
        }
        if (!blocks.isEmpty() && blocks.size() * 4 < peakBlocks) {
            // the list keeps the capacity of its largest size: release it once a quarter of it is in use
            blocks.trimToSize();
            firstEntryIds = Arrays.copyOf(firstEntryIds, Math.max(blocks.size(), 1));
            peakBlocks = blocks.size();
        }
    }

    /** Merges a sparse block with the next or the previous one, when they fit in one block together. */
    private void mergeWithNeighbor(int blockIndex, Block block) {
        if (blockIndex + 1 < blocks.size() && block.size() + blocks.get(blockIndex + 1).size() <= BLOCK_CAPACITY / 2) {
            moveInto(block, blocks.get(blockIndex + 1));
            removeBlock(blockIndex + 1);
        } else if (blockIndex > 0 && block.size() + blocks.get(blockIndex - 1).size() <= BLOCK_CAPACITY / 2) {
            moveInto(blocks.get(blockIndex - 1), block);
            removeBlock(blockIndex);
        }
    }

    /** Appends the entries of the following block to the target block. */
    private static void moveInto(Block target, Block following) {
        int count = target.size() + following.size();
        if (count > target.capacity()) {
            target.grow(count);
        } else {
            target.compact();
        }
        System.arraycopy(following.entryIds, following.start, target.entryIds, target.end, following.size());
        System.arraycopy(following.values, following.start, target.values, target.end, following.size());
        target.end += following.size();
    }
}
