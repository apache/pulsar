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
package org.apache.bookkeeper.mledger.impl.cache;

import io.netty.util.IllegalReferenceCountException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Special type of cache where get() and delete() operations can be done over a range of keys.
 *
 * <p>The entries are stored in pages, each holding up to {@link #PAGE_SIZE} consecutive entry IDs of one ledger in an
 * array, and the pages are kept in a ConcurrentSkipListMap ordered by ledger ID and page. Entries are inserted in
 * order: a managed ledger adds its entries at the tail, and a read from storage inserts the consecutive entries that
 * it read, at any earlier position. Each of them inserts with an {@link Inserter}, which remembers the page of its
 * previous insert, so that an insert usually costs a write to an array slot instead of a search of a skip list of all
 * the cached entries, and a new page is found with a search of the pages. A lookup is a page lookup and an array read,
 * and a range is visited page by page. A page is removed from the map when its last entry is removed.
 *
 * <p>The implementation avoids locks and synchronization by relying on the ConcurrentSkipListMap and atomic array
 * slots. Since there are no locks, it's necessary to ensure that a single entry in the cache is removed exactly once.
 * Removing an entry multiple times could result in the entries of the cache being released multiple times,
 * even while they are still in use. This is prevented by using a custom wrapper around the value to store in the
 * slots that ensures that the value is removed only if the exact same instance is present in the slot.
 * There's also a check that ensures that the value matches the key. This is used to detect races without impacting
 * consistency.
 */
@CustomLog
class RangeCache {
    static final int PAGE_SHIFT = 6;
    // The number of entry IDs in a page, a power of two so that an entry's page and slot are a shift and a mask
    static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final int SLOT_MASK = PAGE_SIZE - 1;

    private final ConcurrentNavigableMap<PageKey, Page> pages;
    private final RangeCacheRemovalQueue removalQueue;
    private final AtomicLong size; // Total size of values stored in cache

    /**
     * Construct a new RangeCache.
     */
    public RangeCache(RangeCacheRemovalQueue removalQueue) {
        this.removalQueue = removalQueue;
        this.pages = new ConcurrentSkipListMap<>();
        this.size = new AtomicLong(0);
    }

    /**
     * Insert.
     *
     * @param key
     * @param value       ref counted value with at least 1 ref to pass on the cache
     * @param entryLength size of the entry in bytes
     * @return whether the entry was inserted in the cache
     */
    public boolean put(Position key, ReferenceCountedEntry value, int entryLength) {
        return newInserter().put(key, value, entryLength);
    }

    /**
     * Returns an inserter for entries that are inserted in order, such as a managed ledger's added entries or the
     * entries of a read from storage.
     */
    public Inserter newInserter() {
        return new Inserter();
    }

    /**
     * Inserts entries into the cache, remembering the page of its previous insert, so that the next entry in order
     * usually goes to the same page without a lookup. An entry at any other position is inserted too, after a lookup
     * of its page. An inserter is used by one thread at a time.
     */
    public final class Inserter implements Function<RangeCacheEntryWrapper, Boolean> {
        // the page of the previous insert; its fields that are used here are final
        private Page page;

        private Inserter() {
        }

        /**
         * Insert.
         *
         * @param key         the position of the entry
         * @param value       ref counted value with at least 1 ref to pass on the cache
         * @param entryLength size of the entry in bytes
         * @return whether the entry was inserted in the cache
         */
        public boolean put(Position key, ReferenceCountedEntry value, int entryLength) {
            // retain value so that it's not released before we put it in the cache and calculate the weight
            value.retain();
            try {
                if (!value.matchesPosition(key)) {
                    throw new IllegalArgumentException("Value '" + value + "' does not match key '" + key + "'");
                }
                return RangeCacheEntryWrapper.withNewInstance(RangeCache.this, key, value, entryLength, this);
            } finally {
                value.release();
            }
        }

        /**
         * Runs the update on the entry that the cache has at the position, if it has one, and returns whether it has.
         * Cached entries are immutable and never replaced, so an entry that the cache has doesn't need to be prepared
         * for an insert, such as copied, but its mutable state, its expected read count, can be updated. The update
         * doesn't count as an access of the entry for the eviction.
         */
        public boolean updateIfCached(Position key, Consumer<ReferenceCountedEntry> update) {
            long ledgerId = key.getLedgerId();
            long entryId = key.getEntryId();
            long pageIndex = entryId >> PAGE_SHIFT;
            Page current = page;
            if (current == null || !current.covers(ledgerId, pageIndex)) {
                current = pages.get(new PageKey(ledgerId, pageIndex));
                if (current == null) {
                    return false;
                }
                page = current;
            }
            RangeCacheEntryWrapper wrapper = current.slots.get((int) (entryId & SLOT_MASK));
            if (wrapper == null) {
                return false;
            }
            ReferenceCountedEntry value =
                    getRetainedValueMatchingPosition(ledgerId, entryId, wrapper.getValue(ledgerId, entryId, false));
            if (value == null) {
                return false;
            }
            try {
                update.accept(value);
            } finally {
                value.release();
            }
            return true;
        }

        /**
         * Adds the new wrapper to the cache. withNewInstance holds the wrapper's write lock while its initialized
         * fields are used.
         */
        @Override
        public Boolean apply(RangeCacheEntryWrapper newWrapper) {
            if (addToPage(newWrapper) && removalQueue.addEntry(newWrapper)) {
                size.addAndGet(newWrapper.size);
                return true;
            } else {
                // recycle the new wrapper as it was not used
                newWrapper.recycle();
                return false;
            }
        }

        /**
         * Stores the wrapper in the slot of its key, unless the slot already has an entry.
         */
        private boolean addToPage(RangeCacheEntryWrapper wrapper) {
            long ledgerId = wrapper.key.getLedgerId();
            long entryId = wrapper.key.getEntryId();
            long pageIndex = entryId >> PAGE_SHIFT;
            int slot = (int) (entryId & SLOT_MASK);
            while (true) {
                Page current = page;
                if (current == null || !current.covers(ledgerId, pageIndex)) {
                    current = getOrCreatePage(ledgerId, pageIndex);
                    page = current;
                }
                if (!current.reserveSlot()) {
                    // the page lost its last entry and is being removed, so the entry goes to a new page
                    removePage(current);
                    page = null;
                    continue;
                }
                if (current.slots.compareAndSet(slot, null, wrapper)) {
                    return true;
                }
                releaseSlot(current);
                return false;
            }
        }
    }

    private Page getOrCreatePage(long ledgerId, long pageIndex) {
        PageKey pageKey = new PageKey(ledgerId, pageIndex);
        Page page = pages.get(pageKey);
        if (page == null) {
            Page newPage = new Page(pageKey);
            page = pages.putIfAbsent(pageKey, newPage);
            if (page == null) {
                page = newPage;
            }
        }
        return page;
    }

    /**
     * Returns the page of the entry ID, or null when there isn't one.
     */
    private Page findPage(long ledgerId, long entryId) {
        return pages.get(new PageKey(ledgerId, entryId >> PAGE_SHIFT));
    }

    private void releaseSlot(Page page) {
        if (page.releaseSlot()) {
            removePage(page);
        }
    }

    private void removePage(Page page) {
        pages.remove(page.key, page);
    }

    /**
     * Insert to cache with entry length determined directly from the value.
     * This method is used in tests.
     * @param key
     * @param value
     * @return
     */
    public boolean put(Position key, ReferenceCountedEntry value) {
        return put(key, value, value.getLength());
    }

    public boolean exists(Position key) {
        return key != null ? getWrapper(key) != null : true;
    }

    private RangeCacheEntryWrapper getWrapper(Position key) {
        Page page = findPage(key.getLedgerId(), key.getEntryId());
        return page != null ? page.slots.get((int) (key.getEntryId() & SLOT_MASK)) : null;
    }

    /**
     * Get the value associated with the key and increment the reference count of it.
     * The caller is responsible for releasing the reference.
     */
    public ReferenceCountedEntry get(Position key) {
        RangeCacheEntryWrapper valueWrapper = getWrapper(key);
        if (valueWrapper == null) {
            return null;
        } else {
            ReferenceCountedEntry value = valueWrapper.getValue(key);
            return getRetainedValueMatchingPosition(key.getLedgerId(), key.getEntryId(), value);
        }
    }

    /**
     * @apiNote the returned value must be released if it's not null
     */
    private static ReferenceCountedEntry getRetainedValueAt(RangeCacheEntryWrapper wrapper, long ledgerId,
                                                            long entryId) {
        return getRetainedValueMatchingPosition(ledgerId, entryId, wrapper.getValue(ledgerId, entryId));
    }

    // validates that the value matches the position and that the value has not been recycled
    // which are possible due to the lack of exclusive locks in the cache and the use of reference counted objects
    /**
     * @apiNote the returned value must be released if it's not null
     */
    private static ReferenceCountedEntry getRetainedValueMatchingPosition(long ledgerId, long entryId,
                                                                          ReferenceCountedEntry value) {
        if (value == null) {
            // the wrapper has been recycled and contains another key
            return null;
        }
        try {
            value.retain();
        } catch (IllegalReferenceCountException e) {
            // Value was already deallocated
            return null;
        }
        // check that the value matches the key and that there's at least 2 references to it since
        // the cache should be holding one reference and a new reference was just added in this method
        if (value.refCnt() > 1 && value.getLedgerId() == ledgerId && value.getEntryId() == entryId) {
            return value;
        } else {
            // Value or IdentityWrapper was recycled and already contains another value
            // release the reference added in this method
            value.release();
            return null;
        }
    }

    /**
     *
     * @param first
     *            the first key in the range
     * @param last
     *            the last key in the range (inclusive)
     * @return a collections of the value found in cache
     */
    public Collection<ReferenceCountedEntry> getRange(Position first, Position last) {
        List<ReferenceCountedEntry> values = new ArrayList<>();

        // Return the values of the entries found in cache
        forEachWrapperInRange(first, last, true, (wrapper, ledgerId, entryId) -> {
            ReferenceCountedEntry value = getRetainedValueAt(wrapper, ledgerId, entryId);
            if (value != null) {
                values.add(value);
            }
        });

        return values;
    }

    /**
     * Visits matching entries in order without collecting them. Each entry is retained during the callback and
     * released afterwards, including when the callback throws. The visitor must retain entries it needs to keep.
     */
    public void forEachInRange(Position first, Position last, Consumer<ReferenceCountedEntry> visitor) {
        forEachWrapperInRange(first, last, true, (wrapper, ledgerId, entryId) -> {
            ReferenceCountedEntry value = getRetainedValueAt(wrapper, ledgerId, entryId);
            if (value != null) {
                try {
                    visitor.accept(value);
                } finally {
                    value.release();
                }
            }
        });
    }

    private interface WrapperVisitor {
        void visit(RangeCacheEntryWrapper wrapper, long ledgerId, long entryId);
    }

    /**
     * Visits the occupied slots of the range in order.
     */
    private void forEachWrapperInRange(Position first, Position last, boolean lastInclusive,
                                       WrapperVisitor visitor) {
        long firstLedgerId = first.getLedgerId();
        long firstEntryId = first.getEntryId();
        long lastLedgerId = last.getLedgerId();
        long lastEntryId = lastInclusive ? last.getEntryId() : last.getEntryId() - 1;
        if (firstLedgerId > lastLedgerId || (firstLedgerId == lastLedgerId && firstEntryId > lastEntryId)) {
            return;
        }
        PageKey firstPage = new PageKey(firstLedgerId, firstEntryId >> PAGE_SHIFT);
        PageKey lastPage = new PageKey(lastLedgerId, lastEntryId >> PAGE_SHIFT);
        for (Page page : pages.subMap(firstPage, true, lastPage, true).values()) {
            int firstSlot = page.key.equals(firstPage) ? (int) (firstEntryId & SLOT_MASK) : 0;
            int lastSlot = page.key.equals(lastPage) ? (int) (lastEntryId & SLOT_MASK) : PAGE_SIZE - 1;
            long ledgerId = page.key.ledgerId();
            long pageFirstEntryId = page.key.pageIndex() << PAGE_SHIFT;
            for (int slot = firstSlot; slot <= lastSlot; slot++) {
                RangeCacheEntryWrapper wrapper = page.slots.get(slot);
                if (wrapper != null) {
                    visitor.visit(wrapper, ledgerId, pageFirstEntryId + slot);
                }
            }
        }
    }

    /**
     *
     * @param first
     * @param last
     * @param lastInclusive
     * @return an pair of ints, containing the number of removed entries and the total size
     */
    public Pair<Integer, Long> removeRange(Position first, Position last, boolean lastInclusive) {
        log.debug().attr("first", first)
                .attr("last", last)
                .attr("lastInclusive", lastInclusive)
                .log("Removing entries in range");
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        forEachWrapperInRange(first, last, lastInclusive,
                (wrapper, ledgerId, entryId) -> removeEntryWithWriteLock(wrapper, ledgerId, entryId, counters));
        return handleRemovalResult(counters);
    }

    private boolean removeEntryWithWriteLock(RangeCacheEntryWrapper entryWrapper, long ledgerId, long entryId,
                                             RangeCacheRemovalCounters counters) {
        return entryWrapper.withWriteLock(e -> {
            if (e.key == null || e.rangeCache != this || e.key.compareTo(ledgerId, entryId) != 0) {
                // entry has already been removed, and the wrapper may have been reused for another entry
                return false;
            }
            return removeEntry(e.key, e.value, e, counters, false);
        });
    }

    /**
     * Remove the entry from the cache. This must be called within a function passed to
     * {@link RangeCacheEntryWrapper#withWriteLock(Function)}.
     * @param key the expected key of the entry
     * @param value the expected value of the entry
     * @param entryWrapper the entry wrapper instance
     * @param counters the removal counters
     * @return true if the entry was removed, false otherwise
     */
    boolean removeEntry(Position key, ReferenceCountedEntry value, RangeCacheEntryWrapper entryWrapper,
                        RangeCacheRemovalCounters counters, boolean updateSize) {
        // always remove the entry from its page
        removeFromPage(key, entryWrapper);
        if (value == null) {
            // the wrapper has already been recycled and contains another key
            return false;
        }
        try {
            // add extra retain to avoid value being released while we are removing it
            value.retain();
        } catch (IllegalReferenceCountException e) {
            return false;
        }
        try {
            if (!value.matchesPosition(key)) {
                return false;
            }
            long removedSize = entryWrapper.markRemoved(key, value);
            if (removedSize > -1) {
                counters.entryRemoved(removedSize);
                if (updateSize) {
                    size.addAndGet(-removedSize);
                }
                if (value.refCnt() > 1) {
                    // remove the cache reference
                    value.release();
                } else {
                    log.info().attr("refCnt", value.refCnt())
                            .attr("key", key)
                            .log("Unexpected refCnt, removed entry without releasing the value");
                }
                return true;
            } else {
                return false;
            }
        } finally {
            // remove the extra retain
            value.release();
        }
    }

    private void removeFromPage(Position key, RangeCacheEntryWrapper entryWrapper) {
        Page page = findPage(key.getLedgerId(), key.getEntryId());
        if (page != null && page.slots.compareAndSet((int) (key.getEntryId() & SLOT_MASK), entryWrapper, null)) {
            releaseSlot(page);
        }
    }

    private Pair<Integer, Long> handleRemovalResult(RangeCacheRemovalCounters counters) {
        size.addAndGet(-counters.removedSize);
        Pair<Integer, Long> result = Pair.of(counters.removedEntries, counters.removedSize);
        counters.recycle();
        return result;
    }

    /**
     * Just for testing. Counts the entries of all the pages.
     */
    protected long getNumberOfEntries() {
        long numberOfEntries = 0;
        for (Page page : pages.values()) {
            numberOfEntries += Math.max(page.count, 0);
        }
        return numberOfEntries;
    }

    /**
     * Just for testing. The number of pages in the cache.
     */
    int getNumberOfPages() {
        return pages.size();
    }

    public long getSize() {
        return size.get();
    }

    /**
     * Remove all the entries from the cache.
     *
     * @return size of removed entries
     */
    public Pair<Integer, Long> clear() {
        log.debug().attr("numPages", () -> pages.size()).attr("size", size.get()).log("Clearing the cache");
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        // Passes over the pages until one finds no entry, so that an entry that a concurrent insert added to a page
        // that an earlier pass had visited is removed too
        boolean found = true;
        while (found && !Thread.currentThread().isInterrupted()) {
            found = false;
            for (Page page : pages.values()) {
                long ledgerId = page.key.ledgerId();
                long pageFirstEntryId = page.key.pageIndex() << PAGE_SHIFT;
                for (int slot = 0; slot < PAGE_SIZE; slot++) {
                    RangeCacheEntryWrapper wrapper = page.slots.get(slot);
                    if (wrapper != null) {
                        found = true;
                        removeEntryWithWriteLock(wrapper, ledgerId, pageFirstEntryId + slot, counters);
                    }
                }
            }
        }
        return handleRemovalResult(counters);
    }

    /**
     * The key of a page: the ledger ID and the entry ID divided by {@link #PAGE_SIZE}.
     */
    record PageKey(long ledgerId, long pageIndex) implements Comparable<PageKey> {
        @Override
        public int compareTo(PageKey other) {
            int result = Long.compare(ledgerId, other.ledgerId);
            return result != 0 ? result : Long.compare(pageIndex, other.pageIndex);
        }
    }

    /**
     * The slots of the consecutive entry IDs of a page, and the number of slots that have an entry. A page that loses
     * its last entry is sealed, so that no entry is added to it while it's being removed from the map.
     */
    static final class Page {
        private static final int SEALED = -1;
        private static final AtomicIntegerFieldUpdater<Page> COUNT_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(Page.class, "count");

        final PageKey key;
        final AtomicReferenceArray<RangeCacheEntryWrapper> slots = new AtomicReferenceArray<>(PAGE_SIZE);
        // the number of slots that have an entry or are reserved for one, or SEALED
        private volatile int count;

        Page(PageKey key) {
            this.key = key;
        }

        boolean covers(long ledgerId, long pageIndex) {
            return key.ledgerId() == ledgerId && key.pageIndex() == pageIndex;
        }

        /**
         * Reserves a slot for an entry to add, unless the page has been sealed.
         */
        boolean reserveSlot() {
            while (true) {
                int current = count;
                if (current == SEALED) {
                    return false;
                }
                if (COUNT_UPDATER.compareAndSet(this, current, current + 1)) {
                    return true;
                }
            }
        }

        /**
         * Releases a slot whose entry was removed or wasn't added, and seals the page when it was the last one.
         *
         * @return whether the page was sealed
         */
        boolean releaseSlot() {
            while (true) {
                int current = count;
                int next = current == 1 ? SEALED : current - 1;
                if (COUNT_UPDATER.compareAndSet(this, current, next)) {
                    return next == SEALED;
                }
            }
        }
    }
}
