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

import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;

/**
 * Cache of entries used by a single ManagedLedger. An EntryCache is compared to other EntryCache instances using their
 * size (the memory that is occupied by each of them).
 */
public interface EntryCache {

    /**
     * @return the name of the cache
     */
    String getName();

    /**
     * Insert an entry in the cache.
     *
     * <p/>If the overall limit have been reached, this will trigger the eviction of other entries, possibly from
     * other EntryCache instances
     *
     * @param entry the entry to be cached
     * @return whether the entry was inserted in cache
     */
    boolean insert(Entry entry);

    /**
     * Remove from cache all the entries related to a ledger up to lastPosition excluded.
     *
     * @param lastPosition
     *            the position of the last entry to be invalidated (non-inclusive)
     */
    void invalidateEntries(Position lastPosition);

    /**
     * Remove from the cache all the entries belonging to a specific ledger.
     *
     * @param ledgerId
     *            the ledger id
     */
    void invalidateAllEntries(long ledgerId);

    /**
     * Remove all the entries from the cache.
     */
    void clear();

    /**
     * Read entries from the cache or from bookkeeper.
     *
     * <p/>Get the entry data either from cache or bookkeeper and mixes up the results in a single list.
     *
     * @param lh
     *            the ledger handle
     * @param firstEntry
     *            the first entry to read (inclusive)
     * @param lastEntry
     *            the last entry to read (inclusive)
     * @param maxSizeBytes
     *            the size limit in bytes of the read that triggered this call, used to bound each storage read
     *            request; a non-positive value means no limit
     * @param expectedReadCount resolves the expected read count for the given entry. When the expected read count is
     *                         >0, the entry can be cached and reused later.
     * @param callback
     *            the callback object that will be notified when read is done
     * @param ctx
     *            the context object
     */
    void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, long maxSizeBytes,
            IntSupplier expectedReadCount, ReadEntriesCallback callback, Object ctx);

    /**
     * Read entry at given position from the cache or from bookkeeper.
     *
     * <p/>Get the entry data either from cache or bookkeeper and mixes up the results in a single list.
     *
     * @param lh
     *            the ledger handle
     * @param position
     *            position to read the entry from
     * @param callback
     *            the callback object that will be notified when read is done
     * @param ctx
     *            the context object
     */
    void asyncReadEntry(ReadHandle lh, Position position, ReadEntryCallback callback, Object ctx);

    /**
     * Read an entry, as {@link #asyncReadEntry(ReadHandle, Position, ReadEntryCallback, Object)} does, caching it
     * with the expected read count when it's read from storage.
     *
     * @param expectedReadCount
     *            the expected read count that the entry is cached with when it's read from storage, a negative value
     *            not to cache it, or {@link #DEFAULT_EXPECTED_READ_COUNT} for the cache's default for a single entry
     */
    default void asyncReadEntry(ReadHandle lh, Position position, int expectedReadCount, ReadEntryCallback callback,
                                Object ctx) {
        asyncReadEntry(lh, position, callback, ctx);
    }

    /**
     * The expected read count argument for the cache's default for a single entry read.
     */
    int DEFAULT_EXPECTED_READ_COUNT = Integer.MIN_VALUE;

    /**
     * Whether the cache has the entry, without reading it.
     *
     * @param position
     *            the entry's position
     * @return whether the cache has the entry; false when the cache keeps no entries
     */
    default boolean hasEntry(Position position) {
        return false;
    }

    /**
     * Reads a range of entries ahead of a cursor into the cache, unless the range's first entry is cached, the cache
     * is at its eviction watermark, a read of an overlapping range is pending, the cache wouldn't keep the entries, or
     * the permits of the reads in flight aren't available right away. The entries that the read returns are released
     * without counting as reads of the cached entries.
     *
     * @param cacheKeepsEntries whether the cache would keep the entries, checked after the cheaper conditions
     * @param onComplete run once when the read ahead completes or fails, if it was started
     * @return whether the read ahead was started
     */
    default boolean readAhead(ReadHandle lh, long firstEntry, long lastEntry, long maxSizeBytes,
                              BooleanSupplier cacheKeepsEntries, IntSupplier expectedReadCount, Runnable onComplete) {
        return false;
    }

    /**
     * Get the total size in bytes of all the entries stored in this cache.
     *
     * @return the size of the entry cache
     */
    long getSize();
}
