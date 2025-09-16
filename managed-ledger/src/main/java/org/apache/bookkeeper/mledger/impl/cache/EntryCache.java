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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.client.api.ReadHandle;
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
     * @param expectedReadCount resolves the expected read count for the given entry. When the expected read count is
     *                         >0, the entry can be cached and reused later.
     * @return the future of entries
     */
   CompletableFuture<List<Entry>> asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry,
                                                 IntSupplier expectedReadCount);

    /**
     * Read entry at given position from the cache or from bookkeeper.
     *
     * <p/>Get the entry data either from cache or bookkeeper and mixes up the results in a single list.
     *
     * @param lh
     *            the ledger handle
     * @param position
     *            position to read the entry from
     * @return the future of the entry
     */
    CompletableFuture<Entry> asyncReadEntry(ReadHandle lh, Position position);

    /**
     * Get the total size in bytes of all the entries stored in this cache.
     *
     * @return the size of the entry cache
     */
    long getSize();
}
