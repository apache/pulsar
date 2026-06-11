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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.util.Recycler;
import java.util.Iterator;
import java.util.List;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;

public class CompositeLedgerEntriesImpl implements LedgerEntries {
    private List<LedgerEntry> entries;
    private List<LedgerEntries> ledgerEntries;
    private final Recycler.Handle<CompositeLedgerEntriesImpl> recyclerHandle;

    private CompositeLedgerEntriesImpl(Recycler.Handle<CompositeLedgerEntriesImpl> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<CompositeLedgerEntriesImpl> RECYCLER = new Recycler<>() {
        @Override
        protected CompositeLedgerEntriesImpl newObject(Recycler.Handle<CompositeLedgerEntriesImpl> handle) {
            return new CompositeLedgerEntriesImpl(handle);
        }
    };

    public static LedgerEntries create(List<LedgerEntry> entries, List<LedgerEntries> ledgerEntries) {
        checkArgument(!entries.isEmpty(), "entries for create should not be empty.");
        checkArgument(!ledgerEntries.isEmpty(), "ledgerEntries for create should not be empty.");
        CompositeLedgerEntriesImpl instance = RECYCLER.get();
        instance.entries = entries;
        instance.ledgerEntries = ledgerEntries;
        return instance;
    }

    private void recycle() {
        if (ledgerEntries == null) {
            return;
        }
        ledgerEntries.forEach(LedgerEntries::close);
        entries = null;
        ledgerEntries = null;
        recyclerHandle.recycle(this);
    }

    @Override
    public LedgerEntry getEntry(long entryId) {
        checkNotNull(entries, "entries has been recycled");
        long firstId = entries.get(0).getEntryId();
        long lastId = entries.get(entries.size() - 1).getEntryId();
        if (entryId < firstId || entryId > lastId) {
            throw new IndexOutOfBoundsException("required index: " + entryId
                    + " is out of bounds: [ " + firstId + ", " + lastId + " ].");
        }
        int index = (int) (entryId - firstId);
        LedgerEntry entry = entries.get(index);
        if (entry.getEntryId() != entryId) {
            throw new IllegalStateException("Non-contiguous entries detected: expected entryId "
                    + entryId + " at index " + index + " but found entryId " + entry.getEntryId());
        }
        return entry;
    }

    @Override
    public Iterator<LedgerEntry> iterator() {
        checkNotNull(entries, "entries has been recycled");
        return entries.iterator();
    }

    @Override
    public void close() {
        recycle();
    }
}
