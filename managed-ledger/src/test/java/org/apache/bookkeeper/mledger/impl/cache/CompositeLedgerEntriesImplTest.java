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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.testng.annotations.Test;

public class CompositeLedgerEntriesImplTest {

    private LedgerEntryImpl createEntry(long ledgerId, long entryId, byte[] data) {
        return LedgerEntryImpl.create(ledgerId, entryId, data.length, Unpooled.wrappedBuffer(data));
    }

    @Test
    public void testCreateAndIterate() {
        LedgerEntryImpl e0 = createEntry(1L, 0L, new byte[]{0});
        LedgerEntryImpl e1 = createEntry(1L, 1L, new byte[]{1});
        LedgerEntryImpl e2 = createEntry(1L, 2L, new byte[]{2});

        List<LedgerEntry> entries = new ArrayList<>();
        entries.add(e0);
        entries.add(e1);
        entries.add(e2);

        List<LedgerEntries> containers = new ArrayList<>();
        // Wrap in a simple LedgerEntries mock-like container using a real impl
        // For test, we use a list-based approach
        containers.add(new MockLedgerEntries(entries));

        CompositeLedgerEntriesImpl combined = (CompositeLedgerEntriesImpl) CompositeLedgerEntriesImpl.create(
                entries, containers);

        // Verify iteration
        Iterator<LedgerEntry> it = combined.iterator();
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next().getEntryId()).isEqualTo(0L);
        assertThat(it.next().getEntryId()).isEqualTo(1L);
        assertThat(it.next().getEntryId()).isEqualTo(2L);
        assertThat(it.hasNext()).isFalse();

        combined.close();
    }

    @Test
    public void testGetEntry() {
        LedgerEntryImpl e0 = createEntry(1L, 5L, new byte[]{0});
        LedgerEntryImpl e1 = createEntry(1L, 6L, new byte[]{1});
        LedgerEntryImpl e2 = createEntry(1L, 7L, new byte[]{2});

        List<LedgerEntry> entries = new ArrayList<>();
        entries.add(e0);
        entries.add(e1);
        entries.add(e2);

        List<LedgerEntries> containers = new ArrayList<>();
        containers.add(new MockLedgerEntries(entries));

        CompositeLedgerEntriesImpl combined = (CompositeLedgerEntriesImpl) CompositeLedgerEntriesImpl.create(
                entries, containers);

        assertThat(combined.getEntry(5L).getEntryId()).isEqualTo(5L);
        assertThat(combined.getEntry(6L).getEntryId()).isEqualTo(6L);
        assertThat(combined.getEntry(7L).getEntryId()).isEqualTo(7L);

        combined.close();
    }

    @Test
    public void testGetEntryOutOfRange() {
        LedgerEntryImpl e0 = createEntry(1L, 0L, new byte[]{0});
        LedgerEntryImpl e1 = createEntry(1L, 1L, new byte[]{1});

        List<LedgerEntry> entries = new ArrayList<>();
        entries.add(e0);
        entries.add(e1);

        List<LedgerEntries> containers = new ArrayList<>();
        containers.add(new MockLedgerEntries(entries));

        CompositeLedgerEntriesImpl combined = (CompositeLedgerEntriesImpl) CompositeLedgerEntriesImpl.create(
                entries, containers);

        // Out of lower bound
        assertThatThrownBy(() -> combined.getEntry(-1L))
                .isInstanceOf(IndexOutOfBoundsException.class);
        // Out of upper bound
        assertThatThrownBy(() -> combined.getEntry(2L))
                .isInstanceOf(IndexOutOfBoundsException.class);

        combined.close();
    }

    @Test
    public void testCloseAndRecycle() {
        LedgerEntryImpl e0 = createEntry(1L, 0L, new byte[]{0});

        List<LedgerEntry> entries = new ArrayList<>();
        entries.add(e0);

        List<LedgerEntries> containers = new ArrayList<>();
        containers.add(new MockLedgerEntries(entries));

        CompositeLedgerEntriesImpl combined = (CompositeLedgerEntriesImpl) CompositeLedgerEntriesImpl.create(
                entries, containers);

        // Should be usable before close
        assertThat(combined.iterator().hasNext()).isTrue();

        combined.close();

        // After close, iterator should throw
        assertThatThrownBy(combined::iterator)
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testMultipleLedgerEntriesContainers() {
        // Simulate entries from 2 separate batch reads
        LedgerEntryImpl e0 = createEntry(1L, 0L, new byte[]{0});
        LedgerEntryImpl e1 = createEntry(1L, 1L, new byte[]{1});
        LedgerEntryImpl e2 = createEntry(1L, 2L, new byte[]{2});
        LedgerEntryImpl e3 = createEntry(1L, 3L, new byte[]{3});

        List<LedgerEntry> batch1 = new ArrayList<>();
        batch1.add(e0);
        batch1.add(e1);

        List<LedgerEntry> batch2 = new ArrayList<>();
        batch2.add(e2);
        batch2.add(e3);

        List<LedgerEntry> allEntries = new ArrayList<>();
        allEntries.addAll(batch1);
        allEntries.addAll(batch2);

        List<LedgerEntries> containers = new ArrayList<>();
        containers.add(new MockLedgerEntries(batch1));
        containers.add(new MockLedgerEntries(batch2));

        CompositeLedgerEntriesImpl combined = (CompositeLedgerEntriesImpl) CompositeLedgerEntriesImpl.create(
                allEntries, containers);

        // Verify all entries accessible
        assertThat(combined.getEntry(0L).getEntryId()).isEqualTo(0L);
        assertThat(combined.getEntry(1L).getEntryId()).isEqualTo(1L);
        assertThat(combined.getEntry(2L).getEntryId()).isEqualTo(2L);
        assertThat(combined.getEntry(3L).getEntryId()).isEqualTo(3L);

        combined.close();
    }

    @Test
    public void testGetEntryWithNonContiguousEntries() {
        // Create entries with a gap: IDs 5, 6, 8 (missing 7)
        LedgerEntryImpl e5 = createEntry(1L, 5L, new byte[]{0});
        LedgerEntryImpl e6 = createEntry(1L, 6L, new byte[]{1});
        LedgerEntryImpl e8 = createEntry(1L, 8L, new byte[]{2});

        List<LedgerEntry> entries = new ArrayList<>();
        entries.add(e5);
        entries.add(e6);
        entries.add(e8);

        List<LedgerEntries> containers = new ArrayList<>();
        containers.add(new MockLedgerEntries(entries));

        CompositeLedgerEntriesImpl combined = (CompositeLedgerEntriesImpl) CompositeLedgerEntriesImpl.create(
                entries, containers);

        // Valid entries should still work
        assertThat(combined.getEntry(5L).getEntryId()).isEqualTo(5L);
        assertThat(combined.getEntry(6L).getEntryId()).isEqualTo(6L);
        // Entry 7 computes index 2 (7-5=2), but entries.get(2) has ID 8, not 7 — should throw
        assertThatThrownBy(() -> combined.getEntry(7L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Non-contiguous");

        combined.close();
    }

    /**
     * Simple LedgerEntries implementation for testing.
     */
    private static class MockLedgerEntries implements LedgerEntries {
        private final List<LedgerEntry> entries;
        private boolean closed = false;

        MockLedgerEntries(List<LedgerEntry> entries) {
            this.entries = entries;
        }

        @Override
        public LedgerEntry getEntry(long entryId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<LedgerEntry> iterator() {
            return entries.iterator();
        }

        @Override
        public void close() {
            closed = true;
            entries.forEach(LedgerEntry::close);
        }
    }
}
