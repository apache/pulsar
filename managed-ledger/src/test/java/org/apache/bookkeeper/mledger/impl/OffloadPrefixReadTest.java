/**
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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.impl.OffloadPrefixTest.assertEventuallyTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.MockClock;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OffloadPrefixReadTest extends MockedBookKeeperTestCase {
    @Test
    public void testOffloadRead() throws Exception {
        MockLedgerOffloader offloader = spy(MockLedgerOffloader.class);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        for (int i = 0; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getComplete());
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(1).getOffloadContext().getComplete());
        Assert.assertFalse(ledger.getLedgersInfoAsList().get(2).getOffloadContext().getComplete());

        UUID firstLedgerUUID = new UUID(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getUidMsb(),
                ledger.getLedgersInfoAsList().get(0).getOffloadContext().getUidLsb());
        UUID secondLedgerUUID = new UUID(ledger.getLedgersInfoAsList().get(1).getOffloadContext().getUidMsb(),
                ledger.getLedgersInfoAsList().get(1).getOffloadContext().getUidLsb());

        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        int i = 0;
        for (Entry e : cursor.readEntries(10)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }
        verify(offloader, times(1))
                .readOffloaded(anyLong(), (UUID) any(), anyMap());
        verify(offloader).readOffloaded(anyLong(), eq(firstLedgerUUID), anyMap());

        for (Entry e : cursor.readEntries(10)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }
        verify(offloader, times(2))
                .readOffloaded(anyLong(), (UUID) any(), anyMap());
        verify(offloader).readOffloaded(anyLong(), eq(secondLedgerUUID), anyMap());

        for (Entry e : cursor.readEntries(5)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }
        verify(offloader, times(2))
                .readOffloaded(anyLong(), (UUID) any(), anyMap());

        ledger.close();
        // Ensure that all the read handles had been closed
        assertEquals(offloader.openedReadHandles.get(), 0);
    }

    @Test
    public void testBookkeeperFirstOffloadRead() throws Exception {
        MockLedgerOffloader offloader = spy(MockLedgerOffloader.class);
        MockClock clock = new MockClock();
        offloader.getOffloadPolicies()
                .setManagedLedgerOffloadedReadPriority(OffloadedReadPriority.BOOKKEEPER_FIRST);
        //delete after 5 minutes
        offloader.getOffloadPolicies()
                .setManagedLedgerOffloadDeletionLagInMillis(300000L);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);


        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_bookkeeper_first_test_ledger", config);

        for (int i = 0; i < 25; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        assertEquals(ledger.getLedgersInfoAsList().size(), 3);

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        assertEquals(ledger.getLedgersInfoAsList().size(), 3);
        assertEquals(ledger.getLedgersInfoAsList().stream()
                .filter(e -> e.getOffloadContext().getComplete()).count(), 2);

        LedgerInfo firstLedger = ledger.getLedgersInfoAsList().get(0);
        Assert.assertTrue(firstLedger.getOffloadContext().getComplete());
        LedgerInfo secondLedger;
        secondLedger = ledger.getLedgersInfoAsList().get(1);
        Assert.assertTrue(secondLedger.getOffloadContext().getComplete());

        UUID firstLedgerUUID = new UUID(firstLedger.getOffloadContext().getUidMsb(),
                firstLedger.getOffloadContext().getUidLsb());
        UUID secondLedgerUUID = new UUID(secondLedger.getOffloadContext().getUidMsb(),
                secondLedger.getOffloadContext().getUidLsb());

        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.EARLIEST);
        int i = 0;
        for (Entry e : cursor.readEntries(10)) {
            Assert.assertEquals(new String(e.getData()), "entry-" + i++);
        }
        // For offloaded first and not deleted ledgers, they should be read from bookkeeper.
        verify(offloader, never())
                .readOffloaded(anyLong(), (UUID) any(), anyMap());

        // Delete offladed message from bookkeeper
        assertEventuallyTrue(() -> bkc.getLedgers().contains(firstLedger.getLedgerId()));
        assertEventuallyTrue(() -> bkc.getLedgers().contains(secondLedger.getLedgerId()));
        clock.advance(6, TimeUnit.MINUTES);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        ledger.internalTrimConsumedLedgers(promise);
        promise.join();

        // assert bk ledger is deleted
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedger.getLedgerId()));
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(secondLedger.getLedgerId()));
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(0).getOffloadContext().getBookkeeperDeleted());
        Assert.assertTrue(ledger.getLedgersInfoAsList().get(1).getOffloadContext().getBookkeeperDeleted());

        for (Entry e : cursor.readEntries(10)) {
            Assert.assertEquals(new String(e.getData()), "entry-" + i++);
        }

        // Ledgers deleted from bookkeeper, now should read from offloader
        verify(offloader, atLeastOnce())
                .readOffloaded(anyLong(), (UUID) any(), anyMap());
        verify(offloader).readOffloaded(anyLong(), eq(secondLedgerUUID), anyMap());

    }


    static class MockLedgerOffloader implements LedgerOffloader {
        ConcurrentHashMap<UUID, ReadHandle> offloads = new ConcurrentHashMap<UUID, ReadHandle>();


        OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create("S3", "", "", "",
                null, null,
                null, null,
                OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS,
                OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY);


        @Override
        public String getOffloadDriverName() {
            return "mock";
        }

        @Override
        public CompletableFuture<Void> offload(ReadHandle ledger,
                                               UUID uuid,
                                               Map<String, String> extraMetadata) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            try {
                offloads.put(uuid, new MockOffloadReadHandle(ledger));
                promise.complete(null);
            } catch (Exception e) {
                promise.completeExceptionally(e);
            }
            return promise;
        }

        @SneakyThrows
        @Override
        public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uuid,
                                                           Map<String, String> offloadDriverMetadata) {
            return CompletableFuture.completedFuture(new VerifyClosingReadHandle(offloads.get(uuid)));
        }

        @Override
        public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uuid,
                                                       Map<String, String> offloadDriverMetadata) {
            offloads.remove(uuid);
            return CompletableFuture.completedFuture(null);
        };

        @Override
        public OffloadPoliciesImpl getOffloadPolicies() {
            return offloadPolicies;
        }

        @Override
        public void close() {

        }

        private final AtomicInteger openedReadHandles = new AtomicInteger(0);

        class VerifyClosingReadHandle extends MockOffloadReadHandle {
            VerifyClosingReadHandle(ReadHandle toCopy) throws Exception {
                super(toCopy);
                openedReadHandles.incrementAndGet();
            }

            @Override
            public CompletableFuture<Void> closeAsync() {
                openedReadHandles.decrementAndGet();
                return super.closeAsync();
            }
        }
    }

    static class MockOffloadReadHandle implements ReadHandle {
        final long id;
        final List<ByteBuf> entries = Lists.newArrayList();
        final LedgerMetadata metadata;

        MockOffloadReadHandle(ReadHandle toCopy) throws Exception {
            id = toCopy.getId();
            long lac = toCopy.getLastAddConfirmed();
            try (LedgerEntries entries = toCopy.read(0, lac)) {
                for (LedgerEntry e : entries) {
                    this.entries.add(e.getEntryBuffer().retainedSlice());
                }
            }
            metadata = new MockMetadata(toCopy.getLedgerMetadata());
        }

        @Override
        public long getId() { return id; }

        @Override
        public LedgerMetadata getLedgerMetadata() {
            return metadata;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
            List<LedgerEntry> readEntries = Lists.newArrayList();
            for (long eid = firstEntry; eid <= lastEntry; eid++) {
                ByteBuf buf = entries.get((int)eid).retainedSlice();
                readEntries.add(LedgerEntryImpl.create(id, eid, buf.readableBytes(), buf));
            }
            return CompletableFuture.completedFuture(LedgerEntriesImpl.create(readEntries));
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
            return unsupported();
        }

        @Override
        public CompletableFuture<Long> readLastAddConfirmedAsync() {
            return unsupported();
        }

        @Override
        public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
            return unsupported();
        }

        @Override
        public long getLastAddConfirmed() {
            return entries.size() - 1;
        }

        @Override
        public long getLength() {
            return metadata.getLength();
        }

        @Override
        public boolean isClosed() {
            return metadata.isClosed();
        }

        @Override
        public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                          long timeOutInMillis,
                                                                                          boolean parallel) {
            return unsupported();
        }

        private <T> CompletableFuture<T> unsupported() {
            CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(new UnsupportedOperationException());
            return future;
        }
    }

    static class MockMetadata implements LedgerMetadata {
        private final int ensembleSize;
        private final int writeQuorumSize;
        private final int ackQuorumSize;
        private final long lastEntryId;
        private final long length;
        private final DigestType digestType;
        private final long ctime;
        private final boolean isClosed;
        private final int metadataFormatVersion;
        private final State state;
        private final byte[] password;
        private final Map<String, byte[]> customMetadata;
        private final long ledgerId;
        MockMetadata(LedgerMetadata toCopy) {
            ledgerId = toCopy.getLedgerId();
            ensembleSize = toCopy.getEnsembleSize();
            writeQuorumSize = toCopy.getWriteQuorumSize();
            ackQuorumSize = toCopy.getAckQuorumSize();
            lastEntryId = toCopy.getLastEntryId();
            length = toCopy.getLength();
            digestType = toCopy.getDigestType();
            ctime = toCopy.getCtime();
            isClosed = toCopy.isClosed();
            metadataFormatVersion = toCopy.getMetadataFormatVersion();
            state = toCopy.getState();
            password = Arrays.copyOf(toCopy.getPassword(), toCopy.getPassword().length);
            customMetadata = ImmutableMap.copyOf(toCopy.getCustomMetadata());
        }

        @Override
        public long getLedgerId() {
            return ledgerId;
        }

        @Override
        public boolean hasPassword() { return true; }

        @Override
        public State getState() { return state; }

        @Override
        public int getMetadataFormatVersion() { return metadataFormatVersion; }

        @Override
        public long getCToken() {
            return 0;
        }

        @Override
        public int getEnsembleSize() { return ensembleSize; }

        @Override
        public int getWriteQuorumSize() { return writeQuorumSize; }

        @Override
        public int getAckQuorumSize() { return ackQuorumSize; }

        @Override
        public long getLastEntryId() { return lastEntryId; }

        @Override
        public long getLength() { return length; }

        @Override
        public DigestType getDigestType() { return digestType; }

        @Override
        public byte[] getPassword() { return password; }

        @Override
        public long getCtime() { return ctime; }

        @Override
        public boolean isClosed() { return isClosed; }

        @Override
        public Map<String, byte[]> getCustomMetadata() { return customMetadata; }

        @Override
        public List<BookieId> getEnsembleAt(long entryId) {
            throw new UnsupportedOperationException("Pulsar shouldn't look at this");
        }

        @Override
        public NavigableMap<Long, ? extends List<BookieId>> getAllEnsembles() {
            throw new UnsupportedOperationException("Pulsar shouldn't look at this");
        }

        @Override
        public String toSafeString() {
            return toString();
        }
    }
}
