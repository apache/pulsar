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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.impl.OffloadPrefixTest.assertEventuallyTrue;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.MockClock;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OffloadLedgerDeleteTest extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(OffloadLedgerDeleteTest.class);


    static class MockFileSystemLedgerOffloader implements LedgerOffloader {
        interface InjectAfterOffload {
            void call();
        }

        private String storageBasePath = "/Users/pulsar_filesystem_offloader";

        private static String getStoragePath(String storageBasePath, String managedLedgerName) {
            return storageBasePath == null ? managedLedgerName + "/" : storageBasePath + "/" + managedLedgerName + "/";
        }

        private static String getDataFilePath(String storagePath, long ledgerId, UUID uuid) {
            return storagePath + ledgerId + "-" + uuid.toString();
        }

        ConcurrentHashMap<Long, String> offloads = new ConcurrentHashMap<Long, String>();
        ConcurrentHashMap<Long, String> deletes = new ConcurrentHashMap<Long, String>();
        OffloadPrefixTest.MockLedgerOffloader.InjectAfterOffload inject = null;

        Set<Long> offloadedLedgers() {
            return offloads.keySet();
        }

        Set<Long> deletedOffloads() {
            return deletes.keySet();
        }

        OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create("filesystem", "", "", "",
                null, null,
                null, null,
                OffloadPoliciesImpl.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_READ_BUFFER_SIZE_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_SECONDS,
                OffloadPoliciesImpl.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS,
                OffloadPoliciesImpl.DEFAULT_OFFLOADED_READ_PRIORITY);

        @Override
        public String getOffloadDriverName() {
            return "mockfilesystem";
        }

        @Override
        public CompletableFuture<Void> offload(ReadHandle ledger,
                                               UUID uuid,
                                               Map<String, String> extraMetadata) {
            Assert.assertNotNull(extraMetadata.get("ManagedLedgerName"));
            String storagePath = getStoragePath(storageBasePath, extraMetadata.get("ManagedLedgerName"));
            String dataFilePath = getDataFilePath(storagePath, ledger.getId(), uuid);
            CompletableFuture<Void> promise = new CompletableFuture<>();
            if (offloads.putIfAbsent(ledger.getId(), dataFilePath) == null) {
                promise.complete(null);
            } else {
                promise.completeExceptionally(new Exception("Already exists exception"));
            }

            if (inject != null) {
                inject.call();
            }
            return promise;
        }

        @Override
        public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uuid,
                                                           Map<String, String> offloadDriverMetadata) {
            CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
            promise.completeExceptionally(new UnsupportedOperationException());
            return promise;
        }

        @Override
        public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uuid,
                                                       Map<String, String> offloadDriverMetadata) {
            Assert.assertNotNull(offloadDriverMetadata.get("ManagedLedgerName"));
            String storagePath = getStoragePath(storageBasePath, offloadDriverMetadata.get("ManagedLedgerName"));
            String dataFilePath = getDataFilePath(storagePath, ledgerId, uuid);
            CompletableFuture<Void> promise = new CompletableFuture<>();
            if (offloads.remove(ledgerId, dataFilePath)) {
                deletes.put(ledgerId, dataFilePath);
                promise.complete(null);
            } else {
                promise.completeExceptionally(new Exception("Not found"));
            }
            return promise;
        };

        @Override
        public OffloadPoliciesImpl getOffloadPolicies() {
            return offloadPolicies;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testLaggedDelete() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(300000L);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        int i = 0;
        for (; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(2, TimeUnit.MINUTES);
        CompletableFuture<List<LedgerInfo>> promise = ledger.asyncTrimConsumedLedgers();
        promise.join();
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<List<LedgerInfo>> promise2 = ledger.asyncTrimConsumedLedgers();
        promise2.join();

        // assert bk ledger is deleted
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedgerId));

        // ledger still exists in list
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());

        // move past retention, should be deleted from offloaded also
        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<List<LedgerInfo>> promise3 = ledger.asyncTrimConsumedLedgers();
        promise3.join();

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedgerId));
    }

    @Test
    public void testGetReadLedgerHandleAfterTrimOffloadedLedgers() throws Exception {
        // Create managed ledger.
        final long offloadThresholdSeconds = 5;
        final long offloadDeletionLagInSeconds = 1;
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(offloadDeletionLagInSeconds * 1000);
        offloader.getOffloadPolicies().setManagedLedgerOffloadThresholdInSeconds(offloadThresholdSeconds);
        offloader.getOffloadPolicies().setManagedLedgerOffloadedReadPriority(OffloadedReadPriority.BOOKKEEPER_FIRST);
        config.setLedgerOffloader(offloader);
        ManagedLedgerImpl ml =
                (ManagedLedgerImpl)factory.open("testGetReadLedgerHandleAfterTrimOffloadedLedgers", config);
        ml.openCursor("c1");

        // Write entries.
        int i = 0;
        for (; i < 35; i++) {
            String content = "entry-" + i;
            ml.addEntry(content.getBytes());
        }
        Assert.assertEquals(ml.getLedgersInfoAsList().size(), 4);
        long ledger1 = ml.getLedgersInfoAsList().get(0).getLedgerId();
        long ledger2 = ml.getLedgersInfoAsList().get(1).getLedgerId();
        long ledger3 = ml.getLedgersInfoAsList().get(2).getLedgerId();
        long ledger4 = ml.getLedgersInfoAsList().get(3).getLedgerId();

        // Offload ledgers.
        Thread.sleep(offloadThresholdSeconds * 2 * 1000);
        CompletableFuture<PositionImpl> offloadFuture = new CompletableFuture<PositionImpl>();
        ml.maybeOffloadInBackground(offloadFuture);
        offloadFuture.join();

        // Cache ledger handle.
        CountDownLatch readCountDownLatch = new CountDownLatch(4);
        AsyncCallbacks.ReadEntryCallback readCb = new AsyncCallbacks.ReadEntryCallback(){

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                readCountDownLatch.countDown();
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                readCountDownLatch.countDown();
            }
        };
        ml.asyncReadEntry(PositionImpl.get(ledger1, 0), readCb, null);
        ml.asyncReadEntry(PositionImpl.get(ledger2, 0), readCb, null);
        ml.asyncReadEntry(PositionImpl.get(ledger3, 0), readCb, null);
        ml.asyncReadEntry(PositionImpl.get(ledger4, 0), readCb, null);
        readCountDownLatch.await();
        ReadHandle originalReadHandle4 = ml.getLedgerHandle(ledger4).join();

        // Trim offloaded BK ledger handles.
        Thread.sleep(offloadDeletionLagInSeconds * 2 * 1000);
        CompletableFuture<List<LedgerInfo>> trimLedgerFuture = new CompletableFuture<List<LedgerInfo>>();
        ml.internalTrimLedgers(false, trimLedgerFuture);
        trimLedgerFuture.join();
        MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo1 = ml.getLedgerInfo(ledger1).get();
        MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo2 = ml.getLedgerInfo(ledger2).get();
        MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo3 = ml.getLedgerInfo(ledger3).get();
        MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo4 = ml.getLedgerInfo(ledger4).get();
        Assert.assertTrue(ledgerInfo1.hasOffloadContext() && ledgerInfo1.getOffloadContext().getBookkeeperDeleted());
        Assert.assertTrue(ledgerInfo2.hasOffloadContext() && ledgerInfo2.getOffloadContext().getBookkeeperDeleted());
        Assert.assertTrue(ledgerInfo3.hasOffloadContext() && ledgerInfo3.getOffloadContext().getBookkeeperDeleted());
        Assert.assertFalse(ledgerInfo4.hasOffloadContext() || ledgerInfo4.getOffloadContext().getBookkeeperDeleted());

        Awaitility.await().untilAsserted(() -> {
            try {
                factory.getBookKeeper().get().openLedger(ledger3, ml.digestType, ml.config.getPassword());
                Assert.fail("Should fail: the ledger has been deleted");
            } catch (BKException.BKNoSuchLedgerExistsException ex) {
                // Expected.
            }
            try {
                factory.getBookKeeper().get().openLedger(ledger2, ml.digestType, ml.config.getPassword());
                Assert.fail("Should fail: the ledger has been deleted");
            } catch (BKException.BKNoSuchLedgerExistsException ex) {
                // Expected.
            }
            try {
                factory.getBookKeeper().get().openLedger(ledger1, ml.digestType, ml.config.getPassword());
                Assert.fail("Should fail: the ledger has been deleted");
            } catch (BKException.BKNoSuchLedgerExistsException ex) {
                // Expected.
            }
        });

        // Verify: "ml.getLedgerHandle" returns a correct ledger handle.
        ReadHandle currentReadHandle4 = ml.getLedgerHandle(ledger4).join();
        Assert.assertEquals(currentReadHandle4, originalReadHandle4);
        try {
            ml.getLedgerHandle(ledger3).join();
            Assert.fail("should get a failure: MockLedgerOffloader does not support read");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause().getCause().getMessage()
                    .contains("MockLedgerOffloader does not support read"));
        }
        try {
            ml.getLedgerHandle(ledger2).join();
            Assert.fail("should get a failure: MockLedgerOffloader does not support read");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause().getCause().getMessage()
                    .contains("MockLedgerOffloader does not support read"));
        }
        try {
            ml.getLedgerHandle(ledger1).join();
            Assert.fail("should get a failure: MockLedgerOffloader does not support read");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause().getCause().getMessage()
                    .contains("MockLedgerOffloader does not support read"));
        }
    }

    @Test(timeOut = 5000)
    public void testFileSystemOffloadDeletePath() throws Exception {
        MockFileSystemLedgerOffloader offloader = new MockFileSystemLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(3, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(300000L);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger_filesystem", config);
        int i = 0;
        for (; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                        .filter(e -> e.getOffloadContext().getComplete())
                        .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        // ledger still exists in list
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                        .filter(e -> e.getOffloadContext().getComplete())
                        .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                offloader.offloadedLedgers());

        // move past retention, should be deleted from offloaded also
        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<List<LedgerInfo>> promise3 = ledger.asyncTrimConsumedLedgers();
        promise3.join();

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 1);
        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedgerId));
    }

    @Test
    public void testLaggedDeleteRetentionSetLower() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(5, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(600000L);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        int i = 0;
        for (; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(2, TimeUnit.MINUTES);
        CompletableFuture<List<LedgerInfo>> promise = ledger.asyncTrimConsumedLedgers();
        promise.join();
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<List<LedgerInfo>> promise2 =  ledger.asyncTrimConsumedLedgers();
        promise2.join();

        // ensure it gets deleted from both bookkeeper and offloader
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedgerId));
        assertEventuallyTrue(() -> offloader.deletedOffloads().contains(firstLedgerId));
    }

    @Test
    public void testLaggedDeleteSlowConsumer() throws Exception {
        OffloadPrefixTest.MockLedgerOffloader offloader = new OffloadPrefixTest.MockLedgerOffloader();

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setMaxEntriesPerLedger(10);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        offloader.getOffloadPolicies().setManagedLedgerOffloadDeletionLagInMillis(300000L);
        config.setLedgerOffloader(offloader);
        config.setClock(clock);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl)factory.open("my_test_ledger", config);
        ManagedCursor cursor = ledger.openCursor("sub1");

        for (int i = 0; i < 15; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        long firstLedgerId = ledger.getLedgersInfoAsList().get(0).getLedgerId();

        ledger.offloadPrefix(ledger.getLastConfirmedEntry());

        Assert.assertEquals(ledger.getLedgersInfoAsList().size(), 2);
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(2, TimeUnit.MINUTES);

        CompletableFuture<List<LedgerInfo>> promise = ledger.asyncTrimConsumedLedgers();
        Assert.assertTrue(bkc.getLedgers().contains(firstLedgerId));

        clock.advance(5, TimeUnit.MINUTES);
        CompletableFuture<List<LedgerInfo>> promise2 = ledger.asyncTrimConsumedLedgers();
        promise2.join();

        // assert bk ledger is deleted
        assertEventuallyTrue(() -> !bkc.getLedgers().contains(firstLedgerId));

        // ledger still exists in list
        Assert.assertEquals(ledger.getLedgersInfoAsList().stream()
                            .filter(e -> e.getOffloadContext().getComplete())
                            .map(e -> e.getLedgerId()).collect(Collectors.toSet()),
                            offloader.offloadedLedgers());
    }

    @Test
    public void isOffloadedNeedsDeleteTest() throws Exception {
        OffloadPoliciesImpl offloadPolicies = new OffloadPoliciesImpl();
        LedgerOffloader ledgerOffloader = Mockito.mock(LedgerOffloader.class);
        Mockito.when(ledgerOffloader.getOffloadPolicies()).thenReturn(offloadPolicies);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        MockClock clock = new MockClock();
        config.setLedgerOffloader(ledgerOffloader);
        config.setClock(clock);

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) factory.open("isOffloadedNeedsDeleteTest", config);

        MLDataFormats.OffloadContext offloadContext = MLDataFormats.OffloadContext.newBuilder()
                .setTimestamp(config.getClock().millis() - 1000)
                .setComplete(true)
                .setBookkeeperDeleted(false)
                .build();

        boolean needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertFalse(needsDelete);

        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(500L);
        needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertTrue(needsDelete);

        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(1000L * 2);
        needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertFalse(needsDelete);

        offloadContext = MLDataFormats.OffloadContext.newBuilder()
                .setTimestamp(config.getClock().millis() - 1000)
                .setComplete(false)
                .setBookkeeperDeleted(false)
                .build();
        needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertFalse(needsDelete);

        offloadContext = MLDataFormats.OffloadContext.newBuilder()
                .setTimestamp(config.getClock().millis() - 1000)
                .setComplete(true)
                .setBookkeeperDeleted(true)
                .build();
        needsDelete = managedLedger.isOffloadedNeedsDelete(offloadContext, Optional.of(offloadPolicies));
        Assert.assertFalse(needsDelete);

    }
}
